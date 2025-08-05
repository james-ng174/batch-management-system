import ast
import json
import uuid
from datetime import datetime
from typing import List

import pytz
import requests
from celery.result import AsyncResult
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django_celery_beat.models import PeriodicTask, ClockedSchedule

from batchbe.celery import app
from batchbe.settings import BASE_URL
from logger import get_logger
from request_entities.schedule_task import SchedulerTaskResponse
from .models import JobSettings, TaskDetail
from .utils import get_list_run_date, convert_epoch_to_datetime, datetime_to_epoch, validate_rrule, \
    get_next_run_date

logger = get_logger()


class CeleryBeTask:
    def __init__(self, celery_task_name, run_date, next_run_date):
        self.celery_task_name = celery_task_name
        self.run_date = run_date
        self.next_run_date = next_run_date


@csrf_exempt
def delete_task(request, task_id):
    if request.method == 'DELETE':
        task = PeriodicTask.objects.get(id=task_id)
        task.delete()
        return JsonResponse({'status': 'success', 'message': 'Task deleted successfully.'})

    return JsonResponse({'status': 'failure', 'message': 'Invalid request method.'})


def validate_job_settings(job_settings):
    """Validate required fields in job settings."""
    logger.info("Validating job settings...")
    required_fields = ['job_type', 'job_id', 'queue_name']
    for field in required_fields:
        if not getattr(job_settings, field, None):
            logger.error(f"Validation failed: Missing required field {field}")
            raise ValueError(f"Missing required field: {field}")
    logger.info("Job settings validation successful.")


def parse_job_action(job_type, job_action):
    """Parse job_action for REST API type jobs."""
    logger.info(f"Parsing job_action for job_type: {job_type}")
    if job_type.lower() == 'rest_api':
        job_action = ast.literal_eval(job_action)
        logger.info("Parsed job_action for REST API.")
    return job_action


def validate_rrule_input(rrule_str):
    """Validate RRULE input and return its dictionary form."""
    logger.info(f"Validating RRULE input: {rrule_str}")
    validity, rrule_dict = validate_rrule(rrule_str)
    if not validity:
        logger.error(f"RRULE validation failed: {rrule_str}")
        raise ValueError(f"Invalid RRULE FORMAT: {rrule_str}")
    logger.info("RRULE validation successful.")
    return rrule_dict


def calculate_next_runs(start_date, rrule_str, run_count, max_run_val, end_date, timezone):
    """Calculate the next run date for the task and return it as a list of datetime objects."""
    logger.info("Calculating next run dates...")

    # Retrieve the next run date as a string or formatted datetime
    next_run_date = get_next_run_date(start_date=start_date, rrule_str=rrule_str, run_count=run_count, tz=timezone)
    logger.info(f"Initial next run date: {next_run_date}")

    # Convert next_run_date to a datetime object with UTC tzinfo if necessary
    if isinstance(next_run_date, str):
        next_run_date = datetime.strptime(next_run_date, "%Y-%m-%d %H:%M:%S%z")
    elif next_run_date.tzinfo is None:
        next_run_date = next_run_date.replace(tzinfo=pytz.UTC)

    # Return as a list containing the datetime object
    next_runs = [next_run_date]
    logger.info(f"Calculated next run dates: {next_runs}")

    return next_runs


def create_clocked_schedules(next_runs: List[datetime]) -> List[ClockedSchedule]:
    """Create clocked schedules based on next run times."""
    logger.info("Creating clocked schedules...")
    clocked_schedules = []
    for run_time in next_runs:
        clocked_schedule = ClockedSchedule.objects.create(clocked_time=run_time)
        logger.info(f"Created ClockedSchedule at time: {run_time}")
        clocked_schedules.append(clocked_schedule)
    return clocked_schedules


def create_celery_tasks(next_runs: List[datetime]) -> List[CeleryBeTask]:
    """Create Celery tasks based on the next run times."""
    logger.info("Creating Celery tasks...")
    celery_tasks = []
    for i, next_run in enumerate(next_runs):
        next_run_date = next_runs[i + 1] if i + 1 < len(next_runs) else None
        celery_task = CeleryBeTask(
            celery_task_name=str(uuid.uuid4()),
            run_date=next_run,
            next_run_date=str(next_run_date)
        )
        logger.info(f"Created CeleryBeTask: {celery_task.celery_task_name}, run_date: {next_run}")
        celery_tasks.append(celery_task)
    return celery_tasks


def save_periodic_tasks(job_settings, clocked_schedules, celery_tasks, function_name, queue_name):
    """Create and save PeriodicTask objects."""
    logger.info("Saving PeriodicTask objects...")
    tasks = []
    for i, clocked_schedule in enumerate(clocked_schedules):
        task_name = celery_tasks[i].celery_task_name
        try:
            task, _ = PeriodicTask.objects.get_or_create(
                clocked=clocked_schedule,
                name=task_name,
                task=function_name,
                defaults={'one_off': True},
                description=job_settings.job_id,
                queue=queue_name,
                exchange=queue_name,
                routing_key=queue_name,
                enabled=job_settings.is_enable,
                priority=job_settings.priority,
                args=json.dumps([job_settings.job_action, task_name, job_settings.job_id, job_settings.run_account,
                                 celery_tasks[i].next_run_date]),
            )
            tasks.append(task_name)
            logger.info(f"Saved PeriodicTask: {task_name}")
        except Exception as e:
            logger.error(f"Failed to save PeriodicTask {task_name}: {e}")
            raise
    return tasks


def save_task_details(job_settings, clocked_schedules, celery_tasks):
    """Create and save TaskDetail objects."""
    logger.info("Saving TaskDetail objects...")
    for i, clocked_schedule in enumerate(clocked_schedules):
        try:
            TaskDetail.objects.get_or_create(
                job=job_settings,
                task_name=celery_tasks[i].celery_task_name,
                run_date=clocked_schedule.clocked_time
            )
            logger.info(f"Saved TaskDetail: {celery_tasks[i].celery_task_name}")
        except Exception as e:
            logger.error(f"Failed to save TaskDetail for {celery_tasks[i].celery_task_name}: {e}")
            raise


@csrf_exempt
def rrule_schedule_task(job_settings) -> List[str]:
    """Main function to schedule tasks."""
    try:
        logger.info(f"Received request to create interval task: {job_settings}")
        validate_job_settings(job_settings)

        # Parse and validate inputs
        job_action = parse_job_action(job_settings.job_type, job_settings.job_action)
        rrule_dict = validate_rrule_input(job_settings.repeat_interval)

        # Calculate next run dates
        max_run_val = 1 if job_settings.run_forever else job_settings.max_run
        next_runs = calculate_next_runs(job_settings.start_date, job_settings.repeat_interval,
                                        job_settings.run_count, max_run_val, job_settings.end_date, job_settings.timezone)

        # Create schedules and tasks
        clocked_schedules = create_clocked_schedules(next_runs)
        celery_tasks = create_celery_tasks(next_runs)

        # Save tasks and details
        function_name = f'celerytasks.tasks.{job_settings.job_type.lower()}'
        task_names = save_periodic_tasks(job_settings, clocked_schedules, celery_tasks, function_name,
                                         job_settings.queue_name)
        save_task_details(job_settings, clocked_schedules, celery_tasks)

        logger.info(f"Successfully created tasks: {task_names}")
        return task_names

    except ValueError as ve:
        logger.error(f"Validation Error: {str(ve)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected Error: {str(e)}")
        raise


@csrf_exempt
def force_terminate_task(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        job_id = data.get('job_id')
        latest_task = get_latest_running_task(job_id)
        if latest_task is None:
            return JsonResponse({'status': 'error', 'message': f'No running job found for job_id {job_id}'}, status=404)

        task_id = latest_task.celery_task_id
        result = AsyncResult(task_id, app=app)
        result.revoke(terminate=True, signal='SIGKILL')
        latest_task.save_status(TaskDetail.STATUS_STOPPED)
        request_body = convert_scheduler_log_update_body(task_detail=latest_task, operation='BLOCKED',
                                                         job_settings=None, result=None)
        if request_body is not None:
            success = call_scheduler_run_log(request_body, 'update')
            if success:
                logger.info(f'Successfully update scheduler run log for force stop {job_id}')
            else:
                logger.error(f'Failed to update scheduler run log for force stop {job_id}')

        return JsonResponse(
            {'status': 'success', 'message': f'Job {job_id} force terminated with task_id {task_id}.'})
    return JsonResponse({'status': 'failure', 'message': 'Invalid request method.'})


# Fields that only exist in JobSettings
JOB_SETTINGS_ONLY_FIELDS = {'start_date', 'end_date', 'repeat_interval', 'max_run',
                            'max_run_duration', 'max_failure', 'auto_drop', 'restart_on_failure',
                            'restartable', 'priority', 'job_action', 'job_body', 'job_type',
                            'queue_name', 'user_name',
                            'retry_delay'}


@csrf_exempt
def update_job_function(request):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': f'Invalid HTTP Method for function Update Job'}, status=500)

    try:
        data = json.loads(request.body)
        logger.info(f'Received request to update job with body {data}')

        # Get job_id from the request data
        job_id = data.get('job_id')
        if not job_id:
            return JsonResponse({'status': 'error', 'message': 'job_id parameter is required'}, status=400)

        # Retrieve the JobSettings object
        job_settings = JobSettings.objects.filter(job_id=job_id).first()
        logger.info(f'Start update job settings for job_id:{job_id}')
        logger.info(f'Typeof {type(job_settings)} obj:{job_settings}')
        if not job_settings:
            return JsonResponse({'status': 'error', 'message': f'Cannot find JobSettings with job_id={job_id}'},
                                status=400)

        max_run_duration = data.get('max_run_duration', job_settings.max_run_duration)
        max_failures = data.get('max_failure', job_settings.max_failure)
        enable = data.get('is_enabled', job_settings.is_enable)
        auto_drop = data.get('auto_drop', job_settings.auto_drop)
        restart_on_fail = data.get('restart_on_failure', job_settings.restart_on_failure)
        restartable = data.get('restartable', job_settings.restartable)
        priority = data.get('priority', job_settings.priority)
        job_type = data.get('job_type', job_settings.job_type)
        job_action = data.get('job_action', job_settings.job_action)
        job_body = data.get('job_body', job_settings.job_body)
        rrule_str = data.get('repeat_interval', job_settings.repeat_interval)
        max_run = data.get('max_run', job_settings.max_run)
        start_date_obj = data.get('start_date', int(job_settings.start_date.timestamp() * 1000))
        request_end_date = data.get('end_date')
        if request_end_date is not None:
            end_date_obj = request_end_date
        else:
            db_end_date_obj = job_settings.end_date
            end_date_obj = int(db_end_date_obj.timestamp() * 1000) if db_end_date_obj else 0
        queue_name = data.get('queue_name', job_settings.queue_name)
        run_account = data.get('user_name', job_settings.run_account)
        retry_delay = data.get('retry_delay', job_settings.retry_delay)

        validity, rrule_dict = validate_rrule(rrule_str)

        if validity:
            return JsonResponse(
                {'status': 'error', 'message': f'Invalid RRULE FORMAT: {rrule_str}'},
                status=400)

        function_name = f'celerytasks.tasks.{job_type.lower()}'
        task_details = TaskDetail.objects.filter(job=job_settings, already_run=False, soft_delete=False)
        logger.info(f'task_details: {task_details}')

        # Check if any of the specified fields are present in the data
        fields_to_check = ['start_date', 'end_date', 'repeat_interval', 'max_run']

        if not any(field in data for field in fields_to_check):
            return JsonResponse(
                {'status': 'success', 'outdated_tasks': [], 'updated_tasks': []}
            )

        if task_details:
            for task in task_details:
                periodic_task = PeriodicTask.objects.filter(name=task.task_name).first()
                if not periodic_task:
                    return JsonResponse(
                        {'status': 'error', 'message': f'No enabled tasks found for task_name {task.task_name}'},
                        status=404)

                periodic_task.delete()
                logger.info(f'Delete success related periodic task: {periodic_task}')
                task.soft_delete_func()

        outdated_tasks_responses = [
            SchedulerTaskResponse(task.task_name, datetime_to_epoch(task.run_date))
            for task in task_details]

        start_date = convert_epoch_to_datetime(start_date_obj)
        end_date = convert_epoch_to_datetime(end_date_obj) if end_date_obj else None

        # Calculate the next run times
        next_runs = get_list_run_date(rrule_str, max_run, start_date, end_date)

        # Create a ClockedSchedule for each run time
        clocked_schedules = []
        celery_tasks = []
        for i, next_run in enumerate(next_runs):
            clocked_schedule = ClockedSchedule.objects.create(clocked_time=next_run)
            clocked_schedules.append(clocked_schedule)

            # Determine the next_run_date (next element in the list or None if it's the last one)
            next_run_date = next_runs[i + 1] if i + 1 < len(next_runs) else None

            # Create CeleryBeTask object with the current run time and next_run_date
            celery_be_task = CeleryBeTask(
                celery_task_name=str(uuid.uuid4()),
                run_date=next_run,
                next_run_date=str(next_run_date)
            )
            celery_tasks.append(celery_be_task)

        # Create PeriodicTask for each ClockedSchedule
        # TODO: Refactor
        tasks = []
        for i, clocked_schedule in enumerate(clocked_schedules):
            task_name = celery_tasks[i].celery_task_name

            try:
                task = PeriodicTask.objects.get_or_create(
                    clocked=clocked_schedule,
                    name=task_name,
                    task=function_name,
                    defaults={'one_off': True},
                    description=job_id,
                    queue=queue_name,
                    exchange=queue_name,
                    routing_key=queue_name,
                    enabled=True,
                    priority=priority,
                    args=json.dumps(
                        [job_action, task_name, job_id, run_account, celery_tasks[i].next_run_date]),
                )
                tasks.append(task)
                logger.info(f'Save PeriodicTask: {task} to database successfully')

            except Exception as e:
                logger.error(f'Cannot save PeriodicTask to database error {e}')
                return JsonResponse(
                    {'status': 'error', 'message': f'Cannot save PeriodicTask to database exception {e}'},
                    status=405)
            try:
                task_detail = TaskDetail.objects.get_or_create(job=job_settings, task_name=task_name,
                                                               run_date=clocked_schedule.clocked_time)
                logger.info(f'Save TaskDetail: {task_detail} to database successfully')
            except Exception as e:
                logger.error(f'Cannot save TaskDetail to database error {e}')
                return JsonResponse(
                    {'status': 'error', 'message': f'Cannot save TaskDetail to database exception {e}'},
                    status=405)

        # Update JobSettings object
        job_settings = JobSettings.objects.filter(job_id=job_id).first()
        if job_settings is None:
            return JsonResponse({'status': 'error', 'message': f'No job found for job_id {job_id}'}, status=404)

        for key, value in data.items():
            if key == 'is_enabled':
                setattr(job_settings, 'is_enable', value)
            elif key == 'start_date':
                setattr(job_settings, 'start_date', convert_epoch_to_datetime(value))
            elif key == 'end_date':
                setattr(job_settings, 'end_date', convert_epoch_to_datetime(value))
            elif key in JOB_SETTINGS_ONLY_FIELDS:
                setattr(job_settings, key, value)

        job_settings.save()

        tasks_responses = [
            SchedulerTaskResponse(task.celery_task_name, datetime_to_epoch(task.run_date))
            for task in celery_tasks]
        updated_tasks = [tasks_responses.to_dict() for tasks_responses in tasks_responses]
        outdated_tasks = [outdated_tasks_responses.to_dict() for outdated_tasks_responses in outdated_tasks_responses]

        return JsonResponse(
            {'status': 'success', 'outdated_tasks': outdated_tasks, 'updated_tasks': updated_tasks})

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def get_latest_running_task(job_id):
    job_settings = JobSettings.objects.filter(job_id=job_id).first()
    latest_job = TaskDetail.objects.filter(job=job_settings, status=TaskDetail.STATUS_NONE).order_by(
        '-run_date').first()
    return latest_job


@csrf_exempt
def update_state_job(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            logger.info(f'Received request to update status job with body {data}')

            # Get job_id from the request data
            job_id = data.get('job_id')
            if not job_id:
                return JsonResponse({'status': 'error', 'message': 'job_id parameter is required'}, status=400)

            # Get status from the request data
            status = data.get('status')
            if status is None:
                return JsonResponse({'status': 'error', 'message': 'status parameter is required'}, status=400)

            # Check if status is a boolean
            if not isinstance(status, bool):
                return JsonResponse({'status': 'error', 'message': 'status parameter must be a boolean'}, status=400)

            # Update JobSettings object
            job_settings = JobSettings.objects.filter(job_id=job_id).first()
            if job_settings is None:
                return JsonResponse({'status': 'error', 'message': f'No job found for job_id {job_id}'}, status=404)

            job_settings.update_status(status)
            task_details = TaskDetail.objects.filter(job=job_settings, already_run=False, soft_delete=False)
            if not task_details:
                return JsonResponse({'status': 'success', 'message': 'Job ran completely, no need to update status'},
                                    status=200)

            for task in task_details:
                periodic_task = PeriodicTask.objects.filter(name=task.task_name).first()
                if not periodic_task:
                    return JsonResponse(
                        {'status': 'error', 'message': f'No enabled tasks found for task_name {task.task_name}'},
                        status=404)
                periodic_task.enabled = status
                periodic_task.save()

            logger.info(f'Successfully updated status for job_id {job_id} to {status}')
            return JsonResponse({'status': 'success', 'message': f'Successfully updated status for job_id {job_id}'})

        except Exception as e:
            logger.error(f'Error updating job status: {str(e)}')
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def get_request_data(request, required_fields):
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format")
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

    return {field: data[field] for field in required_fields}


def health_check(request):
    return JsonResponse({"status": "ok"})


def call_scheduler_run_log(body, method):
    url = f'{BASE_URL}/logs/{method}'
    headers = {'Content-Type': 'application/json'}
    if body is None:
        logger.error('Failed to convert task detail to log data.')
        return False

    try:
        response = requests.post(url, json=body, headers=headers)
        response.raise_for_status()
        logger.info(f'Success push task result to scheduler with body {body}')
        return True
    except requests.RequestException as e:
        logger.error('Error in pushing task result to scheduler: %s', e)
        return False


def convert_scheduler_log_update_body(task_detail, job_settings, result, operation):
    try:
        log_body = {
            # Task detail attributes
            "job_id": task_detail.job.job_id,
            "operation": operation,
            "status": task_detail.status,
            "user_name": task_detail.run_account,
            "actual_start_date": int(task_detail.run_date.timestamp() * 1000),
            "run_duration": str(task_detail.run_duration) if task_detail.run_duration else None,
            "additional_info": None,
            "celery_task_name": task_detail.task_name,
        }

        # Add job settings attributes if job_settings is not None
        if job_settings:
            log_body.update({
                "error_no": job_settings.failure_count,
                "run_count": job_settings.run_count,
                "failure_count": job_settings.failure_count,
                "retry_count": job_settings.retry_count,
            })

        # Add result attributes if result is not None
        if result is not None:
            log_body.update({
                "errors": str(result) if job_settings and job_settings.failure_count > 0 else None,
                "output": str(result),
            })

        return log_body
    except Exception as e:
        logger.error(f'Error in converting task detail to log data: {e}')
        return None


def call_scheduler_workflow_update(body):
    url = f'{BASE_URL}/workflow/update_status'
    headers = {'Content-Type': 'application/json'}
    if body is None:
        logger.error('Failed to convert task detail to workflow data.')
        return False

    try:
        response = requests.post(url, json=body, headers=headers)
        response.raise_for_status()
        logger.info(f'Success update task result to workflow with body {body}')
        return True
    except requests.RequestException as e:
        logger.error('Error in pushing task result to workflow: %s', e)
        return False
