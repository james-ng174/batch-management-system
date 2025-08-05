import json
import uuid
from datetime import datetime, timedelta

import requests
from celery.result import AsyncResult
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django_celery_beat.models import PeriodicTask, ClockedSchedule
from django_celery_results.models import TaskResult

from batchbe.celery import app
from batchbe.settings import BASE_URL
from logger import get_logger
from request_entities.schedule_task import SchedulerTaskResponse
from .models import JobSettings, TaskDetail, WorkflowRunDetail
from .utils import (
    get_list_run_date, convert_epoch_to_datetime, get_current_time, datetime_to_epoch,
    validate_rrule, check_rrule_run_forever, validate_time_stub
)

logger = get_logger()


class CeleryBeTask:
    """
    Represents a Celery backend task with its name, run date, and next run date.
    """

    def __init__(self, celery_task_name, run_date, next_run_date):
        self.celery_task_name = celery_task_name
        self.run_date = run_date
        self.next_run_date = next_run_date


def log_and_return_json_response(log_func, log_msg, response_data, status=200):
    """
    Helper function to log a message and return a JsonResponse.
    """
    log_func(log_msg)
    return JsonResponse(response_data, status=status)


def get_job_settings_by_id(job_id):
    """
    Retrieve JobSettings object by job_id.
    """
    return JobSettings.objects.filter(job_id=job_id).first()


def get_periodic_task_by_name(task_name):
    """
    Retrieve PeriodicTask object by task_name.
    """
    return PeriodicTask.objects.filter(name=task_name).first()


def get_task_detail_by_name(task_name):
    """
    Retrieve TaskDetail object by task_name.
    """
    return TaskDetail.objects.filter(task_name=task_name).first()


def get_task_result_by_periodic_task_name(periodic_task_name):
    """
    Retrieve TaskResult object by periodic_task_name.
    """
    return TaskResult.objects.filter(periodic_task_name=periodic_task_name).first()


def create_clocked_schedules_and_tasks(next_runs, celery_tasks):
    """
    Create ClockedSchedule and CeleryBeTask objects for each run time.
    """
    clocked_schedules = []
    for i, next_run in enumerate(next_runs):
        clocked_schedule = ClockedSchedule.objects.create(clocked_time=next_run)
        clocked_schedules.append(clocked_schedule)
        next_run_date = next_runs[i + 1] if i + 1 < len(next_runs) else None
        celery_be_task = CeleryBeTask(
            celery_task_name=str(uuid.uuid4()),
            run_date=next_run,
            next_run_date=str(next_run_date)
        )
        celery_tasks.append(celery_be_task)
    return clocked_schedules


def create_periodic_and_task_details(clocked_schedules, celery_tasks, function_name, job_id, queue_name, enable,
                                     priority, run_account, job_settings):
    """
    Create PeriodicTask and TaskDetail objects for each ClockedSchedule.
    """
    tasks = []
    for i, clocked_schedule in enumerate(clocked_schedules):
        task_name = celery_tasks[i].celery_task_name
        try:
            task, _ = PeriodicTask.objects.get_or_create(
                clocked=clocked_schedule,
                name=task_name,
                task=function_name,
                defaults={'one_off': True},
                description=job_id,
                queue=queue_name,
                exchange=queue_name,
                routing_key=queue_name,
                enabled=enable,
                priority=priority,
                args=json.dumps(["job_action", task_name, job_id, run_account, celery_tasks[i].next_run_date]),
            )
            tasks.append(task)
            logger.info(f'Save PeriodicTask: {task} to database successfully')
        except Exception as e:
            logger.error(f'Cannot save PeriodicTask to database error {e}')
            return None, JsonResponse(
                {'status': 'error', 'message': f'Cannot save PeriodicTask to database exception {e}'},
                status=405)
        try:
            task_detail, _ = TaskDetail.objects.get_or_create(job=job_settings, task_name=task_name,
                                                              run_date=clocked_schedule.clocked_time, run_number=i)
            logger.info(f'Save TaskDetail: {task_detail} to database successfully')
        except Exception as e:
            logger.error(f'Cannot save TaskDetail to database error {e}')
            return None, JsonResponse(
                {'status': 'error', 'message': f'Cannot save TaskDetail to database exception {e}'},
                status=405)
    return tasks, None


@csrf_exempt
def delete_task(request, task_id):
    """
    Delete a periodic task by its ID.
    """
    if request.method == 'DELETE':
        try:
            task = PeriodicTask.objects.get(id=task_id)
            task.delete()
            return log_and_return_json_response(logger.info, f'Task {task_id} deleted successfully.',
                                                {'status': 'success', 'message': 'Task deleted successfully.'})
        except PeriodicTask.DoesNotExist:
            return log_and_return_json_response(logger.error, f'Task {task_id} does not exist.',
                                                {'status': 'failure', 'message': 'Task does not exist.'}, status=404)
        except Exception as e:
            return log_and_return_json_response(logger.error, f'Error deleting task {task_id}: {e}',
                                                {'status': 'failure', 'message': str(e)}, status=500)
    return log_and_return_json_response(logger.error, 'Invalid request method for delete_task.',
                                        {'status': 'failure', 'message': 'Invalid request method.'}, status=405)


def _parse_rrule_schedule_request(request):
    """
    Parse and validate the incoming request for rrule_schedule_task.
    Returns a dict of validated data or raises ValueError/KeyError.
    """
    required_fields = [
        'job_id', 'max_run_duration', 'max_failure', 'is_enabled', 'auto_drop',
        'restart_on_failure', 'restartable', 'job_type', 'job_action',
        'repeat_interval', 'start_date', 'queue_name', 'user_name',
        'retry_delay', 'max_run', 'timezone'
    ]
    data = get_request_data(request, required_fields)
    logger.info(f'Received request to create interval task with body {json.loads(request.body)}')
    return data


def _create_job_settings_from_data(data, function_name, start_date, end_date, run_forever):
    """
    Create a JobSettings object from validated data.
    Returns the created JobSettings instance.
    """
    try:
        job_settings = JobSettings.objects.create(
            job_id=data.get('job_id'),
            queue_name=data.get('queue_name'),
            function_name=function_name,
            start_date=start_date,
            end_date=end_date,
            repeat_interval=data['repeat_interval'],
            max_run_duration=data.get('max_run_duration'),
            max_run=data.get('max_run'),
            max_failure=data.get('max_failure'),
            priority=data.get('priority'),
            is_enable=data.get('is_enabled'),
            auto_drop=data.get('auto_drop'),
            restart_on_failure=data.get('restart_on_failure'),
            restartable=data.get('restartable'),
            job_type=data.get('job_type'),
            job_action=data.get('job_action'),
            run_account=data.get('user_name'),
            retry_delay=data.get('retry_delay'),
            job_body=data.get('job_body'),
            run_forever=run_forever,
            timezone=data.get('timezone')
        )
        logger.info(f'Save job_settings: {job_settings} to database successfully')
        return job_settings
    except Exception as e:
        logger.error(f'Cannot save job_settings to database error: {e}')
        raise


def _orchestrate_rrule_task_creation(data, job_settings, function_name, start_date, end_date, run_forever):
    """
    Orchestrate the creation of schedules and tasks for the rrule_schedule_task endpoint.
    Returns (next_runs, celery_tasks, error_response) tuple.
    """
    max_run_val = 1 if run_forever else data.get('max_run')
    next_runs = get_list_run_date(
        data['repeat_interval'], max_run_val, start_date, end_date, False, data.get('timezone')
    )
    celery_tasks = []
    clocked_schedules = create_clocked_schedules_and_tasks(
        next_runs, celery_tasks
    )
    tasks, error_response = create_periodic_and_task_details(
        clocked_schedules, celery_tasks, function_name, data.get('job_id'), data.get('queue_name'),
        data.get('is_enabled'), data.get('priority'), data.get('user_name'), job_settings
    )
    return next_runs, celery_tasks, error_response


def _format_rrule_schedule_response(next_runs, celery_tasks):
    """
    Format the response for rrule_schedule_task.
    """
    task_responses = [
        SchedulerTaskResponse(task.celery_task_name, datetime_to_epoch(task.run_date))
        for task in celery_tasks
    ]
    task_dicts = [task_response.to_dict() for task_response in task_responses]
    logger.info(f'Created interval task with tasks {task_dicts}')
    return JsonResponse({
        'status': 'success',
        'next_run_date': datetime_to_epoch(next_runs[0]),
        'tasks': task_dicts
    })


@csrf_exempt
def rrule_schedule_task(request):
    """
    Schedule a recurring task using an RRULE string.
    """
    if request.method != 'POST':
        return log_and_return_json_response(
            logger.error, 'Invalid request method for rrule_schedule_task.',
            {'status': 'error', 'message': 'Invalid request method'}, status=405)
    try:
        data = _parse_rrule_schedule_request(request)
        job_id = data.get('job_id')
        job_type = data.get('job_type')
        queue_name = data.get('queue_name')
        rrule_str = data['repeat_interval']
        start_date = convert_epoch_to_datetime(data['start_date'])
        end_date_obj = data.get('end_date')
        end_date = convert_epoch_to_datetime(end_date_obj) if end_date_obj else None
        if not job_type:
            return log_and_return_json_response(
                logger.error, 'Job type is required.',
                {'status': 'failure', 'message': 'Job type is required.'}, status=400)
        function_name = f'celerytasks.tasks.{job_type.lower()}'
        validity, rrule_dict = validate_rrule(rrule_str)
        if not job_type or not job_id or not queue_name:
            return log_and_return_json_response(
                logger.error, 'Job function, job name, and queue name are required.',
                {'status': 'failure', 'message': 'Job function and job name and queue name are required.'}, status=400)
        if not validity:
            return log_and_return_json_response(
                logger.error, f'Invalid RRULE FORMAT: {rrule_str}',
                {'status': 'error', 'message': f'Invalid RRULE FORMAT: {rrule_str}'}, status=400)
        valid_time_stub = validate_time_stub(data, None, 'create')
        if not valid_time_stub:
            return log_and_return_json_response(
                logger.error, f'Invalid time data input: {start_date}, {end_date}',
                {'status': 'error', 'message': f'Invalid time data input: {start_date}, {end_date}'}, status=400)
        run_forever = check_rrule_run_forever(rrule_dict=rrule_dict, end_date=end_date, max_run=data.get('max_run'))
        try:
            job_settings = _create_job_settings_from_data(data, function_name, start_date, end_date, run_forever)
        except Exception as e:
            return log_and_return_json_response(
                logger.error, f'Save job_settings to database with exception {e}',
                {'status': 'error', 'message': f'Save job_settings to database with exception {e}'}, status=405)
        next_runs, celery_tasks, error_response = _orchestrate_rrule_task_creation(
            data, job_settings, function_name, start_date, end_date, run_forever
        )
        if error_response:
            return error_response
        return _format_rrule_schedule_response(next_runs, celery_tasks)
    except KeyError as e:
        logger.error(f'Missing parameter: {str(e)}')
        return JsonResponse({'status': 'error', 'message': f'Missing parameter: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f'Error in creating interval task: {e}')
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@csrf_exempt
def manually_run(request):
    if request.method == 'POST':
        logger.info(f'Received request Manually RUN with data={request.body}')
        try:
            required_fields = ['job_id', 'job_type', 'job_action', 'queue_name', 'user_name']
            data = get_request_data(request, required_fields)
            job_id = data.get('job_id')
            job_type = data.get('job_type')
            job_action = data.get('job_action')
            queue_name = data.get('queue_name')
            run_account = data.get('user_name')

            if not job_type or not queue_name or not job_action or not job_id:
                return JsonResponse(
                    {'status': 'failure', 'message': 'Job type, job action, job_id and queue_name are required.'})

            job_settings = JobSettings.objects.filter(job_id=job_id).first()

            clocked_schedule = ClockedSchedule.objects.create(clocked_time=datetime.now())
            # Create PeriodicTask for each ClockedSchedule
            task_name = str(uuid.uuid4())

            try:
                task = PeriodicTask.objects.get_or_create(
                    clocked=clocked_schedule,
                    name=task_name,
                    task=f'celerytasks.tasks.{job_type.lower()}',
                    defaults={'one_off': True},
                    description=job_id,
                    queue=queue_name,
                    exchange=queue_name,
                    routing_key=queue_name,
                    args=json.dumps([job_action, task_name, job_id, run_account, None]),
                )
                logger.info(f'Save PeriodicTask: {task} to database successfully')

            except Exception as e:
                logger.error(f'Cannot save PeriodicTask to database error {e}')
                return JsonResponse(
                    {'status': 'error', 'message': f'Cannot save PeriodicTask to database exception {e}'},
                    status=405)
            try:
                task_detail = TaskDetail.objects.get_or_create(job=job_settings, task_name=task_name,
                                                               run_date=clocked_schedule.clocked_time,
                                                               manually_run=True)
                logger.info(f'Save TaskDetail: {task_detail} to database successfully')
            except Exception as e:
                logger.error(f'Cannot save TaskDetail to database error {e}')
                return JsonResponse(
                    {'status': 'error', 'message': f'Cannot save TaskDetail to database exception {e}'},
                    status=405)

            logger.info(f'Success manually run job: {job_id} with celery_task_name: {task_name}')

            return JsonResponse({'status': 'success', 'celery_task_name': task_name})

        except KeyError as e:
            return JsonResponse({'status': 'error', 'message': f'Missing parameter: {str(e)}'}, status=400)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    else:
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)


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


def change_periodic_tasks(data, periodic_tasks):
    for task in periodic_tasks:
        for key, value in data.items():
            if key == 'is_enabled':
                setattr(task, 'enabled', value)
            elif key == 'queue_name':
                task.exchange = value
                task.routing_key = value
                task.queue = value
            elif key == 'job_type':
                function_name = f'celerytasks.tasks.{value.lower()}'
                setattr(task, 'task', function_name)
            task.save()


def _parse_update_job_request(request):
    """
    Parse and validate the incoming request for update_job_function.
    Returns (data, job_id, job_settings) or returns a JsonResponse on error.
    """
    try:
        data = json.loads(request.body)
        logger.info(f'Received request to update job with body {data}')
        job_id = data.get('job_id')
        if not job_id:
            return None, None, JsonResponse({'status': 'error', 'message': 'job_id parameter is required'}, status=400)
        job_settings = JobSettings.objects.filter(job_id=job_id).first()
        logger.info(f'Start update job settings for job_id:{job_id}')
        logger.info(f'Typeof {type(job_settings)} obj:{job_settings}')
        if not job_settings:
            return None, None, JsonResponse(
                {'status': 'error', 'message': f'Cannot find JobSettings with job_id={job_id}'}, status=400)
        return data, job_id, job_settings
    except Exception as e:
        return None, None, JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def _prepare_job_settings_update(data, job_settings):
    """
    Prepare new values for job settings, using request data or defaults from job_settings.
    Returns a dict of all relevant values.
    """
    return {
        'max_run_duration': data.get('max_run_duration', job_settings.max_run_duration),
        'max_failures': data.get('max_failure', job_settings.max_failure),
        'enable': data.get('is_enabled', job_settings.is_enable),
        'auto_drop': data.get('auto_drop', job_settings.auto_drop),
        'restart_on_fail': data.get('restart_on_failure', job_settings.restart_on_failure),
        'restartable': data.get('restartable', job_settings.restartable),
        'priority': data.get('priority', job_settings.priority),
        'job_type': data.get('job_type', job_settings.job_type),
        'job_action': data.get('job_action', job_settings.job_action),
        'job_body': data.get('job_body', job_settings.job_body),
        'rrule_str': data.get('repeat_interval', job_settings.repeat_interval),
        'max_run': data.get('max_run', job_settings.max_run),
        'timezone': job_settings.timezone,
        'start_date_obj': data.get('start_date', int(job_settings.start_date.timestamp() * 1000)),
        'end_date_obj': data.get('end_date',
                                 int(job_settings.end_date.timestamp() * 1000) if job_settings.end_date else 0),
        'queue_name': data.get('queue_name', job_settings.queue_name),
        'run_account': data.get('user_name', job_settings.run_account),
        'retry_delay': data.get('retry_delay', job_settings.retry_delay),
    }


def _delete_old_tasks(task_details):
    """
    Delete old PeriodicTasks and mark TaskDetails as soft deleted.
    Returns a list of SchedulerTaskResponse for outdated tasks.
    Optimized to avoid N+1 queries.
    """
    # Batch fetch all periodic tasks in one query
    task_names = [task.task_name for task in task_details]
    periodic_tasks = {pt.name: pt for pt in PeriodicTask.objects.filter(name__in=task_names)}
    outdated_tasks_responses = []
    for task in task_details:
        periodic_task = periodic_tasks.get(task.task_name)
        if periodic_task:
            periodic_task.delete()
            logger.info(f'Delete success related periodic task: {periodic_task}')
        task.soft_delete_func()
        outdated_tasks_responses.append(SchedulerTaskResponse(task.task_name, datetime_to_epoch(task.run_date)))
    return outdated_tasks_responses


def _create_new_schedules_and_tasks(next_runs, function_name, job_id, queue_name, enable, priority, run_account,
                                    job_settings):
    """
    Create new ClockedSchedules, CeleryBeTasks, PeriodicTasks, and TaskDetails for the updated job.
    Returns (celery_tasks, error_response)
    """
    celery_tasks = []
    clocked_schedules = []
    for i, next_run in enumerate(next_runs):
        clocked_schedule = ClockedSchedule.objects.create(clocked_time=next_run)
        clocked_schedules.append(clocked_schedule)
        next_run_date = next_runs[i + 1] if i + 1 < len(next_runs) else None
        celery_be_task = CeleryBeTask(
            celery_task_name=str(uuid.uuid4()),
            run_date=next_run,
            next_run_date=str(next_run_date)
        )
        celery_tasks.append(celery_be_task)
    tasks = []
    for i, clocked_schedule in enumerate(clocked_schedules):
        task_name = celery_tasks[i].celery_task_name
        try:
            task, _ = PeriodicTask.objects.get_or_create(
                clocked=clocked_schedule,
                name=task_name,
                task=function_name,
                defaults={'one_off': True},
                description=job_id,
                queue=queue_name,
                exchange=queue_name,
                routing_key=queue_name,
                enabled=enable,
                priority=priority,
                args=json.dumps([
                    "job_action", task_name, job_id, run_account, celery_tasks[i].next_run_date
                ]),
            )
            tasks.append(task)
            logger.info(f'Save PeriodicTask: {task} to database successfully')
        except Exception as e:
            logger.error(f'Cannot save PeriodicTask to database error {e}')
            return None, JsonResponse(
                {'status': 'error', 'message': f'Cannot save PeriodicTask to database exception {e}'},
                status=405)
        try:
            task_detail, _ = TaskDetail.objects.get_or_create(job=job_settings, task_name=task_name,
                                                              run_date=clocked_schedule.clocked_time)
            logger.info(f'Save TaskDetail: {task_detail} to database successfully')
        except Exception as e:
            logger.error(f'Cannot save TaskDetail to database error {e}')
            return None, JsonResponse(
                {'status': 'error', 'message': f'Cannot save TaskDetail to database exception {e}'},
                status=405)
    return celery_tasks, None


def _format_update_job_response(outdated_tasks_responses, celery_tasks, next_runs):
    """
    Format the response for update_job_function.
    """
    updated_tasks = [SchedulerTaskResponse(task.celery_task_name, datetime_to_epoch(task.run_date)).to_dict() for task
                     in celery_tasks]
    outdated_tasks = [task_response.to_dict() for task_response in outdated_tasks_responses]
    logger.info(f'Updated tasks: {updated_tasks}, Outdated tasks: {outdated_tasks}')
    return JsonResponse({'status': 'success', 'outdated_tasks': outdated_tasks, 'updated_tasks': updated_tasks})


@csrf_exempt
def update_job_function(request):
    """
    Update an existing job's function, schedule, and related tasks.
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': f'Invalid HTTP Method for function Update Job'}, status=500)

    data, job_id, job_settings_or_response = _parse_update_job_request(request)
    if job_settings_or_response is not None and not isinstance(job_settings_or_response, JobSettings):
        return job_settings_or_response
    job_settings = job_settings_or_response

    values = _prepare_job_settings_update(data, job_settings)
    rrule_str = values['rrule_str']
    validity, rrule_dict = validate_rrule(rrule_str)
    if not validity:
        return JsonResponse({'status': 'error', 'message': f'Invalid RRULE FORMAT: {rrule_str}'}, status=400)

    run_forever = check_rrule_run_forever(rrule_dict=rrule_dict,
                                          end_date=convert_epoch_to_datetime(values['end_date_obj']) if values[
                                              'end_date_obj'] else None, max_run=values['max_run'])
    function_name = f'celerytasks.tasks.{values["job_type"].lower()}'

    task_details = TaskDetail.objects.filter(job=job_settings, already_run=False, soft_delete=False)
    periodic_tasks = PeriodicTask.objects.filter(description=job_id, enabled=True)
    logger.info(f'task_details: {task_details}')

    # Check if any of the specified fields are present in the data
    fields_to_check = ['start_date', 'end_date', 'repeat_interval', 'max_run']
    if not data or not any(field in data for field in fields_to_check):
        change_job_settings(data, job_settings, run_forever)
        change_periodic_tasks(data, periodic_tasks)
        return JsonResponse({'status': 'success', 'outdated_tasks': [], 'updated_tasks': []})

    valid_time_stub = validate_time_stub(data, job_settings, 'update')
    if not valid_time_stub:
        return JsonResponse({'status': 'error',
                             'message': f'Invalid datetime start_date: {convert_epoch_to_datetime(values["start_date_obj"])}, end_date: {convert_epoch_to_datetime(values["end_date_obj"]) if values["end_date_obj"] else None}'},
                            status=400)

    outdated_tasks_responses = _delete_old_tasks(task_details) if task_details else []

    max_run_val = 1 if run_forever else values['max_run'] - job_settings.run_count
    next_run_date = job_settings.next_run_date if job_settings.next_run_date else convert_epoch_to_datetime(
        values['start_date_obj'])
    if job_settings.job_operation == 'COMPLETED':
        next_run_date = convert_epoch_to_datetime(get_current_time())
    next_runs = get_list_run_date(rrule_str, max_run_val, next_run_date,
                                  convert_epoch_to_datetime(values['end_date_obj']) if values['end_date_obj'] else None,
                                  False, values['timezone'])
    logger.info(f'List next runs {next_runs}')

    celery_tasks, error_response = _create_new_schedules_and_tasks(
        next_runs, function_name, job_id, values['queue_name'], values['enable'], values['priority'],
        values['run_account'], job_settings
    )
    if error_response:
        return error_response

    # Update JobSettings object
    job_settings = JobSettings.objects.filter(job_id=job_id).first()
    if job_settings is None:
        return JsonResponse({'status': 'error', 'message': f'No job found for job_id {job_id}'}, status=404)
    change_job_settings(data, job_settings, run_forever)

    return _format_update_job_response(outdated_tasks_responses, celery_tasks, next_runs)


def change_job_settings(data, job_settings, run_forever):
    for key, value in data.items():
        if job_settings.run_forever != run_forever:
            setattr(job_settings, 'run_forever', run_forever)
        if key == 'is_enabled':
            setattr(job_settings, 'is_enable', value)
        elif key == 'start_date':
            setattr(job_settings, 'start_date', convert_epoch_to_datetime(value))
        elif key == 'end_date':
            setattr(job_settings, 'end_date', convert_epoch_to_datetime(value))
        elif key in JOB_SETTINGS_ONLY_FIELDS:
            setattr(job_settings, key, value)
    job_settings.save()


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
            "batch_type": 'Manual' if task_detail.manually_run else 'Auto',
            "status": "stopped"
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


@csrf_exempt
def get_task_detail(request):
    """
    Optimized: Batch fetch related objects to avoid N+1 queries.
    """
    data = json.loads(request.body)
    logger.info(f'Received request to update status job with body {data}')
    task_list = data.get('tasks')
    result = []
    if not task_list:
        return JsonResponse({'status': 'success', 'tasks': result})

    # Batch fetch TaskDetails
    task_details = list(TaskDetail.objects.filter(task_name__in=task_list).select_related('job'))
    task_details_by_name = {td.task_name: td for td in task_details}
    # Batch fetch JobSettings
    job_ids = {td.job_id for td in task_details if hasattr(td, 'job_id')}
    job_settings_map = {js.job_id: js for js in JobSettings.objects.filter(job_id__in=job_ids)}
    # Batch fetch PeriodicTasks
    periodic_tasks = {pt.name: pt for pt in PeriodicTask.objects.filter(name__in=task_list)}
    # Batch fetch TaskResults
    periodic_task_names = [pt.name for pt in periodic_tasks.values()]
    task_results = {tr.periodic_task_name: tr for tr in
                    TaskResult.objects.filter(periodic_task_name__in=periodic_task_names)}

    for celery_task_name in task_list:
        try:
            task_detail = task_details_by_name.get(celery_task_name)
            logger.info(f'task_detail: {task_detail}')
            if task_detail:
                job_settings = job_settings_map.get(getattr(task_detail, 'job_id', None))
                logger.info(f'job_settings: {job_settings}')
                periodic_task = periodic_tasks.get(task_detail.task_name)
                logger.info(f'periodic_task: {periodic_task}')
                if periodic_task:
                    task_result = task_results.get(periodic_task.name)
                    logger.info(f'task_result: {task_result}')
                else:
                    task_result = None

                # Only fetch latest_tasks if needed
                list_tasks = TaskDetail.objects.filter(job=job_settings, soft_delete=False,
                                                       manually_run=False).order_by(
                    '-created_at') if job_settings else []
                latest_tasks = list_tasks[0] if list_tasks else None
                logger.info(
                    f'Find latest tasks with detail: {getattr(latest_tasks, "task_name", None)}, status = {getattr(latest_tasks, "status", None)}')

                operation = 'RUN'
                if latest_tasks and latest_tasks.status not in (TaskDetail.STATUS_CREATED, TaskDetail.STATUS_NONE):
                    operation = 'BROKEN' if task_detail.status == TaskDetail.STATUS_FAILURE else 'COMPLETED'

                result.append({
                    'celery_task_name': celery_task_name,
                    'operation': operation,
                    'status': task_detail.status,
                    'error_no': job_settings.failure_count if job_settings else None,
                    'req_start_date': datetime_to_epoch(task_detail.run_date),
                    'actual_start_date': datetime_to_epoch(task_detail.run_date),
                    'run_duration': task_detail.run_duration,
                    'errors': task_result.traceback if task_result else None,
                    'output': task_result.result if task_result else None,
                })
        except Exception as e:
            logger.error(f'Get latest status of task throw exception: {e}')
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'success', 'tasks': result})


def parse_request_body(request):
    """Parses and validates the request body."""
    try:
        data = json.loads(request.body)
        workflow_id = data.get('workflow_id')
        list_priority_groups = data.get('list_priority_groups', [])
        if not workflow_id:
            return None, JsonResponse({'status': 'error', 'message': 'workflow_id is required'}, status=400)
        return {'workflow_id': workflow_id, 'list_priority_groups': list_priority_groups}, None
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        return None, JsonResponse({'status': 'error', 'message': 'Invalid JSON payload'}, status=400)


def process_jobs(list_priority_groups, workflow_id):
    """Processes jobs, validates them, and organizes tasks by priority."""

    for group in list_priority_groups:
        list_jobs = group.get('list_jobs')
        priority = group.get('priority')
        ignore_result = group.get('ignore_result')

        # Validate the presence of required fields
        if not list_jobs or not priority:
            return None, JsonResponse({
                'status': 'error',
                'message': 'Each dependency must include list_jobs, priority'
            }, status=400)

        for job_id in list_jobs:
            job_settings = JobSettings.objects.filter(job_id=job_id).first()

            # Validate job_id
            if not job_settings:
                return None, JsonResponse({
                    'status': 'error',
                    'message': f'Invalid job_id: {job_id}'
                }, status=404)

            # Update workflow and priority
            job_settings.update_workflow(workflow_id, priority, ignore_result)

            WorkflowRunDetail.objects.create(
                job_id=job_id,
                workflow_id=workflow_id,
                run_forever=job_settings.run_forever,
                priority=priority,
            )
            update_clocked_time_with_priority(job_id, priority)


@csrf_exempt
def assign_job_to_workflow(request):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

    try:
        # Parse the request body
        parsed_data, error_response = parse_request_body(request)
        if error_response:
            return error_response

        if parsed_data is None:
            return JsonResponse({'status': 'error', 'message': 'Invalid request data'}, status=400)

        workflow_id = parsed_data['workflow_id']
        list_priority_groups = parsed_data['list_priority_groups']

        # Insert WorkflowRunDetail records with correct logic
        process_jobs(list_priority_groups, workflow_id)

        return JsonResponse({
            'status': 'success',
            'workflow_id': workflow_id
        }, status=200)

    except Exception as e:
        logger.error(f"Error creating workflow: {e}")
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def update_clocked_time_with_priority(description: str, priority: int):
    """
    Updates the clocked time for periodic tasks in Celery Beat based on priority and description.

    :param description: The description to filter periodic tasks.
    :param priority: The priority value to calculate the new clocked time.
    :return: None
    """
    try:
        # Filter periodic tasks by description
        tasks = PeriodicTask.objects.filter(description=description)

        if not tasks.exists():
            raise ValueError(f"No tasks found with description '{description}'.")

        for task in tasks:
            # Ensure the task has an associated clocked schedule
            if not task.clocked:
                logger.warn(f"Task '{task.name}' does not have a clocked schedule. Skipping.")
                continue

            # Retrieve the last clocked time
            last_time = task.clocked.clocked_time

            # Calculate the new clocked time
            increment = timedelta(milliseconds=priority * 500)
            new_time = last_time + increment

            # Find or create a new clocked schedule for the calculated time
            clocked_schedule, created = ClockedSchedule.objects.get_or_create(
                clocked_time=new_time
            )

            # Update the task to use the new clocked schedule
            task.clocked = clocked_schedule
            task.save()

            logger.info(f"Clocked time for task '{task.name}' updated to {new_time} (priority: {priority}).")

    except ValueError as ve:
        logger.error(ve)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
