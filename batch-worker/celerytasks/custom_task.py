import time

from celery import Task
from django.utils import timezone
from requests.exceptions import HTTPError

from celerytasks.models import TaskDetail, JobSettings, WorkflowRunDetail
from exceptions.executable_exception import ExecutableError
from logger import get_logger
from .redis_helper import set_key, get_key, remove_key, get_keys_by_pattern, add_to_set, get_set, remove_from_set
from .utils import (
    get_current_time,
    get_next_run_date,
    datetime_to_epoch,
    convert_epoch_to_datetime,
    extract_error_message,
)
from .views import (
    rrule_schedule_task,
    call_scheduler_run_log,
    call_scheduler_workflow_update,
)

logger = get_logger()


class RepeatTask(Task):
    """Base task for executing repeated tasks."""

    def __call__(self, *args, **kwargs):
        task_name = args[1]
        job_id = args[2]

        # Fetch JobSettings
        job_settings = JobSettings.objects.filter(job_id=job_id).first()
        if job_settings is None:
            logger.error(f"JobSettings not found for job_id {job_id}.")
            raise Exception(f"JobSettings not found for job_id {job_id}")

        # Fetch TaskDetail
        task_detail = TaskDetail.objects.filter(task_name=task_name).first()
        if task_detail is None:
            logger.error(f"TaskDetail not found for task_name {task_name}.")
            raise Exception(f"TaskDetail not found for task_name {task_name}")

        workflow_id = job_settings.workflow_id
        current_job_priority = job_settings.priority

        if workflow_id and current_job_priority is not None:
            logger.info(
                f"Job {job_id} is part of group {workflow_id}. Checking status of the nearest prerequisite job."
            )

            workflow_run_detail = WorkflowRunDetail.objects.filter(job_id=job_settings.job_id).first()
            if workflow_run_detail is None:
                logger.error(f"WorkflowRunDetail not found for job_id {job_id}.")
                raise Exception(f"WorkflowRunDetail not found for job_id {job_id}")

            # Get job IDs as a list from the QuerySet
            list_higher_priority_jobs = list(
                WorkflowRunDetail.objects.filter(workflow_id=workflow_id, priority__lt=current_job_priority)
                .exclude(job_id=job_id)
                .values_list('job_id', flat=True)  # Extract 'job_id' values as a list
            )

            waiting_set_task = set()

            logger.info(f'=========list_higher_priority_jobs: {list_higher_priority_jobs}')

            related_tasks = fetch_related_tasks(list_higher_priority_jobs)
            if related_tasks:
                waiting_set_task.update(related_tasks)
            is_required_check = have_to_wait_related_jobs(related_tasks)
            while is_required_check:
                logger.info(
                    f"Job {job_id} is not complete. "
                    f"Waiting for 10 seconds before retrying."
                )

                body = {
                    "workflow_id": workflow_id,
                    "latest_status": "RUNNING",
                    "current_job_id": job_id,
                }
                call_scheduler_workflow_update(body)
                related_tasks = fetch_related_tasks(list_higher_priority_jobs)
                if related_tasks:
                    waiting_set_task.update(related_tasks)
                is_required_check = have_to_wait_related_jobs(related_tasks)

                time.sleep(10)

                if not is_required_check:
                    logger.debug(f"Start running task {task_name} in workflow in: {timezone.now()}")
                    task_detail.save_actual_start_date()
            try:
                for related_task_name in waiting_set_task:
                    related_task = TaskDetail.objects.filter(task_name=related_task_name).first()
                    related_job_id = related_task.job_id
                    related_job_detail = JobSettings.objects.filter(job_id=related_job_id).first()
                    if related_task.task_result == 'FAILED':
                        if not related_job_detail.ignore_result:
                            logger.error(
                                f"Job with ID {related_job_id} is BROKEN and ignore_result is False. Ending process."
                            )
                            raise Exception(
                                f"Critical issue: Job {related_job_id} is BROKEN and cannot be ignored."
                            )
                        else:
                            logger.info(
                                f"Job with ID {related_job_id} is BROKEN but ignoring. Continuing..."
                            )
                            continue

            except Exception as e:
                body = task_call_update_run_log(task_detail, job_settings)

                if body is not None:
                    success = call_scheduler_run_log(body, "update")
                    if success:
                        logger.info(
                            f"Successfully update at hook call scheduler run log for task {task_detail}"
                        )
                    else:
                        logger.error(
                            f"Failed to update at hook call scheduler run log for task {task_detail}"
                        )
                raise e

        # Execute task with error handling
        try:
            body = task_call_update_run_log(task_detail, job_settings)

            if body is not None:
                success = call_scheduler_run_log(body, "update")
                if success:
                    logger.info(
                        f"Successfully update at hook call scheduler run log for task {task_detail}"
                    )
                else:
                    logger.error(
                        f"Failed to update at hook call scheduler run log for task {task_detail}"
                    )
            return super(RepeatTask, self).__call__(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in task {self.name}: {e}")
            if job_settings.restart_on_failure:
                self._retry_task(job_settings, task_detail, job_id, e, args, kwargs)
            else:
                raise

    def _retry_task(self, job_settings, task_detail, job_id, exception, args, kwargs):
        """
        Handles task retries based on job settings.
        """
        max_retries = job_settings.max_failure
        time_limit = job_settings.max_run_duration.total_seconds()
        retry_delay = float(job_settings.retry_delay)

        # Increase retry count in TaskDetail
        task_detail.increase_retry_count()

        logger.info(
            f"Trying to retry task: {job_id} with details: "
            f"retry_delay={retry_delay}, max_retries={max_retries - 1}, time_limit={time_limit}"
        )

        self.retry(
            exc=exception,
            countdown=retry_delay,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries - 1,
            time_limit=time_limit,
        )

    def before_start(self, task_id, args, kwargs):
        logger.info(f"Hook before_start for task with id: {task_id}")
        try:
            task_name = args[1]
            run_account = args[3]
            next_run_date = args[4]
            task_detail = TaskDetail.objects.filter(task_name=task_name).first()
            if task_detail is None:
                logger.error(f"TaskDetail not found for task_name {task_name}.")
            task_detail.start_running(task_id, run_account)
            job_settings = JobSettings.objects.filter(job_id=task_detail.job_id).first()
            if job_settings is None:
                logger.error(f"JobSettings not found for job_id {task_detail.job_id}.")
                return

            task_detail.mark_as_ran()
            if job_settings.run_forever and not task_detail.manually_run:
                try:
                    task_names = rrule_schedule_task(job_settings=job_settings)
                except Exception as e:
                    logger.error("Fail to create forever task", e)

            next_run_date = get_next_run_date(
                start_date=job_settings.start_date,
                rrule_str=job_settings.repeat_interval,
                run_count=job_settings.run_count,
                tz=job_settings.timezone,
            )

            job_settings.save_next_run(next_run_date)
            job_settings.update_operation('RUNNING')

            if not task_detail.manually_run:
                job_settings.increase_run_count()

            logger.info(f"run count: {job_settings.run_count}")

            if job_settings.retry_count > 0:
                operation = "RETRY_RUN"
            else:
                operation = "RUN"

            current_run_date = convert_epoch_to_datetime(get_current_time())
            redis_key = f'job_id:{job_settings.job_id}'
            add_to_set(redis_key, task_name)

            method = "create" if task_detail.retry_count == 0 else "update"
            body = convert_task_detail_to_log_data(
                task_detail=task_detail,
                job_settings=job_settings,
                result=None,
                operation=operation,
                method=method,
                error_no=None,
                error_detail=None,
                next_run_date=int(next_run_date.timestamp() * 1000),
            )
            if body is not None:
                success = call_scheduler_run_log(body, method)
                if success:
                    logger.info(
                        f"Successfully update at hook before_start scheduler run log for task {task_id}"
                    )
                else:
                    logger.error(
                        f"Failed to update at hook before_start scheduler run log for task {task_id}"
                    )

        except Exception as e:
            logger.error(f"Error in before_start for {task_id}: {e}")
            return

    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f"Hook on_success for task with id: {task_id}")
        try:
            task_detail = TaskDetail.objects.filter(celery_task_id=task_id).first()
            if task_detail is None:
                logger.error(f"TaskDetail not found for task_id {task_id}.")
                return
            task_detail.count_duration()
            task_detail.save_status(TaskDetail.STATUS_SUCCESS)
            task_detail.update_task_result('SUCCESS')
            job_settings = JobSettings.objects.filter(job_id=task_detail.job_id).first()
            if job_settings is None:
                logger.error(f"JobSettings not found for job_id {task_detail.job_id}.")
                return
            if job_settings.function_name == "celerytasks.tasks.rest_api":
                error_no = 200
            else:
                error_no = None
            body = convert_task_detail_to_log_data(
                task_detail=task_detail,
                job_settings=job_settings,
                result=None,
                operation=None,
                method="update",
                error_no=error_no,
                error_detail=None,
                next_run_date=None,
            )
            if body is not None:
                success = call_scheduler_run_log(body, "update")
                if success:
                    logger.info(
                        f"Successfully update at hook on_success scheduler run log for task {task_id}"
                    )
                else:
                    logger.error(
                        f"Failed to update at hook on_success scheduler run log for task {task_id}"
                    )

            is_required_check = check_related_workflow_job(job_settings)
            if is_required_check:
                body = {
                    "workflow_id": job_settings.workflow_id,
                    "latest_status": "SUCCESS",
                    "current_job_id": job_settings.job_id,
                }
                call_scheduler_workflow_update(body)
        except Exception as e:
            logger.error(f"Error in task {self.name}: {e}")

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.info(f"Hook on_failure for task with id: {task_id}")
        try:
            task_detail = TaskDetail.objects.filter(celery_task_id=task_id).first()
            if task_detail is None:
                logger.error(f"TaskDetail not found for task_id {task_id}.")
                return
            task_detail.save_status(TaskDetail.STATUS_FAILURE)
            task_detail.update_task_result('FAILED')
            job_settings = JobSettings.objects.filter(job_id=task_detail.job_id).first()
            if job_settings is None:
                logger.error(f"JobSettings not found for job_id {task_detail.job_id}.")
                return
            job_settings.increase_failure_count()

            logger.warn(f"Execute job id={task_detail.job_id} exec ={exc}, args={args}")
            if isinstance(exc, HTTPError):
                logger.info(f"Exception: {exc}")
                error_no = exc.response.status_code
                logger.info(f"Status Code: {error_no}")
                # Accessing the response body as text
                output = exc.response.text
                logger.info(f"Body: {output}")
            elif isinstance(exc, ExecutableError):
                logger.info(f"Exception: {exc}")
                error_no = exc.status_code
                logger.info(f"Status Code: {error_no}")
                # Accessing the response body as text
                output = exc.error_details if exc.error_details else exc.output
                logger.info(f"Body: {output}")
            else:
                error_no = -104
                output = str(exc)

            body = convert_task_detail_to_log_data(
                task_detail=task_detail,
                job_settings=job_settings,
                result=None,
                operation=None,
                method="update",
                error_no=error_no,
                error_detail=extract_error_message(output),
                next_run_date=None,
            )
            if body is not None:
                success = call_scheduler_run_log(body, "update")
                if success:
                    logger.info(
                        f"Successfully update at hook on_failure scheduler run log for task {task_id}"
                    )
                else:
                    logger.error(
                        f"Failed to update at hook on_failure at hook on_success scheduler run log for task {task_id}"
                    )

            is_required_check = check_related_workflow_job(job_settings)
            if is_required_check:
                body = {
                    "workflow_id": job_settings.workflow_id,
                    "latest_status": "FAILED",
                    "current_job_id": job_settings.job_id,
                }
                call_scheduler_workflow_update(body)

            logger.error(f"Task {task_id}: {exc} marked as FAILED")
        except Exception as e:
            logger.error(f"Error in task {self.name}: {e}")
            return

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        logger.info(f"Hook on_retry for task with id: {task_id}")
        try:
            task_name = args[1]
            task_detail = TaskDetail.objects.filter(celery_task_id=task_id).first()
            if task_detail is None:
                logger.error(f"TaskDetail not found for task_id {task_id}.")
                return
            task_detail.save_status(TaskDetail.STATUS_FAILURE)
            job_settings = JobSettings.objects.filter(job_id=task_detail.job_id).first()
            if job_settings is None:
                logger.error(f"JobSettings not found for job_id {task_detail.job_id}.")
                return
            job_settings.increase_retry_count()
            logger.info(f"retry fail: {job_settings.retry_count}")
            body = convert_task_detail_to_log_data(
                task_detail=task_detail,
                job_settings=job_settings,
                result=None,
                operation="FAILED",
                method="update",
                error_no=None,
                error_detail=exc,
                next_run_date=None,
            )
            if body is not None:
                success = call_scheduler_run_log(body, "update")
                if success:
                    logger.info(
                        f"Successfully update at hook on_retry scheduler run log for task {task_id}"
                    )
                else:
                    logger.error(
                        f"Failed to update at hook on_retry scheduler run log for task {task_id}"
                    )
            logger.info(f"Retrying task {task_name} for job {job_settings.job_id}.")
        except Exception as e:
            logger.error(f"Error in on_retry for {task_id}: {e}")
            return

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        logger.info(f"Hook after_return for task with id: {task_id}")
        try:
            task_detail = TaskDetail.objects.filter(celery_task_id=task_id).first()
            if task_detail is None:
                logger.error(f"TaskDetail not found for task_id {task_id}.")
                return

            job_id = task_detail.job_id
            job_settings = JobSettings.objects.filter(job_id=job_id).first()
            if job_settings is None:
                logger.error(f"JobSettings not found for job_id {task_detail.job_id}.")
                return

            if task_detail.status == TaskDetail.STATUS_FAILURE:
                operation = "BROKEN"
            else:
                list_tasks = TaskDetail.objects.filter(
                    job=job_settings, soft_delete=False, manually_run=False
                ).order_by("-created_at")
                latest_tasks = list_tasks[0]
                logger.info(f"Length of array: {len(list_tasks)}")
                logger.info(
                    f"Find latest tasks with detail: {latest_tasks.task_name}, status = {latest_tasks.status}"
                )

                operation = "RUN"
                if (
                        latest_tasks.status != TaskDetail.STATUS_CREATED
                        and latest_tasks.status != TaskDetail.STATUS_NONE
                ):
                    operation = (
                        "BROKEN"
                        if task_detail.status == TaskDetail.STATUS_FAILURE
                        else "COMPLETED"
                    )

            next_run_date = 1 if operation == "COMPLETED" else None

            logger.info(
                f"Update task with operation: {operation}, status: {task_detail.status}, max_failure: {job_settings.max_failure}, max_run: {job_settings.max_run}, failure_count: {job_settings.failure_count}, run_count: {job_settings.run_count}"
            )
            request_body = convert_task_detail_to_log_data(
                task_detail,
                job_settings,
                retval,
                operation,
                "update",
                None,
                None,
                next_run_date,
            )
            if request_body is not None:
                success = call_scheduler_run_log(request_body, "update")
                if success:
                    logger.info(
                        f"Successfully update at hook after_return scheduler run log for task {task_id}"
                    )
                else:
                    logger.error(
                        f"Failed to update at hook after_return scheduler run log for task {task_id}"
                    )

            job_settings.save_operation(operation, task_detail.status)
            redis_key = f'job_id:{job_settings.job_id}'
            remove_from_set(redis_key, task_detail.task_name)

        except Exception as e:
            logger.error(f"Error in after_return for {task_id}: {e}")


def task_call_update_run_log(task_detail, job_settings):
    try:
        output = {
            "job_id": task_detail.job.job_id,
            "user_name": task_detail.run_account,
            "actual_start_date": get_current_time(),
            "run_duration": None,
            "additional_info": None,
            "celery_task_name": task_detail.task_name,
            "run_count": job_settings.run_count,
            "failure_count": job_settings.failure_count,
            "job_retry_count": job_settings.retry_count,
            "retry_count": task_detail.retry_count,
        }

        if task_detail.status != TaskDetail.STATUS_NONE:
            output["status"] = task_detail.status

        if task_detail.manually_run:
            output["batch_type"] = "Manual"

        return output
    except Exception as e:
        logger.error(f"Error in converting task detail to log data: {e}")
        return None


def convert_task_detail_to_log_data(
        task_detail,
        job_settings,
        result,
        operation,
        method,
        error_no,
        error_detail,
        next_run_date,
):
    try:
        output = {
            "job_id": task_detail.job.job_id,
            "user_name": task_detail.run_account,
            "run_duration": (
                str(task_detail.run_duration) if task_detail.run_duration else None
            ),
            "additional_info": None,
            "celery_task_name": task_detail.task_name,
            "run_count": job_settings.run_count,
            "failure_count": job_settings.failure_count,
            "job_retry_count": job_settings.retry_count,
            "retry_count": task_detail.retry_count,
        }

        ## DO NOT CHANGE !IMPORTANT
        if job_settings.priority is not None:
            priority = job_settings.priority
        else:
            priority = 0
        if method == "create":
            output["req_start_date"] = datetime_to_epoch(task_detail.run_date) - 400 * priority
            output["actual_start_date"] = get_current_time()

        if operation:
            output["operation"] = operation

        if task_detail.status != TaskDetail.STATUS_NONE:
            output["status"] = task_detail.status

        if task_detail.check_final_state():
            output["actual_end_date"] = get_current_time()

        if task_detail.status != TaskDetail.STATUS_FAILURE:
            output["output"] = str(result)

        if error_no is not None:
            output["error_no"] = error_no
            output["errors"] = error_detail

        if task_detail.manually_run:
            output["batch_type"] = "Manual"

        if next_run_date is not None:
            output["next_run_date"] = next_run_date

        logger.info(f" /logs/{method}: {output}")
        return output
    except Exception as e:
        logger.error(f"Error in converting task detail to log data: {e}")
        return None


def check_related_workflow_job(job_settings):
    workflow_id = job_settings.workflow_id
    current_job_priority = job_settings.priority
    job_id = job_settings.job_id
    if not job_id:
        raise Exception(f"Job_id is required")
    if not workflow_id or not current_job_priority:
        return False

    logger.info(
        f"Job {job_id} is part of group {workflow_id}. Checking status of the nearest prerequisite job."
    )
    nearest_job = (
        JobSettings.objects.filter(
            workflow_id=workflow_id,
            priority__lt=current_job_priority,  # Only jobs with priority less than the current job
        )
        .order_by("-priority")
        .first()
    )  # Get the nearest job with the highest priority less than current job
    is_required_check = True if nearest_job else False
    if not is_required_check:
        logger.info(
            f"Job {job_id} has the highest priority in workflow {workflow_id}. No prerequisite jobs to wait for."
        )
    return is_required_check


def have_to_wait_related_jobs(list_related_tasks):
    logger.info("Checking for running or broken tasks in the list of higher priority tasks...")

    if not list_related_tasks:
        logger.info("The list of higher priority tasks is empty or None.")
        return False

    return True


def fetch_related_tasks(list_higher_priority_jobs):
    result = []
    for job_id in list_higher_priority_jobs:
        # Query the job settings for the job ID
        job = JobSettings.objects.filter(job_id=job_id).first()

        if not job:
            logger.info(f"No JobSettings found for job ID {job_id}. Skipping...")
            continue

        redis_key = f'job_id:{job_id}'
        set_tasks = get_set(redis_key)
        logger.info(f'set_tasks: {set_tasks} list with key: {redis_key}')
        for related_task_name in set_tasks:
            if related_task_name is not None:
                result.append(related_task_name)
    logger.info(f'list related_tasks: {result}')
    return result
