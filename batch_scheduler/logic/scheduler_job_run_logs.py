from sqlalchemy.databases import postgres
from utils.postgres_helper import PostgresClient
from utils import common_func
from models.scheduler_job_run_logs import SchedulerJobRunLogs
from models.scheduler_jobs import SchedulerJobs
from logic import scheduler_jobs
from logic import scheduler_job_last_log
from logic import scheduler_workflow

CURRENT_STATE_MAPPING = {
    'CREATE': 'READY TO RUN',
    'RUNNING': 'RUNNING',
    'SCHEDULED': 'SCHEDULED',
    'READY_TO_RUN': 'READY TO RUN',
    'RETRY_SCHEDULED': 'RETRY SCHEDULED',
    'BLOCKED': 'BLOCKED',
    'BROKEN': 'BROKEN',
    'COMPLETED': 'COMPLETED',
    'DISABLED': 'DISABLED',
    'FAILED': 'FAILED',
    'DELETED': 'DELETED'
}

# operation which does not change the state of the job when update log
OPERATION_NOT_CHANGE_STATE = ['UPDATE']
MANUAL_BATCH_TYPE = 'manual'
AUTO_BATCH_TYPE = 'auto'

def scheduler_job_run_logs_mapping(record):
    return {
        "log_id": record.log_id,
        "celery_task_name": record.celery_task_name,
        "log_date": record.log_date,
        "system_id": record.system_id,
        "group_id": record.group_id,
        "job_id": record.job_id,
        "system_name": record.system_name,
        "group_name": record.group_name,
        "job_name": record.job_name,
        "operation": record.operation,
        "batch_type": record.batch_type,
        "status": record.status.upper() if record.status else None,
        "retry_count": record.retry_count,
        "user_name": record.user_name,
        "error_no": record.error_no,
        "req_start_date": record.req_start_date,
        "actual_start_date": record.actual_start_date,
        "actual_end_date": record.actual_end_date,
        "run_duration": str(record.run_duration) if record.run_duration else None,
        "additional_info": record.additional_info,
        "errors": record.errors,
        "output": record.output
    }


def scheduler_job_run_logs_read(logger, params, related_group):
    try:
        logger.info('============ SCHEDULER_JOB_RUN_LOGS_READ ============')
        logger.debug(f'params: {params}')

        filter_params = [
            'page_number',
            'page_size',
            'req_start_date_from',
            'req_start_date_to',
            'text_search',
            'job_name',
        ]
        filter_values = {}
        for value in filter_params:
            if params.get(value) and value == 'job_name':
                filter_job_by_name = scheduler_jobs.scheduler_jobs_read(logger, {'job_name': params.get(value)}, [])['data']
                if not filter_job_by_name:
                    raise Exception('Invalid job id')
                current_job = next(iter(filter_job_by_name), {})
                params['job_id'] = current_job.get('job_id')
            else:
                filter_values[value] = params.get(value)
            params.pop(value, None)

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerJobRunLogs, params, scheduler_job_run_logs_mapping)

        logger.debug(f'original len: {len(result['data'])}')
        __filter_data(logger, filter_values, result)

        __filter_by_group(logger, related_group, result)

        result['total'] = len(result['data'])
        result['data'] = list(filter(lambda x: x['operation'] != 'OUTDATED', result['data']))
        result['data'].sort(key=lambda x: x['log_id'], reverse=True)
        result['data'] = common_func.paginate_data(result['data'], filter_values.get('page_number'),
                                                   filter_values.get('page_size'))

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_job_run_logs_create(logger, params):
    try:
        logger.info('============ SCHEDULER_JOB_RUN_LOGS_CREATE ============')
        logger.debug(f'params: {params}')

        job_data = scheduler_jobs.scheduler_jobs_read(logger, {'job_id': params['job_id']}, [])['data']
        if not job_data:
            raise Exception('Invalid job id')
        job_data_obj = next(iter(job_data), {})
        logger.debug(f'job_data_obj: {job_data_obj}')

        postgres_client = PostgresClient(logger)
        if 'tasks' in params:
            result = __create_multiple_run_logs(logger, postgres_client, params, job_data_obj)
        else:
            result = __create_single_run_logs(logger, postgres_client, params, job_data_obj)

        __update_job_info(logger, postgres_client, params, job_data_obj)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_job_run_logs_update(logger, params):
    try:
        logger.info('============ SCHEDULER_JOB_RUN_LOGS_UPDATE ============')
        logger.debug(f'params: {params}')

        filter_params = {'celery_task_name': params['celery_task_name']}
        input_val = {
            x: params[x] for x in params if
            x not in {'log_id', 'job_id', 'celery_task_name', 'run_count', 'failure_count', 'job_retry_count',
                      'next_run_date'}
        }
        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(SchedulerJobRunLogs, filter_params, input_val)

        job_data = scheduler_jobs.scheduler_jobs_read(logger, {'job_id': params['job_id']}, [])['data']
        if not job_data:
            raise Exception('Invalid job id')
        job_data_obj = next(iter(job_data), {})
        logger.debug(f'job_data_obj: {job_data_obj}')

        params_new = {
            'job_id': params.get('job_id'),
            'run_count': params.get('run_count') if params.get('run_count') else job_data_obj.get('run_count'),
            'failure_count': params.get('failure_count') if params.get('failure_count') else job_data_obj.get('failure_count'),
            'retry_count': params.get('retry_count') if params.get('retry_count') else job_data_obj.get('retry_count'),
            'batch_type': params.get('batch_type'),
            'current_state': job_data_obj.get('current_state'),
            'status': params.get('status'),
            'operation': params.get('operation'),
        }

        if params.get('next_run_date') and params.get('next_run_date') > 0:
            params_new['next_run_date'] = params.get('next_run_date')

        if params.get('actual_start_date'):
            params_new['actual_start_date'] = params.get('actual_start_date')

        __update_job_info(logger, postgres_client, params_new, job_data_obj)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


# ================= Support func =================
def __filter_data(logger, filter_values, result):
    logger.debug(f'filter_values: {filter_values}')

    req_start_date_from = filter_values.get('req_start_date_from')
    if req_start_date_from:
        logger.info('============ Filter req_start_date_from')
        result['data'] = list(
            filter(lambda x: x.get('req_start_date')
                             and str(x.get('req_start_date')) >= str(req_start_date_from), result['data']))
        logger.debug(f'filtered req_start_date_from: {len(result['data'])}')

    req_start_date_to = filter_values.get('req_start_date_to')
    if req_start_date_to:
        logger.info('============ Filter req_start_date_to')
        result['data'] = list(
            filter(lambda x: x.get('req_start_date')
                             and str(x.get('req_start_date')) <= str(req_start_date_to), result['data']))
        logger.debug(f'filtered req_start_date_to: {len(result['data'])}')

    text_search = filter_values.get('text_search')
    if text_search:
        logger.info('============ Text search')
        logger.debug(f'text_search: {text_search}')
        search_attributes = ['job_name', 'operation', 'status']
        searched_list = common_func.search_obj_by_keywords(result['data'], search_attributes, text_search)
        logger.debug(f'searched len: {len(searched_list)}')
        result['data'] = searched_list


def __create_multiple_run_logs(logger, postgres_client, params, job_data_obj):
    logger.info('============ Create multiple run logs')
    tasks = params.get('tasks') or []
    if not tasks:
        raise Exception('Missing tasks')
    obj_list = [
        {
            **task,
            'job_id': job_data_obj['job_id'],
            'system_id': job_data_obj['system_id'],
            'group_id': job_data_obj['group_id'],
            'job_name': job_data_obj['job_name'],
            "system_name": job_data_obj['system_name'],
            "group_name": job_data_obj['group_name']
        } for task in tasks
    ]
    logger.debug(f'run logs object list: {obj_list}')
    result = postgres_client.create_multiple_record(SchedulerJobRunLogs, obj_list)

    return result


def __create_single_run_logs(logger, postgres_client, params, job_data_obj):
    logger.info('============ Create single run log')
    params.update({
        'system_id': job_data_obj['system_id'],
        'group_id': job_data_obj['group_id'],
        'job_name': job_data_obj['job_name'],
        "system_name": job_data_obj['system_name'],
        "group_name": job_data_obj['group_name']
    })
    logger.debug(f'run logs object: {params}')
    result = postgres_client.create_record(SchedulerJobRunLogs, params)

    return result


def __update_job_info(logger, postgres_client, params, job_data_obj):
    job_logs = postgres_client.get_records(SchedulerJobRunLogs, {'job_id': params['job_id']},
                                           scheduler_job_run_logs_mapping)['data']
    job_logs = list(
        filter(
            lambda x: x and x.get('operation') != 'OUTDATED' and (x.get('status') or x.get('actual_start_date')),
            job_logs
        )
    )
    job_logs.sort(key=lambda x: (x.get('actual_start_date') or ''), reverse=True)
    latest_log = next(iter(job_logs), {})
    logger.debug(
        f"latest_log job {latest_log.get('job_id')}- log_id {latest_log.get('log_id')}: {latest_log}"
    )

    __process_last_log(logger, params, latest_log)
    
    __update_job_data(logger, params, job_data_obj, latest_log)


def __process_last_log(logger, params, latest_log):
    last_log = scheduler_job_last_log.scheduler_job_last_log_read(
        logger, {"job_id": params["job_id"]}
    )["data"]
    if not last_log:
        logger.info("============ Create last log")
        actual_start_date = params.get("actual_start_date")
        params_create = {
            "job_id": params.get("job_id"),
            "actual_start_date": actual_start_date,
            "log_id": latest_log.get("log_id"),
            "last_state_job": "SCHEDULED",
        }
        scheduler_job_last_log.scheduler_job_last_log_create(logger, params_create)
    else:
        last_log_obj = next(iter(last_log), {})
        params_update = {
            "id": last_log_obj.get("id"),
            "log_id": latest_log.get("log_id"),
        }

            
        if params.get("actual_start_date"):
            params_update["actual_start_date"] = params.get("actual_start_date")

        logger.info(f"============ Update last log: {params_update}")

        scheduler_job_last_log.scheduler_job_last_log_update(logger, params_update)


def __update_job_data(logger, params, job_data_obj, latest_log):
    logger.info(f'============ Update job data with state: {job_data_obj.get("current_state")}')
    actual_start_date = params.get('actual_start_date') or latest_log.get('actual_start_date')
    current_state, is_enabled = __get_job_current_state(logger, job_data_obj, latest_log, params)
    params_new = {
        'job_id': params['job_id'],
        'run_count': params.get('run_count') if params.get('run_count') else job_data_obj.get('run_count'),
        'failure_count': params.get('failure_count') if params.get('failure_count') else job_data_obj.get('failure_count'),
        'retry_count': params.get('retry_count') if params.get('retry_count') else job_data_obj.get('retry_count'),
        'last_start_date': actual_start_date,
        'last_run_duration': latest_log.get('run_duration'),
        'current_state': current_state,
        'is_enabled': is_enabled
    }

    if params.get("next_run_date"):
        params_new["next_run_date"] = (params.get("next_run_date"),)

    if params.get("batch_type") is None or (
        __check_batch_type(params.get("batch_type"), AUTO_BATCH_TYPE)
        and params.get("operation") not in OPERATION_NOT_CHANGE_STATE
    ):
        logger.debug(
            f'=== Update State Job when Manual Job with state: {job_data_obj.get("current_state")}'
        )
        last_log = scheduler_job_last_log.scheduler_job_last_log_read(
            logger, {"job_id": params["job_id"]}
        )["data"]
        last_log_obj = next(iter(last_log), {})
        params_update = {
            "id": last_log_obj.get("id"),
            "last_state_job": current_state,
        }
        scheduler_job_last_log.scheduler_job_last_log_update(logger, params_update)

    logger.info(f"params_new: {params_new}")

    scheduler_jobs.scheduler_jobs_update_data(logger, params_new)

    logger.info(f'============ Update Workflow for job {params["job_id"]}')
    postgres_client = PostgresClient(logger)
    job_data = (
        postgres_client.get_session()
        .query(SchedulerJobs)
        .filter_by(job_id=params["job_id"])
        .first()
    )
    if job_data and job_data.priority_group_id:
        latest_status = None
        if job_data.current_state in ("RUNNING", "RETRY SCHEDULED"):
            latest_status = "RUNNING"
        if job_data.current_state == "COMPLETED":
            latest_status = "SUCCESS"
        if job_data.current_state in ("FAILED", "BLOCKED", "DISABLED", "BROKEN"):
            latest_status = "FAILED"

        logger.info(
            f"Update Status for Workflow: {job_data.priority_group_id} to status {latest_status}"
        )
        scheduler_workflow.update_workflow_status(
            logger,
            {"priority_group_id": job_data.priority_group_id, "latest_status": latest_status},
        )
    else:
        logger.info(f"Job {params["job_id"]}not have any workflow")


def __get_job_current_state(logger, job_data_obj, latest_log, params):
    log_status = (latest_log.get('status') or '').upper()
    log_operation = (latest_log.get('operation') or '').upper()
    max_run = str(job_data_obj.get('max_run'))
    is_enabled = job_data_obj.get('is_enabled')
    batch_type = (params.get('batch_type') or '').lower()

    if __check_batch_type(batch_type, MANUAL_BATCH_TYPE):
        return __logic_state_manual_run(logger, params, latest_log, job_data_obj)

    if not is_enabled:
        current_state = CURRENT_STATE_MAPPING['DISABLED']

    elif log_operation != 'BROKEN' and log_status == 'FAILED':
        current_state = CURRENT_STATE_MAPPING['FAILED']

    elif log_operation == 'COMPLETED' and job_data_obj.get('auto_drop'):
        current_state = CURRENT_STATE_MAPPING['DELETED']

    elif log_operation == 'ENABLED':
        mapping_value = 'SCHEDULED' if max_run == '1' else 'RETRY_SCHEDULED'
        current_state = CURRENT_STATE_MAPPING[mapping_value]

    elif log_operation in {'RUN', 'RETRY_RUN', 'RECOVERY_RUN'}:
        if not log_status:
            current_state = CURRENT_STATE_MAPPING['RUNNING']
        else:
            mapping_value = 'SCHEDULED' if max_run == '1' else 'RETRY_SCHEDULED'
            current_state = CURRENT_STATE_MAPPING[mapping_value]
    else:
        current_state = CURRENT_STATE_MAPPING.get(log_operation) or job_data_obj.get('current_state')

    if current_state in {'COMPLETED', 'DELETED'}:
        is_enabled = False

    return current_state, is_enabled


def __filter_by_group(logger, filter_group_list, result):
    # Check if filter_group_list is valid (not None, not empty, and not [])
    if filter_group_list is not None and bool(filter_group_list):
        job_log_list = result['data']
        logger.debug(f'filter_job_list: {filter_group_list}')
        filtered_jobs = []

        for job_log in job_log_list:
            if job_log['group_id'] in filter_group_list:
                # The user has a related_group and group_id matches
                filtered_jobs.append(job_log)
            else:
                logger.debug(f"Removing job: {job_log['group_id']}")

        result['data'] = filtered_jobs


def __logic_state_manual_run(logger, params, latest_log, job_data_obj):
    logger.debug(
        f'Manual run {job_data_obj.get("job_id")} logic: {params.get("status")} - {latest_log}'
    )
    # operation = 'RUNNING' if latest_log.get('operation') == 'RUN' else latest_log.get('operation')
    if params.get("status"):
        if params.get("status").lower() in ["succeed"]:
            last_log = scheduler_job_last_log.scheduler_job_last_log_read(
                logger, {"job_id": params["job_id"]}
            )["data"]
            last_log_obj = next(iter(last_log), {})
            return last_log_obj.get("last_state_job"), job_data_obj.get("is_enabled")
        else:
            if params.get("status").lower() in ["failed"]:
                operation = "FAILED"
            else:
                if latest_log.get("status").lower() == "stopped":
                    operation = "BLOCKED"
    else:
        operation = "RUNNING"

    logger.debug(f'Manual run: {params.get("status")} - {operation}')
    return CURRENT_STATE_MAPPING[operation], job_data_obj.get("is_enabled")


# constant_type is lower case.
def __check_batch_type(batch_type, constants_type):
    return (batch_type or "").lower() == constants_type
