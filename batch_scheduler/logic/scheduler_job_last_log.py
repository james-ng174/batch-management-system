from utils.postgres_helper import PostgresClient
from utils import common_func
from logic import scheduler_jobs
from models.scheduler_job_last_log import SchedulerJobLastLog


def scheduler_job_last_log_mapping(record):
    return {
        "id": record.id,
        "system_id": record.system_id,
        "group_id": record.group_id,
        "job_id": record.job_id,
        "job_name": record.job_name,
        "actual_start_date": record.actual_start_date,
        "log_id": record.log_id,
        "last_state_job": record.last_state_job
    }


def scheduler_job_last_log_read(logger, params):
    try:
        logger.info('============ SCHEDULER_JOB_LAST_LOG_READ ============')
        logger.debug(f'params: {params}')
        page_number = params.get('page_number')
        page_size = params.get('page_size')
        params.pop('page_number', None)
        params.pop('page_size', None)

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerJobLastLog, params, scheduler_job_last_log_mapping)
        result['total'] = len(result['data'])
        result['data'] = common_func.paginate_data(result['data'], page_number, page_size)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_job_last_log_create(logger, params):
    try:
        logger.info('============ SCHEDULER_JOB_LAST_LOG_CREATE ============')
        logger.debug(f'params: {params}')

        job_data = scheduler_jobs.scheduler_jobs_read(logger, {'job_id': params['job_id']}, [])['data']
        if not job_data:
            raise Exception('Invalid job id')

        job_data_obj = next(iter(job_data), {})
        params.update({
            'system_id': job_data_obj['system_id'],
            'group_id': job_data_obj['group_id'],
            'job_name': job_data_obj['job_name']
        })

        postgres_client = PostgresClient(logger)
        result = postgres_client.create_record(SchedulerJobLastLog, params)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_job_last_log_update(logger, params):
    try:
        logger.info('============ SCHEDULER_JOB_LAST_LOG_UPDATE ============')
        logger.debug(f'params: {params}')

        filter_params = {'id': params['id']}
        params.pop('id', None)

        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(SchedulerJobLastLog, filter_params, params)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result
