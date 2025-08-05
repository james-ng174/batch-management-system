import json
import uuid
from dateutil.rrule import rrulestr
from dateutil.parser import parse
from utils.celery_helper import CeleryClient
from utils.postgres_helper import PostgresClient
from utils import common_func, constants
from logic import scheduler_users, scheduler_job_groups, scheduler_job_servers, scheduler_job_run_logs
from models.scheduler_jobs import SchedulerJobs
from models.scheduler_workflow_priotiry_group import SchedulerWorkflowPriorityGroup

RRULE_DATE_FORMAT = '%Y%m%dT%H%M%S'
FREQ_LIST = ["YEARLY", "MONTHLY", "WEEKLY", "DAILY", "HOURLY", "MINUTELY", "SECONDLY"]


def scheduler_workflow_priority_group_mapping(record):
    return {
        "workflow_id": record.workflow_id,
        "priority_group_id": record.id,
        "latest_status": record.latest_status,
        "priority": record.priority,
        "frst_reg_date": record.frst_reg_date,
        "frst_reg_user_id": record.frst_reg_user_id,
        "last_chg_date": record.last_chg_date,
        "last_reg_user_id": record.last_reg_user_id
    }


def frontend_mapping_convert(logger, list_data):
    try:
        final_result = []
        for obj in list_data:
            new_obj = {
                "jobId": obj.get('job_id'),
                "systemId": obj.get('system_id'),
                "system": obj.get('system_name'),
                "groupId": obj.get('group_id'),
                "group": obj.get('group_name'),
                "creator": obj.get('frst_reg_user_name'),
                "jobName": obj.get('job_name'),
                "comment": obj.get('job_comments'),
                "currentState": obj.get('current_state'),
                "schedule": obj.get('schedule_str'),
                "lastStartDate": obj.get('last_start_date'),
                "duration": obj.get('last_run_duration'),
                "lastResult": obj.get('last_result'),
                "startDate": obj.get('start_date'),
                "endDate": obj.get('end_date'),
                "repeatInterval": obj.get('repeat_interval'),
                "nextRunDate": obj.get('next_run_date'),
                "enable": obj.get('is_enabled'),
                "jobType": obj.get('job_type'),
                "jobAction": obj.get('job_action'),
                "jobBody": obj.get('job_body') if obj.get('job_body') != 'null' else None,
                "jobCreateDate": obj.get('frst_reg_date'),
                "retryDelay": obj.get('retry_delay'),
                "maxRunDuration": obj.get('max_run_duration'),
                "maxRun": obj.get('max_run'),
                "maxFailure": obj.get('max_failure'),
                "jobPriority": obj.get('priority'),
                "autoDrop": obj.get('auto_drop'),
                "restartOnFailure": obj.get('restart_on_failure'),
                "restartable": obj.get('restartable'),
                "jobLastChgDate": obj.get('last_chg_date'),
                "runCount": obj.get('run_count'),
                "failureCount": obj.get('failure_count'),
                "retryCount": obj.get('retry_count'),
            }

            final_result.append(new_obj)
    except Exception as e:
        logger.error(f'Exception: {e}')
        final_result = []

    return final_result


def scheduler_jobs_mapping(record):
    try:
        my_rrule = rrulestr(str(record.repeat_interval))
        schedule_str = f'{FREQ_LIST[my_rrule._freq]}({my_rrule._interval})'
    except Exception as e:
        schedule_str = record.repeat_interval

    last_result = None
    if record.last_log and hasattr(record.last_log, 'run_log') and record.last_log.run_log:
        last_result = record.last_log.run_log.status

    run_logs = []
    if record.run_logs:
        for log in record.run_logs:
            if log.status:
                run_logs.append({'log_id': log.log_id, 'status': log.status})

    return {
        "job_id": record.job_id,
        "system_id": record.system_id,
        "system_name": record.server.system_name,
        "queue_name": record.server.queue_name,
        "group_id": record.group_id,
        "group_name": record.group.group_name,
        "job_name": record.job_name,
        "start_date": record.start_date,
        "end_date": record.end_date,
        "repeat_interval": record.repeat_interval,
        'schedule_str': schedule_str,
        "max_run_duration": str(record.max_run_duration) if record.max_run_duration else None,
        "max_run": record.max_run,
        "max_failure": record.max_failure,
        "retry_delay": record.retry_delay,
        "priority": record.priority,
        "is_enabled": record.is_enabled,
        "auto_drop": record.auto_drop,
        "restart_on_failure": record.restart_on_failure,
        "restartable": record.restartable,
        "job_comments": record.job_comments,
        "job_type": record.job_type,
        "job_action": record.job_action,
        "job_body": record.job_body,
        "frst_reg_date": record.frst_reg_date,
        "frst_reg_user_id": record.frst_reg_user_id,
        "frst_reg_user_name": record.frst_user.user_name,
        "last_chg_date": record.last_chg_date,
        "last_reg_user_id": record.last_reg_user_id,
        "last_reg_user_name": record.last_user.user_name if record.last_user else None,
        "run_count": record.run_count,
        "failure_count": record.failure_count,
        "retry_count": record.retry_count,
        "next_run_date": record.next_run_date,
        "current_state": record.current_state,
        "last_start_date": record.last_start_date,
        "last_run_duration": str(record.last_run_duration) if record.last_run_duration else None,
        "last_result": last_result,
        "priority_group_id": record.priority_group_id,
        "run_logs": run_logs,
        "timezone": record.timezone
    }


def scheduler_jobs_read(logger, params, related_groups):
    try:
        logger.info('============ SCHEDULER_JOBS_READ ============')
        logger.debug(f'params: {params}')

        filter_params = [
            'page_number',
            'page_size',
            'last_start_date_from',
            'last_start_date_to',
            'text_search',
            'last_result'
        ]
        filter_values = {}
        for value in filter_params:
            filter_values[value] = params.get(value)
            params.pop(value, None)

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerJobs, params, scheduler_jobs_mapping)

        logger.debug(f'original len: {len(result['data'])}')
        __filter_data(logger, filter_values, result)

        result['data'].sort(key=lambda x: x['job_name'].lower(), reverse=False)
        __filter_by_group(logger, related_groups, result)
        result['data'] = common_func.paginate_data(result['data'], filter_values.get('page_number'),
                                                   filter_values.get('page_size'))

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = {
            'success': False,
            'error_msg': str(e.args),
            'data': []
        }

    return result


def scheduler_jobs_create(logger, params):
    try:
        logger.info('============ SCHEDULER_JOBS_CREATE ============')
        logger.debug(f'params: {params}')

        check_system, check_user = __validate_jobs_create_input(logger, params)

        job_id = str(uuid.uuid4())
        user_name = check_user.get('user_name')
        if params.get('auto_drop') is not None:
            auto_drop = params.get('auto_drop')
            params['max_run'] = params.get('max_run') if not auto_drop else 1
        params['job_id'] = job_id
        params['user_name'] = user_name

        celery_client = CeleryClient(logger)
        response = celery_client.celery_create_jobs({**params, 'queue_name': check_system.get('queue_name')})
        response_data = json.loads(response.data)

        params['next_run_date'] = response_data.get('next_run_date')
        params['job_body'] = json.dumps(params.get('job_body'))
        postgres_client = PostgresClient(logger)
        postgres_client.create_record(SchedulerJobs, params)

        __create_log_record(logger, job_id, user_name, 'CREATE')

        error_msg = None

    except Exception as e:
        logger.error(f'Exception: {e}')
        error_msg = str(e.args)

    return {
        'success': True if not error_msg else False,
        'error_msg': error_msg,
        'data': None
    }


def __update_log_record(logger, params, job_obj):
    old_enable_state = job_obj['is_enabled']
    new_enable_state = params['is_enabled']
    if old_enable_state != new_enable_state:
        input_val = {
            'is_enabled': params['is_enabled'],
            'current_state': 'DISABLED' if not params['is_enabled'] else 'READY TO RUN'
        }
        postgres_client = PostgresClient(logger)

        postgres_client.update_record(SchedulerJobs, {'job_id': params['job_id']}, input_val, check_record=False)


def __calculate_next_run_date(input_val, job_id, postgres_client, logger):
    try:
        job_info = postgres_client.get_records(SchedulerJobs, {'job_id': job_id},
                                               scheduler_jobs_mapping)['data']

        job_obj = next(iter(job_info), {})
        if not job_obj:
            logger.error(f'Invalid job: {job_obj}')
            raise Exception(f'Invalid job:')

        start_date = input_val.get('start_date') or job_info.get('start_date')
        repeat_interval = input_val.get('repeat_interval') or job_info.get('repeat_interval')

        next_run_date = common_func.get_next_run_date(start_date,
                                                      repeat_interval,
                                                      job_info.get('run_count'),
                                                      job_info.get('timezone'),
                                                      logger)

        return next_run_date
    except Exception as e:
        logger.error(f'Fail to calculate next_run_date', e)
        return None


def scheduler_jobs_update(logger, params):
    try:
        logger.info('============ SCHEDULER_JOBS_UPDATE ============')
        logger.debug(f'params: {params}')

        postgres_client = PostgresClient(logger)
        job_obj = __check_valid_job(logger, postgres_client, params)
        logger.debug(f'old job obj: {job_obj}')
        __check_valid_input(logger, postgres_client, params)

        celery_update_input = {}
        start_date_changed = False
        for key in params:
            if key in {'job_type', 'job_action', 'job_body', 'system_id'}:
                celery_update_input[key] = params[key]

            elif params[key] != job_obj[key]:
                if key == 'start_date':
                    start_date_changed = True

                celery_update_input[key] = params[key]

        check_system, check_user = __validate_jobs_update_input(logger, {
            **celery_update_input,
            'start_date': params.get('start_date'),
            'start_date_changed': start_date_changed,
            'last_reg_user_id': params.get('last_reg_user_id')
        })

        celery_update_input.update({
            'job_id': params['job_id'],
            'user_name': check_user.get('user_name'),
            'queue_name': str(check_system.get('queue_name'))
        })

        celery_client = CeleryClient(logger)
        response = celery_client.celery_update_jobs(celery_update_input)
        response_data = json.loads(response.data)

        if response_data.get('status') == 'success':
            updated_tasks = response_data.get('updated_tasks')
            if updated_tasks:  # Check if the list is not empty
                next_run_date = updated_tasks[0].get('req_start_date')  # Access the first element safely
                logger.info(f'next_run_date: {next_run_date}')
                params['next_run_date'] = next_run_date

        params['last_chg_date'] = common_func.get_current_utc_time(in_epoch=True)
        params['job_body'] = json.dumps(params.get('job_body'))

        input_val = {x: params[x] for x in params if x not in ['job_id', 'frst_reg_user_id', 'frst_reg_date']}
        postgres_client.update_record(SchedulerJobs, {'job_id': params['job_id']}, input_val, check_record=False)

        __create_log_record(logger, params['job_id'], check_user.get('user_name'), 'UPDATE')

        __update_log_record(logger, params, job_obj)

        error_msg = None

    except Exception as e:
        logger.error(f'Exception: {e}')
        error_msg = str(e.args)

    return {
        'success': True if not error_msg else False,
        'error_msg': error_msg,
        'data': None
    }


def scheduler_jobs_update_workflow(logger, params):
    try:
        logger.info('============ SCHEDULER_JOBS_UPDATE ============')
        logger.debug(f'params: {params}')

        postgres_client = PostgresClient(logger)
        job_obj = __check_valid_job(logger, postgres_client, params)
        logger.debug(f'old job obj: {job_obj}')
        __check_valid_input(logger, postgres_client, params)
        params['last_chg_date'] = common_func.get_current_utc_time(in_epoch=True)

        input_val = {x: params[x] for x in params if x not in ['job_id']}
        result = postgres_client.update_record(SchedulerJobs, {'job_id': params['job_id']}, input_val,
                                               check_record=False)
        # TODO: Update to batch-celery

    except Exception as e:
        logger.error(f'Exception: {e}')
        message = str(e.args)
        result = {
            'success': False,
            'error_msg': message,
            'data': None
        }

    return result


def scheduler_delete_workflow(logger, workflow_id):
    """
    Deletes a workflow from the scheduler, updating related records.

    :param logger: Logger object for logging messages.
    :param workflow_id: ID of the workflow to delete.
    :return: Result of the operation, or error details in case of failure.
    """
    logger.info('============ SCHEDULER_JOBS_DELETE_WORKFLOW ============')
    logger.debug(f'workflow_id: {workflow_id}')

    try:
        postgres_client = PostgresClient(logger)

        # Fetch workflow priority groups related to the workflow ID
        workflow_priority_groups_detail = postgres_client.get_records(
            SchedulerWorkflowPriorityGroup,
            {'workflow_id': workflow_id},
            scheduler_workflow_priority_group_mapping
        )

        workflow_priority_groups = workflow_priority_groups_detail['data']

        if not workflow_priority_groups:
            logger.warning(f"No priority groups found for workflow_id: {workflow_id}")
            return {"status": "No records found", "workflow_id": workflow_id}

        logger.info(f'workflow_priority_groups: {workflow_priority_groups}')

        # Iterate over priority groups and update related SchedulerJobs records
        for group in workflow_priority_groups:
            input_val = {
                'last_chg_date': common_func.get_current_utc_time(in_epoch=True),
                'priority': None,
                'priority_group_id': None,
                'workflow_id': None,
            }
            postgres_client.update_record(
                SchedulerJobs,
                {'priority_group_id': group['priority_group_id']},
                input_val,
                check_record=False
            )
            logger.debug(f"Updated SchedulerJobs for priority_group_id: {group['priority_group_id']}")

        postgres_client.delete_record(SchedulerWorkflowPriorityGroup, {'workflow_id': workflow_id})

        logger.info(f"Successfully deleted workflow_id: {workflow_id}")
        return {
            'success': True,
            'error_msg': None,
            'data': None
        }

    except Exception as e:
        logger.error(f"Exception occurred while deleting workflow_id: {workflow_id}, Exception: {e}")
        message = str(e.args)
        result = {
            'success': False,
            'error_msg': message,
            'data': None
        }
        return result


def scheduler_jobs_update_data(logger, params):
    try:
        logger.info('============ SCHEDULER_JOBS_UPDATE_DATA ============')
        logger.debug(f'params: {params}')

        filter_params = {'job_id': params['job_id']}
        params['last_chg_date'] = common_func.get_current_utc_time(in_epoch=True)
        postgres_client = PostgresClient(logger)
        postgres_client.update_record(SchedulerJobs, filter_params, params)

        # todo enhancement by using socket with FE for realtime data
        # socket_client = SocketClient(logger)
        # socket_client.socket_send('Job updated data', params)

        error_msg = None

    except Exception as e:
        logger.error(f'Exception: {e}')
        error_msg = str(e.args)

    return {
        'success': True if not error_msg else False,
        'error_msg': error_msg,
        'data': None
    }


def __check_valid_input(logger, postgres_client, params):
    job_info = postgres_client.get_records(SchedulerJobs, {'job_id': params.get('job_id')},
                                           scheduler_jobs_mapping)['data']
    job_obj = next(iter(job_info), {})
    fields_to_check = ['start_date', 'end_date', 'repeat_interval', 'max_run']
    logger.info(f'Check valid for job update id={params.get('job_id')} params={params}')

    if job_obj.get('is_enabled') == False and params.get('is_enabled') == False:
        if any(field in params for field in fields_to_check):
            logger.error(f'Job is disable, Please update state to change those settings: {job_obj}')
            raise Exception('Job is disable, Please update state to change those settings')

    return job_obj


def update_job_status(logger, params):
    try:
        logger.info('============ UPDATE_JOB_STATUS ============')
        logger.debug(f'params: {params}')

        check_user = scheduler_users.scheduler_users_read(logger, {'id': params['last_reg_user_id']})['data']
        check_user = next(iter(check_user), {})
        if not check_user:
            raise Exception('Invalid user')

        postgres_client = PostgresClient(logger)
        __check_valid_job(logger, postgres_client, params)

        celery_client = CeleryClient(logger)
        celery_client.celery_update_job_status({'job_id': params.get('job_id'), 'status': params.get('is_enabled')})

        params['last_chg_date'] = common_func.get_current_utc_time(in_epoch=True)
        input_val = {x: params[x] for x in params if x not in ['job_id']}
        if not params.get('is_enabled'):
            input_val['current_state'] = 'DISABLED'
        postgres_client.update_record(SchedulerJobs, {'job_id': params['job_id']}, input_val,
                                      check_record=False)

        operation = 'ENABLED' if params.get('is_enabled') else 'DISABLED'
        __create_log_record(logger, params['job_id'], check_user.get('user_name'), operation)

        error_msg = None

    except Exception as e:
        logger.error(f'Exception: {e}')
        error_msg = str(e.args)

    return {
        'success': True if not error_msg else False,
        'error_msg': error_msg,
        'data': None
    }


def get_repeat_interval_sample(logger, params):
    try:
        logger.info('============ GET_REPEAT_INTERVAL_SAMPLE ============')
        logger.debug(f'params: {params}')

        repeat_interval = params.get('repeat_interval') or ''
        start_date = params.get('start_date') or ''
        have_limit = ('UNTIL' in repeat_interval) or ('COUNT' in repeat_interval)
        timezone = params.get('timezone') or ''

        if not repeat_interval:
            return 'Missing repeat interval'

        if not timezone:
            return 'Missing timezone'

        if not common_func.validate_rrule(repeat_interval):
            return 'Invalid repeat interval'

        repeat_interval_split = repeat_interval.split(';')
        repeat_interval_split = [i for i in repeat_interval_split if i and i != '\u200b']
        if not have_limit:
            repeat_interval_split.append('COUNT=30')
        repeat_interval_value = ';'.join(repeat_interval_split)

        if start_date:
            dt_start = common_func.convert_date_str_to_format_with_timezone(str(start_date), RRULE_DATE_FORMAT,
                                                                            timezone)
            if not dt_start:
                return {
                    'success': False,
                    'error_msg': 'Invalid start date',
                    'data': None
                }
            repeat_interval_sample = list(rrulestr(repeat_interval_value, dtstart=parse(dt_start)))
        else:
            repeat_interval_sample = list(rrulestr(repeat_interval_value))

        repeat_interval_sample = [
            x.strftime(constants.DATETIME_FORMAT).replace(' ', ' 오후 ') for x in repeat_interval_sample
        ]

        result = {
            'success': True,
            'error_msg': None,
            'data': repeat_interval_sample
        }

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def manually_run(logger, params):
    try:
        logger.info('============ MANUALLY_RUN ============')
        logger.debug(f'params: {params}')

        postgres_client = PostgresClient(logger)
        job_obj = __check_valid_job(logger, postgres_client, params)

        check_user = scheduler_users.scheduler_users_read(logger, {'id': params['user_id']})['data']
        check_user = next(iter(check_user), {})
        if not check_user:
            raise Exception('Invalid user')

        celery_client = CeleryClient(logger)
        celery_client.celery_manually_run({
            "job_id": job_obj.get('job_id'),
            "job_type": job_obj.get('job_type'),
            "job_action": job_obj.get('job_action'),
            "job_body": job_obj.get('job_body'),
            "queue_name": job_obj.get('queue_name'),
            "user_name": check_user.get('user_name')
        })
        error_msg = None

    except Exception as e:
        logger.error(f'Exception: {e}')
        error_msg = str(e.args)

    return {
        'success': True if not error_msg else False,
        'error_msg': error_msg,
        'data': None
    }


def force_stop(logger, params):
    try:
        logger.info('============ FORCE_STOP ============')
        logger.debug(f'params: {params}')

        postgres_client = PostgresClient(logger)
        params['function_check'] = 'FORCE_STOP'
        __check_valid_job(logger, postgres_client, params)

        celery_client = CeleryClient(logger)
        celery_client.celery_force_stop({"job_id": params.get('job_id')})
        error_msg = None

    except Exception as e:
        logger.error(f'Exception: {e}')
        error_msg = str(e.args)

    return {
        'success': True if not error_msg else False,
        'error_msg': error_msg,
        'data': None
    }


def get_job_filter(logger, params, related_groups):
    try:
        logger.info("============ GET_JOB_FILTER ============")
        logger.debug(f"params: {params}")

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(
            SchedulerJobs, params, scheduler_jobs_mapping
        )

        if len(related_groups) > 0:
            __filter_by_group(logger, related_groups, result)

        filter_list = []
        status_list = set()
        for obj in result["data"]:
            filter_list.append(obj.get("job_name"))

            for log in obj.get("run_logs"):
                status_list.add(log.get("status"))

        filter_list.sort()
        result["data"] = filter_list
        result["status_list"] = list(status_list)

    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    return result


def get_job_detail(logger, params):
    try:
        logger.info('============ GET_JOB_DETAIL ============')
        logger.debug(f'params: {params}')

        postgres_client = PostgresClient(logger)
        job_info = postgres_client.get_records(SchedulerJobs, {'job_id': params.get('job_id')},
                                               scheduler_jobs_mapping)

        if not job_info:
            raise Exception(f'Invalid job_id: {params.get('job_id')}')

        if job_info['data']:
            result = job_info['data'][0]

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


# ================= Support func =================
def __filter_data(logger, filter_values, result):
    logger.debug(f'filter_values: {filter_values}')

    last_start_date_from = filter_values.get('last_start_date_from')
    if last_start_date_from:
        logger.info('============ Filter last_start_date_from')
        result['data'] = list(
            filter(lambda x: x.get('last_start_date')
                             and str(x.get('last_start_date')) >= str(last_start_date_from), result['data']))
        logger.debug(f'filtered last_start_date_from: {len(result['data'])}')

    last_start_date_to = filter_values.get('last_start_date_to')
    if last_start_date_to:
        logger.info('============ Filter last_start_date_to')
        result['data'] = list(
            filter(lambda x: x.get('last_start_date')
                             and str(x.get('last_start_date')) <= str(last_start_date_to), result['data']))
        logger.debug(f'filtered last_start_date_to: {len(result['data'])}')

    last_result = filter_values.get('last_result')
    if last_result:
        logger.info('============ Filter last_result')
        result['data'] = list(
            filter(lambda x: str(x.get('last_result')) == str(last_result), result['data']))
        logger.debug(f'filtered last_result: {len(result['data'])}')

    text_search = filter_values.get('text_search')
    if text_search:
        logger.info('============ Text search')
        logger.debug(f'text_search: {text_search}')
        search_attributes = ['job_name', 'job_comments', 'schedule_str']
        searched_list = common_func.search_obj_by_keywords(result['data'], search_attributes, text_search)
        logger.debug(f'searched len: {len(searched_list)}')
        result['data'] = searched_list


def __validate_jobs_create_input(logger, params):
    logger.debug(f'check params: {params}')
    if not params.get('system_id') or not params.get('group_id'):
        raise Exception('Invalid system or group')

    if params.get('job_type') not in ('REST_API', 'EXECUTABLE'):
        raise Exception('Invalid job type')

    current_time = str(common_func.get_current_epoch_time_millis())
    logger.debug(f'current_time: {current_time}')
    start_date = str(params.get('start_date') or '')
    end_date = str(params.get('end_date') or '')

    if not start_date or start_date < current_time:
        raise Exception('Invalid start date')

    if end_date and start_date > end_date:
        raise Exception('Invalid end date')

    check_group = scheduler_job_groups.scheduler_job_groups_read(logger, {"group_id": params["group_id"]}, [])
    if not check_group['data']:
        raise Exception('Group not existed')

    check_system = scheduler_job_servers.scheduler_job_servers_read(logger, {
        "system_id": params["system_id"]
    })['data']
    check_system = next(iter(check_system), {})
    if not check_system:
        raise Exception('Server not existed')

    check_user = None
    if params.get('frst_reg_user_id'):
        check_user = scheduler_users.scheduler_users_read(logger, {'id': params['frst_reg_user_id']})['data']
        check_user = next(iter(check_user), {})

    if not check_user:
        raise Exception('Invalid user')

    return check_system, check_user


def __validate_jobs_update_input(logger, params):
    logger.debug(f'check params: {params}')

    if params.get('job_type') not in ('REST_API', 'EXECUTABLE'):
        raise Exception('Invalid job type')

    check_user = scheduler_users.scheduler_users_read(logger, {'id': params['last_reg_user_id']})['data']
    check_user = next(iter(check_user), {})

    if not check_user:
        raise Exception('Invalid user')

    if params.get('group_id'):
        check_group = scheduler_job_groups.scheduler_job_groups_read(logger, {"group_id": params["group_id"]}, [])
        if not check_group['data']:
            raise Exception('Group not existed')

    check_system = scheduler_job_servers.scheduler_job_servers_read(logger, {
        "system_id": params["system_id"]
    })['data']
    check_system = next(iter(check_system), {})
    if not check_system:
        raise Exception('Server not existed')

    current_time = str(common_func.get_current_epoch_time_millis())
    logger.debug(f'current_time: {current_time}')
    start_date = str(params.get('start_date') or '')
    end_date = str(params.get('end_date') or '')
    start_date_changed = params.get('start_date_changed')
    logger.debug(f'start_date_changed: {start_date_changed}')

    if start_date_changed:
        if not start_date or start_date < current_time:
            raise Exception('Invalid start date')

    if end_date:
        if start_date > end_date:
            raise Exception('Invalid end date')

    return check_system, check_user


def __check_valid_job(logger, postgres_client, params):
    function_check = params.get('function_check', None)
    job_info = postgres_client.get_records(SchedulerJobs, {'job_id': params.get('job_id')},
                                           scheduler_jobs_mapping)['data']
    job_obj = next(iter(job_info), {})
    if not job_obj:
        logger.error(f'Invalid job: {job_obj}')
        raise Exception(f'Invalid job:')

    if function_check != 'FORCE_STOP' and job_obj.get('current_state') in {'DELETED', 'RUNNING'}:
        logger.error(f'Invalid job state: {job_obj} due to job in state: {job_obj.get('current_state')}')
        raise Exception(f'Cant update job due to job in state: {job_obj.get('current_state')}')

    return job_obj


def __create_log_record(logger, job_id, user_name, operation):
    scheduler_job_run_logs.scheduler_job_run_logs_create(logger, {
        "job_id": job_id,
        'celery_task_name': None,
        'operation': operation,
        'status': None,
        'user_name': user_name,
        'error_no': 0,
        'req_start_date': None,
        'actual_start_date': None,
        'run_duration': None,
        'additional_info': None,
        'errors': None,
        'output': None
    })


def __filter_by_group(logger, filter_group_list, result):
    # Check if filter_group_list is valid (not None, not empty, and not [])
    if filter_group_list is not None and bool(filter_group_list):
        job_list = result['data']
        logger.debug(f'filter_job_list: {filter_group_list}')
        filtered_jobs = []

        for job in job_list:
            if job['group_id'] in filter_group_list:
                # The user has a related_group and group_id matches
                filtered_jobs.append(job)
            else:
                logger.debug(f"Removing job: {job['group_id']}")

        result['data'] = filtered_jobs
        result['total'] = len(result['data'])
