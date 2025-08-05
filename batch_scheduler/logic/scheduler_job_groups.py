from utils.postgres_helper import PostgresClient
from utils import common_func
from models.scheduler_job_groups import SchedulerJobGroups
from models.scheduler_related_group_user import SchedulerRelatedGroupUser
from logic import scheduler_users


def scheduler_job_groups_mapping(record):
    return {
        "group_id": record.group_id,
        "group_name": record.group_name,
        "group_comments": record.group_comments,
        "frst_reg_date": record.frst_reg_date,
        "frst_reg_user_id": record.frst_reg_user_id,
        "last_chg_date": record.last_chg_date,
        "last_reg_user_id": record.last_reg_user_id,
        **__get_record_extend_data(record)
    }


def scheduler_job_groups_read(logger, params, related_groups):
    try:
        logger.info('============ SCHEDULER_JOB_GROUPS_READ ============')
        logger.debug(f'params: {params}')
        page_number = params.get('page_number')
        page_size = params.get('page_size')
        params.pop('page_number', None)
        params.pop('page_size', None)

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerJobGroups, params, scheduler_job_groups_mapping)
        result['total'] = len(result['data'])
        result['data'] = common_func.paginate_data(result['data'], page_number, page_size)
        __filter_by_group(logger, related_groups, result)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_job_groups_create(logger, params):
    try:
        logger.info('============ SCHEDULER_JOB_GROUPS_CREATE ============')
        logger.debug(f'params: {params}')

        check_user = scheduler_users.scheduler_users_read(logger, {'id': params['frst_reg_user_id']})['data']
        if not check_user:
            raise Exception('Invalid user')

        postgres_client = PostgresClient(logger)
        result = postgres_client.create_record(SchedulerJobGroups, params)
        if result.get('success'):
            new_group = postgres_client.get_records(SchedulerJobGroups, {'group_name': params['group_name']},
                                                    scheduler_job_groups_mapping).get('data') or []

            logger.info(f'new group: {new_group}')
            group = next(iter(new_group), {})
            if not group:
                raise Exception('Group not existed')

            related_group_params = {
                'user_id': params['frst_reg_user_id'],
                'group_id': group['group_id']
            }

            postgres_client.create_record(SchedulerRelatedGroupUser, related_group_params)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_job_groups_update(logger, params):
    try:
        logger.info('============ SCHEDULER_JOB_GROUPS_UPDATE ============')
        logger.debug(f'params: {params}')

        check_user = scheduler_users.scheduler_users_read(logger, {'id': params['last_reg_user_id']})['data']
        if not check_user:
            raise Exception('Invalid user')

        filter_params = {'group_id': params['group_id']}
        params['last_chg_date'] = common_func.get_current_utc_time(in_epoch=True)
        input_val = {
            x: params[x] for x in params if x in ['group_name', 'group_comments', 'last_chg_date', 'last_reg_user_id']
        }

        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(SchedulerJobGroups, filter_params, input_val)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def get_group_filter(logger, params, related_groups):
    try:
        logger.info('============ GET_GROUP_FILTER ============')
        logger.debug(f'params: {params}')

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerJobGroups, params, scheduler_job_groups_mapping)
        __filter_by_group(logger, related_groups, result)

        filter_list = []
        for obj in result['data']:
            filter_list.append({
                'id': obj.get('group_id'),
                'name': obj.get('group_name'),
                'related': obj.get('related')
            })

        filter_list.sort(key=lambda x: (x.get('name')))
        result['data'] = filter_list

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def get_group_detail(logger, params):
    try:
        logger.info("============ GET_GROUP_DETAIL ============")
        logger.debug(f"params: {params}")

        postgres_client = PostgresClient(logger)

        db_data = (
            postgres_client.get_records(
                SchedulerJobGroups,
                {"group_id": params["group_id"]},
                scheduler_job_groups_mapping,
            ).get("data")
            or []
        )

        group = next(iter(db_data), {})
        if not group:
            raise Exception(f"Group has id '{params["group_id"]}' not existed")
        
        return {
            'success': True,
            'error_msg': None,
            'data': group
        }
    
    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    return result


# ================= Support func =================
def __get_record_extend_data(record):
    jobs = []
    for job in record.jobs:
        jobs.append(
            {
                "job_id": job.job_id,
                "current_state": job.current_state,
                "name": job.job_name,
                "server_name": job.server.system_name,
                "priority_group_id": job.priority_group_id,
                "frst_reg_date": job.frst_reg_date,
                "workflow_id": job.workflow_id,
            }
        )

    return {"jobs": jobs}


def __filter_by_group(logger, filter_group_list, result):
    group_list = result['data']
    logger.debug(f'filter_group_list: {filter_group_list}')
    filtered_groups = []
    if filter_group_list is not None and bool(filter_group_list):
        for group in group_list:
            if group['group_id'] in filter_group_list:
                group['related'] = True
                # The user has a related_group and group_id matches
            else:
                group['related'] = False
                logger.debug(f"Removing group: {group['group_id']}")
            filtered_groups.append(group)

        result['data'] = filtered_groups
