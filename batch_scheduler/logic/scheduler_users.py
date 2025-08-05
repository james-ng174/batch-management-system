import jwt
from utils.postgres_helper import PostgresClient
from utils import common_func, validate_helper, secret_helper
from models.scheduler_users import SchedulerUsers
from models.scheduler_related_group_user import SchedulerRelatedGroupUser
from config import Config

HOUR = 60 * 60


def scheduler_users_mapping(record):
    return {
        "id": record.id,
        "user_id": record.user_id,
        "user_name": record.user_name,
        "celp_tlno": secret_helper.decrypt_data(record.celp_tlno),
        "email_addr": secret_helper.decrypt_data(record.email_addr),
        "frst_reg_date": record.frst_reg_date,
        "last_chg_date": record.last_chg_date,
        "user_type": record.user_type,
        "user_status": record.user_status,
        "last_pwd_chg_date": record.last_pwd_chg_date,
        "last_lgin_timr": record.last_lgin_timr,
        "lgin_fail_ncnt": record.lgin_fail_ncnt,
    }


def login_users_mapping(record):
    return {
        "id": record.id,
        "user_id": record.user_id,
        "user_name": record.user_name,
        "frst_reg_date": record.frst_reg_date,
        "last_chg_date": record.last_chg_date,
        "user_type": record.user_type,
        "user_pwd": record.user_pwd,
        "user_status": record.user_status,
        "last_pwd_chg_date": record.last_pwd_chg_date,
        "lgin_fail_ncnt": record.lgin_fail_ncnt,
    }


def related_group_users_mapping(record):
    return {
        "id": record.id,
        "user_id": record.user_id,
        "group_id": record.group_id,
    }


def users_read(logger, params):
    try:
        logger.info(f'============ LOGIN_USERS_READ ============')
        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerUsers, params, login_users_mapping)
        logger.info(f'result: {result}')

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_users_read(logger, params):
    try:
        logger.info('============ SCHEDULER_USERS_READ ============')
        logger.debug(f'params: {params}')
        filter_group_id = params.get('group_id')
        if filter_group_id:
            params.pop('group_id', None)

        filter_params = [
            'page_number',
            'page_size',
            'text_search'
        ]
        filter_values = {}
        for value in filter_params:
            filter_values[value] = params.get(value)
            params.pop(value, None)

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerUsers, params, scheduler_users_mapping)

        logger.debug(f'original len: {len(result['data'])}')
        __filter_data(logger, filter_values, result)
        user_list = result['data']

        for i in range(0, len(user_list)):
            group_params = {'user_id': user_list[i]['id']}

            related_group = postgres_client.get_records(SchedulerRelatedGroupUser, group_params,
                                                        related_group_users_mapping).get('data') or []
            # Map the group_id values to related_scheduler_group for each user
            user_list[i]['related_scheduler_group'] = [group['group_id'] for group in related_group]

        if filter_group_id:
            __filter_group(logger, filter_group_id, result)

        result['total'] = len(result['data'])
        result['data'] = common_func.paginate_data(result['data'], filter_values.get('page_number'),
                                                   filter_values.get('page_size'))

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_users_create(logger, params):
    try:
        logger.info('============ SCHEDULER_USERS_CREATE ============')
        logger.debug(f'params: {params}')
        postgres_client = PostgresClient(logger)
        password = params['password']
        valid, message = validate_helper.validate_password(password)
        if not valid:
            return {
                'success': False,
                'error_msg': message,
                'data': None
            }
        user_id = params['user_id']
        valid_id, message_id = validate_helper.validate_user_id(user_id)
        if not valid_id:
            return {
                'success': False,
                'error_msg': message_id,
                'data': None
            }

        params['user_pwd'] = common_func.sha256_hash(password)
        params['last_pwd_chg_date'] = common_func.get_current_epoch_time_seconds()

        # Get group_list which is an array of group_id
        group_list = params.get('related_scheduler_group', [])

        result = postgres_client.create_record(SchedulerUsers, params)

        # If user creation was successful, create related group user entries
        if result.get('success') and group_list:
            new_user = users_read(logger, {'user_id': params['user_id']}).get('data') or []
            user = next(iter(new_user), {})
            if not user:
                raise Exception('User not existed')

            user_id = user['id']
            for group_id in group_list:
                related_group_user = {
                    'group_id': group_id,
                    'user_id': user_id
                }
                # Create the related SchedulerRelatedGroupUser entry
                postgres_client.create_record(SchedulerRelatedGroupUser, related_group_user)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_users_update(logger, params):
    try:
        logger.info('============ SCHEDULER_USERS_UPDATE ============')
        logger.debug(f'params: {params}')

        filter_params = {'id': params['id']}
        params['last_chg_date'] = common_func.get_current_utc_time(in_epoch=True)
        params['celp_tlno'] = secret_helper.encrypt_data(params.get('celp_tlno'))
        params['email_addr'] = secret_helper.encrypt_data(params.get('email_addr'))
        input_val = {
            x: params[x] for x in params if x in ['user_name', 'last_chg_date', 'celp_tlno', 'email_addr', 'user_type']
        }

        postgres_client = PostgresClient(logger)
        related_groups = params['related_scheduler_group']
        delete_params = {
            'user_id': params['id'],
        }
        postgres_client.delete_record(SchedulerRelatedGroupUser, delete_params)
        for group in related_groups:
            new_groups = {
                'user_id': params['id'],
                'group_id': group
            }
            postgres_client.create_record(SchedulerRelatedGroupUser, new_groups)
        result = postgres_client.update_record(SchedulerUsers, filter_params, input_val)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_user_get_info(logger, user_id):
    try:
        logger.info('============ SCHEDULER_USERS_GET_INFO ============')
        logger.debug(f'user_id: {user_id}')
        filter_params = {'user_id': user_id}
        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(SchedulerUsers, filter_params, scheduler_users_mapping)
        logger.info(f'result: {result}')

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_users_reset_account(logger, user_id, params):
    try:
        logger.info('============ SCHEDULER_USERS_RESET_ACCOUNT ============')
        logger.debug(f'params: {params}')
        filter_params = {'user_id': user_id}
        user_list = users_read(logger, filter_params).get('data') or []
        user = next(iter(user_list), {})
        if not user:
            raise Exception('User not existed')

        target_params = {'user_id': params['target_user_id']}

        target_user_list = users_read(logger, target_params).get('data') or []
        target_user = next(iter(target_user_list), {})
        if not target_user:
            raise Exception('Target Reset User not existed')

        logger.debug(f'target_user  {target_user}')

        input_val = {
            'user_pwd': common_func.reset_pass(),
            'last_pwd_chg_date': common_func.get_current_epoch_time_seconds(),
            'last_chg_date': common_func.get_current_epoch_time_seconds(),
            'lgin_fail_ncnt': 0
        }

        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(SchedulerUsers, target_params, input_val)
    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def check_password_lock(user, logger):
    wrong_pass_count = user['lgin_fail_ncnt']
    if wrong_pass_count + 1 > 5:
        input_val = {
            'user_status': False,
        }

        filter_params = {'id': user['id']}

        postgres_client = PostgresClient(logger)
        postgres_client.update_record(SchedulerUsers, filter_params, input_val)
        return 'Your account has been locked due to invalid password over 5 times'

    input_val = {
        'lgin_fail_ncnt': wrong_pass_count + 1,
    }

    filter_params = {'id': user['id']}

    postgres_client = PostgresClient(logger)
    postgres_client.update_record(SchedulerUsers, filter_params, input_val)
    return 'Invalid password'


def scheduler_users_login(logger, params):
    try:
        user_id = params.get('user_id')
        password = params.get('password')
        if not user_id or not password:
            raise Exception('Missing input')

        password = common_func.sha256_hash(password)
        # Read the user list based on the user_name
        user_list = users_read(logger, {'user_id': user_id}).get('data') or []
        user = next(iter(user_list), {})
        if not user:
            raise Exception('User not existed')

        if not user['user_status']:
            raise Exception('Account deactivated, Please contact to Administrators to unlock')

        # If the user exists but the password does not match, raise an exception
        if user['user_pwd'] != password:
            message = check_password_lock(user, logger)
            raise Exception(message)

        expired_date = user['last_pwd_chg_date'] + 90 * 24 * 60 * 60
        if expired_date < common_func.get_current_epoch_time_seconds():
            raise Exception('Password is expired, Please update new password')

        secret_key = common_func.read_secret_key(Config.SECRET_KEY_PATH)

        group_params = {'user_id': user['id']}
        postgres_client = PostgresClient(logger)
        related_group = postgres_client.get_records(SchedulerRelatedGroupUser, group_params,
                                                    related_group_users_mapping).get('data') or []
        # Map the group_id values to related_scheduler_group for each user
        user['related_scheduler_group'] = [group['group_id'] for group in related_group]

        # token should expire after 24 hrs
        token = jwt.encode(
            {"id": user["id"], "user_id": user["user_id"], "user_type": user["user_type"],
             "user_name": user["user_name"], 'related_group': user['related_scheduler_group']},
            secret_key, algorithm="HS256")

        filter_params = {'user_id': user['user_id']}
        input_val = {
            'last_activity_time': common_func.get_current_epoch_time_seconds(),
            'last_lgin_timr': common_func.get_current_epoch_time_seconds(),
            'lgin_fail_ncnt': 0,
        }

        postgres_client = PostgresClient(logger)
        postgres_client.update_record(SchedulerUsers, filter_params, input_val)

        result = {
            'success': True,
            'error_msg': None,
            'data': {
                'token': token
            }
        }

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_users_update_status(logger, params):
    try:
        logger.info('============ SCHEDULER_USERS_UPDATE_STATUS ============')
        logger.debug(f'params: {params}')

        filter_params = {'user_id': params['target_user_id']}
        input_val = {
            'user_status': params['user_status'],
            'last_chg_date': common_func.get_current_epoch_time_seconds(),
        }

        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(SchedulerUsers, filter_params, input_val)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def scheduler_users_update_password(logger, params):
    try:
        logger.info('============ SCHEDULER_USERS_UPDATE_STATUS ============')
        logger.debug(f'params: {params}')
        user_id = params['user_id']
        old_password = params['old_pwd']
        if not user_id or not old_password:
            raise Exception('Missing input')
        password = params['user_pwd']

        if password == old_password:
            raise Exception('New password can not be the same with old password, Please choose another password')

        old_pwd = common_func.sha256_hash(old_password)

        user_list = users_read(logger, {'user_id': user_id}).get('data') or []
        user = next(iter(user_list), {})
        if not user:
            raise Exception('User not existed')

        expired_date = user['last_pwd_chg_date'] + 90 * 24 * 60 * 60
        if expired_date > common_func.get_current_epoch_time_seconds():
            raise Exception('Password is not expired')

        if not user['user_status']:
            raise Exception('Account deactivated, Please contact to Administrators to unlock')

        # If the user exists but the password does not match, raise an exception
        if user['user_pwd'] != old_pwd:
            raise Exception('Wrong old password, Please check again')

        filter_params = {'user_id': user_id}

        valid, message = validate_helper.validate_password(password)
        if not valid:
            return {
                'success': False,
                'error_msg': message,
                'data': None
            }

        input_val = {
            'user_pwd': common_func.sha256_hash(password),
            'last_chg_date': common_func.get_current_epoch_time_seconds(),
            'last_pwd_chg_date': common_func.get_current_epoch_time_seconds(),
        }

        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(SchedulerUsers, filter_params, input_val)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


# ================= Support func =================
def __filter_data(logger, filter_values, result):
    logger.debug(f'filter_values: {filter_values}')

    text_search = filter_values.get('text_search')
    if text_search:
        logger.info('============ Text search')
        logger.debug(f'text_search: {text_search}')
        search_attributes = ['user_name', 'email_addr']
        searched_list = common_func.search_obj_by_keywords(result['data'], search_attributes, text_search)
        logger.debug(f'searched len: {len(searched_list)}')
        result['data'] = searched_list


def __filter_group(logger, group_id, result):
    user_list = result['data']
    logger.debug(f'group_id: {group_id}')
    filtered_users = []

    for user in user_list:
        if group_id in user['related_scheduler_group']:
            # The user has a related_group and group_id matches
            filtered_users.append(user)
        else:
            logger.debug(f"Removing user: {user['user_name']} (no match for group_id: {group_id})")

    result['data'] = filtered_users
