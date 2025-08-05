import json
from flask import request, g
from routes.scheduler_users import blueprint
from logic import scheduler_users
from logger import get_logger
from utils import common_func
from decorators.token_validate import token_required, admin_required
from decorators.update_session import update_session


@blueprint.route('/user/all', methods=['GET'])
@token_required
@update_session
def user_list_all():
    logger = get_logger()
    result = scheduler_users.scheduler_users_read(logger, {})

    final_data = common_func.format_api_output(logger, result)

    logger.info('user_list_all end')
    return final_data


@blueprint.route('/user/filter', methods=['POST'])
@token_required
@update_session
def user_list_filter():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_users.scheduler_users_read(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('user_list_filter end')
    return final_data


@blueprint.route('/user/create', methods=['POST'])
@token_required
@update_session
def user_create():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_users.scheduler_users_create(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('user_create end')
    return final_data


@blueprint.route('/user/update', methods=['POST'])
@token_required
@update_session
def user_update():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_users.scheduler_users_update(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('user_update end')
    return final_data


@blueprint.route('/user/info', methods=['GET'])
@token_required
@update_session
def user_get_info():
    logger = get_logger()
    user_id = g.user.get('user_id')
    result = scheduler_users.scheduler_user_get_info(logger, user_id)

    final_data = common_func.format_api_output(logger, result)

    logger.info('user_get_info end')
    return final_data


@blueprint.route('/user/login', methods=['POST'])
def user_login():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_users.scheduler_users_login(logger, params)

    final_data = common_func.format_login_output(logger, result)

    logger.info('user_update end')
    return final_data


@blueprint.route('/user/reset', methods=['POST'])
@token_required
@update_session
@admin_required
def user_reset():
    logger = get_logger()
    params = json.loads(request.data)
    user_id = g.user.get('user_id')
    result = scheduler_users.scheduler_users_reset_account(logger, user_id, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('user_reset end')
    return final_data


@blueprint.route('/user/lock', methods=['POST'])
@token_required
@update_session
@admin_required
def user_update_status():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_users.scheduler_users_update_status(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('user_update_status end')
    return final_data


@blueprint.route('/user/change-password', methods=['POST'])
def user_change_password():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_users.scheduler_users_update_password(logger, params)

    final_data = common_func.format_login_output(logger, result)

    logger.info('user_update_status end')
    return final_data
