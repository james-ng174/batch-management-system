import json
from flask import request
from routes.scheduler_job_servers import blueprint
from logger import get_logger
from logic import scheduler_job_servers
from utils import common_func
from decorators.token_validate import token_required
from decorators.update_session import update_session


@blueprint.route('/server/all', methods=['GET'])
@token_required
@update_session
def server_list_all():
    logger = get_logger()
    result = scheduler_job_servers.scheduler_job_servers_read(logger, {})

    final_data = common_func.format_api_output(logger, result)

    logger.info('server_list_all end')
    return final_data


@blueprint.route('/server/filter', methods=['POST'])
@token_required
@update_session
def server_list_filter():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_job_servers.scheduler_job_servers_read(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('server_list_filter end')
    return final_data


@blueprint.route('/server/create', methods=['POST'])
@token_required
@update_session
def server_create():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_job_servers.scheduler_job_servers_create(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('server_create end')
    return final_data


@blueprint.route('/server/update', methods=['POST'])
@token_required
@update_session
def server_update():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_job_servers.scheduler_job_servers_update(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('server_update end')
    return final_data


@blueprint.route('/server/getFilter', methods=['GET'])
@token_required
@update_session
def get_server_filter():
    logger = get_logger()
    result = scheduler_job_servers.get_server_filter(logger, {})

    final_data = common_func.format_api_output(logger, result)

    logger.info('get_server_filter end')
    return final_data
