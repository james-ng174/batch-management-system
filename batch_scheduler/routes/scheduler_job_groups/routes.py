import json
from flask import request, g
from routes.scheduler_job_servers import blueprint
from logger import get_logger
from logic import scheduler_job_groups
from utils import common_func
from decorators.token_validate import token_required
from decorators.update_session import update_session


@blueprint.route('/group/all', methods=['GET'])
@token_required
@update_session
def group_list_all():
    logger = get_logger()
    related_groups = g.user.get('related_group')
    result = scheduler_job_groups.scheduler_job_groups_read(logger, {}, related_groups)

    final_data = common_func.format_api_output(logger, result)

    logger.info('group_list_all end')
    return final_data


@blueprint.route('/group/filter', methods=['POST'])
@token_required
@update_session
def group_list_filter():
    logger = get_logger()
    params = json.loads(request.data)
    related_groups = g.user.get('related_group')
    result = scheduler_job_groups.scheduler_job_groups_read(logger, params, related_groups)

    final_data = common_func.format_api_output(logger, result)

    logger.info('group_list_filter end')
    return final_data


@blueprint.route('/group/create', methods=['POST'])
@token_required
@update_session
def group_create():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_job_groups.scheduler_job_groups_create(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('group_create end')
    return final_data


@blueprint.route('/group/update', methods=['POST'])
@token_required
@update_session
def group_update():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_job_groups.scheduler_job_groups_update(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('group_update end')
    return final_data


@blueprint.route('/group/getFilter', methods=['GET'])
@token_required
@update_session
def get_group_filter():
    logger = get_logger()
    related_groups = g.user.get('related_group')
    result = scheduler_job_groups.get_group_filter(logger, {}, related_groups)

    final_data = common_func.format_api_output(logger, result)

    logger.info('get_group_filter end')
    return final_data


@blueprint.route("/group/<group_id>", methods=["GET"])
@token_required
@update_session
def get_group_detail(group_id):
    logger = get_logger()
    result = scheduler_job_groups.get_group_detail(logger, {"group_id": group_id})

    final_data = common_func.format_api_output(logger, result)

    logger.info("get_group_filter end")
    return final_data
