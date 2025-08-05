import json
from flask import request, g
from routes.scheduler_job_servers import blueprint
from logger import get_logger
from logic import scheduler_workflow
from utils import common_func
from decorators.token_validate import token_required, admin_required
from decorators.update_session import update_session


@blueprint.route('/workflow/all', methods=['GET'])
@token_required
@update_session
def workflow_list_all():
    logger = get_logger()
    user_type = g.user.get('user_type')
    if user_type == 1:
        related_groups = g.user.get('related_group')
    else: 
        related_groups = None
    result = scheduler_workflow.scheduler_workflow_read(logger, {}, related_groups)
    final_data = common_func.format_api_output(logger, result)

    logger.info('workflow_list_all end')
    return final_data

@blueprint.route('/workflow/create', methods=['POST'])
@token_required
@update_session
def workflow_create():
    logger = get_logger()
    params = json.loads(request.data)
    user_table_id = g.user.get('table_id')
    result = scheduler_workflow.scheduler_workflow_create(logger, params, user_table_id)

    final_data = common_func.format_api_output(logger, result)

    logger.info('workflow_create end')
    return final_data


@blueprint.route('/workflow/update', methods=['POST'])
@token_required
@update_session
def workflow_update():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_workflow.scheduler_workflow_update(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('workflow_update end')
    return final_data

@blueprint.route('/workflow/assign-job', methods=['POST'])
@token_required
@admin_required
@update_session
def workflow_assign_job():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_workflow.assign_job_to_workflow(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('workflow_assign_job end')
    return final_data

@blueprint.route('/workflow/delete', methods=['POST'])
@token_required
@admin_required
@update_session
def workflow_delete():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_workflow.delete_workflow_and_update_jobs(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('workflow_delete end')
    return final_data

@blueprint.route('/workflow/detail', methods=['POST'])
@token_required
@update_session
def workflow_detail():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_workflow.get_workflow_detail(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('workflow_detail end')
    return final_data

@blueprint.route('/workflow/update_status', methods=['POST'])
def workflow_update_status():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_workflow.update_workflow_status(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('workflow_update_status end')
    return final_data


@blueprint.route("/workflow/filter", methods=["POST"])
@token_required
@update_session
def workflow_filter():
    logger = get_logger()
    params = json.loads(request.data)
    related_groups = g.user.get("related_group")

    result = scheduler_workflow.get_workflow_filter(logger, params, related_groups)

    final_data = common_func.format_api_output(logger, result)

    logger.info("workflow_filter end")
    return final_data
