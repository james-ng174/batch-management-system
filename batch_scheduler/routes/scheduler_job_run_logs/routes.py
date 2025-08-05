import json
from flask import request, g
from routes.scheduler_job_run_logs import blueprint
from logger import get_logger
from logic import scheduler_job_run_logs
from utils import common_func
from decorators.token_validate import token_required
from decorators.update_session import update_session


@blueprint.route('/logs/all', methods=['GET'])
@token_required
@update_session
def run_logs_list_all():
    logger = get_logger()
    related_groups = g.user.get('related_group')
    result = scheduler_job_run_logs.scheduler_job_run_logs_read(logger, {}, related_groups)

    final_data = common_func.format_api_output(logger, result)

    logger.info('run_logs_list_all end')
    return final_data


@blueprint.route('/logs/filter', methods=['POST'])
@token_required
@update_session
def run_logs_list_filter():
    logger = get_logger()
    params = json.loads(request.data)
    related_groups = g.user.get('related_group')
    result = scheduler_job_run_logs.scheduler_job_run_logs_read(logger, params, related_groups)

    final_data = common_func.format_api_output(logger, result)

    logger.info('run_logs_list_filter end')
    return final_data


@blueprint.route('/logs/create', methods=['POST'])
def run_logs_create():
    logger = get_logger()
    # IMPORTANT this api is used by the celery backend
    params = json.loads(request.data)
    result = scheduler_job_run_logs.scheduler_job_run_logs_create(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('run_logs_create end')
    return final_data


@blueprint.route('/logs/update', methods=['POST'])
def run_logs_update():
    logger = get_logger()
    # IMPORTANT this api is used by the celery backend
    params = json.loads(request.data)
    result = scheduler_job_run_logs.scheduler_job_run_logs_update(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('run_logs_update end')
    return final_data
