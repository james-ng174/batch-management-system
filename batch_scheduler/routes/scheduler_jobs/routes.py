import json
from flask import request, g
from routes.scheduler_job_servers import blueprint
from logger import get_logger
from logic import scheduler_jobs
from utils import common_func
from decorators.token_validate import token_required
from decorators.update_session import update_session


@blueprint.route('/job/all', methods=['GET'])
@token_required
@update_session
def job_list_all():
    logger = get_logger()
    related_groups = g.user.get('related_group')
    result = scheduler_jobs.scheduler_jobs_read(logger, {}, related_groups)
    result['data'] = scheduler_jobs.frontend_mapping_convert(logger, result['data'])

    final_data = common_func.format_api_output(logger, result)

    logger.info('job_list_all end')
    return final_data


@blueprint.route('/job/filter', methods=['POST'])
@token_required
@update_session
def job_list_filter():
    logger = get_logger()
    params = json.loads(request.data)
    related_groups = g.user.get('related_group')
    result = scheduler_jobs.scheduler_jobs_read(logger, params, related_groups)
    result['data'] = scheduler_jobs.frontend_mapping_convert(logger, result['data'])

    final_data = common_func.format_api_output(logger, result)

    logger.info('job_list_filter end')
    return final_data


@blueprint.route('/job/create', methods=['POST'])
@token_required
@update_session
def job_create():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_jobs.scheduler_jobs_create(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('job_create end')
    return final_data


@blueprint.route('/job/update', methods=['POST'])
@token_required
@update_session
def job_update():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_jobs.scheduler_jobs_update(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('job_update end')
    return final_data


@blueprint.route('/job/updateData', methods=['POST'])
def job_update_data():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_jobs.scheduler_jobs_update_data(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('job_update_data end')
    return final_data


@blueprint.route('/job/repeatIntervalSample', methods=['POST'])
@token_required
@update_session
def job_get_repeat_interval_sample():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_jobs.get_repeat_interval_sample(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('job_get_repeat_interval_sample end')
    return final_data


@blueprint.route('/job/manuallyRun', methods=['POST'])
@token_required
@update_session
def manually_run():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_jobs.manually_run(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('manually_run end')
    return final_data


@blueprint.route('/job/forceStop', methods=['POST'])
@token_required
@update_session
def force_stop():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_jobs.force_stop(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('force_stop end')
    return final_data


@blueprint.route('/job/updateJobStatus', methods=['POST'])
@token_required
@update_session
def update_job_status():
    logger = get_logger()
    params = json.loads(request.data)
    result = scheduler_jobs.update_job_status(logger, params)

    final_data = common_func.format_api_output(logger, result)

    logger.info('update_job_status end')
    return final_data


@blueprint.route("/job/getFilter", methods=["POST"])
@token_required
@update_session
def get_job_filter():
    logger = get_logger()
    params = json.loads(request.data)
    related_groups = g.user.get("related_group")
    result = scheduler_jobs.get_job_filter(logger, params, related_groups)

    final_data = common_func.format_api_output(logger, result)

    logger.info("get_job_filter end")
    return final_data
