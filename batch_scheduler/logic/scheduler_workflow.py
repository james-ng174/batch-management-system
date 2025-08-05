from typing import Dict, Any, Optional

from logic import scheduler_users, scheduler_jobs
from models.scheduler_jobs import SchedulerJobs
from models.scheduler_workflow import SchedulerWorkflow
from models.scheduler_workflow_priotiry_group import SchedulerWorkflowPriorityGroup
from utils import common_func
from utils.celery_helper import CeleryClient
from utils.postgres_helper import PostgresClient


def scheduler_workflow_mapping(record):
    return {
        "workflow_id": record.id,
        "workflow_name": record.workflow_name,
        "latest_status": record.latest_status,
        "group_id": record.group_id,
        "frst_reg_date": record.frst_reg_date,
        "frst_reg_user_id": record.frst_reg_user_id,
        "last_chg_date": record.last_chg_date,
        "last_reg_user_id": record.last_reg_user_id
    }


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


def scheduler_job_mapping(record):
    return {
        "job_id": record.job_id,
        "job_name": record.job_name,
        "current_state": record.current_state,
        "priority": record.priority,
        "ignore_result": record.ignore_result,
        "priority_group_id": record.priority_group_id,
        "workflow_id": record.workflow_id,
        "duration": str(record.last_run_duration),
    }


def get_workflow_detail(logger, params):
    try:
        logger.info('============ SCHEDULER_WORKFLOW_DETAIL ============')
        logger.debug(f'params: {params}')

        # Validate workflow_id parameter
        workflow_id = params.get('workflow_id')
        if not workflow_id:
            raise ValueError('Invalid parameter: workflow_id is required.')

        # Connect to the database
        postgres_client = PostgresClient(logger)

        # Fetch workflow details
        workflow_data = postgres_client.get_records(
            SchedulerWorkflow, {'id': workflow_id}, scheduler_workflow_mapping
        ).get('data') or []

        if not workflow_data:
            raise ValueError(f'Workflow with id {workflow_id} does not exist.')

        # Extract the first workflow detail (assuming unique workflow_id)
        workflow_detail = next(iter(workflow_data), None)
        if not workflow_detail:
            raise ValueError(f'Workflow with id {workflow_id} does not exist.')

        # Fetch related priority groups
        workflow_priority_groups = postgres_client.get_records(
            SchedulerWorkflowPriorityGroup, {'workflow_id': workflow_id}, scheduler_workflow_priority_group_mapping
        ).get('data') or []

        if not workflow_priority_groups:
            logger.warning(f'No priority groups found for workflow_id {workflow_id}.')

        related_priority_group = {}

        for group in workflow_priority_groups:
            priority_group_id = group.get('priority_group_id')
            if not priority_group_id:
                logger.warning(f'Missing priority_group_id in group: {group}')
                continue

            # Fetch related jobs for the current priority group
            related_jobs = postgres_client.get_records(
                SchedulerJobs, {'priority_group_id': priority_group_id}, scheduler_job_mapping
            ).get('data') or []

            logger.info(
                f'Related jobs fetched for priority_group_id {priority_group_id}: {len(related_jobs)} jobs found.')

            # Map the priority group ID to its related jobs
            related_priority_group[priority_group_id] = related_jobs

        # Return the final result with workflow detail and related priority groups
        return {
            'success': True,
            'error_msg': None,
            'data': {
                'workflow_detail': workflow_detail,
                'related_priority_group': related_priority_group
            }
        }

    except Exception as e:
        logger.error(f'Exception occurred in get_workflow_detail: {e}')
        return {
            'success': False,
            'error_msg': str(e),
            'data': None
        }


def scheduler_workflow_read(logger, params, related_groups):
    try:
        logger.info("============ SCHEDULER_WORKFLOW_READ ============")
        logger.debug(f"params: {params}")
        page_number = params.get("page_number")
        page_size = params.get("page_size")
        params.pop("page_number", None)
        params.pop("page_size", None)

        postgres_client = PostgresClient(logger)
        if related_groups is None:
            result = postgres_client.get_records(
                SchedulerWorkflow, params, scheduler_workflow_mapping
            )
            __filter_by_group(logger, related_groups, result)
        else:
            result = (
                postgres_client.get_session()
                .query(SchedulerWorkflow)
                .filter(SchedulerWorkflow.group_id.in_(related_groups))
                .all()
            )
            result = {
                "data": [scheduler_workflow_mapping(r) for r in result],
                "error_msg": None,
                "success": True,
            }
        result["total"] = len(result["data"])
        result["data"] = common_func.paginate_data(
            result["data"], page_number, page_size
        )

    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    return result


def scheduler_workflow_create(logger, params, create_user_id):
    try:
        logger.info('============ SCHEDULER_WORKFLOW_CREATE ============')
        logger.debug(f'params: {params}')

        check_user = scheduler_users.scheduler_users_read(logger, {'id': create_user_id})['data']
        if not check_user:
            raise Exception('Invalid user')

        new_params = {
            **params,
            "frst_reg_user_id": create_user_id,
            "latest_status": "CREATED",
        }

        postgres_client = PostgresClient(logger)
        create_result = postgres_client.create_record(SchedulerWorkflow, new_params)
        if create_result.get('success'):
            record = postgres_client.get_records(SchedulerWorkflow, params, scheduler_workflow_mapping)
            logger.info(f'record: {record}')
            return {
                'success': True,
                'error_msg': None,
                'data': record
            }

    except Exception as e:
        logger.error(f'Exception: {e}')
        return {
            'success': False,
            'error_msg': str(e),
            'data': None
        }


def assign_job_to_workflow(logger, params):
    try:
        logger.info('============ SCHEDULER_WORKFLOW_ASSIGN_JOB ============')
        logger.debug(f'params: {params}')

        if not params.get('workflow_id') or not params.get('list_priority_groups'):
            raise ValueError('Invalid parameters: workflow_id and list_jobs are required.')

        workflow_id = params['workflow_id']
        list_priority_groups = params['list_priority_groups']

        postgres_client = PostgresClient(logger)

        workflow_data = postgres_client.get_records(
            SchedulerWorkflow, {'id': workflow_id}, scheduler_workflow_mapping
        ).get('data') or []

        if not workflow_data:
            raise ValueError(f'Invalid workflow_id: {workflow_id}')

        workflow = next(iter(workflow_data), {})
        if not workflow:
            return {'status': 'error', 'message': 'Invalid workflow'}, 401

        updated_jobs = []
        for group in list_priority_groups:
            list_jobs = group.get('list_jobs')
            priority = group.get('priority')
            ignore_result = group.get('ignore_result', False)

            if not list_jobs or priority is None:
                raise ValueError(f'Invalid job entry: list_jobs and priority are required. Entry: {list_jobs}')

            # Create workflow_priority_group
            result = postgres_client.create_record(SchedulerWorkflowPriorityGroup,
                                                   {'workflow_id': workflow_id, 'priority': priority,
                                                    'ignore_result': ignore_result, 'latest_status': 'CREATED'})

            if result.get('success'):
                new_workflow_priority_group = postgres_client.get_records(SchedulerWorkflowPriorityGroup,
                                                                          {'workflow_id': workflow_id,
                                                                           'priority': priority},
                                                                          scheduler_workflow_priority_group_mapping).get(
                    'data') or []

                logger.info(f'new new_workflow_priority_group: {new_workflow_priority_group}')
                priority_group = next(iter(new_workflow_priority_group), {})
                for job_id in list_jobs:
                    try:
                        job_data = scheduler_jobs.get_job_detail(logger, {'job_id': job_id})
                        if not job_data:
                            raise ValueError(f'Invalid job_id: {job_id}')
                    except Exception as e:
                        raise ValueError(f'Error while retrieving job details for job_id={job_id}: {e}')

                    logger.info(f'=================== job_data: {job_data}')

                    update_values = {'job_id': job_data['job_id'],
                                     'priority_group_id': priority_group['priority_group_id'], 'priority': priority,
                                     'ignore_result': ignore_result, 'workflow_id': workflow_id}

                    record = scheduler_jobs.scheduler_jobs_update_workflow(logger, update_values)
                    if not record.get('success'):
                        return {'status': 'error', 'message': record.get('error_msg')}

                    updated_jobs.append(
                        {'job_id': job_id,
                         'priority_group_id': priority_group['priority_group_id'],
                         'priority': priority,
                         'workflow_id': workflow_id,
                         'ignore_result': ignore_result})

        logger.info(f'Updated jobs: {updated_jobs}')

        # Call to batch celery update worker
        # TODO: Check response before save
        celery_client = CeleryClient(logger)
        response = celery_client.celery_assign_workflow_jobs(params)

        postgres_client.update_record(SchedulerWorkflow, {'id': workflow_id}, {'latest_status': 'ASSIGNED'},
                                      check_record=False)

        return {
            'success': True,
            'error_msg': None,
            'data': updated_jobs
        }

    except Exception as e:
        logger.error(f'Exception: {e}')
        return {
            'success': False,
            'error_msg': str(e),
            'data': None
        }


def scheduler_workflow_update(logger, params):
    result = None
    try:
        logger.info('============ SCHEDULER_WORKFLOW_UPDATE ============')
        logger.debug(f'params: {params}')

        check_user = scheduler_users.scheduler_users_read(logger, {'id': params['last_reg_user_id']})['data']
        if not check_user:
            raise Exception('Invalid user')

        filter_params = {'workflow_id': params['workflow_id']}
        params['last_chg_date'] = common_func.get_current_utc_time(in_epoch=True)
        input_val = {
            x: params[x] for x in params if x in ['workflow_name', 'last_chg_date', 'last_reg_user_id']
        }

        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(SchedulerWorkflow, filter_params, input_val)

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


def delete_workflow_and_update_jobs(logger, params):
    try:
        logger.info('============ SCHEDULER_WORKFLOW_DELETE ============')
        logger.debug(f'params: {params}')

        workflow_id = params.get('workflow_id')

        if not workflow_id:
            raise ValueError('Invalid parameter: workflow_id is required.')

        postgres_client = PostgresClient(logger)

        # Check if workflow exists
        workflow_data = postgres_client.get_records(
            SchedulerWorkflow, {'id': workflow_id}, scheduler_workflow_mapping
        ).get('data') or []

        if not workflow_data:
            raise ValueError(f'Invalid workflow_id: {workflow_id}')

        # Delete the workflow
        postgres_client.delete_record(SchedulerWorkflow, {'id': workflow_id})
        logger.info(f'Workflow with id {workflow_id} has been deleted.')
        scheduler_jobs.scheduler_delete_workflow(logger, workflow_id)
        logger.info(f'Jobs related to workflow_id {workflow_id} have been updated to null.')

        return {
            'success': True,
            'error_msg': None,
            'data': None
        }

    except Exception as e:
        logger.error(f'Exception: {e}')
        return {
            'success': False,
            'error_msg': str(e),
            'data': None
        }


def get_workflow_filter(logger, params, related_groups):
    try:
        logger.info("============ GET_WORKFLOW_FILTER ============")
        logger.debug(f'params: {params}')

        filter_params = [
            'page_number',
            'page_size',
            'text_search',
            'latest_status'
        ]

        filter_values = {}
        for value in filter_params:
            filter_values[value] = params.get(value)
            params.pop(value, None)

        postgres_client = PostgresClient(logger)
        # result = postgres_client.get_records(SchedulerWorkflow, params, scheduler_workflow_mapping)
        result = postgres_client.get_records(
            SchedulerWorkflow, params, scheduler_workflow_mapping
        )
        _filter_workflow = []
        if related_groups is not None and len(related_groups) > 0:
            for workflow in result["data"]:
                if workflow.get("group_id") in related_groups:
                    _filter_workflow.append(workflow)
            result["data"] = _filter_workflow

        logger.debug(f"original len: {len(result['data'])}")
        __filter_data(logger, filter_values, result)
        result["total"] = len(result["data"])
        result["data"].sort(key=lambda x: x["workflow_name"].lower(), reverse=False)
        result['data'] = common_func.paginate_data(result['data'], filter_values.get('page_number'),
                                                   filter_values.get('page_size'))

    except Exception as e:
        logger.error(f'Exception: {e}')
        result = str(e.args)

    return result


# Constants for better readability and maintainability
JOB_STATES_NOT_FINISHED = [
    "RUNNING",
    "WAITING",
]
JOB_WAITING_STATE = "WAITING"


def get_priority_group_id(postgres_client, current_job_id):
    job_info = postgres_client.get_records(SchedulerJobs, {'job_id': current_job_id}, scheduler_job_mapping)

    if not job_info:
        raise Exception(f'Invalid job_id: {current_job_id}')

    if not job_info.get('data'):
        raise Exception(f'Invalid job_id: {current_job_id}')

    job_data = job_info['data'][0]

    return job_data.get('priority_group_id')


def update_workflow_status(logger, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update the workflow status based on the provided parameters.

    Args:
        logger: Logger instance for logging.
        params: Dictionary containing workflow update parameters.

    Returns:
        A dictionary indicating success or failure of the operation.
    """
    logger.info("============ UPDATE_WORKFLOW_STATUS ============")
    logger.debug(f"params: {params}")

    try:
        latest_status = params.get("latest_status")
        workflow_id = params.get("workflow_id")
        current_job_id = params.get("current_job_id")

        postgres_client = PostgresClient(logger)

        priority_group_id = get_priority_group_id(postgres_client, current_job_id)

        # Check for unfinished jobs in workflow
        if latest_status in ("SUCCESS", "FAILED"):
            incomplete_check = check_unfinished_jobs(
                logger, postgres_client, priority_group_id
            )
            if incomplete_check:
                return incomplete_check

        # Handle case when latest_status is None
        if latest_status is None:
            return handle_missing_status(logger)

        # Update the workflow record
        update_workflow_record(
            logger, postgres_client, workflow_id, latest_status
        )

        # Update the current job state, if applicable
        if current_job_id:
            update_job_state(
                logger, postgres_client, current_job_id, latest_status
            )

        return {"success": True, "error_msg": None, "data": None}

    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        return {"success": False, "error_msg": str(e), "data": None}


def check_unfinished_jobs(logger, postgres_client, priority_group_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Check if there are unfinished jobs for the given priority group.

    Args:
        logger: Logger instance.
        postgres_client: Database client instance.
        priority_group_id: ID of the priority group.

    Returns:
        None if no unfinished jobs are found, otherwise a failure response dictionary.
    """
    if not priority_group_id:
        logger.debug("Priority group ID is missing, skipping unfinished jobs check.")
        return None

    unfinished_jobs = (
        postgres_client.get_session()
        .query(SchedulerJobs)
        .filter(
            SchedulerJobs.priority_group_id == priority_group_id,
            SchedulerJobs.current_state.in_(JOB_STATES_NOT_FINISHED),
        )
        .all()
    )

    if unfinished_jobs:
        logger.warning(
            f"Not Completed Workflow {priority_group_id} because "
            f"it has {len(unfinished_jobs)} unfinished jobs"
        )
        return {
            "success": False,
            "error_msg": f"Not Completed Workflow {priority_group_id} due to unfinished jobs",
            "data": None,
        }

    logger.info(f"No unfinished jobs found for priority group ID: {priority_group_id}")
    return None


def handle_missing_status(logger) -> Dict[str, Any]:
    """
    Handle the case where the latest_status parameter is missing.

    Args:
        logger: Logger instance.

    Returns:
        A success response dictionary indicating no update was made.
    """
    logger.debug("latest_status is None => Not updating workflow")
    return {
        "success": True,
        "error_msg": "latest_status is None",
        "data": None,
    }


def update_workflow_record(logger, postgres_client, workflow_id: str, latest_status: str) -> None:
    """
    Update the workflow record with the latest status.

    Args:
        logger: Logger instance.
        postgres_client: Database client instance.
        workflow_id: ID of the workflow to update.
        latest_status: New status to set.
    """
    input_val = {"latest_status": latest_status}
    logger.info(f"Updating workflow ID {workflow_id} with latest_status: {latest_status}")
    postgres_client.update_record(
        SchedulerWorkflow, {"id": workflow_id}, input_val, check_record=False
    )


def update_job_state(logger, postgres_client, job_id: str, latest_status: str) -> None:
    """
    Update the current job state to 'WAITING'.

    Args:
        logger: Logger instance.
        postgres_client: Database client instance.
        job_id: ID of the current job to update.
        latest_status: latest status of workflow
    """
    logger.info(f"Checking job state for job ID: {job_id}")
    current_job = postgres_client.get_records(
        SchedulerJobs, {"job_id": job_id}, scheduler_job_mapping
    )

    current_state = JOB_WAITING_STATE if latest_status == 'RUNNING' else None

    if current_job and current_state:
        logger.info(f"Updating job ID {job_id} state to {JOB_WAITING_STATE}")
        postgres_client.update_record(
            SchedulerJobs,
            {"job_id": job_id},
            {"current_state": current_state},
            check_record=False,
        )


def __filter_by_group(logger, filter_group_list, result):
    workflow_list = result['data']
    logger.debug(f'filter_group_list: {filter_group_list}')
    filtered_workflows = []
    if filter_group_list is not None and bool(filter_group_list):
        for workflow in workflow_list:
            if workflow['group_id'] in filter_group_list:
                workflow['related'] = True
                # The workflow has a related_group and group_id matches
            else:
                workflow['related'] = False
                logger.debug(f"Removing workflow: {workflow['group_id']}")
            filtered_workflows.append(workflow)

        result['data'] = filtered_workflows


# ================= Support func =================
def __filter_data(logger, filter_values, result):
    logger.debug(f'filter_values: {filter_values}')
    latest_status = filter_values.get('latest_status')
    if latest_status:
        logger.info('============ Filter latest_status')
        result['data'] = list(
            filter(lambda x: str(x.get('latest_status')) == str(latest_status), result['data']))
        logger.debug(f'filtered last_result: {len(result['data'])}')

    text_search = filter_values.get('text_search')
    if text_search:
        logger.info('============ Text search')
        logger.debug(f'text_search: {text_search}')
        search_attributes = ['workflow_name', ]
        searched_list = common_func.search_obj_by_keywords(result['data'], search_attributes, text_search)
        logger.debug(f'searched len: {len(searched_list)}')
        result['data'] = searched_list
