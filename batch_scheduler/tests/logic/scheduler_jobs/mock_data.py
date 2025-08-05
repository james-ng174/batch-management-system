import json


class MockSchedulerUser:
    user_name = 'test'


class MockSchedulerGroup:
    group_name = 'test'


class MockSchedulerServer:
    system_name = 'test'
    queue_name = 'test'


class MockSchedulerJobRunLogs:
    log_id = 'test'
    celery_task_name = 'test'
    log_date = 'test'
    system_id = 'test'
    group_id = 'test'
    job_id = 'test'
    system_name = 'test'
    group_name = 'test'
    job_name = 'test'
    operation = 'test'
    status = 'test'
    user_name = 'test'
    error_no = 'test'
    req_start_date = 'test'
    actual_start_date = 'test'
    run_duration = 'test'
    additional_info = 'test'
    errors = 'test'
    output = 'test'


class MockSchedulerJobLastLog:
    id = 'test'
    job_id = 'test'
    system_id = 'test'
    group_id = 'test'
    job_name = 'test'
    actual_start_date = 'test'
    log_id = 'test'
    run_log = MockSchedulerJobRunLogs()


class MockSchedulerJobs:
    job_id = 'test'
    system_id = 'test'
    group_id = 'test'
    job_name = 'test'
    start_date = 'test'
    end_date = 'test'
    repeat_interval = 'FREQ=MINUTELY'
    max_run_duration = 'test'
    max_run = 'test'
    max_failure = 'test'
    retry_delay = 'test'
    priority = 'test'
    is_enabled = 'test'
    auto_drop = 'test'
    restart_on_failure = 'test'
    restartable = 'test'
    job_comments = 'test'
    job_type = 'test'
    job_action = 'test'
    job_body = 'test'
    last_start_date = 'test'
    next_run_date = 'test'
    last_run_duration = 'test'
    current_state = 'test'
    run_count = 'test'
    failure_count = 'test'
    retry_count = 'test'
    frst_reg_date = 'test'
    frst_reg_user_id = 'test'
    last_chg_date = 'test'
    last_reg_user_id = 'test'
    last_log = MockSchedulerJobLastLog()
    server = MockSchedulerServer()
    group = MockSchedulerGroup()
    frst_user = MockSchedulerUser()
    last_user = MockSchedulerUser()


class MockCeleryResponse:
    data = json.dumps({'next_run_date': 'test', 'tasks': [{'celery_task_name': 'test', 'req_start_date': 'test'}]})


class MockCeleryUpdateResponse:
    data = json.dumps({
        'outdated_tasks': [{'celery_task_name': 'test'}],
        'updated_tasks': [{'celery_task_name': 'test'}]
    })


mock_request = {
    'system_id': 'test',
    'group_id': 'test',
    'job_type': 'REST_API',
    'frst_reg_user_id': 'test'
}

mock_result = [
    {
        'auto_drop': 'test',
        'current_state': 'test',
        'end_date': 'test',
        'failure_count': 'test',
        'frst_reg_date': 'test',
        'frst_reg_user_id': 'test',
        'frst_reg_user_name': 'test',
        'group_id': 'test',
        'group_name': 'test',
        'is_enabled': 'test',
        'job_action': 'test',
        'job_body': 'test',
        'job_comments': 'test',
        'job_id': 'test',
        'job_name': 'test',
        'job_type': 'test',
        'last_chg_date': 'test',
        'last_reg_user_id': 'test',
        'last_reg_user_name': 'test',
        'last_result': 'test',
        'last_run_duration': 'test',
        'last_start_date': 'test',
        'max_failure': 'test',
        'max_run': 'test',
        'max_run_duration': 'test',
        'next_run_date': 'test',
        'priority': 'test',
        'queue_name': 'test',
        'repeat_interval': 'FREQ=MINUTELY',
        'restart_on_failure': 'test',
        'restartable': 'test',
        'retry_count': 'test',
        'retry_delay': 'test',
        'run_count': 'test',
        'schedule_str': 'MINUTELY(1)',
        'start_date': 'test',
        'system_id': 'test',
        'system_name': 'test'
    }
]

mock_fe_output = [
    {
        'autoDrop': 'test',
        'comment': 'test',
        'creator': 'test',
        'currentState': 'test',
        'duration': 'test',
        'enable': 'test',
        'endDate': 'test',
        'failureCount': 'test',
        'group': 'test',
        'groupId': 'test',
        'jobAction': 'test',
        'jobBody': 'test',
        'jobCreateDate': 'test',
        'jobId': 'test',
        'jobLastChgDate': 'test',
        'jobName': 'test',
        'jobPriority': 'test',
        'jobType': 'test',
        'lastResult': 'test',
        'lastStartDate': 'test',
        'maxFailure': 'test',
        'maxRun': 'test',
        'maxRunDuration': 'test',
        'nextRunDate': 'test',
        'repeatInterval': 'FREQ=MINUTELY',
        'restartOnFailure': 'test',
        'restartable': 'test',
        'retryCount': 'test',
        'retryDelay': 'test',
        'runCount': 'test',
        'schedule': 'MINUTELY(1)',
        'startDate': 'test',
        'system': 'test',
        'systemId': 'test'
    }
]
