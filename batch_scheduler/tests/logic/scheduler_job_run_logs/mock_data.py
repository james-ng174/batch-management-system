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


mock_create_multiple_params = {
    'job_id': 'test',
    'tasks': [
        {
            'job_id': 'test'
        }
    ]
}

mock_create_single_params = {
    'job_id': 'test'
}

mock_job_data = {
    'job_id': 'test',
    'system_id': 'test',
    'group_id': 'test',
    'job_name': 'test',
    'system_name': 'test',
    'group_name': 'test',
    'is_enabled': True
}

mock_result = [
    {
        'actual_start_date': 'test',
        'additional_info': 'test',
        'celery_task_name': 'test',
        'error_no': 'test',
        'errors': 'test',
        'group_id': 'test',
        'group_name': 'test',
        'job_id': 'test',
        'job_name': 'test',
        'log_date': 'test',
        'log_id': 'test',
        'operation': 'test',
        'output': 'test',
        'req_start_date': 'test',
        'run_duration': 'test',
        'status': 'TEST',
        'system_id': 'test',
        'system_name': 'test',
        'user_name': 'test'
    }
]
