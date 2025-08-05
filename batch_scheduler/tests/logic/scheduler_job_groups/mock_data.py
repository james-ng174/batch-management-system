class MockSchedulerJobGroupsServer:
    system_name = 'test'


class MockSchedulerJobGroupsJobs:
    job_name = 'test'
    server = MockSchedulerJobGroupsServer()


class MockSchedulerJobGroups:
    group_id = 'test'
    group_name = 'test'
    group_comments = 'test'
    jobs = [MockSchedulerJobGroupsJobs()]
    frst_reg_date = 'test'
    frst_reg_user_id = 'test'
    last_chg_date = 'test'
    last_reg_user_id = 'test'


mock_result = [
    {
        'frst_reg_date': 'test',
        'frst_reg_user_id': 'test',
        'group_comments': 'test',
        'group_id': 'test',
        'group_name': 'test',
        'jobs': [
            {
                'name': 'test',
                'server_name': 'test',
            },
        ],
        'last_chg_date': 'test',
        'last_reg_user_id': 'test',
    },
    {
        'frst_reg_date': 'test',
        'frst_reg_user_id': 'test',
        'group_comments': 'test',
        'group_id': 'test 2',
        'group_name': 'test 2',
        'jobs': [
            {
                'name': 'test 2',
                'server_name': 'test 2',
            },
        ],
        'last_chg_date': 'test',
        'last_reg_user_id': 'test',
    }
]

mock_filter = [
    {
        'id': 'test',
        'name': 'test'
    },
    {
        'id': 'test 2',
        'name': 'test 2'
    }
]
