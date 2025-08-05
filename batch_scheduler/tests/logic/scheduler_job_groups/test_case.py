from logic import scheduler_job_groups
from . import mock_data
from utils.postgres_helper import PostgresClient
from logic import scheduler_users
from logger import get_logger

logger = get_logger()


def test_scheduler_job_groups_mapping(monkeypatch):
    mock_list = [mock_data.MockSchedulerJobGroups()]
    result = []
    for obj in mock_list:
        result.append(scheduler_job_groups.scheduler_job_groups_mapping(obj))

    assert result == [mock_data.mock_result[0]]


def test_scheduler_job_groups_read(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_job_groups.scheduler_job_groups_read(logger, {})

    assert result == {'data': mock_data.mock_result, 'total': len(mock_data.mock_result)}


def test_scheduler_job_groups_read_exception(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        raise Exception('Exception')

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_job_groups.scheduler_job_groups_read(logger, {})

    assert result == "('Exception',)"


def test_scheduler_job_groups_create(monkeypatch):
    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_postgres_create_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(PostgresClient, "create_record", mock_postgres_create_record)

    result = scheduler_job_groups.scheduler_job_groups_create(logger, {'frst_reg_user_id': 'test'})

    assert result == {'success': True}


def test_scheduler_job_groups_create_invalid_user(monkeypatch):
    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': []}

    def mock_postgres_create_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(PostgresClient, "create_record", mock_postgres_create_record)

    result = scheduler_job_groups.scheduler_job_groups_create(logger, {'frst_reg_user_id': 'test'})

    assert result == "('Invalid user',)"


def test_scheduler_job_groups_update(monkeypatch):
    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_postgres_update_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)

    result = scheduler_job_groups.scheduler_job_groups_update(logger, {
        'group_id': 'test',
        'last_reg_user_id': 'test'
    })

    assert result == {'success': True}


def test_get_group_filter(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_job_groups.get_group_filter(logger, {})

    assert result == {'data': mock_data.mock_filter}
