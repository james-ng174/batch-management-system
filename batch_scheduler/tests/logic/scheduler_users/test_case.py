from logic import scheduler_users
from . import mock_data
from utils.postgres_helper import PostgresClient
from logger import get_logger

logger = get_logger()


def test_scheduler_users_mapping(monkeypatch):
    mock_list = [mock_data.MockSchedulerUser()]
    result = []
    for obj in mock_list:
        result.append(scheduler_users.scheduler_users_mapping(obj))

    assert result == mock_data.mock_result


def test_scheduler_users_read(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_users.scheduler_users_read(logger, {})

    assert result == {'data': mock_data.mock_result, 'total': len(mock_data.mock_result)}


def test_scheduler_users_create(monkeypatch):
    def mock_postgres_create_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(PostgresClient, "create_record", mock_postgres_create_record)

    result = scheduler_users.scheduler_users_create(logger, {})

    assert result == {'success': True}


def test_scheduler_users_update(monkeypatch):
    def mock_postgres_update_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)

    result = scheduler_users.scheduler_users_update(logger, {
        'id': 'test'
    })

    assert result == {'success': True}
