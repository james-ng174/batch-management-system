from logic import scheduler_job_last_log
from . import mock_data
from utils.postgres_helper import PostgresClient
from logger import get_logger

logger = get_logger()


def test_scheduler_job_last_log_mapping(monkeypatch):
    mock_list = [mock_data.MockSchedulerJobLastLog()]
    result = []
    for obj in mock_list:
        result.append(scheduler_job_last_log.scheduler_job_last_log_mapping(obj))

    assert result == mock_data.mock_result


def test_scheduler_job_last_log_update(monkeypatch):
    def mock_postgres_update_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)

    result = scheduler_job_last_log.scheduler_job_last_log_update(logger, {
        'id': 'test'
    })

    assert result == {'success': True}
