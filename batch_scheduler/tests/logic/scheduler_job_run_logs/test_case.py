from logic import scheduler_job_run_logs
from . import mock_data
from utils.postgres_helper import PostgresClient
from logic import scheduler_jobs, scheduler_job_last_log
from logger import get_logger

logger = get_logger()


def test_scheduler_job_run_logs_mapping(monkeypatch):
    mock_list = [mock_data.MockSchedulerJobRunLogs()]
    result = []
    for obj in mock_list:
        result.append(scheduler_job_run_logs.scheduler_job_run_logs_mapping(obj))

    assert result == [mock_data.mock_result[0]]


def test_scheduler_job_run_logs_read(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_job_run_logs.scheduler_job_run_logs_read(logger, {'text_search': 'test'})

    assert result == {'data': mock_data.mock_result, 'total': len(mock_data.mock_result)}


def test_scheduler_job_run_logs_create_multiple(monkeypatch):
    def mock_scheduler_jobs_read(*args, **kwargs):
        return {'data': [mock_data.mock_job_data]}

    def mock_postgres_create_multiple_record(*args, **kwargs):
        return {'success': True}

    def mock_postgres_get_records(*args, **kwargs):
        return {'data': [{'operation': 'RUN', 'actual_start_date': 'test'}]}

    def mock_scheduler_job_last_log_read(*args, **kwargs):
        return {'data': []}

    def mock_scheduler_job_last_log_create(*args, **kwargs):
        return {}

    def mock_scheduler_jobs_update_data(*args, **kwargs):
        return {}

    monkeypatch.setattr(scheduler_jobs, "scheduler_jobs_read", mock_scheduler_jobs_read)
    monkeypatch.setattr(PostgresClient, "create_multiple_record", mock_postgres_create_multiple_record)
    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)
    monkeypatch.setattr(scheduler_job_last_log, "scheduler_job_last_log_read", mock_scheduler_job_last_log_read)
    monkeypatch.setattr(scheduler_job_last_log, "scheduler_job_last_log_create", mock_scheduler_job_last_log_create)
    monkeypatch.setattr(scheduler_jobs, "scheduler_jobs_update_data", mock_scheduler_jobs_update_data)

    result = scheduler_job_run_logs.scheduler_job_run_logs_create(logger, mock_data.mock_create_multiple_params)

    assert result == {'success': True}


def test_scheduler_job_run_logs_create_single(monkeypatch):
    def mock_scheduler_jobs_read(*args, **kwargs):
        return {'data': [mock_data.mock_job_data]}

    def mock_postgres_create_record(*args, **kwargs):
        return {'success': True}

    def mock_postgres_get_records(*args, **kwargs):
        return {'data': [{'operation': 'RUN', 'actual_start_date': 'test'}]}

    def mock_scheduler_job_last_log_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_scheduler_job_last_log_update(*args, **kwargs):
        return {}

    def mock_scheduler_jobs_update_data(*args, **kwargs):
        return {}

    monkeypatch.setattr(scheduler_jobs, "scheduler_jobs_read", mock_scheduler_jobs_read)
    monkeypatch.setattr(PostgresClient, "create_record", mock_postgres_create_record)
    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)
    monkeypatch.setattr(scheduler_job_last_log, "scheduler_job_last_log_read", mock_scheduler_job_last_log_read)
    monkeypatch.setattr(scheduler_job_last_log, "scheduler_job_last_log_update", mock_scheduler_job_last_log_update)
    monkeypatch.setattr(scheduler_jobs, "scheduler_jobs_update_data", mock_scheduler_jobs_update_data)

    result = scheduler_job_run_logs.scheduler_job_run_logs_create(logger, mock_data.mock_create_single_params)

    assert result == {'success': True}


def test_scheduler_job_run_logs_update(monkeypatch):
    def mock_postgres_update_record(*args, **kwargs):
        return {'success': True}

    def mock_scheduler_jobs_read(*args, **kwargs):
        return {'data': [mock_data.mock_job_data]}

    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)
    monkeypatch.setattr(scheduler_jobs, "scheduler_jobs_read", mock_scheduler_jobs_read)

    result = scheduler_job_run_logs.scheduler_job_run_logs_update(logger, {
        'job_id': 'test',
        'celery_task_name': 'test'
    })

    assert result == {'success': True}


def test_get_job_current_state(monkeypatch):
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '', 'is_enabled': False},
        {'status': '', 'operation': ''}
    )
    assert result == 'DISABLED'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '', 'is_enabled': True},
        {}
    )
    assert result == 'READY TO RUN'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '', 'is_enabled': True},
        {'status': '', 'operation': 'CREATE'}
    )
    assert result == 'READY TO RUN'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '', 'is_enabled': True},
        {'status': 'FAILED', 'operation': 'CREATE'}
    )
    assert result == 'FAILED'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '', 'is_enabled': True, 'auto_drop': True},
        {'status': '', 'operation': 'COMPLETED'}
    )
    assert result == 'DELETED'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '1', 'is_enabled': True},
        {'status': '', 'operation': 'ENABLED'}
    )
    assert result == 'SCHEDULED'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '', 'is_enabled': True},
        {'status': '', 'operation': 'RUN'}
    )
    assert result == 'RUNNING'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '1', 'is_enabled': True},
        {'status': 'test', 'operation': 'RUN'}
    )
    assert result == 'SCHEDULED'
    result = scheduler_job_run_logs.__get_job_current_state(
        {'max_run': '1', 'is_enabled': True},
        {'status': 'test', 'operation': 'RETRY_SCHEDULED'}
    )
    assert result == 'RETRY SCHEDULED'
