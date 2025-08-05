from logic import scheduler_jobs, scheduler_job_groups, scheduler_job_servers, scheduler_users, scheduler_job_run_logs
from . import mock_data
from utils.postgres_helper import PostgresClient
from utils.celery_helper import CeleryClient
from logger import get_logger

logger = get_logger()


def test_frontend_mapping_convert(monkeypatch):
    result = scheduler_jobs.frontend_mapping_convert(logger, mock_data.mock_result)
    assert result == mock_data.mock_fe_output


def test_scheduler_jobs_mapping(monkeypatch):
    mock_list = [mock_data.MockSchedulerJobs()]
    result = []
    for obj in mock_list:
        result.append(scheduler_jobs.scheduler_jobs_mapping(obj))

    assert result == mock_data.mock_result


def test_scheduler_jobs_read(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_jobs.scheduler_jobs_read(logger, {'text_search': 'test'})

    assert result == {'data': mock_data.mock_result, 'total': len(mock_data.mock_result)}


def test_scheduler_jobs_create(monkeypatch):
    def mock_scheduler_job_groups_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_scheduler_job_servers_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_celery_create_jobs(*args, **kwargs):
        return mock_data.MockCeleryResponse()

    def mock_postgres_create_record(*args, **kwargs):
        return {'success': True}

    def mock_scheduler_job_run_logs_create(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(scheduler_job_groups, "scheduler_job_groups_read", mock_scheduler_job_groups_read)
    monkeypatch.setattr(scheduler_job_servers, "scheduler_job_servers_read", mock_scheduler_job_servers_read)
    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(CeleryClient, "celery_create_jobs", mock_celery_create_jobs)
    monkeypatch.setattr(PostgresClient, "create_record", mock_postgres_create_record)
    monkeypatch.setattr(scheduler_job_run_logs, "scheduler_job_run_logs_create", mock_scheduler_job_run_logs_create)

    result = scheduler_jobs.scheduler_jobs_create(logger, mock_data.mock_request)

    assert result == {
        'data': None,
        'error_msg': None,
        'success': True
    }


def test_scheduler_jobs_update(monkeypatch):
    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'id': 'test', 'user_name': 'test'}]}

    def mock_scheduler_job_servers_read(*args, **kwargs):
        return {'data': [{'queue_name': 'test'}]}

    def mock_celery_update_jobs(*args, **kwargs):
        return mock_data.MockCeleryUpdateResponse()

    def mock_scheduler_job_run_logs_update(*args, **kwargs):
        return None

    def mock_scheduler_job_run_logs_create(*args, **kwargs):
        return None

    def mock_postgres_update_record(*args, **kwargs):
        return {'success': True}

    def mock_postgres_get_records(*args, **kwargs):
        return {'data': [{'job_id': 'test', 'current_state': 'RUN', 'system_id': 'test', 'last_reg_user_id': 'test'}]}

    def mock_scheduler_job_run_logs_read(*args, **kwargs):
        return {'data': [{'celery_task_name': 'test'}]}

    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(scheduler_job_servers, "scheduler_job_servers_read", mock_scheduler_job_servers_read)
    monkeypatch.setattr(CeleryClient, "celery_update_jobs", mock_celery_update_jobs)
    monkeypatch.setattr(scheduler_job_run_logs, "scheduler_job_run_logs_update", mock_scheduler_job_run_logs_update)
    monkeypatch.setattr(scheduler_job_run_logs, "scheduler_job_run_logs_create", mock_scheduler_job_run_logs_create)
    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)
    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)
    monkeypatch.setattr(scheduler_job_run_logs, "scheduler_job_run_logs_read", mock_scheduler_job_run_logs_read)

    result = scheduler_jobs.scheduler_jobs_update(logger, {
        'job_id': 'test',
        'system_id': 'test',
        'last_reg_user_id': 'test'
    })

    assert result == {
        'data': None,
        'error_msg': None,
        'success': True
    }


def test_get_repeat_interval_sample(monkeypatch):
    result = scheduler_jobs.get_repeat_interval_sample(logger, {
        'repeat_interval': 'FREQ=DAILY;INTERVAL=1;BYHOUR=20;BYMINUTE=27;BYSECOND=47',
        'start_date': '1724297262000',
        'timezone': 'Asia/Bangkok'
    })

    assert result == {
        'data': [
            '2024-08-22 오후 20:27:47',
            '2024-08-23 오후 20:27:47',
            '2024-08-24 오후 20:27:47',
            '2024-08-25 오후 20:27:47',
            '2024-08-26 오후 20:27:47',
            '2024-08-27 오후 20:27:47',
            '2024-08-28 오후 20:27:47',
            '2024-08-29 오후 20:27:47',
            '2024-08-30 오후 20:27:47',
            '2024-08-31 오후 20:27:47',
            '2024-09-01 오후 20:27:47',
            '2024-09-02 오후 20:27:47',
            '2024-09-03 오후 20:27:47',
            '2024-09-04 오후 20:27:47',
            '2024-09-05 오후 20:27:47',
            '2024-09-06 오후 20:27:47',
            '2024-09-07 오후 20:27:47',
            '2024-09-08 오후 20:27:47',
            '2024-09-09 오후 20:27:47',
            '2024-09-10 오후 20:27:47',
            '2024-09-11 오후 20:27:47',
            '2024-09-12 오후 20:27:47',
            '2024-09-13 오후 20:27:47',
            '2024-09-14 오후 20:27:47',
            '2024-09-15 오후 20:27:47',
            '2024-09-16 오후 20:27:47',
            '2024-09-17 오후 20:27:47',
            '2024-09-18 오후 20:27:47',
            '2024-09-19 오후 20:27:47',
            '2024-09-20 오후 20:27:47'
        ],
        'error_msg': None,
        'success': True,
    }


def test_manually_run(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'user_name': 'test'}]}

    def mock_celery_manually_run(*args, **kwargs):
        return None

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)
    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(CeleryClient, "celery_manually_run", mock_celery_manually_run)

    result = scheduler_jobs.manually_run(logger, {'user_id': 'test'})

    assert result == {
        'data': None,
        'error_msg': None,
        'success': True,
    }


def test_force_stop(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    def mock_celery_force_stop(*args, **kwargs):
        return None

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)
    monkeypatch.setattr(CeleryClient, "celery_force_stop", mock_celery_force_stop)

    result = scheduler_jobs.force_stop(logger, {'job_id': 'test'})

    assert result == {
        'data': None,
        'error_msg': None,
        'success': True,
    }


def test_update_job_status(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    def mock_celery_update_job_status(*args, **kwargs):
        return None

    def mock_postgres_update_record(*args, **kwargs):
        return {
            'data': None,
            'error_msg': None,
            'success': True,
        }

    def mock_scheduler_job_run_logs_read(*args, **kwargs):
        return {'data': [{'celery_task_name': 'test', 'status': None}]}

    def mock_scheduler_job_run_logs_update(*args, **kwargs):
        return None

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)
    monkeypatch.setattr(CeleryClient, "celery_update_job_status", mock_celery_update_job_status)
    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)
    monkeypatch.setattr(scheduler_job_run_logs, "scheduler_job_run_logs_read", mock_scheduler_job_run_logs_read)
    monkeypatch.setattr(scheduler_job_run_logs, "scheduler_job_run_logs_update", mock_scheduler_job_run_logs_update)

    result = scheduler_jobs.update_job_status(logger, {'job_id': 'test', 'last_reg_user_id': 'test'})

    assert result == {
        'data': None,
        'error_msg': None,
        'success': True,
    }


def test_get_job_filter(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_jobs.get_job_filter(logger, {'text_search': 'test'})

    assert result == {'data': ['test']}
