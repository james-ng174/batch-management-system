from logic import scheduler_job_servers, scheduler_users
from . import mock_data
from utils.ssh_helper import SshClient
from utils.postgres_helper import PostgresClient
from logger import get_logger

logger = get_logger()


def test_scheduler_job_servers_mapping(monkeypatch):
    mock_list = [mock_data.MockSchedulerJobServers()]
    result = []
    for obj in mock_list:
        result.append(scheduler_job_servers.scheduler_job_servers_mapping(obj))

    assert result == mock_data.mock_result


def test_scheduler_job_servers_read(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_job_servers.scheduler_job_servers_read(logger, {})

    assert result == {'data': mock_data.mock_result, 'total': len(mock_data.mock_result)}


def test_scheduler_job_servers_create(monkeypatch):
    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_copy_celery_worker_code(*args, **kwargs):
        return None

    def mock_start_celery_worker(*args, **kwargs):
        return True, None

    def mock_postgres_create_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(SshClient, "copy_celery_worker_code", mock_copy_celery_worker_code)
    monkeypatch.setattr(SshClient, "start_celery_worker", mock_start_celery_worker)
    monkeypatch.setattr(PostgresClient, "create_record", mock_postgres_create_record)

    result = scheduler_job_servers.scheduler_job_servers_create(logger, {
        'host_ip_addr': '@test',
        'folder_path': 'test',
        'frst_reg_user_id': 'test'
    })

    assert result == {'success': True}


def test_scheduler_job_servers_update(monkeypatch):
    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_scheduler_job_servers_read(*args, **kwargs):
        return {'data': [{'host_ip_addr': '@test', 'folder_path': 'test'}]}

    def mock_stop_celery_worker(*args, **kwargs):
        return None

    def mock_copy_celery_worker_code(*args, **kwargs):
        return None

    def mock_start_celery_worker(*args, **kwargs):
        return True, None

    def mock_postgres_update_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(scheduler_job_servers, "scheduler_job_servers_read", mock_scheduler_job_servers_read)
    monkeypatch.setattr(SshClient, "stop_celery_worker", mock_stop_celery_worker)
    monkeypatch.setattr(SshClient, "copy_celery_worker_code", mock_copy_celery_worker_code)
    monkeypatch.setattr(SshClient, "start_celery_worker", mock_start_celery_worker)
    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)

    result = scheduler_job_servers.scheduler_job_servers_update(logger, {
        'system_id': 'test',
        'host_ip_addr': '@test1',
        'folder_path': 'test',
        'frst_reg_user_id': 'test',
        'last_reg_user_id': 'test'
    })

    assert result == {'success': True}


def test_scheduler_job_servers_update_fail_start_new_worker(monkeypatch):
    def mock_scheduler_users_read(*args, **kwargs):
        return {'data': [{'id': 'test'}]}

    def mock_scheduler_job_servers_read(*args, **kwargs):
        return {'data': [{'host_ip_addr': '@test', 'folder_path': 'test'}]}

    def mock_stop_celery_worker(*args, **kwargs):
        return None

    def mock_copy_celery_worker_code(*args, **kwargs):
        return None

    def mock_start_celery_worker(*args, **kwargs):
        return False, None

    def mock_postgres_update_record(*args, **kwargs):
        return {'success': True}

    monkeypatch.setattr(scheduler_users, "scheduler_users_read", mock_scheduler_users_read)
    monkeypatch.setattr(scheduler_job_servers, "scheduler_job_servers_read", mock_scheduler_job_servers_read)
    monkeypatch.setattr(SshClient, "stop_celery_worker", mock_stop_celery_worker)
    monkeypatch.setattr(SshClient, "copy_celery_worker_code", mock_copy_celery_worker_code)
    monkeypatch.setattr(SshClient, "start_celery_worker", mock_start_celery_worker)
    monkeypatch.setattr(PostgresClient, "update_record", mock_postgres_update_record)

    result = scheduler_job_servers.scheduler_job_servers_update(logger, {
        'system_id': 'test',
        'host_ip_addr': '@test1',
        'folder_path': 'test',
        'frst_reg_user_id': 'test',
        'last_reg_user_id': 'test'
    })

    assert result == '("Exception create celery worker: (\'New worker start failed: None\',)",)'


def test_get_server_filter(monkeypatch):
    def mock_postgres_get_records(*args, **kwargs):
        return {'data': mock_data.mock_result}

    monkeypatch.setattr(PostgresClient, "get_records", mock_postgres_get_records)

    result = scheduler_job_servers.get_server_filter(logger, {})

    assert result == mock_data.mock_filter
