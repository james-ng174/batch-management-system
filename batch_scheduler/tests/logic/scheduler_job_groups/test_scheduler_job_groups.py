from logic import scheduler_job_groups
from . import mock_data
from utils import common_func, postgres_helper


def test_scheduler_job_groups_mapping(monkeypatch):
    def mock_get_logger(*args, **kwargs):
        return None

    monkeypatch.setattr(scheduler_job_groups, "get_logger", mock_get_logger)

    mock_list = [mock_data.MockSchedulerJobGroups()]
    result = []
    for obj in mock_list:
        result.append(scheduler_job_groups.scheduler_job_groups_mapping(obj))

    assert result == mock_data.mock_result


def test_scheduler_job_groups_read(monkeypatch):
    def mock_get_logger(*args, **kwargs):
        return None

    def mock_postgres_get_records(*args, **kwargs):
        return mock_data.mock_result

    monkeypatch.setattr(scheduler_job_groups, "get_logger", mock_get_logger)
    monkeypatch.setattr(postgres_helper, "get_records", mock_postgres_get_records)

    result = scheduler_job_groups.scheduler_job_groups_read({})

    assert result == mock_data.mock_result
