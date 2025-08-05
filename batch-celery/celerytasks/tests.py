from django.test import TestCase, RequestFactory
from django.urls import reverse
from unittest.mock import patch, MagicMock
from django.http import JsonResponse
import json
from celerytasks.views import rrule_schedule_task  # Adjust the import path based on your project structure

class RruleScheduleTaskTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def test_invalid_request_method(self):
        request = self.factory.get(reverse('rrule_schedule_task'))
        response = rrule_schedule_task(request)
        self.assertEqual(response.status_code, 405)
        self.assertJSONEqual(
            str(response.content, encoding='utf8'),
            {'status': 'error', 'message': 'Invalid request method'}
        )

    @patch('celerytasks.views.get_request_data')
    @patch('celerytasks.views.convert_epoch_to_datetime')
    def test_missing_required_field(self, mock_convert_epoch_to_datetime, mock_get_request_data):
        mock_get_request_data.side_effect = KeyError('job_id')

        request = self.factory.post(reverse('rrule_schedule_task'), data=json.dumps({}), content_type='application/json')
        response = rrule_schedule_task(request)

        self.assertEqual(response.status_code, 400)
        self.assertJSONEqual(
            str(response.content, encoding='utf8'),
            {'status': 'error', 'message': 'Missing parameter: job_id'}
        )

    @patch('celerytasks.views.JobSettings.objects.create')
    @patch('celerytasks.views.get_request_data')
    @patch('celerytasks.views.validate_rrule')
    @patch('celerytasks.views.convert_epoch_to_datetime')
    def test_invalid_rrule_format(self, mock_convert_epoch_to_datetime, mock_validate_rrule, mock_get_request_data, mock_create_job_settings):
        mock_get_request_data.return_value = {
            'job_id': '1234',
            'max_run_duration': 100,
            'max_failure': 3,
            'is_enabled': True,
            'auto_drop': False,
            'restart_on_failure': True,
            'restartable': True,
            'priority': 1,
            'job_type': 'SomeJobType',
            'job_action': 'SomeAction',
            'repeat_interval': 'INVALID_RRULE',
            'start_date': '1633081600',
            'queue_name': 'default',
            'user_name': 'test_user',
            'retry_delay': 5,
            'max_run': 10,
        }
        mock_validate_rrule.return_value = (False, None)

        request = self.factory.post(reverse('rrule_schedule_task'), data=json.dumps(mock_get_request_data.return_value), content_type='application/json')
        response = rrule_schedule_task(request)

        self.assertEqual(response.status_code, 400)
        self.assertJSONEqual(
            str(response.content, encoding='utf8'),
            {'status': 'error', 'message': 'Invalid RRULE FORMAT: INVALID_RRULE'}
        )

    @patch('celerytasks.views.JobSettings.objects.create')
    @patch('celerytasks.views.get_request_data')
    @patch('celerytasks.views.validate_rrule')
    @patch('celerytasks.views.convert_epoch_to_datetime')
    @patch('celerytasks.views.get_list_run_date')
    def test_successful_task_creation(self, mock_get_list_run_date, mock_convert_epoch_to_datetime, mock_validate_rrule, mock_get_request_data, mock_create_job_settings):
        mock_get_request_data.return_value = {
            'job_id': '1234',
            'max_run_duration': 100,
            'max_failure': 3,
            'is_enabled': True,
            'auto_drop': False,
            'restart_on_failure': True,
            'restartable': True,
            'priority': 1,
            'job_type': 'SomeJobType',
            'job_action': 'SomeAction',
            'repeat_interval': 'FREQ=DAILY;INTERVAL=1',
            'start_date': '1633081600',
            'queue_name': 'default',
            'user_name': 'test_user',
            'retry_delay': 5,
            'max_run': 10,
        }
        mock_validate_rrule.return_value = (True, {})
        mock_get_list_run_date.return_value = [mock_convert_epoch_to_datetime.return_value]

        request = self.factory.post(reverse('rrule_schedule_task'), data=json.dumps(mock_get_request_data.return_value), content_type='application/json')
        response = rrule_schedule_task(request)

        self.assertEqual(response.status_code, 200)
        self.assertIn('status', json.loads(response.content))
        self.assertIn('next_run_date', json.loads(response.content))
        self.assertIn('tasks', json.loads(response.content))

