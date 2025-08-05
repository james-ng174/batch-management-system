import json
import urllib3
from config import CELERY

HTTP_METHODS = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE']


class CeleryClient:
    def __init__(self, logger):
        self.logger = logger

    def celery_create_jobs(self, params):
        self.logger.info('============ celery_create_jobs')
        payload = self.__build_celery_payload(params)
        self.__process_job_data(params, payload)
        return self.fetch_data('POST', '/celerytasks/create_rrule_task/', payload)

    def celery_assign_workflow_jobs(self, params):
        self.logger.info('============ celery_assign_workflow_jobs')
        payload = self.__build_celery_payload(params)
        self.__process_job_data(params, payload)
        return self.fetch_data('POST', '/celerytasks/assign_job_to_workflow/', payload)

    def celery_update_jobs(self, params):
        self.logger.info('============ celery_update_jobs')
        payload = self.__build_celery_payload(params)
        self.__process_job_data(params, payload)
        return self.fetch_data('POST', f'/celerytasks/update_job/', payload)

    def celery_manually_run(self, params):
        self.logger.info('============ celery_manually_run')
        self.__process_job_data(params, params)
        return self.fetch_data('POST', '/celerytasks/manually_run/', params)

    def celery_force_stop(self, params):
        self.logger.info('============ celery_force_stop')
        return self.fetch_data('POST', '/celerytasks/force_stop/', params)

    def celery_update_job_status(self, params):
        self.logger.info('============ celery_update_job_status')
        return self.fetch_data('POST', '/celerytasks/update_job_status/', params)

    def fetch_data(self, method, path, payload):
        url = f'{CELERY.HOST}:{CELERY.PORT}{path}'
        self.logger.info(f"url: {url}")

        http = urllib3.PoolManager()
        headers = {"Content-Type": "application/json"}
        response = http.urlopen(method, url, headers=headers, body=json.dumps(payload))
        self.logger.info(f"response status: {response.status}")

        if response.status == 200 and json.loads(response.data).get('status') == 'success':
            self.logger.debug(f"payload: {payload}")
            self.logger.debug(f"response data: {response.data}")
        else:
            self.logger.error(f"payload: {payload}")
            self.logger.error(f"response data: {response.data}")
            raise Exception(str(response.data))

        return response

    # ================= Support func =================
    def __build_celery_payload(self, params):
        payload = {}
        for key, value in params.items():
            if value is None:
                continue

            payload[key] = value

        return payload

    def __process_job_data(self, params, payload):
        if not params.get('job_type') == 'REST_API':
            return

        self.logger.debug(f'process_job_data: {params}')
        job_action = params.get('job_action') or ''
        job_action_data = job_action.split('\n')
        payload_method_index = None
        payload_method = ''
        url = ''

        self.logger.info('============ Process method, url')
        for data in job_action_data:
            for method in HTTP_METHODS:
                if method.lower() in data.lower():
                    payload_method_index = job_action_data.index(data)

                    method_data = data.split(' ')
                    payload_method = method_data[0]
                    url = method_data[1]
                    break

            if payload_method_index:
                break

        self.logger.debug(f'payload_method_index: {payload_method_index}')
        self.logger.debug(f'payload_method: {payload_method}')
        self.logger.debug(f'url: {url}')

        self.logger.info('============ Process host, headers')
        host = ''
        headers = {}

        if payload_method_index is not None:
            del job_action_data[payload_method_index]

        self.logger.debug(f'job_action_data: {job_action_data}')
        for data in job_action_data:
            if 'host:' in data.lower():
                host_data = data.split(' ')
                host = host_data[1]
                continue

            header_data = data.split(': ')
            headers.update({header_data[0]: header_data[1]})

        self.logger.debug(f'host: {host}')
        self.logger.debug(f'headers: {headers}')

        if not payload_method or not url or not host:
            self.logger.error(f'Invalid job action: {job_action}')
            raise Exception('Invalid job action')

        payload['job_action'] = {
            'method': payload_method,
            'url': host + url,
            'headers': headers,
            'body': params.get('job_body')
        }
