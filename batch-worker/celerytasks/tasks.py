import json

from celery import shared_task
from .custom_task import RepeatTask
from logger import get_logger
import subprocess
import shlex
import requests
import ast

from .models import JobSettings
from .utils import is_valid_http_method
from requests.exceptions import HTTPError
from exceptions.executable_exception import ExecutableError

logger = get_logger()


@shared_task(bind=True, base=RepeatTask)
def rest_api(self, action, job_name, job_id, run_account, next_run_date):
    """
        Calls a REST API using an input string formatted with the HTTP method, URL, and headers.

        Args:
            request_string (str): The full HTTP request string including method, URL, headers.

        Returns:
            response: The response object from the API call.
        """
    logger.info(
        f'Start execute schedule task id: {job_id} with type REST_API with user :{run_account}')
    try:
        job_settings = JobSettings.objects.filter(job_id=job_id).first()
        if job_settings is None:
            logger.error(f'JobSettings not found for job_id {job_id}.')
            return None
        job_action = ast.literal_eval(job_settings.job_action)
        logger.info(f'Execute REST_API with action: {job_action}')
        job_duration = job_settings.max_run_duration.total_seconds()
        method = job_action.get('method')
        if not is_valid_http_method(method):
            raise Exception(f'Not found or not valid method in job_action: {job_action}')

        url = job_action.get('url')
        if not url:
            raise Exception(f'Not found URL in job_action :{job_action}')

        headers = job_action.get('headers', {})
        body = job_action.get('body', None)

        # Execute the appropriate request with timeout
        if method.upper() == 'GET':
            response = requests.get(url, headers=headers, timeout=job_duration)
        elif method.upper() == 'POST':
            response = requests.post(url, headers=headers, json=body, timeout=job_duration)
        elif method.upper() == 'PUT':
            response = requests.put(url, headers=headers, json=body, timeout=job_duration)
        elif method.upper() == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=job_duration)
        else:
            raise ValueError(f'Invalid HTTP method: {method}')

        response.raise_for_status()
        logger.info(
            f'Successfully execute schedule task id: {job_id} with type REST_API with response = {response}')
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f'Timeout occurred for job_id {job_id} with user :{run_account}')
        raise TimeoutError(f"Request timed out after for job_id {job_id}")
    except HTTPError as http_error:
        logger.error(f'Request failed with status code: {http_error.response.status_code}, response: {http_error}')
        raise HTTPError(response=http_error.response)
    except Exception as e:
        logger.error(f'Fail to execute schedule task id: {job_id} with error :{e}')
        raise Exception(e)


@shared_task(bind=True, base=RepeatTask)
def executable(self, action, job_name, job_id, run_account, next_run_date):
    """
    Calls a Bash shell script with the specified command line.

    Args:
        action (str): The action or command to execute.
        job_name (str): The name of the job.
        job_id (int): The ID of the job.
        run_account (str): The user account running the job.
        next_run_date (datetime): The next scheduled run date.

    Returns:
        output (str): The standard output from the script.
        error (str): The standard error output from the script, if any.
        returncode (int): The return code from the script execution.
    """
    logger.info(f'Start executing schedule task id: {job_id} with type EXECUTABLE using user: {run_account}')

    try:
        # Retrieve job settings from the database
        job_settings = JobSettings.objects.filter(job_id=job_id).first()
        if job_settings is None:
            logger.error(f'JobSettings not found for job_id {job_id}.')
            return None

        # Extract the command from the job settings
        job_action = job_settings.job_action
        job_duration = job_settings.max_run_duration.total_seconds()

        logger.info(f'Executing EXECUTABLE with action: {job_action}')

        # Split the command into the script and its arguments
        command = shlex.split(job_action)

        # Execute the command
        result = subprocess.run(job_action, capture_output=True, text=True, check=False, shell=True, timeout=job_duration)
        logger.info(f'result of command:{job_action} ========> result: {result}')

        # Check if the return code indicates an error
        if result.returncode != 0:
            logger.error(f"Command failed with return code {result.returncode} for job_id {job_id}. "
                         f"Error output: {result.stderr}")
            raise ExecutableError(job_id, job_action, result.returncode, output=result.stdout,
                                  error_details=result.stderr)

        # Log the successful execution
        logger.info(f'Successfully executed schedule task id: {job_id} with type EXECUTABLE. '
                    f'Response: {result.stdout}, {result.returncode}, {result.stderr}')

        return result.stdout, result.stderr, result.returncode

    except subprocess.TimeoutExpired:
        logger.error(f'Timeout occurred for job_id {job_id} using user: {run_account}')
        raise TimeoutError(f"Script execution timed out for job_id {job_id}")

    except subprocess.CalledProcessError as e:
        logger.error(f'Command failed for job_id {job_id} with error: {e}')
        raise e
    except FileNotFoundError as e:
        raise e

    except Exception as e:
        logger.error(f'Failed to execute schedule task id: {job_id} due to an error {type(e)}: {e}')
        raise Exception(e)
