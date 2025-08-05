import re
import time
from datetime import datetime, timezone

from dateutil.rrule import rrulestr
from pytz import timezone as pytz_timezone, UTC
from zoneinfo import ZoneInfo

from logger import get_logger

logger = get_logger()


def get_list_run_date(rrule_str, run_count=None, start_date=None, end_date=None):
    """
    Calculate the next run times based on the input rrule string, start date, and run count.

    :param rrule_str: The recurrence rule string (RFC 2445 format).
    :param run_count: The number of next occurrences to find (optional).
    :param start_date: The start datetime for the recurrence rule (required).
    :param end_date: The end datetime for the recurrence rule (optional).
    :return: A list of next run datetimes.
    """
    if not start_date:
        raise ValueError("start_date is required")

    rule = rrulestr(rrule_str, dtstart=start_date)
    list_run_dates = []

    if run_count is None and end_date is None:
        run_count = 30  # Default size if both run_count and end_date are not provided

    if run_count is not None:
        current_time = start_date
        for _ in range(run_count):
            next_run = rule.after(current_time)
            if next_run is None or (end_date is not None and next_run > end_date):
                break
            list_run_dates.append(next_run)
            current_time = next_run
    else:
        # When run_count is None and end_date is specified
        current_time = start_date
        while True:
            next_run = rule.after(current_time)
            if next_run is None or next_run > end_date:
                break
            list_run_dates.append(next_run)
            current_time = next_run

    return list_run_dates


def get_next_run_date(start_date, rrule_str: str, run_count: int = 0, tz: str = "UTC") -> datetime:
    """
    Calculate the next run date of the rrule given a start date, run count, and timezone.

    :param start_date: The start date of the rrule as a string in 'YYYY-MM-DD HH:MM:SS' format or as a datetime object.
                       This date is assumed to be in UTC if not naive.
    :param rrule_str: The rrule string (e.g., 'FREQ=MINUTELY;INTERVAL=1').
    :param run_count: The number of times the rule has already run.
    :param tz: The timezone string (e.g., 'Asia/Seoul').
    :return: The next run date as a datetime object in the specified timezone.
    """
    logger.info(f"Received start_date: {start_date}, rrule_str: {rrule_str}, run_count: {run_count}, timezone: {tz}")

    # Parse the timezone
    try:
        tz_obj = pytz_timezone(tz)
    except Exception as e:
        logger.error(f"Error resolving timezone {tz}: {e}")
        raise

    # Parse start_date
    if isinstance(start_date, str):
        try:
            # Assume start_date is in UTC if given as a string
            start_date_dt = tz_obj.localize(datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S'))
            logger.info(f"Converted start_date string to UTC datetime: {start_date_dt}")
        except ValueError as e:
            logger.error(f"Error converting start_date: {e}")
            raise
    elif isinstance(start_date, datetime):
        utc_datetime = start_date.replace(tzinfo=ZoneInfo("UTC"))
        start_date_dt = utc_datetime.astimezone(ZoneInfo(tz))
    else:
        logger.error("start_date must be a string or datetime object")
        raise TypeError("start_date must be a string or datetime object")

    # Parse the rrule string into an rrule object
    try:
        rule = rrulestr(rrule_str, dtstart=start_date_dt)
        logger.info("Parsed rrule string successfully")
    except Exception as e:
        logger.error(f"Error parsing rrule string: {e}")
        raise

    # Calculate the next occurrence after the given number of runs
    try:
        logger.debug(f'start {datetime.now()}')
        next_run_gen = rule.xafter(start_date_dt, count=run_count + 1)
        next_run = list(next_run_gen)[-1]
        if int(time.time()) >= int(next_run.timestamp()):
            new_next_run_gen = rule.xafter(start_date_dt, count=run_count + 2)
            next_run = list(new_next_run_gen)[-1]
    except Exception as e:
        print(f"Error during rrule calculation: {e}")
        raise
    logger.debug(f'end {datetime.now()}')

    # Ensure next_run is in the desired timezone
    try:
        next_run = next_run.astimezone(tz_obj)
        logger.info(f"Next run date calculated in timezone {tz}: {next_run}")
    except Exception as e:
        logger.error(f"Error converting next run date to timezone {tz}: {e}")
        raise

    return next_run


def get_current_time():
    """
    Get the current time as an epoch timestamp in milliseconds.

    :return: The current epoch timestamp in milliseconds as an integer.
    """
    # Get the current time and remove seconds and microseconds (round down to nearest minute)
    # current_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    current_time = datetime.now(timezone.utc).replace(microsecond=0)

    # Convert the rounded time to epoch timestamp in milliseconds
    epoch_timestamp = int(current_time.timestamp() * 1000)

    return epoch_timestamp


def datetime_to_epoch(dt):
    """
    Convert a datetime object to an epoch timestamp.

    :param dt: The datetime object to convert.
    :return: The epoch timestamp as an integer.
    """
    # Ensure the datetime is in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    epoch_timestamp = int(dt.timestamp() * 1000)
    logger.info(f"Convert obj datetime to epoch success in: {dt}, out: {epoch_timestamp}")
    return epoch_timestamp


# Dictionary of valid HTTP methods
valid_http_methods = {
    "GET": True,
    "POST": True,
    "PUT": True,
    "DELETE": True,
    "HEAD": True,
    "OPTIONS": True,
    "PATCH": True,
    "CONNECT": True,
    "TRACE": True
}


def is_valid_http_method(method):
    """
    Validate if the input is a valid HTTP method.

    Args:
    method (str): The HTTP method to validate.

    Returns:
    bool: True if valid, False otherwise.
    """
    return False if not method else valid_http_methods.get(method.upper(), False)


def validate_rrule(rrule: str):
    # Define the valid components according to RFC 5545
    valid_freqs = {'SECONDLY', 'MINUTELY', 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY'}
    valid_keys = {'FREQ', 'UNTIL', 'COUNT', 'INTERVAL', 'BYSECOND', 'BYMINUTE', 'BYHOUR',
                  'BYDAY', 'BYMONTHDAY', 'BYYEARDAY', 'BYWEEKNO', 'BYMONTH', 'BYSETPOS',
                  'WKST'}

    # Split the rrule into components
    components = rrule.split(';')
    rrule_dict = {}

    # Ensure the string is not empty and does not end with a trailing semicolon without valid components
    if len(components) < 1 or components[-1] == '':
        return False, {}

    for component in components:
        if '=' not in component:
            return False, {}  # Invalid format, no '=' found

        key, value = component.split('=', 1)
        if key not in valid_keys:
            return False, {}  # Invalid key found

        rrule_dict[key.lower()] = value  # Store the key in lowercase for consistency

    # FREQ is mandatory
    if 'freq' not in rrule_dict:
        return False, {}

    # Validate FREQ value
    if rrule_dict['freq'].upper() not in valid_freqs:
        return False, {}

    # Validate INTERVAL if present
    if 'interval' in rrule_dict:
        try:
            interval = int(rrule_dict['interval'])
            if interval <= 0:
                return False, {}  # INTERVAL must be a positive integer
        except ValueError:
            return False, {}  # INTERVAL is not an integer

    # Validate UNTIL if present
    if 'until' in rrule_dict:
        try:
            datetime.strptime(rrule_dict['until'], '%Y%m%dT%H%M%SZ')
        except ValueError:
            return False, {}  # UNTIL does not match the required format

    # Additional logical checks can be added here as needed

    return True, rrule_dict


def convert_epoch_to_datetime_millis(epoch_time):
    # Convert epoch_time from milliseconds to seconds and get the integer part (seconds)
    seconds = epoch_time // 1000

    # Get the remainder (milliseconds) and convert to microseconds
    milliseconds = epoch_time % 1000
    microseconds = milliseconds * 1000

    # Create the datetime object with millisecond precision
    datetime_obj = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(microsecond=microseconds)

    # Log the success message with millisecond precision
    logger.info(
        f"Convert epoch to obj datetime success in: {epoch_time}, out: {datetime_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

    return datetime_obj


def check_rrule_run_forever(rrule_dict: dict, end_date=None, max_run=None) -> bool:
    """
    Check if the rrule will run forever.
    :param rrule_dict: Dictionary containing the parsed rrule components.
    :param end_date: Optional; if provided, indicates the rule will not run forever.
    :param max_run: Optional; if provided, indicates the rule will not run forever.
    :return: True if the rrule will run forever, False otherwise.
    """
    # If either end_date or max_run is provided, the rule will not run forever
    if end_date is not None or max_run is not None:
        return False

    # Otherwise, check the rrule_dict for 'count' and 'until'
    if 'count' not in rrule_dict and 'until' not in rrule_dict:
        return True

    return False


def convert_epoch_to_datetime(epoch_time):
    # Convert milliseconds to seconds for the datetime.fromtimestamp() method
    timestamp_obj = epoch_time / 1000

    # Convert timestamps to datetime objects
    datetime_obj = datetime.fromtimestamp(timestamp_obj, tz=timezone.utc)
    return datetime_obj


def extract_error_message(error_string):
    """
    Extracts and cleans the error message from a wrapped structure like Exception(...) or similar.

    Args:
        error_string (str): The raw error string.

    Returns:
        str: The cleaned error message.
    """
    # Regex to extract content inside parentheses of Exception(ExecutableError(...))
    match = re.search(r'\((.*)\)', error_string)
    if match:
        inner_content = match.group(1)
        # Further unwrap if nested structures exist
        if '(' in inner_content and ')' in inner_content:
            return extract_error_message(inner_content)
        else:
            return inner_content.strip()
    else:
        return error_string.strip()


def datetime_to_epoch(dt):
    """
    Convert a datetime object to an epoch timestamp.

    :param dt: The datetime object to convert.
    :return: The epoch timestamp as an integer.
    """
    # Ensure the datetime is in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    epoch_timestamp = int(dt.timestamp() * 1000)
    return epoch_timestamp
