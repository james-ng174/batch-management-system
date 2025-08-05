from dateutil.rrule import rrulestr
from datetime import datetime, timezone, timedelta
import time
from pytz import timezone as pytz_timezone
from logger import get_logger

logger = get_logger()


def get_list_run_date(rrule_str, run_count=None, start_date=None, end_date=None, run_now=False, tz=None):
    """
    Calculate the next run times based on the input rrule string, start date, run count, priority, run_now flag, and timezone.

    :param rrule_str: The recurrence rule string (RFC 2445 format).
    :param run_count: The number of next occurrences to find (optional).
    :param start_date: The start datetime for the recurrence rule (required).
    :param end_date: The end datetime for the recurrence rule (optional).
    :param run_now: If True, include current_time as the first element of the result (optional).
    :param tz: The desired timezone for calculations (optional, default is UTC).
    :return: A list of next run datetimes.
    """
    if not start_date:
        raise ValueError("start_date is required")

    logger.info(
        f'Process get_list_run_date with data: rrule_str={rrule_str}, run_count={run_count}, '
        f'start_date={start_date}, end_date={end_date}, run_now={run_now}, tz={tz}')

    # Ensure the timezone is specified or default to UTC
    if tz is None:
        tz = timezone.utc
    else:
        tz = pytz_timezone(tz) if isinstance(tz, str) else tz

    # Ensure start_date is timezone-aware
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=tz)
    else:
        start_date = start_date.astimezone(tz)

    rule = rrulestr(rrule_str, dtstart=start_date)
    list_run_dates = []

    # Default run_count if both run_count and end_date are not provided
    if run_count is None and end_date is None:
        run_count = 1

    current_time = datetime.now(tz)  # Get the current time in the specified timezone
    # Adjust current_time to round down to the nearest minute (removing the odd seconds)
    current_time = current_time.replace(second=0, microsecond=0)

    # Add current time as the first element if run_now is True
    if run_now:
        list_run_dates.append(current_time)  # Append the current time to the list
        rule = rrulestr(rrule_str, dtstart=current_time)  # Modify the rrule to start from current time
    else:
        current_time = start_date

    # Calculate next occurrences based on rrule
    first_occurrence = rule.after(current_time, inc=not run_now)  # Skip current time if run_now=True

    # If run_now is False and start_date matches an occurrence, add it first
    if not run_now and first_occurrence == start_date:
        list_run_dates.append(start_date)
        current_time = start_date

    # If run_count is specified, adjust it to generate the correct number of occurrences
    if run_count is not None:
        for _ in range(run_count - len(list_run_dates)):
            next_run = rule.after(current_time)
            if next_run is None or (end_date is not None and next_run > end_date):
                break
            adjusted_run = next_run.astimezone(tz)
            list_run_dates.append(adjusted_run)
            current_time = next_run
    else:
        # When run_count is None and end_date is specified
        while True:
            next_run = rule.after(current_time)
            if next_run is None or next_run > end_date:
                break
            adjusted_run = next_run.astimezone(tz)
            list_run_dates.append(adjusted_run)
            current_time = next_run

    return list_run_dates


def get_next_run(list_run_dates, last_run):
    """
    Get the next run datetime based on the input rrule string, list of run datetimes, and the last run datetime.
    :param list_run_dates: A list of next run datetimes.
    :param last_run: The last run datetime.
    :return: The next run datetime.
    """
    if last_run not in list_run_dates:
        raise ValueError("last_run is not in list_run_dates")

    last_run_index = list_run_dates.index(last_run)
    if last_run_index + 1 < len(list_run_dates):
        return list_run_dates[last_run_index + 1]

    return None


def convert_epoch_to_datetime(epoch_time):
    # Convert milliseconds to seconds for the datetime.fromtimestamp() method
    timestamp_obj = epoch_time / 1000

    # Convert timestamps to datetime objects
    datetime_obj = datetime.fromtimestamp(timestamp_obj, tz=timezone.utc)
    logger.info(f"Convert epoch to obj datetime success in: {epoch_time}, out: {datetime_obj}")
    return datetime_obj


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


def get_current_time():
    """
    Get the current time as an epoch timestamp in milliseconds.

    :return: The current epoch timestamp in milliseconds as an integer.
    """
    return int(time.time() * 1000)


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


def get_next_run_date(start_date, rrule_str: str, run_count: int = 0) -> datetime:
    """
    Calculate the next run date of the rrule given a start date and run count.

    :param start_date: The start date of the rrule as a string in 'YYYY-MM-DD HH:MM:SS' format or as a datetime object.
    :param rrule_str: The rrule string (e.g., 'FREQ=MINUTELY;INTERVAL=1').
    :param run_count: The number of times the rule has already run.
    :return: The next run date as a datetime object.
    """
    logger.info(f"Received start_date: {start_date}, rrule_str: {rrule_str}, run_count: {run_count}")

    # Check if start_date is a string and convert it to a datetime object if necessary
    if isinstance(start_date, str):
        try:
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            logger.info(f"Converted start_date string to datetime: {start_date_dt}")
        except ValueError as e:
            logger.error(f"Error converting start_date: {e}")
            raise
    elif isinstance(start_date, datetime):
        start_date_dt = start_date
        logger.info(f"Received start_date as datetime object: {start_date_dt}")
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
    next_run = start_date_dt
    for _ in range(run_count + 1):  # Add +1 to move to the next occurrence after the given run_count
        next_run = rule.after(next_run)

    logger.info(f"Next run date calculated: {next_run}")

    return next_run


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


def check_rrule_run_forever(rrule_dict: dict, end_date=None, max_run=None) -> bool:
    """
    Check if the rrule will run forever.
    :param rrule_dict: Dictionary containing the parsed rrule components.
    :param end_date: Optional; if provided, indicates the rule will not run forever.
    :param max_run: Optional; if provided, indicates the rule will not run forever.
    :return: True if the rrule will run forever, False otherwise.
    """
    # If either end_date or max_run is provided, the rule will not run forever
    logger.info(f'Check run forever with info rrule_dict: {rrule_dict}, end_date: {end_date}, max_run: {max_run}')
    if end_date or max_run:
        logger.info(f'Return False with max_run and end_date')
        return False

    # Otherwise, check the rrule_dict for 'count' and 'until'
    if 'count' not in rrule_dict and 'until' not in rrule_dict:
        logger.info('Return with count and until')
        return True

    logger.info('Return other')
    return False


def validate_time_stub(data, job_settings, method):
    if data.get('max_run') and method == 'update':
        new_max_run = data.get('max_run')
        if new_max_run <= job_settings.run_count:
            logger.warn(f'"New max_run {new_max_run} cannot be less than or equal to the existing run_count."')
            return False,

    if data.get('end_date'):
        new_end_date = data.get('end_date')
        if new_end_date < get_current_time():
            logger.warn(
                f'New end date {convert_epoch_to_datetime_millis(new_end_date)} cannot be less than current time')
            return False

    if data.get('start_date'):
        new_start_date = data.get('start_date')
        if new_start_date < get_current_time():
            logger.warn(
                f'New start date {convert_epoch_to_datetime_millis(new_start_date)} cannot be less than current time')
            return False

    return True
