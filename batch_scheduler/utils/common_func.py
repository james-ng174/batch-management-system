import os
import re
import time
from calendar import timegm
from datetime import datetime, timezone
from hashlib import sha256
from string import punctuation
from zoneinfo import ZoneInfo

import ngram
import pytz
from dateutil.rrule import rrulestr
from flask import jsonify, make_response
from pytz import timezone as pytz_timezone

from utils import constants

DEFAULT_PASS = 'tango1q2w3e4r%T'


def get_current_utc_time(in_epoch=False):
    if in_epoch:
        time_now = datetime.now(timezone.utc)
        time_now = convert_date_str_to_format(str(time_now), constants.DATETIME_FORMAT)
        return int(convert_date_str_to_epoch(time_now))
    else:
        return datetime.now(timezone.utc)


def correct_none_value_to_bigint(value):
    if value is None:
        return 0
    else:
        return value


def get_current_epoch_time_millis():
    return int(time.time() * 1e3)


def get_current_epoch_time_seconds():
    return int(time.time())


def sha256_hash(input_str):
    return sha256(input_str.encode('utf-8')).hexdigest()


def reset_pass():
    return sha256(DEFAULT_PASS.encode('utf-8')).hexdigest()


def try_parse_int(val):
    try:
        return int(val)
    except Exception:
        return val


def convert_date_str_to_format(date_string, desired_format):
    if not date_string or not isinstance(date_string, str):
        return date_string

    date_string = date_string.replace("\\", "")

    if '+' in date_string:
        date_string = next(iter(date_string.split('+')), '')

    if not desired_format:
        return date_string

    epoch_int = try_parse_int(date_string)

    if isinstance(epoch_int, int):
        epoch_dt = datetime.fromtimestamp(epoch_int / 1e3)
        return epoch_dt.strftime(desired_format)
    else:
        utc_time = __convert_date_str_to_utc_time(date_string)
        return utc_time.strftime(desired_format) if utc_time else None


def convert_date_str_to_format_with_timezone(date_string, desired_format, timezone):
    """
    Convert a date string to a specific format and timezone.

    Args:
        date_string (str): The input date string to be converted.
        desired_format (str): The desired output date format.
        timezone (str): The desired timezone for the output date.

    Returns:
        str: The formatted date string in the specified timezone, or None if conversion fails.
    """
    if not date_string or not isinstance(date_string, str):
        return date_string

    date_string = date_string.replace("\\", "")

    if '+' in date_string:
        date_string = next(iter(date_string.split('+')), '')

    if not desired_format:
        return date_string

    try:
        # Parse epoch timestamp
        epoch_int = try_parse_int(date_string)
        if isinstance(epoch_int, int):
            epoch_dt = datetime.fromtimestamp(epoch_int / 1e3)
        else:
            # Fallback to converting using a UTC parsing helper
            utc_time = __convert_date_str_to_utc_time(date_string)
            if utc_time is None:
                return None
            epoch_dt = utc_time

        # Apply the desired timezone
        target_timezone = pytz.timezone(timezone)
        localized_time = epoch_dt.astimezone(target_timezone)

        # Return formatted date string in the specified timezone
        return localized_time.strftime(desired_format)
    except Exception as e:
        # Log or handle the exception as needed
        return None


def convert_date_str_to_epoch(date_string):
    if not date_string:
        return date_string

    utc_time = __convert_date_str_to_utc_time(date_string)
    return f"{timegm(utc_time.timetuple())}000" if utc_time else None

def get_next_run_date(start_date, rrule_str, run_count, tz, logger) -> datetime:
    logger.info(
        f"Received start_date: {start_date}, rrule_str: {rrule_str}, run_count: {run_count}, timezone: {tz}")

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
        if int(time.time()) == int(next_run.timestamp()):
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


def to_lower_camel_case(snake_str):
    camel_string = "".join(x.capitalize() for x in snake_str.lower().split("_"))
    return snake_str[0].lower() + camel_string[1:]


def format_api_output(logger, result):
    if isinstance(result, dict):
        http_status = 200
        final_data = result
    else:
        http_status = 500
        final_data = {
            'success': False,
            'error_msg': result,
            'data': None
        }
    logger.debug(f'final_data: {final_data}')

    return make_response(jsonify(final_data), http_status)


def format_login_output(logger, result):
    if isinstance(result, dict):
        http_status = 200
        final_data = result
    else:
        http_status = 200
        final_data = {
            'success': False,
            'error_msg': result,
            'data': None
        }
    logger.debug(f'final_data: {final_data}')

    return make_response(jsonify(final_data), http_status)


def search_obj_by_keywords(obj_list, attribute_list, text_search):
    searched_list = []
    for obj in obj_list:
        keywords = []
        for attribute in attribute_list:
            attribute_value = str(obj.get(attribute) or '')
            if not attribute_value:
                continue

            ngram_client = ngram.NGram(N=len(attribute_value))
            keywords += [x.lower().strip().replace('$', '') for x in list(ngram_client.split(attribute_value))]

        if text_search.lower() in keywords:
            searched_list.append(obj)

    return searched_list


def paginate_data(obj_list, page_number, page_size):
    if page_number and page_size:
        if page_number <= 0:
            page_number = 1

        if page_size <= 0:
            page_size = 1

        return obj_list[((page_number - 1) * page_size):(page_number * page_size)]
    else:
        return obj_list


def validate_rrule(rrule):
    # Define the valid components according to RFC 5545
    valid_freqs = {'SECONDLY', 'MINUTELY', 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY'}
    valid_keys = {'FREQ', 'UNTIL', 'COUNT', 'INTERVAL', 'BYSECOND', 'BYMINUTE', 'BYHOUR',
                  'BYDAY', 'BYMONTHDAY', 'BYYEARDAY', 'BYWEEKNO', 'BYMONTH', 'BYSETPOS',
                  'WKST'}

    # Split the rrule into components
    components = rrule.split(';')
    rrule_dict = {}

    # Ensure the string is not empty or only contains FREQ with a trailing semicolon
    if len(components) < 1 or not components[-1]:
        return False

    for component in components:
        if '=' not in component:
            return False  # Invalid format, no '=' found

        key, value = component.split('=', 1)
        if key not in valid_keys:
            return False  # Invalid key found

        rrule_dict[key] = value

    # FREQ is mandatory
    if 'FREQ' not in rrule_dict:
        return False

    # Validate FREQ value
    if rrule_dict['FREQ'] not in valid_freqs:
        return False

    # Validate INTERVAL if present
    if 'INTERVAL' in rrule_dict:
        try:
            interval = int(rrule_dict['INTERVAL'])
            if interval <= 0:
                return False  # INTERVAL must be a positive integer
        except ValueError:
            return False  # INTERVAL is not an integer

    # Validate UNTIL if present
    if 'UNTIL' in rrule_dict:
        try:
            datetime.strptime(rrule_dict['UNTIL'], '%Y%m%dT%H%M%SZ')
        except ValueError:
            return False  # UNTIL does not match the required format

    # Additional logical checks can be added here as needed

    return True


# ================= Support func =================
def __convert_date_str_to_utc_time(date_string):
    date_format_types = __generate_date_format_types(date_string)
    index = 0

    while True:
        try:
            utc_time = datetime.strptime(date_string, date_format_types[index])
            return utc_time
        except Exception as e:
            if str(e) == "list index out of range" or index == len(date_format_types):
                print(f"Date not belong to any know format: {date_string}")
                return None
            else:
                index += 1


def __generate_date_format_types(date_string):
    hour_formats = ["%H:%M:%S", "%H:%M:%SZ", "%H:%M", "%H:%M:%S.%f", "%H:%M:%S.%fZ", "%H:%M:%S.%f%z"]

    date_separation = next(
        iter(list(__get_desired_characters_only(date_string, ["special_char"]))), "-"
    )
    date_format_types = [f"%d{date_separation}%b{date_separation}%Y"]

    base_date = f"%Y{date_separation}%m{date_separation}%d"
    date_format_types.append(base_date)
    date_format_with_t = "T" if "T" in date_string else " "

    for format_val in hour_formats:
        date_format_types.append(f"{base_date}{date_format_with_t}{format_val}")

    return date_format_types


def __get_desired_characters_only(text_str, options):
    if not text_str:
        return text_str

    regex_mapping = {
        "text": "a-zA-Z",
        "number": r"\d",
        "space": " ",
        "special_char": punctuation,
    }

    regex = [regex_mapping[option] for option in options if regex_mapping.get(option)]
    regex = "".join(regex)
    if not regex:
        return text_str

    return "".join(re.findall(f"[{regex}]+", text_str))


def read_secret_key(file_path: str) -> str:
    """
    Reads the secret key from a specified file.

    Args:
        file_path (str): The path to the file containing the secret key.

    Returns:
        str: The secret key if successfully read.

    Raises:
        FileNotFoundError: If the file does not exist.
        PermissionError: If there is an issue with file permissions.
        Exception: For any other unexpected errors.
    """
    try:
        # Ensure the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file at {file_path} does not exist.")

        # Open the file and read the secret key
        with open(file_path, 'r') as file:
            secret_key = file.read().strip()

        if not secret_key:
            raise ValueError("The secret key file is empty.")

        return secret_key

    except FileNotFoundError as e:
        raise e
    except PermissionError as e:
        raise e
    except Exception as e:
        raise Exception(f"An error occurred while reading the secret key: {e}")
