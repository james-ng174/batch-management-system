import os
import uuid
import flask
import logging
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler
from logging_loki import LokiHandler
from utils import loki_helper
from config import Config, LOGGER


def get_request_id():
    if getattr(flask.g, 'request_id', None):
        return flask.g.request_id

    new_uuid = uuid.uuid4().hex
    flask.g.request_id = new_uuid

    return new_uuid


class RequestIdFilter(logging.Filter):
    def filter(self, record):
        record.req_id = get_request_id() if flask.has_request_context() else ''
        return True


try:
    logger = logging.getLogger(LOGGER.LOGGER_NAME)
    logger.setLevel(logging.DEBUG if Config.DEBUG else logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(req_id)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler (local run only)
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.DEBUG)
    # console_handler.addFilter(RequestIdFilter())
    # console_handler.setFormatter(formatter)
    # logger.addHandler(console_handler)

    # Loki handler
    # loki_handler = LokiHandler(url=loki_helper.get_url(), tags={"application": "tes-platform"}, version="1")
    # logger.addHandler(loki_handler)

    # Concurrent log handler
    logfile = os.path.abspath(LOGGER.LOG_DIR)
    rotateHandler = ConcurrentTimedRotatingFileHandler(filename=logfile, when="MIDNIGHT", interval=1, mode="a",
                                                       maxBytes=int(LOGGER.LOG_FILE_MAX_BYTES),
                                                       backupCount=int(LOGGER.LOG_FILE_BACKUP_COUNT), use_gzip=True)
    rotateHandler.addFilter(RequestIdFilter())
    rotateHandler.setFormatter(formatter)
    logger.addHandler(rotateHandler)
except Exception as e:
    print(f'Exception: {e}')


def get_logger():
    return logger
