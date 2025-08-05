import os
from dotenv import load_dotenv

load_dotenv()

basedir = os.path.abspath(os.path.dirname(__file__))


class POSTGRES:
    HOST = os.getenv('POSTGRES_HOST')
    PORT = os.getenv('POSTGRES_PORT')
    USERNAME = os.getenv('POSTGRES_USERNAME')
    PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB = os.getenv('POSTGRES_DB')
    SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')


class Config(object):
    DEBUG = bool(os.getenv('DEBUG'))
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY_PATH = os.getenv('SECRET_KEY_PATH')

    password = POSTGRES.PASSWORD.replace('@', '%40')
    SQLALCHEMY_DATABASE_URI = f'postgresql://{POSTGRES.USERNAME}:{password}@{POSTGRES.HOST}:{POSTGRES.PORT}/{POSTGRES.DB}?options=-c%20search_path={POSTGRES.SCHEMA}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class LOKI:
    HOST = os.getenv('LOKI_HOST')
    PORT = os.getenv('LOKI_PORT')


class LOGGER:
    LOGGER_NAME = os.getenv('LOGGER_NAME')
    LOG_DIR = os.getenv('LOG_DIR')
    LOG_FILE_MAX_BYTES = os.getenv('LOG_FILE_MAX_BYTES')
    LOG_FILE_BACKUP_COUNT = os.getenv('LOG_FILE_BACKUP_COUNT')


class CELERY:
    HOST = os.getenv('CELERY_HOST')
    PORT = os.getenv('CELERY_PORT')


class SSH:
    SSH_KEYS_PATH = os.getenv('SSH_KEYS_PATH')
    DEFAULT_USERNAME = os.getenv('SSH_DEFAULT_USERNAME')
    PASSWORD = os.getenv('SSH_PASSWORD')


class ProductionConfig(Config):
    DEBUG = False


class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


config_dict = {
    'Production': ProductionConfig,
    'Debug': DevelopmentConfig
}
