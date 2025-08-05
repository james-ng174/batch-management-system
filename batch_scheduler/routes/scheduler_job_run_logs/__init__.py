from flask import Blueprint

blueprint = Blueprint(
    'scheduler_job_run_logs',
    __name__,
    url_prefix=''
)
