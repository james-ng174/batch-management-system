from flask import Blueprint

blueprint = Blueprint(
    'scheduler_jobs_blueprint',
    __name__,
    url_prefix=''
)
