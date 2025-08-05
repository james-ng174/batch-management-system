from flask import Blueprint

blueprint = Blueprint(
    'scheduler_job_groups_blueprint',
    __name__,
    url_prefix=''
)
