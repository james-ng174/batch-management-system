from flask import Blueprint

blueprint = Blueprint(
    'scheduler_job_servers_blueprint',
    __name__,
    url_prefix=''
)
