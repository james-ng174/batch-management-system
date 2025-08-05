from flask import Blueprint

blueprint = Blueprint(
    'scheduler_users_blueprint',
    __name__,
    url_prefix=''
)
