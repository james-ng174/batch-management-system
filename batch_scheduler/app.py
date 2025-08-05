from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from importlib import import_module
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
db = SQLAlchemy(app)

modules = ['scheduler_job_servers', 'scheduler_job_groups', 'scheduler_jobs', 'scheduler_job_run_logs',
           'scheduler_users', 'scheduler_workflows']
for module_name in modules:
    module = import_module(f'routes.{module_name}.routes')
    app.register_blueprint(module.blueprint)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
