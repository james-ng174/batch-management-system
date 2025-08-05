import os
from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from config import config_dict
from flask_socketio import SocketIO
from app import app, db
from threading import Lock

DEBUG = os.getenv("DEBUG", "False") == "True"
config = config_dict[("Debug" if DEBUG else "Production").capitalize()]
app.config.from_object(config)

# config for socket
async_mode = None
engineio_logger = False
cors_allowed_origins = "*"
thread = None
thread_lock = Lock()
socketio = SocketIO(
    app, async_mode=async_mode, engineio_logger=engineio_logger, cors_allowed_origins="*"
)

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command("db", MigrateCommand)

if __name__ == "__main__":
    manager.run()
