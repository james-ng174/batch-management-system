from manage import socketio
from flask_socketio import emit, join_room, rooms
from logger import get_logger
from .ssh_helper import SshClient
from logic import scheduler_job_servers

jobs = []
sshUtils = SshClient(get_logger())

def callback_log_server(data):
    if data["id"]:
        job = data["id"]
        emit(
            "logs_response",
            {
                "data": f"In rooms: {job}",
                "linePrefix": data["line_prefix"],
                "logs": data["current_line_decoded"],
                "namespace": "/logs",
            },
            to=job
        )


class SocketClient:

    def __init__(self, logger):
        self.logger = logger


    @socketio.event(namespace="/logs")
    def connect(self):
        emit("my_response", {"data": "Connected"})

    @socketio.event(namespace="/logs")
    def disconnect(self):
        emit("my_response", {"data": "Disconnected"})

    @socketio.event(namespace="/logs")
    def join(message):
        """_summary_
           Join Room
        Args:
            msg (_type_): _description_
        """
        room = message["room"]

        if room in rooms():
            emit("logs_response", {"data": f"SID is in room {room}"})
        else:
            join_room(message["room"])
            if room not in jobs:
                jobs.append(room)

                job_info = scheduler_job_servers.get_server_detail(
                    get_logger(), {"job_id": room}
                )["data"][0]
                if not job_info:
                    raise Exception("Not Found Server")
                job_info["job_id"] = room
                sshUtils.read_log_server(job_info, callback_log_server)
