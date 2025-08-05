import paramiko
from scp import SCPClient
from config import SSH
from lib.paramiko_expect import SSHClientInteraction
import traceback


class SshClient:
    def __init__(self, logger):
        self.logger = logger

    def create_ssh_client(self, host, username=None, password=None):
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if username and password:
                ssh.connect(host, username=username, password=password)
            else:
                key_file_name = f"{SSH.SSH_KEYS_PATH}/{username}/id_rsa"
                private_key = paramiko.RSAKey.from_private_key_file(key_file_name)
                ssh.connect(host, username=username, pkey=private_key)

        except Exception as e:
            raise Exception(f"Exception create_ssh_client {str(e.args)}")

        return ssh

    def scp_put(self, ssh, folder_name, remote_path):
        scp_client = SCPClient(ssh.get_transport())
        try:
            if not folder_name:
                raise Exception("Need folder_name")

            scp_client.put(
                f"././reference/{folder_name}", recursive=True, remote_path=remote_path
            )
            scp_client.close()

        except Exception as e:
            scp_client.close()
            raise Exception(f"Exception scp_put {str(e.args)}")

    def ssh_execute_command(self, ssh, command):
        success = None
        error = None
        try:
            stdin, stdout, stderr = ssh.exec_command(command)
            for line in stdout:
                line_str = line.strip("\n")
                self.logger.info(line_str)
                success = (
                    True if "Celery worker started successfully" in line_str else False
                )
                if not success:
                    error = line_str

        except Exception as e:
            raise Exception(f"Exception ssh_execute_command {str(e.args)}")

        return success, error

    def copy_celery_worker_code(self, host_ip_addr, ssh_user, params):
        self.logger.info("============ Copy celery worker code")
        ssh_client = self.create_ssh_client(host_ip_addr, ssh_user)
        try:
            commands = [f'mkdir -p {params["folder_path"]}']
            success, error = self.ssh_execute_command(
                ssh_client, f'({"; ".join(commands)})'
            )
            self.logger.debug(f"success: {success}")
            self.logger.debug(f"error: {error}")

            self.scp_put(ssh_client, "celery", params["folder_path"])
            ssh_client.close()

        except Exception as e:
            ssh_client.close()
            raise Exception(f"Exception copy_celery_worker_code: {str(e.args)}")

    def copy_celery_worker_code_v2(self, host_ip_addr, ssh_user, folder_path):
        self.logger.info("============ Copy celery worker code")
        self.logger.info(f"============ Copy celery worker code to {ssh_user}@{host_ip_addr}/{folder_path}")
        ssh_client = self.create_ssh_client(host_ip_addr, ssh_user)
        try:
            commands = [f'mkdir -p {folder_path}']
            success, error = self.ssh_execute_command(
                ssh_client, f'({"; ".join(commands)})'
            )
            self.logger.debug(f"success: {success}")
            self.logger.debug(f"error: {error}")

            self.scp_put(ssh_client, "celery", folder_path)
            ssh_client.close()

        except Exception as e:
            ssh_client.close()
            raise Exception(f"Exception copy_celery_worker_code: {str(e.args)}")

    def clean_old_worker_folder(self, check_record):
        self.logger.info("============ Clean old worker folder")
        ssh_client = self.create_ssh_client(
            check_record.get("host_ip_addr"), check_record.get("ssh_user")
        )
        try:
            commands = [f"rm -rf {check_record.folder_path}/celery"]
            self.logger.debug(f"commands: {commands}")
            self.ssh_execute_command(ssh_client, f'({"; ".join(commands)})')
            ssh_client.close()

        except Exception as e:
            ssh_client.close()
            raise Exception(f"Exception clean_old_worker_folder: {str(e.args)}")

    def start_celery_worker(self, host_ip_addr, ssh_user, params):
        self.logger.info("============ Start celery worker")
        ssh_client = self.create_ssh_client(host_ip_addr, ssh_user)
        try:
            commands = [
                f'cd {params["folder_path"]}/celery',
                "unzip -o batch-worker.zip",
                "cd batch-worker",
                "chmod +x start_worker.sh",
                "dos2unix start_worker.sh",
                "dos2unix .env",
                f'./start_worker.sh QUEUE_NAME={params["queue_name"]}',
            ]
            success, error = self.ssh_execute_command(
                ssh_client, f'({"; ".join(commands)})'
            )
            # todo clean unused worker
            ssh_client.close()

        except Exception as e:
            ssh_client.close()
            raise Exception(f"Exception create_celery_worker: {str(e.args)}")

        self.logger.debug(f"success: {success}")
        return success, error

    def start_celery_worker_v2(self, host_ip_addr, ssh_user, folder_path, queue_name):
        self.logger.info("============ Start celery worker")
        ssh_client = self.create_ssh_client(host_ip_addr, ssh_user)
        try:
            commands = [
                f'cd {folder_path}/celery',
                "unzip -o batch-worker.zip",
                "cd batch-worker",
                "chmod +x start_worker.sh",
                "dos2unix start_worker.sh",
                "dos2unix .env",
                f'./start_worker.sh QUEUE_NAME={queue_name}',
            ]
            success, error = self.ssh_execute_command(
                ssh_client, f'({"; ".join(commands)})'
            )
            # todo clean unused worker
            ssh_client.close()

        except Exception as e:
            ssh_client.close()
            raise Exception(f"Exception create_celery_worker: {str(e.args)}")

        self.logger.debug(f"success: {success}")
        return success, error

    def stop_celery_worker(self, params):
        self.logger.info("============ Stop celery worker")
        self.logger.debug(f"stop_celery_worker params: {params}")
        ssh_client = self.create_ssh_client(params["host_ip_addr"], params["ssh_user"])
        try:
            commands = [
                f'cd {params["folder_path"]}/celery',
                "cd batch-celery",
                "chmod +x stop_worker.sh",
                "dos2unix stop_worker.sh",
                f'./stop_worker.sh QUEUE_NAME={params["queue_name"]}',
            ]
            self.ssh_execute_command(ssh_client, f'({"; ".join(commands)})')
            ssh_client.close()

        except Exception as e:
            ssh_client.close()
            raise Exception(f"Exception stop_celery_worker: {str(e.args)}")

    def read_log_server(self, params, callback):
        self.logger.info("============ Read log server")
        self.logger.debug(f"read_log_server params: {params}")
        ssh_client = self.create_ssh_client(params["host_ip_addr"], params["ssh_user"])
        queue_name = params.get("queue_name")
        job_id = params.get("job_id")
        line_prefix = f" {job_id}:" if job_id else f"worker_id_not_found :"

        try:
            command = f"tail -f /var/log/batch-worker/{queue_name}/batch-worker.log"
            self.logger.debug(f"================== Command: {command}")
            interact = SSHClientInteraction(ssh_client, timeout=10, display=False)
            interact.send(command)
            interact.tail(id=job_id, line_prefix=line_prefix, output_callback=callback)

        except KeyboardInterrupt:
            print("Ctrl+C interruption detected, stopping tail")
        except Exception:
            traceback.print_exc()
        finally:
            try:
                ssh_client.close()
            except Exception:
                pass
