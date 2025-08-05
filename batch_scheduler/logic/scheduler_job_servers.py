import uuid
from models.scheduler_job_servers import SchedulerJobServers
from config import SSH
from utils.postgres_helper import PostgresClient
from utils.ssh_helper import SshClient
from utils import common_func
from logic import scheduler_users
from models.scheduler_jobs import SchedulerJobs
from .scheduler_jobs import scheduler_jobs_mapping


def scheduler_job_servers_mapping(record):
    return {
        "system_id": record.system_id,
        "system_name": record.system_name,
        "host_name": record.host_name,
        "host_ip_addr": record.host_ip_addr,
        "secondary_host_ip_addr": record.secondary_host_ip_addr,
        "queue_name": record.queue_name,
        "folder_path": record.folder_path,
        "secondary_folder_path": record.secondary_folder_path,
        "ssh_user": record.ssh_user,
        "system_comments": record.system_comments,
        "frst_reg_date": record.frst_reg_date,
        "frst_reg_user_id": record.frst_reg_user_id,
        "last_chg_date": record.last_chg_date,
        "last_reg_user_id": record.last_reg_user_id,
    }


def scheduler_job_servers_read(logger, params):
    try:
        logger.info("============ SCHEDULER_JOB_SERVERS_READ ============")
        logger.debug(f"params: {params}")
        page_number = params.get("page_number")
        page_size = params.get("page_size")
        params.pop("page_number", None)
        params.pop("page_size", None)

        postgres_client = PostgresClient(logger)

        result = postgres_client.get_records(
            SchedulerJobServers, params, scheduler_job_servers_mapping
        )
        result["total"] = len(result["data"])
        result["data"] = common_func.paginate_data(
            result["data"], page_number, page_size
        )

    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    return result


def scheduler_job_servers_create(logger, params):
    try:
        logger.info("============ SCHEDULER_JOB_SERVERS_CREATE ============")
        logger.debug(f"params: {params}")

        if not params.get("host_ip_addr") or not params.get("folder_path"):
            raise Exception("Missing host_ip_addr or folder_path")

        check_user = scheduler_users.scheduler_users_read(
            logger, {"id": params["frst_reg_user_id"]}
        )["data"]
        if not check_user:
            raise Exception("Invalid user")

        system_id = uuid.uuid4()
        params["system_id"] = str(system_id)
        params["queue_name"] = str(system_id.hex)

        list_host_ip_addr = [params.get("host_ip_addr"), params.get("secondary_host_ip_addr")]
        list_folder_path = [params.get("folder_path"), params.get("secondary_folder_path")]

        for i in range(0, len(list_host_ip_addr)):
            host_ip_addr = list_host_ip_addr[i]
            folder_path = list_folder_path[i]
            logger.info(f'===========host_ip_addr: {host_ip_addr}, folder_path: {folder_path}')
            if host_ip_addr and folder_path:
                scheduler_start_worker(logger, host_ip_addr, folder_path, params["queue_name"])

        postgres_client = PostgresClient(logger)
        result = postgres_client.create_record(SchedulerJobServers, params)

    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    return result

def scheduler_start_worker(logger, host_ip_addr, folder_path, queue_name):
    if host_ip_addr and ("@" in host_ip_addr):
        ssh_user, host_ip_addr = __split_user_host_ip_adr(host_ip_addr)
        logger.info(f"============ __split_user_host_ip_adr {ssh_user}@{host_ip_addr}")
    else:
        ssh_user = SSH.DEFAULT_USERNAME

    ssh_client = SshClient(logger)
    ssh_client.copy_celery_worker_code_v2(
        host_ip_addr, ssh_user, folder_path
    )
    success, error = ssh_client.start_celery_worker_v2(
        host_ip_addr, ssh_user, folder_path, queue_name
    )
    if not success:
        raise Exception(f"Worker start failed: {error}")

def scheduler_job_servers_update(logger, params):
    try:
        logger.info("============ SCHEDULER_JOB_SERVERS_UPDATE ============")
        logger.debug(f"params: {params}")

        check_user = scheduler_users.scheduler_users_read(
            logger, {"id": params["last_reg_user_id"]}
        )["data"]
        if not check_user:
            raise Exception("Invalid user")

        params["last_chg_date"] = common_func.get_current_utc_time(in_epoch=True)
        if params.get("host_ip_addr") and ("@" in params["host_ip_addr"]):
            __split_user_host(params)

        old_record_result = scheduler_job_servers_read(
            logger, {"system_id": params["system_id"]}
        )
        if not isinstance(old_record_result, dict):
            raise Exception("Record not existed")

        old_record = next(iter(old_record_result["data"]), {})

        ssh_client = SshClient(logger)
        if (
            params.get("host_ip_addr")
            and old_record.get("host_ip_addr") != params["host_ip_addr"]
        ):
            logger.info("============ Host ip address change")
            __process_update_celery_worker(
                logger, ssh_client, params["host_ip_addr"], params, old_record
            )

        elif (
            params.get("folder_path")
            and old_record.get("folder_path") != params["folder_path"]
        ):
            logger.info("============ Folder path change")
            __process_update_celery_worker(
                logger, ssh_client, old_record.get("host_ip_addr"), params, old_record
            )
            ssh_client.clean_old_worker_folder(old_record)

        input_val = {
            x: params[x]
            for x in params
            if x
            in [
                "system_name",
                "host_name",
                "host_ip_addr",
                "system_comments",
                "folder_path",
                "ssh_user",
                "last_chg_date",
                "last_reg_user_id",
            ]
        }
        postgres_client = PostgresClient(logger)
        result = postgres_client.update_record(
            SchedulerJobServers, {"system_id": params["system_id"]}, input_val
        )

    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    return result


def get_server_filter(logger, params):
    try:
        logger.info("============ GET_SERVER_FILTER ============")
        logger.debug(f"params: {params}")

        postgres_client = PostgresClient(logger)
        result = postgres_client.get_records(
            SchedulerJobServers, params, scheduler_job_servers_mapping
        )

        filter_list = []
        for obj in result["data"]:
            filter_list.append(
                {"id": obj.get("system_id"), "name": obj.get("system_name")}
            )

        filter_list.sort(key=lambda x: (x.get("name")))
        result["data"] = filter_list

    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    return result


def get_server_detail(logger, params):
    try:
        logger.info("============ GET_SERVER_DETAIL ============")
        logger.debug(f"params: {params}")
        if not params.get("job_id"):
            raise Exception("Missing job_id")
        postgres_client = PostgresClient(logger)

        job_data = postgres_client.get_records(
            SchedulerJobs,
            {"job_id": params["job_id"]},
            scheduler_jobs_mapping,
        )
        if not job_data:
            raise Exception("Invalid job_id")

        result = postgres_client.get_records(
            SchedulerJobServers,
            {"system_id": job_data["data"][0]["system_id"]},
            scheduler_job_servers_mapping,
        )
    except Exception as e:
        logger.error(f"Exception: {e}")
        result = str(e.args)

    logger.info("============ GET_SERVER_DETAIL ============")
    logger.info(f"result: {result}")
    return result


# ================= Support func =================
def __split_user_host(params):
    host_ip_addr_split = params["host_ip_addr"].strip().split("@")
    if len(host_ip_addr_split) != 2:
        raise Exception("Invalid host_ip_addr")

    params["ssh_user"] = host_ip_addr_split[0]
    params["host_ip_addr"] = host_ip_addr_split[1]

# ================= Support func =================
def __split_user_host_ip_adr(host_ip_addr):
    host_ip_addr_split = host_ip_addr.strip().split("@")
    if len(host_ip_addr_split) != 2:
        raise Exception("Invalid host_ip_addr")

    ssh_user = host_ip_addr_split[0]
    host_ip_addr = host_ip_addr_split[1]
    return ssh_user, host_ip_addr


def __process_update_celery_worker(
    logger, ssh_client, host_ip_addr, params, old_record
):
    try:
        __stop_old_celery_worker(ssh_client, old_record)

        ssh_user = params.get("ssh_user") or old_record.get("ssh_user")
        ssh_client.copy_celery_worker_code(
            host_ip_addr, ssh_user, {"folder_path": params["folder_path"]}
        )
        success, error = ssh_client.start_celery_worker(
            host_ip_addr,
            ssh_user,
            {
                "queue_name": old_record.get("queue_name"),
                "folder_path": params["folder_path"],
            },
        )
        if not success:
            __restart_old_celery_worker(logger, ssh_client, error, old_record)

    except Exception as e:
        raise Exception(f"Exception create celery worker: {str(e.args)}")


def __stop_old_celery_worker(ssh_client, old_record):
    ssh_client.stop_celery_worker(
        {
            "host_ip_addr": old_record.get("host_ip_addr"),
            "ssh_user": old_record.get("ssh_user"),
            "folder_path": old_record.get("folder_path"),
            "queue_name": old_record.get("queue_name"),
        }
    )


def __restart_old_celery_worker(logger, ssh_client, error, old_record):
    logger.info("============ Restart old celery worker")
    ssh_client.start_celery_worker(
        old_record.get("host_ip_addr"),
        old_record.get("ssh_user"),
        {
            "queue_name": old_record.get("queue_name"),
            "folder_path": old_record.get("folder_path"),
        },
    )
    logger.error(f"error: {error}")
    raise Exception(f"New worker start failed: {error}")
