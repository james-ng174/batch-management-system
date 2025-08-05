import json
import urllib3
import psycopg2
import psycopg2.extras
from config import POSTGRES, CELERY
from utils import common_func
import uuid


def sync_tasks():
    print('============ SYNC TASKS ============')
    connection = psycopg2.connect(database=POSTGRES.DB, user=POSTGRES.USERNAME, password=POSTGRES.PASSWORD,
                                  host=POSTGRES.HOST, port=POSTGRES.PORT,
                                  options=f"-c search_path={POSTGRES.SCHEMA}")
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    init_admin_user(cursor)

    un_sync_result = __get_unfinished_record(cursor)
    if un_sync_result:
        celery_result = __get_result_from_celery(un_sync_result)
        __update_records_data(cursor, celery_result)

    connection.close()


def init_admin_user(cursor):
    # Define the SQL query with placeholders for values
    sql_command = """
    INSERT INTO batch_scheduler.scheduler_users
    (id, user_id, user_name, user_type, user_status, user_pwd, celp_tlno, email_addr, lgin_fail_ncnt, last_pwd_chg_date, last_lgin_timr, frst_reg_date, frst_reg_user_id, last_reg_user_id, last_chg_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Generate a UUID and hash the password
    id = str(uuid.uuid4())
    hashed_password = common_func.sha256_hash('tango1q2w3e4r%T')

    # Define the values for the placeholders
    values = (
        id,  # id
        'admin',
        'admin',  # user_name
        0,  # user_type
        True,  # user_status
        hashed_password,  # user_pwd
        '',  # celp_tlno
        '',  # email_addr
        0,  # lgin_fail_ncnt
        common_func.get_current_epoch_time_seconds(),  # last_pwd_chg_date
        0,  # last_lgin_timr
        0,  # frst_reg_date
        id,  # frst_reg_user_id
        id,  # last_reg_user_id
        0   # last_chg_date
    )

    # Execute the command with the parameters
    cursor.execute(sql_command, values)
    cursor.connection.commit()
    print('result', cursor.rowcount)


def __get_unfinished_record(cursor):
    try:
        print('============ GET UNFINISHED RECORDS ============')
        cursor.execute("""
            SELECT rl.*, sj.restartable 
            from scheduler_job_run_logs rl 
            join scheduler_jobs sj on rl.job_id = sj.job_id
            where sj.restartable is true 
            and rl.operation in ('CREATE', 'RUNNING', 'SCHEDULED', 'READY_TO_RUN', 'RETRY_SCHEDULED');
        """)
        record = cursor.fetchall()
        record = [dict(x) for x in record]
        un_sync_result = [x.get('celery_task_name') for x in record]
        print(f'un_sync_result: {len(un_sync_result)}')
        return un_sync_result

    except Exception as e:
        raise Exception(f'Exception: {e}')


def __get_result_from_celery(un_sync_result):
    try:
        print('============ GET RESULT FROM CELERY ============')
        url = f'{CELERY.HOST}:{CELERY.PORT}/celerytasks/get_task_detail/'

        http = urllib3.PoolManager()
        headers = {"Content-Type": "application/json"}
        response = http.urlopen('POST', url, headers=headers, body=json.dumps({
            'tasks': un_sync_result
        }))
        print(f"response status: {response.status}")

        if response.status != 200 or json.loads(response.data).get('status') != 'success':
            print(f"response data: {response.data}")
            raise Exception(str(response.data))

        response_data = json.loads(response.data)
        celery_result = response_data.get('tasks') or []
        print(f'celery_result: {len(celery_result)}')
        return celery_result

    except Exception as e:
        raise Exception(f'Exception: {e}')


def __update_records_data(cursor, records):
    try:
        print('============ UPDATE RECORDS DATA ============')
        attribute_list = [
            'celery_task_name',
            'operation',
            'status',
            'error_no',
            'req_start_date',
            'actual_start_date',
            'actual_end_date',
            'run_duration',
            'errors',
            'output'
        ]

        update_data = []
        for record in records:
            update_query = []
            for key in attribute_list:
                if key in {'error_no', 'req_start_date', 'actual_start_date', 'actual_end_date'}:
                    update_query.append(str(record.get(key) or 0))
                else:
                    update_query.append(str(record.get(key) or ''))

            update_data.append(f"({','.join(update_query)})")

        update_data_str = ','.join(update_data)

        update_script = f"""
                update scheduler_job_run_logs as t set
                celery_task_name = c.celery_task_name,
                operation = c.operation,
                status = c.status,
                error_no = c.error_no,
                req_start_date = c.req_start_date,
                actual_start_date = c.actual_start_date,
                run_duration = c.run_duration::interval,
                errors = c.errors,
                output = c.output
            from (values {update_data_str}) 
            as c({', '.join(attribute_list)}) 
            where c.celery_task_name = t.celery_task_name;
        """

        cursor.execute(update_script)
        cursor.connection.commit()
        print('result', cursor.rowcount)

    except Exception as e:
        raise Exception(f'Exception: {e}')


if __name__ == '__main__':
    sync_tasks()
