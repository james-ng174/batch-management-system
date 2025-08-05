from app import db
from sqlalchemy import ForeignKey, UniqueConstraint, BigInteger
from utils import common_func


class SchedulerJobServers(db.Model):
    __tablename__ = 'scheduler_job_servers'

    system_id = db.Column(db.String(), primary_key=True)
    system_name = db.Column(db.String(128))
    host_name = db.Column(db.String(128))
    host_ip_addr = db.Column(db.String(128))
    secondary_host_ip_addr = db.Column(db.String(128))
    system_comments = db.Column(db.String(4000))
    queue_name = db.Column(db.String())
    folder_path = db.Column(db.String())
    secondary_folder_path = db.Column(db.String())
    ssh_user = db.Column(db.String())
    frst_reg_date = db.Column(BigInteger)
    frst_reg_user_id = db.Column(db.String(), ForeignKey('scheduler_users.id'))
    last_chg_date = db.Column(BigInteger)
    last_reg_user_id = db.Column(db.String(), ForeignKey('scheduler_users.id'))
    # This is due to Alembic will not autodetect anonymous constraints
    # Sample if need multiple columns:
    # __table_args__ = (UniqueConstraint('column 1', 'column 2', name='name'),)
    __table_args__ = (UniqueConstraint("system_name", name="server_table_unique"),)

    def __init__(self, params):
        self.system_id = params.get('system_id')
        self.system_name = params.get('system_name')
        self.host_name = params.get('host_name')
        self.host_ip_addr = params.get('host_ip_addr')
        self.secondary_host_ip_addr = params.get('secondary_host_ip_addr')
        self.system_comments = params.get('system_comments')
        self.queue_name = params.get('queue_name')
        self.folder_path = params.get('folder_path')
        self.secondary_folder_path = params.get('secondary_folder_path')
        self.ssh_user = params.get('ssh_user')
        self.frst_reg_date = common_func.get_current_utc_time(in_epoch=True)
        self.frst_reg_user_id = params.get('frst_reg_user_id')
        self.last_chg_date = params.get('last_chg_date')
        self.last_reg_user_id = params.get('last_reg_user_id')

    def __repr__(self):
        return '<id {}>'.format(self.system_id)
