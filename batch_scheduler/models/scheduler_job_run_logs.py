from app import db
from sqlalchemy import ForeignKey, Interval, BigInteger, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from utils import common_func


class SchedulerJobRunLogs(db.Model):
    __tablename__ = 'scheduler_job_run_logs'

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    celery_task_name = db.Column(db.String())
    log_date = db.Column(BigInteger)
    system_id = db.Column(db.String(), ForeignKey('scheduler_job_servers.system_id'))
    group_id = db.Column(db.String(), ForeignKey('scheduler_job_groups.group_id'))
    job_id = db.Column(db.String(), ForeignKey('scheduler_jobs.job_id'))
    system_name = db.Column(db.String())
    group_name = db.Column(db.String())
    job_name = db.Column(db.String())
    operation = db.Column(db.String())
    batch_type = db.Column(db.String(), default='Auto')
    status = db.Column(db.String())
    retry_count = db.Column(db.Integer)
    user_name = db.Column(db.String())
    error_no = db.Column(db.Integer)
    req_start_date = db.Column(BigInteger)
    actual_start_date = db.Column(BigInteger)
    actual_end_date = db.Column(BigInteger)
    run_duration = db.Column(Interval)
    additional_info = db.Column(db.String())
    errors = db.Column(db.String())
    output = db.Column(db.String())
    last_log = relationship("SchedulerJobLastLog", back_populates="run_log")
    __table_args__ = (
        Index('idx_job_system_group', 'job_id', 'group_id', 'system_id'),
    )

    def __init__(self, params):
        self.log_id = params.get('log_id')
        self.celery_task_name = params.get('celery_task_name')
        self.log_date = common_func.get_current_epoch_time_millis()
        self.system_id = params.get('system_id')
        self.group_id = params.get('group_id')
        self.job_id = params.get('job_id')
        self.system_name = params.get('system_name')
        self.group_name = params.get('group_name')
        self.job_name = params.get('job_name')
        self.operation = params.get('operation')
        self.batch_type = params.get('batch_type')
        self.status = params.get('status')
        self.retry_count = params.get('retry_count')
        self.user_name = params.get('user_name')
        self.error_no = params.get('error_no')
        self.req_start_date = params.get('req_start_date')
        self.actual_start_date = params.get('actual_start_date')
        self.actual_end_date = params.get('actual_end_date')
        self.run_duration = params.get('run_duration')
        self.additional_info = params.get('additional_info')
        self.errors = params.get('errors')
        self.output = params.get('output')

    def __repr__(self):
        return '<id {}>'.format(self.log_id)
