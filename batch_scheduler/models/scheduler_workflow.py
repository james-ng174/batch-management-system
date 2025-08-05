import uuid
from sqlalchemy import ForeignKey, BigInteger, UniqueConstraint
from app import db
from utils import common_func


class SchedulerWorkflow(db.Model):
    __tablename__ = 'scheduler_workflow'
    id = db.Column(db.String(), primary_key=True)
    workflow_name = db.Column(db.String())
    latest_status = db.Column(db.String())
    group_id = db.Column(db.String(), ForeignKey('scheduler_job_groups.group_id'))
    frst_reg_date = db.Column(BigInteger)
    frst_reg_user_id = db.Column(db.String())
    last_reg_user_id = db.Column(db.String())
    last_chg_date = db.Column(BigInteger)
    __table_args__ = (UniqueConstraint("workflow_name", name="workflow_name_table_unique"),)

    def __init__(self, params):
        self.id = str(uuid.uuid4())
        self.workflow_name = params.get('workflow_name')
        self.latest_status = params.get('latest_status')
        self.group_id = params.get('group_id')
        self.frst_reg_date = common_func.get_current_utc_time(in_epoch=True)
        self.frst_reg_user_id = params.get('frst_reg_user_id')
        self.last_chg_date = params.get('last_chg_date')
        self.last_reg_user_id = params.get('last_reg_user_id')

    def __repr__(self):
        return '<id {}>'.format(self.id)
