import uuid
from app import db
from sqlalchemy import ForeignKey, UniqueConstraint, BigInteger
from sqlalchemy.orm import relationship
from utils import common_func


class SchedulerJobGroups(db.Model):
    __tablename__ = 'scheduler_job_groups'

    group_id = db.Column(db.String(), primary_key=True)
    group_name = db.Column(db.String(128))
    group_comments = db.Column(db.String(4000))
    jobs = relationship("SchedulerJobs", viewonly=True)
    frst_reg_date = db.Column(BigInteger)
    frst_reg_user_id = db.Column(db.String(), ForeignKey('scheduler_users.id'))
    last_chg_date = db.Column(BigInteger)
    last_reg_user_id = db.Column(db.String(), ForeignKey('scheduler_users.id'))
    # This is due to Alembic will not autodetect anonymous constraints
    # Sample if need multiple columns:
    # __table_args__ = (UniqueConstraint('column 1', 'column 2', name='name'),)
    __table_args__ = (UniqueConstraint("group_name", name="group_table_unique"),)

    def __init__(self, params):
        self.group_id = str(uuid.uuid4())
        self.group_name = params.get('group_name')
        self.group_comments = params.get('group_comments')
        self.frst_reg_date = common_func.get_current_utc_time(in_epoch=True)
        self.frst_reg_user_id = params.get('frst_reg_user_id')
        self.last_chg_date = params.get('last_chg_date')
        self.last_reg_user_id = params.get('last_reg_user_id')

    def __repr__(self):
        return '<id {}>'.format(self.group_id)
