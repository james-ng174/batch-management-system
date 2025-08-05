import uuid
from sqlalchemy import ForeignKey, Interval, Integer, Boolean, BigInteger, UniqueConstraint, DateTime
from app import db
from utils import common_func, secret_helper


class SchedulerUsers(db.Model):
    __tablename__ = 'scheduler_users'

    id = db.Column(db.String(), primary_key=True)
    user_id = db.Column(db.String())
    user_name = db.Column(db.String())
    user_type = db.Column(db.Integer())
    user_status = db.Column(Boolean, default=True)
    user_pwd = db.Column(db.String())
    celp_tlno = db.Column(db.String())
    email_addr = db.Column(db.String())
    lgin_fail_ncnt = db.Column(db.Integer(), default=0)
    last_pwd_chg_date = db.Column(BigInteger)
    last_lgin_timr = db.Column(BigInteger)
    frst_reg_date = db.Column(BigInteger)
    frst_reg_user_id = db.Column(db.String())
    last_reg_user_id = db.Column(db.String())
    last_chg_date = db.Column(BigInteger)
    last_activity_time = db.Column(BigInteger)
    # This is due to Alembic will not autodetect anonymous constraints
    # Sample if need multiple columns:
    # __table_args__ = (UniqueConstraint('column 1', 'column 2', name='name'),)
    __table_args__ = (UniqueConstraint("user_id", name="user_table_unique"),)

    def __init__(self, params):
        self.id = str(uuid.uuid4())
        self.user_id = params.get('user_id')
        self.user_name = params.get('user_name')
        self.user_pwd = params.get('user_pwd')
        self.user_type = params.get('user_type')
        self.celp_tlno = secret_helper.encrypt_data(params.get('celp_tlno'))
        self.email_addr = secret_helper.encrypt_data(params.get('email_addr'))
        self.user_status = True
        self.frst_reg_date = common_func.get_current_utc_time(in_epoch=True)
        self.last_chg_date = params.get('last_chg_date')
        self.last_pwd_chg_date = params.get('last_pwd_chg_date')
        self.last_activity_time = 0

    def __repr__(self):
        return '<id {}>'.format(self.id)
