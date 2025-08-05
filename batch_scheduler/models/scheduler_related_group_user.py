import uuid
from app import db
from sqlalchemy import ForeignKey


class SchedulerRelatedGroupUser(db.Model):
    __tablename__ = 'scheduler_related_group_user'

    id = db.Column(db.String(), primary_key=True)
    group_id = db.Column(db.String(), ForeignKey('scheduler_job_groups.group_id'))
    user_id = db.Column(db.String(), ForeignKey('scheduler_users.id'))

    def __init__(self, params):
        self.id = str(uuid.uuid4())
        self.group_id = params.get('group_id')
        self.user_id = params.get('user_id')

    def __repr__(self):
        return '<id {}>'.format(self.group_id)
