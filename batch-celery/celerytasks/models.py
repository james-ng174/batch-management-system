from __future__ import unicode_literals

from django.db import models
from django.utils import timezone
from django_celery_beat.models import PeriodicTask


class JobSettings(models.Model):
    job_id = models.CharField(max_length=255, primary_key=True)
    queue_name = models.CharField(max_length=255, blank=True, null=True)
    function_name = models.CharField(max_length=255, blank=False, null=False)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    repeat_interval = models.TextField(max_length=4000, blank=True, null=True)
    max_run_duration = models.DurationField(blank=True, null=True)
    max_run = models.IntegerField(blank=True, null=True)
    max_failure = models.IntegerField(blank=True, null=True)
    retry_delay = models.IntegerField(blank=True, null=True)
    priority = models.IntegerField(blank=True, null=True)
    is_enable = models.BooleanField(default=False)
    auto_drop = models.BooleanField(default=False)
    restart_on_failure = models.BooleanField(default=False)
    restartable = models.BooleanField(default=False)
    run_account = models.CharField(max_length=255, blank=True, null=True)
    run_forever = models.BooleanField(default=False)
    job_operation = models.CharField(max_length=255, blank=True, null=True)
    run_number = models.IntegerField(blank=False, null=False, default=0)
    last_task_done = models.BooleanField(default=False)
    timezone = models.CharField(max_length=255, blank=True, null=True)

    class JobTypeEnum(models.TextChoices):
        REST_API = 'REST_API', 'REST API'
        EXECUTABLE = 'EXECUTABLE', 'Executable'

    job_type = models.CharField(max_length=50, choices=JobTypeEnum.choices, default=JobTypeEnum.EXECUTABLE)
    job_action = models.TextField(blank=True, null=True)
    job_body = models.JSONField(blank=True, null=True)
    run_count = models.IntegerField(default=0)
    failure_count = models.IntegerField(default=0)
    retry_count = models.IntegerField(default=0)
    create_at = models.DateTimeField(auto_now_add=True)
    update_at = models.DateTimeField(auto_now=True)
    next_run_date = models.DateTimeField(blank=True, null=True)
    ignore_result = models.BooleanField(default=False)
    STATUS_CREATED = 'created'
    STATUS_SUCCESS = 'succeed'
    STATUS_FAILURE = 'failed'
    STATUS_NONE = 'none'
    STATUS_CHOICES = (
        (STATUS_CREATED, 'Created'),
        (STATUS_SUCCESS, 'Succeed'),
        (STATUS_FAILURE, 'Failed'),
        (STATUS_NONE, 'None')
    )

    last_result = models.CharField(max_length=255, choices=STATUS_CHOICES,
                                   default=STATUS_CREATED)
    workflow_id = models.CharField(max_length=255, blank=True, null=True)

    def increase_run_count(self):
        self.run_count += 1
        self.save(update_fields=('run_count',))
        return self

    def update_operation(self, operation):
        self.job_operation = operation
        self.last_task_done = False
        self.save(update_fields=('job_operation', 'last_task_done',))
        return self

    def increase_failure_count(self):
        self.failure_count += 1
        self.save(update_fields=('failure_count',))
        return self

    def increase_retry_count(self):
        self.retry_count += 1
        self.save(update_fields=('retry_count',))
        return self

    def update_status(self, status):
        self.is_enable = status
        self.save(update_fields=('is_enable',))
        return self

    def save_next_run(self, next_run_date):
        self.next_run_date = next_run_date
        self.save(update_fields=('next_run_date',))
        return self

    def save_operation(self, operation, task_status):
        self.job_operation = operation
        status = JobSettings.STATUS_SUCCESS if task_status == TaskDetail.STATUS_SUCCESS else JobSettings.STATUS_FAILURE
        self.last_result = status
        self.last_task_done = True
        self.run_number += 1
        self.save(update_fields=('job_operation', 'last_result', 'run_number', 'last_task_done',))
        return self

    def update_workflow(self, workflow_id, priority, ignore_result):
        self.workflow_id = workflow_id
        self.priority = priority
        self.ignore_result = ignore_result
        self.save(update_fields=('workflow_id', 'priority', 'ignore_result'))
        return self

    class Meta:
        db_table = 'celery_job_settings'


class WorkflowRunDetail(models.Model):
    id = models.AutoField(primary_key=True, editable=False)
    job_id = models.CharField(max_length=255, blank=True, null=True)
    run_forever = models.BooleanField(default=False)
    workflow_id = models.CharField(max_length=255, blank=True, null=True)
    priority = models.IntegerField(blank=True, null=True)
    # TODO: Remove ???
    task_name = models.CharField(max_length=255, blank=True, null=True)
    previous_job_id = models.CharField(max_length=255, blank=True, null=True)
    previous_task_name = models.CharField(max_length=255, blank=True, null=True)
    run_number = models.IntegerField(blank=True, null=True)

    class Meta:
        db_table = 'celery_workflow_run_detail'


class TaskDetail(models.Model):
    id = models.AutoField(primary_key=True, editable=False)
    job = models.ForeignKey(JobSettings, on_delete=models.CASCADE)
    celery_task_id = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    run_duration = models.DurationField(blank=True, null=True)
    run_date = models.DateTimeField(blank=False, null=False)
    task_name = models.CharField(max_length=255, blank=False, null=False)
    run_account = models.CharField(max_length=255, blank=True, null=True)
    retry_count = models.IntegerField(blank=True, null=True, default=0)
    run_number = models.IntegerField(blank=False, null=False, default=0)
    actual_start_date = models.DateTimeField(blank=True, null=True)

    STATUS_CREATED = 'created'
    STATUS_RUNNING = 'running'
    STATUS_SUCCESS = 'succeed'
    STATUS_FAILURE = 'failed'
    STATUS_CANCELLED = 'cancelled'
    STATUS_STOPPED = 'stopped'
    STATUS_NONE = 'none'
    STATUS_CHOICES = (
        (STATUS_CREATED, 'Created'),
        (STATUS_RUNNING, 'Running'),
        (STATUS_SUCCESS, 'Succeed'),
        (STATUS_FAILURE, 'Failed'),
        (STATUS_CANCELLED, 'Cancelled'),
        (STATUS_STOPPED, 'Stopped'),
        (STATUS_NONE, 'None')
    )

    status = models.CharField(max_length=255, choices=STATUS_CHOICES,
                              default=STATUS_CREATED)
    already_run = models.BooleanField(default=False)
    soft_delete = models.BooleanField(default=False)
    manually_run = models.BooleanField(default=False)
    previous_task_id = models.CharField(max_length=255, blank=True, null=True)
    task_result = models.CharField(max_length=255, blank=True, null=True)

    def save_status(self, status):
        """Sets and saves the status to the scheduled task.

        Arguments:
            status (str): Status.

        Returns:
            ScheduledTask: Current scheduled task instance.
        """
        self.status = status
        self.save(update_fields=('status',))
        return self

    def start_running(self, task_id, run_account):
        # self.run_date = timezone.now().replace(second=0, microsecond=0)
        self.run_date = timezone.now().replace(microsecond=0)
        self.celery_task_id = task_id
        self.status = self.STATUS_NONE
        self.run_account = run_account
        self.save(update_fields=('run_date', 'status', 'celery_task_id', 'run_account'))
        return self

    def save_actual_start_date(self, actual_start_date=None):
        if actual_start_date is None:
            # self.actual_start_date = timezone.now().replace(second=0, microsecond=0)
            self.run_date = timezone.now().replace(microsecond=0)
        else:
            self.actual_start_date = actual_start_date
        self.save(update_fields=('actual_start_date',))

    def count_duration(self):
        # now = timezone.now().replace(second=0, microsecond=0)
        now = timezone.now().replace(microsecond=0)
        if self.actual_start_date:
            self.run_duration = now - self.actual_start_date
        else:
            self.run_duration = now - self.run_date
        self.save()
        return self

    def soft_delete_func(self):
        self.soft_delete = True
        self.save(update_fields=('soft_delete',))

    def mark_as_ran(self):
        self.already_run = True
        self.task_result = 'RUNNING'
        self.save(update_fields=('already_run', 'task_result',))
        return self

    def increase_retry_count(self):
        self.retry_count += 1
        self.save(update_fields=('retry_count',))
        return self

    def check_final_state(self):
        if self.status == TaskDetail.STATUS_SUCCESS:
            return True
        elif self.status == TaskDetail.STATUS_FAILURE:
            return True
        elif self.status == TaskDetail.STATUS_CANCELLED:
            return True
        else:
            return False

    class Meta:
        db_table = 'celery_task_detail'
