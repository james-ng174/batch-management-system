from django.urls import path
from .views import (health_check, rrule_schedule_task, manually_run, force_terminate_task, update_state_job,
                    update_job_function, get_task_detail, assign_job_to_workflow)

urlpatterns = [
    path('create_rrule_task/', rrule_schedule_task, name='rrule_schedule_task'),
    path('force_stop/', force_terminate_task, name='force_stop'),
    path('manually_run/', manually_run, name='manually_run'),
    path('health/', health_check, name='health_check'),
    path('update_job/', update_job_function, name='update_job'),
    path('update_job_status/', update_state_job, name='update_job_status'),
    path('get_task_detail/', get_task_detail, name='get_task_detail'),
    path('assign_job_to_workflow/', assign_job_to_workflow, name='assign_job_to_workflow')
]
