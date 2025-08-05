from django.urls import path
from .views import (health_check, rrule_schedule_task, force_terminate_task)

urlpatterns = [
    path('create_rrule_task/', rrule_schedule_task, name='rrule_schedule_task'),
    path('force_stop/', force_terminate_task, name='force_stop'),
    path('health/', health_check, name='health_check'),
]
