from __future__ import absolute_import

from celery import shared_task
from .healthmap import HealthMapProcessor


@shared_task
def healthmap90days_task():
    processor = HealthMapProcessor()
    processor.run()
