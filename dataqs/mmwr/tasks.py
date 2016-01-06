from __future__ import absolute_import

from celery import shared_task
from dataqs.mmwr.mmwwr import MortalityProcessor


@shared_task()
def mmwwr_task():
    processor = MortalityProcessor()
    processor.run()
