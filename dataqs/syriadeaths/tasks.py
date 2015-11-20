from __future__ import absolute_import

from celery import shared_task
from dataqs.syriadeaths.syria_deaths import SyriaDeathsProcessor

@shared_task
def syria_deaths_task():
    processor = SyriaDeathsProcessor()
    processor.run()
