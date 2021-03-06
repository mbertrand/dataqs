from __future__ import absolute_import

from celery import shared_task
from dataqs.spei.spei import SPEIProcessor


@shared_task
def spei_task():
    processor = SPEIProcessor()
    processor.run()
