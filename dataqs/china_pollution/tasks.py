from __future__ import absolute_import

from celery import shared_task
from .china_pollution import ChinaPollutionIPEProcessor



@shared_task
def chinapollution_task():
    processor = ChinaPollutionIPEProcessor()
    processor.run()