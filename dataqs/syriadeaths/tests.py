import glob
import os
import datetime
from django.test import TestCase
from dataqs.syriadeaths.syria_deaths import SyriaDeathsProcessor, PROV_DICT


class SyriaDeathsProcessorTest(TestCase):
    """
    Tests the dataqs.syriadeaths module.  Since each processor is highly
    dependent on a running GeoNode instance for most functions, only
    independent functions are tested here.
    """

    def setUp(self):
        self.processor = SyriaDeathsProcessor()

    def test_download(self):
        """
        Verify that rows are downloaded and contain expected data.
        """
        html = self.processor.download()
        self.assertIsNotNone(html)

    def test_process(self):
        today = datetime.datetime.utcnow()
        html = self.processor.download()
        rows = self.processor.process_rows(html)
        for row in rows:
            self.assertEquals(len(row), 5)
            self.assertTrue(2010 <= row[0] <= today.year)
            self.assertTrue(row[4] in PROV_DICT.values())
