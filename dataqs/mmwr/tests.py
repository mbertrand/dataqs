import glob
import os
from datetime import datetime
from django.test import TestCase
from dataqs.mmwr.mmwr import MortalityProcessor


class MMWRTest(TestCase):
    """
    Tests the dataqs.mmwr module.  Since each processor is highly
    dependent on a running GeoNode instance for most functions, only
    independent functions are tested here.
    """

    def setUp(self):
        self.processor = MortalityProcessor()

    def test_generate_csv(self):
        """
        Verify that a file is downloaded
        """
        report_date = datetime.today()
        self.processor.test_generate_csv(report_date)
        self.assertTrue(os.path.exists(os.path.join(
            self.processor.tmp_dir, '{}.csv'.format(self.processor.prefix))))

    def test_cleanup(self):
        self.processor.cleanup()
        self.assertEquals([], glob.glob(os.path.join(
            self.processor.tmp_dir, self.processor.prefix + '*')))
