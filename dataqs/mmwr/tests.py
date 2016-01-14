import glob
import os
import datetime
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
        report_date = datetime.datetime.today()
        self.processor.generate_csv(report_date)
        self.assertTrue(os.path.exists(os.path.join(
            self.processor.tmp_dir, '{}.csv'.format(self.processor.prefix))))


    def test_generate_csv_by_date(self):
        """
        Verify that a file is downloaded for the specified date
        """
        report_date = datetime.date(2015, 1, 10)
        self.processor.cleanup()
        self.assertFalse(os.path.exists(os.path.join(
            self.processor.tmp_dir, '{}.csv'.format(self.processor.prefix))))
        self.processor.generate_csv(report_date)
        self.assertTrue(os.path.exists(os.path.join(
            self.processor.tmp_dir, '{}.csv'.format(self.processor.prefix))))
        self.processor.cleanup()
        report_date = datetime.date(2015, 11, 7)
        self.assertFalse(os.path.exists(os.path.join(
            self.processor.tmp_dir, '{}.csv'.format(self.processor.prefix))))
        self.processor.generate_csv(report_date)
        self.assertTrue(os.path.exists(os.path.join(
            self.processor.tmp_dir, '{}.csv'.format(self.processor.prefix))))


    def test_cleanup(self):
        self.processor.cleanup()
        self.assertEquals([], glob.glob(os.path.join(
            self.processor.tmp_dir, self.processor.prefix + '*')))
