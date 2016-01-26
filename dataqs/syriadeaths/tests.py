import httpretty
import os
from django.test import TestCase
from dataqs.syriadeaths.syria_deaths import SyriaDeathsProcessor, PROV_DICT
from dateutil.parser import parse
script_dir = os.path.dirname(os.path.realpath(__file__))


def test_data():
    with open(os.path.join(script_dir, 'resources/test_syria.html')) as html:
        return html.read()


class SyriaDeathsProcessorTest(TestCase):
    """
    Tests the dataqs.syriadeaths module.  Since each processor is highly
    dependent on a running GeoNode instance for most functions, only
    independent functions are tested here.
    """

    def setUp(self):
        self.processor = SyriaDeathsProcessor()
        httpretty.enable()

    def tearDown(self):
        httpretty.disable()
        self.processor.cleanup()

    def test_download(self):
        """
        Verify that rows are downloaded and contain expected data.
        """
        httpretty.register_uri(
            httpretty.GET,
            self.processor.base_url,
            body=test_data(),
            content_type='text/html')
        html = self.processor.download()
        self.assertIsNotNone(html)
        self.assertEquals(html, test_data())

    def test_process(self):
        httpretty.register_uri(
            httpretty.GET,
            self.processor.base_url,
            body=test_data(),
            content_type='text/html')
        html = self.processor.download()
        rows = self.processor.process_rows(html)
        for row in rows:
            self.assertEquals(len(row), 5)
            date_from_str = parse(row[2])
            self.assertEquals(row[0], date_from_str.year)
            self.assertEquals(row[1], date_from_str.month)
            self.assertTrue(isinstance(row[3], int))
            self.assertIn(row[4], PROV_DICT.values())
