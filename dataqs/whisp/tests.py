import glob
import json
import bs4
import os
import datetime
from django.test import TestCase
from dataqs.whisp.whisp import WhispProcessor
import unicodecsv as csv
import mock

script_dir = os.path.dirname(os.path.realpath(__file__))


def mock_download(self):
    with open(os.path.join(script_dir, 'resources/test_whisp.html')) as html:
        return html.read()


def mock_insert_row(self, data):
    with open(os.path.join(
            self.tmp_dir, '().json'.format(self.prefix)), 'w') as testfile:
        json.dump(data, testfile)

class WhispTest(TestCase):
    """
    Tests the dataqs.whisp module.  Since each processor is highly
    dependent on a running GeoNode instance for most functions, only
    independent functions are tested here.
    """

    def setUp(self):
        self.processor = WhispProcessor()

    @mock.patch('dataqs.whisp.whisp.WhispProcessor.insert_row', mock_insert_row)
    @mock.patch('dataqs.whisp.whisp.WhispProcessor.download', mock_download)
    def test_scrape(self):
        """
        Verify that the correct records can be read from html
        :return:
        """
        self.processor.scrape()
        testfile = os.path.join(
            self.processor.tmp_dir, '().json'.format(self.processor.prefix))
        self.assertTrue(os.path.exists(testfile))
        with open(testfile) as test:
            test_json = json.load(test)
            self.assertTrue(test_json['eventtype'])
            self.assertTrue(test_json['the_geom'])
        self.processor.cleanup()

    @mock.patch('dataqs.whisp.whisp.WhispProcessor.insert_row', mock_insert_row)
    @mock.patch('dataqs.whisp.whisp.WhispProcessor.download', mock_download)
    def test_archive_import(self):
        """
        Verify that the correct records can be read from archive
        :return:
        """
        self.processor.import_archive()
        testfile = os.path.join(
            self.processor.tmp_dir, '().json'.format(self.processor.prefix))
        self.assertTrue(os.path.exists(testfile))
        with open(testfile) as test:
            test_json = json.load(test)
            self.assertTrue(test_json['eventtype'])
            self.assertTrue(test_json['the_geom'])
        self.processor.cleanup()


    def test_cleanup(self):
        self.processor.cleanup()
        self.assertEquals([], glob.glob(os.path.join(
            self.processor.tmp_dir, self.processor.prefix + '*')))
