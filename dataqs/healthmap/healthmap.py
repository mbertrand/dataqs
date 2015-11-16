from __future__ import absolute_import

import json
import logging
import urllib
import os
import datetime
from pymongo import MongoClient, GEOSPHERE
from django.conf import settings
from ..processor_base import GeoDataProcessor

logger = logging.getLogger("dataqs.processors")

MONGODB = getattr(settings, 'MONGODB', {'HOST': 'localhost', 'PORT': 27017, 'NAME': 'healthmap'})
HEALTHMAP_AUTH = getattr(settings, 'HEALTHMAP_AUTH', '')


class HealthMapProcessor(GeoDataProcessor):
    """
    Retrieve latest data from Healthmap for a given API account
    and update the relevant collection in a Mongo database
    """

    base_url = "http://www.healthmap.org/HMapi.php?auth={}&".format(getattr(settings, 'HEALTHMAP_AUTH', None))
    collection = 'healthmap90days'
    dbname = MONGODB['NAME']

    healthmap_params = {}
    prefix = "healthmap90days"

    def __init__(self, *args, **kwargs):
        for key in kwargs.keys():
            if key == 'collection':
                self.collection = kwargs['collection']
            else:
                self.healthmap_params[key] = kwargs.get(key)
        self.tmp_collection = 'tmp_{}'.format(self.collection)

        if 'cutoff' not in self.healthmap_params:
            today = datetime.date.today()
            self.healthmap_params['cutoff'] = (
                today - datetime.timedelta(days=90)).strftime("%Y-%m-%d")

        if 'sdate' not in self.healthmap_params:
            today = datetime.date.today()
            self.healthmap_params['sdate'] = (
                today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

        super(HealthMapProcessor, self).__init__(*args, **kwargs)


    def init_db(self, db):
        """
        Initiate the pubmed collection with indices if it doesn't exist yet
        :param db: The Mongo database object to create the collection in
        :return: None
        """
        for coll in [self.collection, self.tmp_collection, 'hmalternate']:
            if coll not in db.collection_names():
                db.create_collection(coll)
                db[coll].create_index("country")
                db[coll].create_index("place_name")
                db[coll].create_index([("the_geom", GEOSPHERE)])

    def download(self, url, filename=None):
        """
        Retrieve HealthMap JSON
        :param url: The URL to download from
        :param username: Required username?
        :param password: Required password?
        :return: Name of the downloaded file (not including path).
        """
        return super(HealthMapProcessor, self).download(url + urllib.urlencode(self.healthmap_params),
                                                        filename=filename)


    def update_db(self, filename):
        client = MongoClient(host=MONGODB['HOST'], port=MONGODB['PORT'])
        db = client[self.dbname]
        tmp_collection = db[self.tmp_collection]
        tmp_collection.drop()
        self.init_db(db)
        alerts = []
        with open(os.path.join(self.tmp_dir, filename)) as jsonfile:
            jsondata = json.loads(jsonfile.read())
            for item in jsondata:
                for alert in item['alerts']:
                    alert['country'] = item['country']
                    alert['place_name'] = item['place_name']
                    alert['lat'] = float(item['lat'])
                    alert['lng'] = float(item['lng'])
                    alert['country_id'] = item['country_id']
                    alert['the_geom'] = {
                        'type': 'Point',
                        'coordinates': [alert['lng'], alert['lat']]
                    }
                    alerts.append(alert)
        alerts_json = json.loads(json.dumps(alerts))
        for alert in alerts_json:
            try:
                db[self.collection].update(alert, alert, upsert=True)
            except Exception as e:
                if "Can't extract geo keys" in e.message:
                    logger.error(e.message)
                    pass
        db[self.collection].remove(
            {'date': {'$lt': self.healthmap_params['cutoff']}})

    def run(self):
        output_file = self.download(self.base_url, filename='{}.json'.format(self.prefix))
        self.update_db(output_file)
        self.update_geonode(self.prefix)
        self.truncate_gs_cache(self.prefix)
        self.cleanup()
