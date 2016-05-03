from __future__ import absolute_import
import logging
import os
import datetime
from zipfile import ZipFile

import unicodecsv as csv
import json

from geonode.geoserver.helpers import ogc_server_settings
from dataqs.processor_base import GeoDataProcessor
from dataqs.helpers import get_band_count, gdal_translate, cdo_invert, ogr2ogr_exec

logger = logging.getLogger("dataqs.processors")
script_dir = os.path.dirname(os.path.realpath(__file__))


class TychoProcessor(GeoDataProcessor):
    """
    Class for processing TYCHO Level 1 and Level 2 Data
    (disease counts from NNDS 1888-2013)
    """
    prefix = "tycho"
    base_url = "http://www.healthdata.gov/sites/default/files/"
    layers = {
        "tycho1": "ProjectTycho_Level1_v1.0.0.csv",
        "tycho2": "ProjectTycho_Level2_v1.1.0_0.zip"
    }
    cities = {}
    states = {}

    def __init__(self, *args):
        super(TychoProcessor, self).__init__(*args)
        with open(os.path.join(
                script_dir, 'resources/city_latlon.json')) as incity:
            self.cities = json.load(incity)
        with open(os.path.join(
                script_dir, 'resources/state_latlon.json')) as instate:
            self.states = json.load(instate)

    def process(self, csvfile, geojson_file=None):
        """
        Convert csv file to a geojson file
        :param csvfile: csv file to convert
        :param geojson_file: geojson file to create
        :return: geojson_file
        """
        if not geojson_file:
            geojson_file = os.path.join(
                self.tmp_dir, '{}.json'.format(self.prefix))
        with open(geojson_file, 'w') as outjson:
            outjson.write('{"type": "FeatureCollection","features": [')
            with open(os.path.join(self.tmp_dir, csvfile)) as csvin:
                reader = csv.DictReader(csvin)
                for i, row in enumerate(reader):
                    row['epi_week'] = datetime.datetime.strptime(
                        row['epi_week'], '%Y%W').isoformat()
                    if row['loc_type'] == 'STATE':
                        coords = self.states[row['state']]
                    else:
                        coords = self.cities['{},{}'.format(
                            row['loc'], row['state'])]
                    feature = {
                        "type": "Feature",
                        "properties": {
                        },
                        "geometry": {
                            "type": "Point",
                            "coordinates": coords
                        }
                    }
                    for item in row.keys():
                        feature['properties'][item] = row[item]
                    jsonstr = '{}{}'.format(
                        ',' if i > 0 else '', json.dumps(feature))
                    outjson.writelines(jsonstr)
            outjson.write(']}')
        return geojson_file

    def run(self):
        """
        Retrieve and process Tycho 1 & 2 data
        """
        for layer in self.layers:
            file = self.layers[layer]
            data = self.download(self.base_url + file)
            if file.endswith('zip'):
                zfile = ZipFile(os.path.join(self.tmp_dir, data))
                zfile.extractall(self.tmp_dir)
                data = file.replace('_0.zip', '.csv')
            processed = self.process(data)
            db = ogc_server_settings.datastore_db
            ogr2ogr_exec("-overwrite -skipfailures -f PostgreSQL \
                \"PG:host={db_host} user={db_user} password={db_pass} \
                dbname={db_name}\" {geojson} -nln {table}".format(
                db_host=db["HOST"], db_user=db["USER"], db_pass=db["PASSWORD"],
                db_name=db["NAME"], geojson=processed, table=self.prefix))
            self.post_geoserver_vector(self.prefix)
            self.update_geonode(self.prefix)
            self.truncate_gs_cache(self.prefix)
        self.cleanup()

if __name__ == '__main__':
    processor = TychoProcessor()
    processor.run()