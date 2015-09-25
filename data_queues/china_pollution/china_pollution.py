from __future__ import absolute_import

import json
import logging
import os
import requests
from geonode.geoserver.helpers import ogc_server_settings
from ..csv_helpers import UnicodeWriter
from ..helpers import ogr2ogr_exec
from ..processor_base import GeoDataProcessor

logger = logging.getLogger("geonodegp.data_queues.processors")

class ChinaPollutionIPEProcessor(GeoDataProcessor):

    prefix = "cn_pollution"
    years = [x for x in xrange(2006,2016)]
    valleys = ["{0:02d}".format(v) for v in xrange(1, 11)]
    types = {0: "cn_pollution_corps_supervision",
            1: "cn_pollution_monitored_enterprises"}
    base_url = ("http://www.ipe.org.cn/pollution/ajax/getpoints.ashx?" +
        "isEn=1&name=&year={year}&space=&type={type}&valley={valley}&itemtype=0" +
        "&swLat=&swLng=&neLat=&neLng=&zoom=4&showcount=1000")

    vrt_content = (
"""<OGRVRTDataSource>
    <OGRVRTLayer name="{name}">
        <SrcDataSource>{dir}/{name}.csv</SrcDataSource>
        <GeometryType>wkbPoint</GeometryType>
        <LayerSRS>WGS84</LayerSRS>
        <GeometryField encoding="PointFromColumns" x="lng" y="lat"/>
    </OGRVRTLayer>
</OGRVRTDataSource>
""")


    def download(self):
        for type in self.types:
            keys = None
            item_set = set()
            for year in self.years:
                for valley in self.valleys:
                    url = self.base_url.format(year=year, valley=valley, type=type)
                    print url
                    r = requests.get(url)
                    data = json.loads(r.content)

                    for idx, item in enumerate(data.items()[1][1]):
                        if not keys:
                            try:
                                keys = data.items()[1][1][0].keys()
                            except:
                                print "==========================", data.items()[1][1]
                        item['emissionyears'] = ','.join(item['emissionyears'])
                        item['years'] = ','.join(item['years'])
                        item_sorted = tuple([unicode(item[key]) for key in sorted(keys)])
                        if float(item['lng']) >= 74.0 and float(item['lat']) <= 55.0:
                            item_set.add(item_sorted)

            csv_file = os.path.join(self.tmp_dir, self.types[type] + '.csv')
            with open(csv_file, 'w') as out:
                uniwriter = UnicodeWriter(out)
                uniwriter.writerow(sorted(keys))
                for item in item_set:
                    uniwriter.writerow(list(item))
            with open(csv_file.replace('.csv', '.vrt'), 'w') as vrt:
                vrt.write(self.vrt_content.format(name=self.types[type], dir=self.tmp_dir))
        return ("{}.vrt".format(name) for name in self.types.values())

    def run(self):
        db = ogc_server_settings.datastore_db
        for vrt in self.download():
            vrtfile = os.path.join(self.tmp_dir, vrt)
            ogr2ogr_exec('-overwrite -skipfailures -f PostgreSQL -a_srs EPSG:4326 ' +
                '"PG:host={db_host} user={db_user} password={db_pass} dbname={db_name}" {vrt} -nln {table}'.format(
                    db_host=db["HOST"], db_user=db["USER"], db_pass=db["PASSWORD"], db_name=db["NAME"],
                    vrt=vrtfile, table=vrt.rstrip(".vrt")))

if __name__ == '__main__':
    processor = ChinaPollutionIPEProcessor()
    processor.run()

