from __future__ import absolute_import

import logging
import os
import datetime
from geonode.geoserver.helpers import ogc_server_settings
from ..helpers import ogr2ogr_exec
from ..processor_base import GeoDataProcessor

logger = logging.getLogger("data_queues.processors")


class GDACSProcessor(GeoDataProcessor):
    """
    Class for processing data from the Global Disaster Alerts & Coordination System website
    (http://gdacs.org/)
    """

    tables = ("gdacs_alerts",)

    def run(self):
        today = datetime.date.today()
        last_week = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        today = today.strftime("%Y-%m-%d")

        rss = self.download("http://www.gdacs.org/rss.aspx?profile=ARCHIVE&from={}&to={}".format(
            last_week, today), filename="gdacs.rss")

        db = ogc_server_settings.datastore_db

        for table in self.tables:
            ogr2ogr_exec("-append -skipfailures -f PostgreSQL \
                \"PG:host={db_host} user={db_user} password={db_pass} dbname={db_name}\" \
                {rss} -nln {table}".format(db_host=db["HOST"], db_user=db["USER"], db_pass=db["PASSWORD"],
                                           db_name=db["NAME"], rss="{}".format(os.path.join(self.tmp_dir, rss)),
                                           table=table))
            self.truncate_gs_cache(table)


if __name__ == '__main__':
    processor = GDACSProcessor()
    processor.run()
