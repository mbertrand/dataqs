from __future__ import absolute_import

import os
import datetime
import logging
from ..processor_base import GeoDataProcessor
from ..helpers import postgres_query, ogr2ogr_exec
from geonode.geoserver.helpers import ogc_server_settings

logger = logging.getLogger("dataqs.processors")

class USGSQuakeProcessor(GeoDataProcessor):
    """
    Class for retrieving and processing the latest earthquake data from USGS.
    """

    tables = ("quakes_weekly", "quakes_monthly",
              "quakes_yearly", "quakes_archive")

    def purge_old_data(self):
        """
        Remove old data from weekly, monthly, and yearly PostGIS tables
        """
        today = datetime.date.today()
        last_week = (today - datetime.timedelta(days=7)).strftime("%s000")
        last_month = (today - datetime.timedelta(days=30)).strftime("%s000")
        last_year = (today - datetime.timedelta(days=365)).strftime("%s000")

        for interval, table in zip([last_week, last_month, last_year],
                                   self.tables):
            postgres_query("UPDATE {} set time = CAST(time as bigint) + 1443109011456".format(table), commit=True)
            postgres_query("UPDATE {} set updated = CAST(updated as bigint) + 1443109011456".format(table), commit=True)
            postgres_query("DELETE FROM {} where CAST(time as numeric) < {};".format(table, interval), commit=True)

    def run(self):
        """
        Retrieve the latest USGS earthquake data and append to all
        PostGIS earthquake tables, then remove old data
        :return:
        """
        rss = self.download("http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson")
        db = ogc_server_settings.datastore_db
        for table in self.tables:
            ogr2ogr_exec("-append -skipfailures -f PostgreSQL \
                \"PG:host={db_host} user={db_user} password={db_pass} dbname={db_name}\" \
                {rss} -nln {table}".format(db_host=db["HOST"], db_user=db["USER"], db_pass=db["PASSWORD"],
                                           db_name=db["NAME"], rss="{}".format(os.path.join(self.tmp_dir, rss)),
                                           table=table))
            self.truncate_gs_cache(table)
        self.purge_old_data()


if __name__ == '__main__':
    processor = USGSQuakeProcessor()
    processor.run()
