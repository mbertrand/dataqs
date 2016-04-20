import logging
import os
import requests
from bs4 import BeautifulSoup
import datetime

from dataqs.helpers import table_exists, postgres_query, style_exists, \
    layer_exists
from dataqs.processor_base import GeoDataProcessor, DEFAULT_WORKSPACE
from geonode.geoserver.helpers import ogc_server_settings

logger = logging.getLogger("dataqs.processors")
script_dir = os.path.dirname(os.path.realpath(__file__))

CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS {table}
(
"id" SERIAL NOT NULL PRIMARY KEY,
"year" integer,
"month" character varying,
"timestamp_start" timestamp with time zone,
"deaths" integer,
"province" character varying,
CONSTRAINT {table}_key UNIQUE ("province", "timestamp_start")
);
"""

INSERT_SQL = """
INSERT INTO "{table}" ({cols}) SELECT {}
"""

PROV_DICT = {
    'Alhasakah': 'Al-Hasakah',
    'Alraqah': 'Ar-Raqqah',
    'Alsuwaida': 'As-Suwayda',
    'Alqunaitrah': 'Quneitra',
    'Lattakia': 'Latakia',
    'Aleppo': 'Aleppo',
    'Hama': 'Hama',
    'Homs': 'Homs',
    'Daraa': 'Daraa',
    'Rif Dimashq': 'Rif Dimashq',
    'Dair Ezzour': 'Deir ez-Zor',
    'Tartous': 'Tartus',
    'Idlib': 'Idlib'
}


META_JSON = """{
  "featureType": {
    "enabled": true,
    "metadata": {
      "entry": [
        {
          "@key": "time",
          "dimensionInfo": {
            "enabled": true,
            "attribute": "timestamp_start",
            "presentation": "LIST",
            "units": "ISO8601",
            "defaultValue": {
              "strategy": "FIXED",
              "referenceValue": "<time>"
            }
          }
        }
      ]
    }
  }
}"""


class SyriaDeathsProcessor(GeoDataProcessor):
    prefix = 'syria_deaths'
    #layer_name = "syria_deaths_map"
    layer_names = ['syria_deaths_map', 'syria_deaths_total_map']
    layer_titles = ['Syrian War Casualties - Month of ',
                    'Syrian War Casualties - Total as of ']
    join_table = 'syria_provinces'
    join_column = 'name_en'
    base_url = "http://syrianshuhada.com/?lang=en&a=st&st=20"

    def download(self):
        r = requests.get(self.base_url)
        r.raise_for_status()
        return r.content

    def update_db(self, rows):
        if not table_exists(self.prefix):
            self.create_table()
        headers = "year, month, timestamp_start, deaths, province"
        for row in rows:
            insert_sql = 'INSERT INTO "{}" ({}) SELECT \n'.format(
                self.prefix, headers)
            insert_sql += '{},{},\'{}\',{},\'{}\' '.format(
                row[0], row[1], row[2], row[3], row[4]
            )
            insert_sql += """WHERE NOT EXISTS (SELECT 1 from {} WHERE
            timestamp_start = \'{}\' and province = \'{}\');""".format(
                self.prefix, row[2], row[-1])
            postgres_query(insert_sql, params=tuple(row), commit=True)
            for layer in self.layer_names:
                if not table_exists(layer):
                    self.create_view()

    def create_table(self):
        postgres_query(CREATE_TABLE_SQL.format(table=self.prefix),
                       commit=True)

    def create_view(self):
        view_sql = 'CREATE OR REPLACE VIEW ' + self.layer_names[0] + \
                   ' AS SELECT i.*, g.the_geom from ' + self.prefix + ' i ' + \
                   ' INNER JOIN ' + self.join_table + ' g on ' + \
                   ' i.province = ' + \
                   ' g.{};'.format(self.join_column)
        postgres_query(view_sql, commit=True)
        view_total_sql = 'CREATE OR REPLACE VIEW ' + self.layer_names[1] + \
                         ' AS SELECT i.province, sum(i.deaths) as deaths, ' + \
                         ' g.the_geom from ' + self.prefix + ' i ' + \
                         ' INNER JOIN ' + self.join_table + ' g on ' + \
                         ' i.province = ' + \
                         ' g.{} group by province, the_geom;'.format(
                             self.join_column)
        postgres_query(view_total_sql, commit=True)

    def process_rows(self, html):
        soup = BeautifulSoup(html)
        firstrow = soup.find('td', title='Year').parent
        tds = firstrow.find_all('td')
        startTd = 0
        endTd = 16
        maxTd = len(tds) - 17

        rows = []
        while endTd <= maxTd:
            row = [x.text.replace("&nbsp", "") for x in tds[startTd:endTd]]
            if not rows:
                row.append('Rif Dimashq')
            else:
                row = [0 if x == '' else int(x) for x in row]
                row.append(row[12] + int(row[14]))
            startTd = endTd + 1
            endTd = startTd + 16
            rows.append(row)

        province = 2
        province_rows = []
        while province < 17:
            if rows[0][province] in PROV_DICT:
                for row in rows[1:]:
                    timestamp = datetime.date(row[0], row[1], 1).strftime(
                        '%Y-%m-%dT00:00:00.000Z')
                    province_row = [row[0],row[1],timestamp,]
                    province_row.extend([row[province],
                                         PROV_DICT[rows[0][province]]])
                    province_rows.append(province_row)
            province += 1
        return province_rows

    def run(self):
        page_html = self.download()
        province_rows = self.process_rows(page_html)
        self.update_db(province_rows)
        row = province_rows[-1]
        latest_date = datetime.date(row[0], row[1], 1)
        for layer_name, layer_title in zip(self.layer_names, self.layer_titles):
            if not layer_exists(layer_name,
                                ogc_server_settings.server.get('DATASTORE'),
                                DEFAULT_WORKSPACE):
                self.post_geoserver_vector(layer_name)
            if not style_exists(layer_name):
                with open(os.path.join(script_dir, 'resources/{}.sld').format(
                        layer_name)) as sld:
                    self.set_default_style(layer_name, layer_name,
                                           sld.read())
            if 'total' not in layer_name:
                self.update_gs_metadata(layer_name, META_JSON.replace(
                    '<time>', latest_date.strftime('%Y-%m-%dT00:00:00.000Z')),
                    vector=True)
            layer_title += latest_date.strftime(' %m/%Y')
            self.update_geonode(layer_name, title=layer_title)
            self.truncate_gs_cache(layer_name)
        self.cleanup()


if __name__ == '__main__':
    processor = SyriaDeathsProcessor()
    processor.run()