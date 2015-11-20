import logging
import requests
from bs4 import BeautifulSoup
import datetime

from dataqs.helpers import table_exists, postgres_query, style_exists, \
    layer_exists
from dataqs.processor_base import GeoDataProcessor, DEFAULT_WORKSPACE
from geonode.geoserver.helpers import ogc_server_settings

logger = logging.getLogger("dataqs.processors")

CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS {}
(
"id" SERIAL NOT NULL PRIMARY KEY,
"year" integer,
"month" character varying,
"timestamp_start" timestamp with timezone,
"deaths" integer,
"province" character varying,
CONSTRAINT wqp_syria_deaths_key UNIQUE ("province", "timestamp")
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

SYRIA_SLD = """<?xml version="1.0" encoding="UTF-8"?><sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml" version="1.0.0">
  <sld:NamedLayer>
    <sld:Name>{layer_name}</sld:Name>
    <sld:UserStyle>
      <sld:Name>{layer_name}</sld:Name>
      <sld:Title>{layer_title}</sld:Title>
      <sld:IsDefault>1</sld:IsDefault>
      <sld:FeatureTypeStyle>
        <sld:Rule>
          <sld:Name>rule1</sld:Name>
          <sld:Title> &lt;= 10</sld:Title>
          <sld:Abstract></sld:Abstract>
          <ogc:Filter>
              <ogc:PropertyIsLessThanOrEqualTo>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>10</ogc:Literal>
              </ogc:PropertyIsLessThanOrEqualTo>
          </ogc:Filter>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#0004FF</sld:CssParameter>
            </sld:Fill>
            <sld:Stroke/>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>rule1</sld:Name>
          <sld:Title> &gt; 10 AND &lt;= 50</sld:Title>
          <sld:Abstract></sld:Abstract>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThan>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>10</ogc:Literal>
              </ogc:PropertyIsGreaterThan>
              <ogc:PropertyIsLessThanOrEqualTo>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>50</ogc:Literal>
              </ogc:PropertyIsLessThanOrEqualTo>
            </ogc:And>
          </ogc:Filter>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#32BB0C</sld:CssParameter>
            </sld:Fill>
            <sld:Stroke/>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>rule1</sld:Name>
          <sld:Title> &gt; 50 AND &lt;= 250</sld:Title>
          <sld:Abstract></sld:Abstract>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThan>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>50</ogc:Literal>
              </ogc:PropertyIsGreaterThan>
              <ogc:PropertyIsLessThanOrEqualTo>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>250</ogc:Literal>
              </ogc:PropertyIsLessThanOrEqualTo>
            </ogc:And>
          </ogc:Filter>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#FFFF00</sld:CssParameter>
            </sld:Fill>
            <sld:Stroke/>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>rule1</sld:Name>
          <sld:Title> &gt; 250 AND &lt;= 500</sld:Title>
          <sld:Abstract></sld:Abstract>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThan>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>250</ogc:Literal>
              </ogc:PropertyIsGreaterThan>
              <ogc:PropertyIsLessThanOrEqualTo>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>500</ogc:Literal>
              </ogc:PropertyIsLessThanOrEqualTo>
            </ogc:And>
          </ogc:Filter>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#FF7F00</sld:CssParameter>
            </sld:Fill>
            <sld:Stroke/>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>rule1</sld:Name>
          <sld:Title> &gt; 500</sld:Title>
          <sld:Abstract></sld:Abstract>
          <ogc:Filter>
              <ogc:PropertyIsGreaterThan>
                <ogc:PropertyName>deaths</ogc:PropertyName>
                <ogc:Literal>500</ogc:Literal>
              </ogc:PropertyIsGreaterThan>
          </ogc:Filter>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#FF0000</sld:CssParameter>
            </sld:Fill>
            <sld:Stroke/>
          </sld:PolygonSymbolizer>
        </sld:Rule>
      </sld:FeatureTypeStyle>
    </sld:UserStyle>
  </sld:NamedLayer>
</sld:StyledLayerDescriptor>
"""

META_JSON = """{
  "featureType": {
    "enabled": true,
    "metadata": {
      "entry": [
        {
          "@key": "time",
          "dimensionInfo": {
            "enabled": true,
            "attribute": "timestamp",
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
    layer_name = "syria_deaths_map"
    #layer_names = ["syria_deaths_map", "syria_deaths_total_map"]
    join_table = 'syria_provinces'
    join_column = 'name_en'
    base_url = "http://syrianshuhada.com/?lang=en&a=st&st=20"
    layer_title = 'Syrian "Martyr" casualties'

    def download(self):
        r = requests.get(self.base_url)
        r.raise_for_status()
        return r.content

    def update_db(self, rows):
        if not table_exists(self.prefix):
            self.create_table()
        headers = "year, month, timestamp, deaths, province"
        for row in rows:
            insert_sql = 'INSERT INTO "{}" ({}) SELECT \n'.format(
                self.prefix, headers)
            insert_sql += '{},{},\'{}\',{},\'{}\' '.format(
                row[0], row[1], row[2], row[3], row[4]
            )
            insert_sql += """WHERE NOT EXISTS (SELECT 1 from {} WHERE timestamp
= \'{}\' and province = \'{}\');""".format(self.prefix, row[2], row[-1])
            postgres_query(insert_sql, params=tuple(row), commit=True)
            if not table_exists(self.layer_name):
                self.create_view()

    def create_table(self):
        postgres_query(CREATE_TABLE_SQL.format(self.prefix),
                       commit=True)

    def create_view(self):
        view_sql = 'CREATE OR REPLACE VIEW ' + self.layer_name + \
                   ' AS SELECT i.*, g.the_geom from ' + self.prefix + ' i ' + \
                   ' INNER JOIN ' + self.join_table + ' g on ' + \
                   ' i.province = ' + \
                   ' g.{};'.format(self.join_column)
        postgres_query(view_sql, commit=True)
        #view_sql = "CREATE OR REPLACE VIEW syria_deaths_total_map AS select" +
        #" province, sum(deaths) from syria_deaths group by province;"

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
        if not layer_exists(self.layer_name,
                            ogc_server_settings.server.get('DATASTORE'),
                            DEFAULT_WORKSPACE):
            self.post_geoserver_vector(self.layer_name)
        if not style_exists(self.layer_name):
            self.set_default_style(self.layer_name, self.layer_name,
                                   SYRIA_SLD.format(
                                       layer_name=self.layer_name,
                                       layer_title=self.layer_title))
        self.update_gs_metadata(self.layer_name, META_JSON.replace(
            '<time>', latest_date.strftime('%Y-%m-%dT00:00:00.000Z')),
                                vector=True)
        self.layer_title += latest_date.strftime(' %m/%Y')
        self.update_geonode(self.layer_name, title=self.layer_title)
        self.truncate_gs_cache(self.layer_name)
        self.cleanup()


if __name__ == '__main__':
    processor = SyriaDeathsProcessor()
    processor.run()
