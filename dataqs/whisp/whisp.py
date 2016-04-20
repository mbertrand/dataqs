import base64
import os
import zipfile
import StringIO
import bs4
from dataqs.helpers import table_exists, postgres_query, layer_exists, \
    style_exists
from dateutil.parser import parse
import re
import requests
import unicodecsv as csv
from dataqs.processor_base import GeoDataProcessor
from geonode.geoserver.helpers import ogc_server_settings
from geonode.settings import DEFAULT_WORKSPACE

script_dir = os.path.dirname(os.path.realpath(__file__))

WHISP_TABLE = u"""
CREATE TABLE IF NOT EXISTS {table}
(
  id serial NOT NULL,
  eventtype character varying(128),
  eventname character varying(128),
  startdate timestamp without time zone,
  enddate timestamp without time zone,
  affected integer,
  states character varying(512),
  counties text,
  species text,
  diagnosis text,
  coords text,
  CONSTRAINT {table}_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
CREATE INDEX {table}_eventname_idx ON {table} (eventname);
CREATE INDEX {table}_startdate_idx ON {table} (startdate);
SELECT AddGeometryColumn ('public','{table}' ,'the_geom',4326,'MULTIPOINT',2);
CREATE INDEX {table}_the_geom ON {table} USING gist (the_geom);
"""


WHISP_SQL = u"""
INSERT INTO {table}
(eventtype, eventname, startdate, enddate, affected, states, counties,
species, diagnosis, coords, the_geom)
 SELECT %(eventtype)s, %(eventname)s, %(startdate)s, %(enddate)s, %(affected)s,
 %(states)s, %(counties)s, %(species)s, %(diagnosis)s, %(coords)s,
 ST_GeomFromText('{geom}', 4326)
 WHERE NOT EXISTS (SELECT 1 from {table} WHERE eventname = %(eventname)s);
"""


class WhispProcessor(GeoDataProcessor):
    """
    Wildlife Health Information Sharing Partnership event reporting system
    with current and historic information on wildlife morbidity or mortality
    events in North America.
    """
    prefix = "whisp"
    base_url = "https://www.nwhc.usgs.gov/whispers/searchForm/recent100"
    title = "Wildlife Health Information Sharing Partnership - Events"

    def download(self, url=None, filename=None):
        if not url:
            url = self.base_url
            req = requests.get(url)
            req.raise_for_status()
            return req.content

    def scrape(self):
        html = self.download()
        soup = bs4.BeautifulSoup(html)
        form = soup.find('form', action='/whispers/events/export')
        input = form.find('input', type='hidden')
        rawdata = base64.b64decode(input.get('value'))
        table = soup.find('table', class_='items')
        headers = [tr.text.replace(' ', '').lower().replace('name', 'eventname')
                   for tr in table.find('thead').findAll('th')]
        headers.extend(['coords', 'the_geom'])
        data = [[td.text.strip() for td in tr if td != '\n'] for tr in
                table.find('tbody').findAll('tr')]
        for row in data:
            id = row[1]
            latlng = re.search('"LatLng";s:\d+:"([0-9\.\-\,\;]+)"',
                               rawdata[rawdata.index(id):]).group(1)
            coords = [tuple([float(a) for a in b.split(',')])
                      for b in latlng.split(';')]
            if len(coords) > 2:
                geom = re.sub(r'(\d),', r'\1', "MULTIPOINT {}".format(
                    tuple([coord[::-1] for coord in coords])))
            else:
                geom = re.sub(r',', r'',
                              "MULTIPOINT ({})".format((coords[0][::-1])))
            row.extend([coords, geom])
            row[2] = parse(row[2]).strftime('%Y-%m-%d')
            if row[3] and row[3] != u'\xa0':
                row[3] = parse(row[3]).strftime('%Y-%m-%d')
            else:
                row[3] = None

            self.insert_row(dict(zip(headers, row)))

    def insert_row(self, data):
        postgres_query(WHISP_SQL.format(
            table=self.prefix, geom=data['the_geom']), params=data, commit=True)

    def import_archive(self):
        zf = zipfile.ZipFile(os.path.join(script_dir,
                                          'resources/whispers_archive.zip'))
        reader = csv.DictReader(
            StringIO.StringIO(zf.read('whispers_archive.csv')))
        for row in reader:
            for key in row.keys():
                if not row[key]:
                    row[key] = None
            self.insert_row(row)

    def run(self):
        if not table_exists(self.prefix):
            postgres_query(WHISP_TABLE.format(table=self.prefix), commit=True)
            self.import_archive()
        self.scrape()
        if not layer_exists(self.prefix,
                            ogc_server_settings.server.get('DATASTORE'),
                            DEFAULT_WORKSPACE):
            self.post_geoserver_vector(self.prefix)
        if not style_exists(self.prefix):
                with open(os.path.join(script_dir,
                                       'resources/whisp.sld')) as sld:
                    self.set_default_style(self.prefix, self.prefix, sld.read())
        self.update_geonode(self.prefix, title=self.title)
        self.truncate_gs_cache(self.prefix)
        self.cleanup()

if __name__ == '__main__':
    processor = WhispProcessor()
    processor.run()
