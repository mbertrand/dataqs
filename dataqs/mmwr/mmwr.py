import json
import os
from datetime import datetime, date, timedelta
import re
from requests import HTTPError
import unicodecsv as csv
from dataqs.helpers import ogr2ogr_exec, layer_exists, style_exists
from dataqs.processor_base import GeoDataProcessor, DEFAULT_WORKSPACE
from geonode.geoserver.helpers import ogc_server_settings

script_dir = os.path.dirname(os.path.realpath(__file__))


vrt_content = (
"""<OGRVRTDataSource>
    <OGRVRTLayer name="{name}">
        <SrcDataSource>{csv}</SrcDataSource>
        <GeometryType>wkbPoint</GeometryType>
        <LayerSRS>WGS84</LayerSRS>
        <GeometryField encoding="PointFromColumns" x="lng" y="lat"/>
    </OGRVRTLayer>
</OGRVRTDataSource>
""")

csvt_content = '"String","Real","Real","Integer","Integer","Integer","Integer","Integer","Integer","Integer","Date"'

class MortalityProcessor(GeoDataProcessor):
    prefix = 'mmwr'
    base_title = 'Mortality and Morbidity Reports'
    titles = ['Weekly', 'Archive']
    base_url = 'http://wonder.cdc.gov/mmwr/mmwr_{year}.asp?request=Export&mmwr_location=Click+here+for+all+Locations&mmwr_table=4A&mmwr_year={year}&mmwr_week={week}'
    params = {}

    def __init__(self, *args, **kwargs):
        for key in kwargs.keys():
            self.params[key] = kwargs.get(key)

        if 'sdate' not in self.params:
            today = date.today()
            self.params['sdate'] = today.strftime("%Y-%m-%d")

        if 'edate' not in self.params:
            today = date.today()
            self.params['edate'] = today.strftime("%Y-%m-%d")

        super(MortalityProcessor, self).__init__(*args)


    def run(self):
        cur_date = datetime.strptime(self.params['sdate'], '%Y-%m-%d')
        end_date = datetime.strptime(self.params['edate'], '%Y-%m-%d')

        csvfile = "{}.csv".format(self.prefix)
        vrt_file = os.path.join(self.tmp_dir, '{}.vrt'.format(self.prefix))
        csvt_file = os.path.join(self.tmp_dir, '{}.csvt'.format(self.prefix))
        if not os.path.exists(vrt_file):
            with open(vrt_file, 'w') as vrt:
                vrt.write(vrt_content.format(
                    name=csvfile.replace('.csv',''),
                    csv=os.path.join(self.tmp_dir, csvfile)))
        if not os.path.exists(csvt_file):
            with open(csvt_file, 'w') as csvt:
                csvt.write(csvt_content)

        while cur_date <= end_date:
            week = cur_date.isocalendar()[1]
            year = cur_date.year

            for x in range(2):
                try:
                    exportfile = self.download(
                        self.base_url.format(
                            year=year, week=week), '{}.txt'.format(self.prefix))
                    with open(
                            os.path.join(self.tmp_dir, exportfile)) as testfile:
                        content = testfile.read().strip()
                        if content.startswith('<html'):
                            raise HTTPError
                except HTTPError:
                    if x < 1 and self.params['sdate'] == self.params['edate']:
                        year = year - 1 if week == 1 else year
                        week = week - 1 if week > 1 else 52
                    elif week == 53:
                        year = year + 1
                        week = 1
                    else:
                        raise

            with open(os.path.join(script_dir, 'mmwr.json')) as jsonfile:
                places = json.load(jsonfile)

            with open(os.path.join(self.tmp_dir, exportfile)) as openfile:
                reader = csv.reader(openfile, delimiter='\t')

                with open(os.path.join(self.tmp_dir, csvfile), 'w') as outfile:
                    writer = csv.writer(outfile)
                    headers = ['place', 'lng', 'lat', 'all', 'a65', 'a45_64',
                               'a25_44', 'a01-24', 'a01', 'flu', 'report_date']
                    writer.writerow(headers)
                    report_date = None
                    for row in reader:
                        if len(row) == 1 and not report_date:
                            datematch = re.search('week ending (.+)', row[0])
                            if datematch:
                                report_date = datetime.strptime(
                                    datematch.group(1), '%B %d, %Y')
                                report_date = report_date.strftime('%Y-%m-%d')
                        if len(row) > 2:
                            place = row[0]
                            if place in places:
                                match = places[place]
                                row.insert(1, match[0])
                                row.insert(1, match[1])
                                row.insert(10, report_date)
                                writer.writerow(row)
                            elif place != 'TOTAL':
                                raise Exception(
                                    'Could not find matching city: {}'.format(
                                        place))
            db = ogc_server_settings.datastore_db
            for title in self.titles:
                table = '{}_{}'.format(self.prefix, title).lower()
                option = 'overwrite' if title.lower() == 'weekly' else 'append'
                ogr2ogr_exec("-{option} -skipfailures -f PostgreSQL \
                    \"PG:host={db_host} user={db_user} password={db_pass} \
                    dbname={db_name}\" {vrt} -nln {table}".format(
                    db_host=db["HOST"], db_user=db["USER"],
                    db_pass=db["PASSWORD"], db_name=db["NAME"],
                    vrt="{}".format(vrt_file), option=option, table=table))
                if not layer_exists(table,
                                    ogc_server_settings.server.get('DATASTORE'),
                                    DEFAULT_WORKSPACE):
                    self.post_geoserver_vector(table)
                if not style_exists(table):
                    with open(os.path.join(script_dir, 'mmwr.sld')) as sldfile:
                        sld = sldfile.read().format(layername=table)
                        self.set_default_style(table, table, sld)
                self.update_geonode(
                    table, title='{} {}'.format(self.base_title, title))
                self.truncate_gs_cache(table)

            cur_date = cur_date + timedelta(days=7)
            self.cleanup()

if __name__ == '__main__':
    processor = MortalityProcessor(sdate='2015-12-01')
    processor.run()
