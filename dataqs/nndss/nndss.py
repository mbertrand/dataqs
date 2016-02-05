import json
from dataqs.processor_base import GeoDataProcessor


class NNDSSProcessor(GeoDataProcessor):
    """
    Parse data from NNDSS
    """
    base_url = 'https://data.cdc.gov/api/views/{}/rows.json'
    indicator_suffixes = (u'_current_week',
                          u'_current_week,_flag',
                          u'_previous_52_weeks_med',
                          u'previous_52_weeks_med,_flag',
                          u'_previous_52_weeks_max',
                          u'_previous_52_weeks_max,_flag',
                          u'_cum_2015', u'_cum_2015,_flag',
                          u'_cum_2014', u'_cum_2014,_flag'
                          )

    urls = {
        2014: [
            ('NNDSS - Table II. Babesiosis to Coccidioidomycosis', 'qz8t-eu2e'),
            ('NNDSS - Table II. Cryptosporidiosis to Dengue Hemorrhagic Fever',
             'b36e-ru3r'),
            ('NNDSS - Table II. Ehrlichiosis/Anaplasmosis', 'edtz-vibe'),
            ('NNDSS - Table II. Giardiasis to Haemophilus influenza',
             'cafy-kah2'),
            ('NNDSS - Table II. Hepatitis (viral, acute)', 'rg4j-6mcc'),
            ('NNDSS - Table II. Invasive Pneumococcal to Legionellosis',
             '23gt-ssfe'),
            ('NNDSS - Table II. Lyme disease to Meningococcal', 'y6uv-t34t'),
            ('NNDSS - Table II. Mumps to Rabies, animal', '8rkx-vimh'),
            ('NNDSS - Table II. Salmonellosis to Shigellosis', '52cr-rw4k'),
            ('NNDSS - Table II. Spotted Fever Rickettsiosis to Syphilis',
             'fqve-8wzt'),
            ('NNDSS - Table IV. Tuberculosis', 'pxa6-asqb'),
            ('NNDSS - Table II. Varicella to West Nile virus disease',
             'ig4m-ub43')
        ],
        2015: [
            ('NNDSS - Table II. Babesiosis to Campylobacteriosis', 's5s8-d82d'),
            ('NNDSS - Table II. Chlamydia to Coccidioidomycosis', '97tt-n3j3',),
            ('NNDSS - Table II. Cryptosporidiosis to Dengue', '9n3x-apcd'),
            ('NNDSS - Table II. Ehrlichiosis/Anaplasmosis', 'j9id-xrm6'),
            ('NNDSS - Table II. Giardiasis to Haemophilus influenza',
             'mpdg-hf57'),
            ('NNDSS - Table II. Hepatitis (viral, acute)', '65xe-6neq'),
            ('NNDSS - Table II. Invasive Pneumococcal to Legionellosis',
             'ydsy-yh5w'),
            ('NNDSS - Table II. Lyme disease to Meningococcal', '7pb7-w9us'),
            ('NNDSS - Table II. Mumps to Rabies, animal', 'd69q-iyrb'),
            ('NNDSS - Table II. Rubella to Salmonellosis', 'd6kj-devz'),
            ('NNDSS - Table II. Shiga toxin to Shigellosis', 'n3wf-wtep'),
            ('NNDSS - Table II. Spotted Fever Rickettsiosis to Syphilis',
             'ekd3-qu3w'),
            ('NNDSS - Table II. Tetanus to Vibriosis', 'nf22-99pv'),
            ('NNDSS - Table IV. Tuberculosis', 'ei7y-3g6s'),
            ('NNDSS - Table II. West Nile virus disease', 'r7hc-32zu')
        ],
        2016: [
            ('NNDSS - Table II. Babesiosis to Campylobacteriosis', '4y34-2pku'),
            ('NNDSS - Table II. Chlamydia to Coccidioidomycosis', 'n835-hpyp',),
            ('NNDSS - Table II. Cryptosporidiosis to Dengue', 'kikd-77zw'),
            ('NNDSS - Table II. Ehrlichiosis/Anaplasmosis', 'keia-pvvn'),
            ('NNDSS - Table II. Giardiasis to Haemophilus influenza',
             'afja-b25e'),
            ('NNDSS - Table II. Hepatitis (viral, acute)', '7vnz-2mjz'),
            ('NNDSS - Table II. Invasive Pneumococcal to Legionellosis',
             'yqwx-bvu7'),
            ('NNDSS - Table II. Lyme disease to Meningococcal', '93k9-hy54'),
            ('NNDSS - Table II. Mumps to Rabies, animal', 'bfe6-2gyq'),
            ('NNDSS - Table II. Rubella to Salmonellosis', '4qb4-rsd8'),
            ('NNDSS - Table II. Shiga toxin to Shigellosis', 'xv7k-8e7s'),
            ('NNDSS - Table II. Spotted Fever Rickettsiosis to Syphilis',
             '6jgb-zrsp'),
            ('NNDSS - Table II. Tetanus to Vibriosis', 'tj26-bdgd'),
            ('NNDSS - Table II. West Nile virus disease', 'sd5c-m3g5')
        ]
    }

    def parse_urls(self, years=None):
        if not years:
            years = [year for year in self.urls.keys()]
        for item in [url for url in self.urls[year] for year in years]:
            self.process_data(self.download(self.base_url.format(item[1]),
                                            html=True))

    def process_data(self, html):
        data = json.loads(html)
        columns = [col['name'].replace(' ', '_').lower()
                   for col in data['meta']['view']['columns']]
        abstract = data['meta']['view']['description']
        indicators = {}
        indicator_rows = {}
        for row in data['data']:
            row_data = dict(zip(columns, row))
            common_fields = {'abstract': abstract}
            for key in row_data:
                if ',' in key:
                    indicator, column = key.split(',', 1)
                    indicators.setdefault(
                        indicator, {})[column.replace(',', '')] = row_data[key]
                else:
                    if key == 'location_1':
                        common_fields['state'] = json.loads(
                            row_data[key][0])['state']
                        common_fields['lat'] = row_data[key][1]
                        common_fields['lng'] = row_data[key][2]

                    else:
                        common_fields[key] = row_data[key]
            for ikey in indicators:
                for ckey in common_fields:
                    indicators[ikey][ckey] = common_fields[ckey]
            for indicator in indicators:
                indicator_rows.setdefault(indicator, []).append(
                    indicators[indicator])
        print 'fooey'




    def run(self):
        self.parse_urls()





if __name__ == '__main__':
    processor = NNDSSProcessor()
    processor.run()