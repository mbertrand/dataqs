import logging
import requests
from bs4 import BeautifulSoup as bs

from dataqs.processor_base import GeoDataProcessor

logger = logging.getLogger("dataqs.processors")


class AirQualityProcessor(GeoDataProcessor):
    base_url = 'http://aqicn.org/city/all/'
    prefix = 'aqicn'

    def download(self, url=None):
        if not url:
            url = self.base_url
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.content

    def parse_city(self, city):
        city_name = city.text
        city_html = bs(self.download(city.get('href')))
city_html.find('div', id='h1header2').find('b').text

    def run(self):
        cities_soup = bs(self.download())
        cities = cities_soup.find_all('a')
        for city in cities:
            city_link = city.get('href')
            if city_link and '/city/' in city_link:
                parse_city(city)
