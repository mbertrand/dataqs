from __future__ import absolute_import

from ftplib import FTP
import logging
import os
import datetime
import re
import shutil
from django.conf import settings
from dataqs.processor_base import GeoDataMosaicProcessor
from dataqs.helpers import warp_image, style_exists

logger = logging.getLogger("dataqs.processors")

AIRNOW_ACCOUNT = getattr(settings, 'AIRNOW_ACCOUNT', 'anonymous:anonymouse')
GS_DATA_DIR = getattr(settings, 'GS_DATA_DIR', '/data/geodata')
GS_TMP_DIR = getattr(settings, 'GS_TMP_DIR', '/tmp')

AIRNOW_SLD="""<?xml version="1.0" encoding="UTF-8"?><sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml" version="1.0.0">
  <sld:NamedLayer>
    <sld:Name>airnow</sld:Name>
    <sld:UserStyle>
      <sld:Name>airnow</sld:Name>
      <sld:FeatureTypeStyle>
        <sld:Rule>
          <sld:RasterSymbolizer>
            <sld:Geometry>
              <ogc:PropertyName>grid</ogc:PropertyName>
            </sld:Geometry>
            <sld:ColorMap>
              <sld:ColorMapEntry color="#008000" opacity="1.0" quantity="0" label="Good"/>
              <sld:ColorMapEntry color="#FFFF00" opacity="1.0" quantity="50" label="Moderate"/>
              <sld:ColorMapEntry color="#FFA500" opacity="1.0" quantity="100" label="Unhealthy for Sensitive Groups"/>
              <sld:ColorMapEntry color="#FF0000" opacity="1.0" quantity="150" label="Unhealthy"/>
              <sld:ColorMapEntry color="#800080" opacity="1.0" quantity="200" label="Very Unhealthy"/>
              <sld:ColorMapEntry color="#800000" opacity="1.0" quantity="300" label="Hazardous"/>
              <sld:ColorMapEntry color="#008000" opacity="0.0001" quantity="500" label=""/>
            </sld:ColorMap>
          </sld:RasterSymbolizer>
        </sld:Rule>
      </sld:FeatureTypeStyle>
    </sld:UserStyle>
  </sld:NamedLayer>
</sld:StyledLayerDescriptor>
"""


class AirNowGRIB2HourlyProcessor(GeoDataMosaicProcessor):
    """
    Class for processing hourly GRIB2 Air Quality Index raster
    time-series images from the AirNow API.
    (http://www.airnowapi.org/docs/AirNowMappingFactSheet.pdf)
    """
    prefix = "US-"
    base_url = "ftp.airnowapi.org"
    layer_names = ["airnow_aqi_ozone", "airnow_aqi_pm25",
                         "airnow_aqi_combined"]
    img_patterns = ["", "_pm25", "_combined"]
    layer_titles = ["Ozone", "PM25", "Combined Ozone & PM25"]

    def download(self, auth_account=AIRNOW_ACCOUNT, days=1):
        """
        Connect to AirNow FTP server to retrieve last x days of hourly images.
        Requires an account with username/password.
        :param filename:
        :param auth_account:
        :param days:
        :return:
        """
        ftp = FTP(self.base_url)
        username, pwd = auth_account.split(":")
        ftp.login(username, pwd)
        ftp.cwd('GRIB2')
        file_list = ftp.nlst()
        dl_files = []
        for pattern in self.img_patterns:
            time_rx = "\d{8}" if days == 1 else "\d{6}12"
            time_pattern = ("US-{}{}\.grib2".format(time_rx, pattern))
            re_1day = re.compile(time_pattern)
            files = sorted([x for x in file_list if re_1day.search(x)])[-days:]
            for file_1day in files:
                with open(os.path.join(self.tmp_dir, file_1day),
                          'wb') as outfile:
                    ftp.retrbinary('RETR %s' % file_1day, outfile.write)
                dl_files.append(file_1day)
        return dl_files

    def parse_name(self, imgname):
        """
        Determine the time, name and title for an image based on the filename
        :param imgname: name of image file
        :return: tuple of layer title, name, and datetime
        """
        name_subs = re.search('US-(\d{8})(.*)\.grib2', imgname)
        imgtime = datetime.datetime.strptime(name_subs.group(1), "%y%m%d%H")
        imgstrtime = imgtime.strftime("%Y-%m-%d %H:00")
        imgpattern = name_subs.group(2)
        imgtitle = self.layer_titles[self.img_patterns.index(imgpattern)]
        layer_name = self.layer_names[self.img_patterns.index(imgpattern)]
        layer_title = "AirNow Hourly Air Quality Index ({}) - {} UTC".format(
            imgtitle, imgstrtime)
        return layer_title, layer_name, imgtime

    def convert(self, grib_file, imgtime, layer_name):
        """
        Convert a GRIB2 image to a GeoTIFF image in EPSG:3857 projection
        with the correctly formatted datetime in the Tiff filename.
        :param grib_file: Path/name of grib2 image file
        :param imgtime: datetime of image
        :param layer_name: Layer name for image
        :return: Path/name of output GeoTIFF
        """
        time_format = imgtime.strftime('%Y%m%dT%H0000000Z')
        tif_out = "{prefix}_{time}.tif".format(
            prefix=layer_name, time=time_format)
        warp_image(os.path.join(self.tmp_dir, grib_file),
                       os.path.join(self.tmp_dir, tif_out))
        return tif_out

    def run(self, days=1):
        """
        Download, convert, and import into GeoNode/Geoserver the last x days
        of AirNow API Grib images.
        :param days: number of days to process
        :return: None
        """
        gribs = self.download(days=days)
        for grib_file in gribs:
            layer_title, layer_name, imgtime = self.parse_name(grib_file)
            tif_out = self.convert(grib_file, imgtime, layer_name)
            dst_file = self.data_dir.format(gsd=GS_DATA_DIR, ws=self.workspace,
                                            layer=layer_name, file=tif_out)
            dst_dir = os.path.dirname(dst_file)
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
            if dst_file.endswith('.tif'):
                shutil.move(os.path.join(self.tmp_dir, tif_out), dst_file)

            self.post_geoserver(dst_file, layer_name)
            self.drop_old_hourly_images(imgtime, layer_name)
            self.drop_old_daily_images(imgtime, layer_name)
            if not style_exists(layer_name):
                self.set_default_style(layer_name, layer_name, AIRNOW_SLD)
            self.update_geonode(layer_name, title=layer_title, store=layer_name)
            self.truncate_gs_cache(layer_name)
        self.cleanup()

if __name__ == '__main__':
    processor = AirNowGRIB2HourlyProcessor()
    processor.run()