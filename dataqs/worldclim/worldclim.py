import calendar
import os
import shutil
from zipfile import ZipFile

from dataqs.helpers import gdal_translate
from dataqs.processor_base import GeoDataProcessor
from geonode.base.models import TopicCategory


class WorldClimProcessor(GeoDataProcessor):
    """
    Class for processing data from the SPEI Global Drought Monitor
    (http://sac.csic.es/spei/map/maps.html)
    """
    prefix = "worldclim_"
    version = "1_4"
    biovars = [
               "Annual Mean Temperature",
               "Mean Diurnal Range",
               "Isothermality",
               "Temperature Seasonality",
               "Max Temperature of Warmest Month",
               "Min Temperature of Coldest Month",
               "Temperature Annual Range",
               "Mean Temperature of Wettest Quarter",
               "Mean Temperature of Driest Quarter",
               "Mean Temperature of Warmest Quarter",
               "Mean Temperature of Coldest Quarter",
               "Annual Precipitation",
               "Precipitation of Wettest Month",
               "Precipitation of Driest Month",
               "Precipitation Seasonality",
               "Precipitation of Wettest Quarter",
               "Precipitation of Driest Quarter",
               "Precipitation of Warmest Quarter",
               "Precipitation of Coldest Quarter"
    ]
    cur_vars = [("tmin", "Minimum Temperature"),
            ("tmax", "Maximum Temperature"),
            ("tmean", "Mean Temperature"),
            ("prec", "Precipitation"),
            ("bio", biovars)]

    resolutions = ['10m', '5m', '2-5m']
    base_description = "WorldClim - Global Climate Data (http://worldclim.org).\n"

    def process_current(self):
        base_url = 'http://biogeo.ucdavis.edu/data/climate/worldclim/{}/grid/cur/{}_{}_bil.zip'
        desc = "Interpolations of observed data, representative of 1960-1990.\n"

        for var in self.cur_vars:
            for res in self.resolutions:
                dl_name = "{}_{}_{}_{}.zip".format(self.prefix, "current", var[0], res)
                print dl_name
                outdir = os.path.join(self.tmp_dir, self.prefix, var[0], res)
                try:
                    if not os.path.exists(os.path.join(outdir)):
                        zipbil = self.download(base_url.format(
                            self.version, var[0], res), dl_name)
                        ZipFile(os.path.join(self.tmp_dir, zipbil)).extractall(path=outdir)
                    layer_name = "WorldClim current conditions: {var}, {res} resolution"
                    if var[0] == "bio":
                        for biovar in range(1, 20):
                            name = '{}_cur_bio{}_{}'.format(self.prefix,
                                                            biovar,
                                                            res)
                            bil = os.path.join(
                                outdir, 'bio{}.bil'.format(biovar))
                            tif = bil.replace('.bil', '.tif')
                            gdal_translate(bil,
                                           tif,
                                           projection='EPSG:4326',
                                           options=['COMPRESS=DEFLATE'])
                            title = layer_name.format(
                                var=self.biovars[biovar-1],
                                res=res.replace('-', '.'))
                            desc = self.base_description + desc
                            self.post_geoserver(tif, name)
                            self.update_geonode(name, title,
                                                description=desc,
                                                store=name)

                    else:
                        for month in range(1, 13):
                            name = '{}_cur_{}_{}_{}'.format(self.prefix, var[0],
                                                            res, month)
                            bil = os.path.join(
                                outdir, '{}{}.bil'.format(var[0], month))
                            tif = bil.replace('.bil', '.tif')
                            gdal_translate(bil,
                                           tif,
                                           projection='EPSG:4326',
                                           options=['COMPRESS=DEFLATE'])
                            title = layer_name.format(
                                var='{}:{}'.format(
                                    var[1], calendar.month_name[month]),
                                res=res.replace('-', '.'))
                            desc = self.base_description + desc
                            category = TopicCategory.objects.get(
                                identifier='climatologyMeteorologyAtmosphere')
                            self.post_geoserver(os.path.join(outdir, tif), name)
                            self.update_geonode(name, title,
                                                description=desc,
                                                category=category,
                                                store=name)
                finally:
                    pass
                    #self.cleanup()
                    #shutil.rmtree(outdir, ignore_errors=True)


def process_past(self):
    base_url = 'http://biogeo.ucdavis.edu/data/climate/worldclim/{}/grid/cur/{}_{}_bil.zip'
    desc = "Interpolations of observed data, representative of 1960-1990."


if __name__ == '__main__':
    pr = WorldClimProcessor()
    # pr.cur_vars = [
    #         ("bio", pr.biovars)]
    #pr.resolutions = ['2-5m']
    pr.process_current()