import gdal
import numpy
import subprocess
from osgeo.gdalconst import *


def wrap_hdf_filename(filename_to_wrap):
    return 'HDF4_EOS:EOS_GRID:"%s":MODIS_Grid_Daily_Fire:FireMask' % filename_to_wrap


def simplify_raster_fire_file(input_raster_hdj_file, output_raster_filename, band_count=8, fire_val_threshold=7):
    driver = gdal.GetDriverByName('GTiff')
    source_ds = gdal.Open(wrap_hdf_filename(input_raster_hdj_file), GA_ReadOnly)
    dest_ds = driver.Create(
        output_raster_filename,
        source_ds.RasterXSize,
        source_ds.RasterYSize,
        band_count,
        gdal.GDT_Byte,
        ['TILED=YES', 'COMPRESS=DEFLATE']
    )
    dest_ds.SetGeoTransform(source_ds.GetGeoTransform())
    dest_ds.SetProjection(source_ds.GetProjection())

    for band_index in xrange(1, band_count + 1):
        source_band = source_ds.GetRasterBand(band_index)
        band_data = source_band.ReadAsArray()

        bin_fire_data = numpy.zeros(shape=(len(band_data), len(band_data[0])))
        for i in range(len(band_data)):
            for j in range(len(band_data[i])):
                if band_data[i][j] >= fire_val_threshold:
                    bin_fire_data[i][j] = 1

        dest_ds.GetRasterBand(band_index).WriteArray(bin_fire_data)
        dest_ds.GetRasterBand(band_index).SetNoDataValue(0)

    dest_ds.FlushCache()


def merge_to_vrt(output_vrt_filename, input_files):
    files_to_merge = reduce(lambda acc, f: f + ' ' + acc, input_files)
    com = 'gdalbuildvrt %s %s' % (files_to_merge, output_vrt_filename)
    p = subprocess.Popen(com, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
