import gdal
import numpy
from osgeo.gdalconst import *


def simplify_raster_fire_file(input_raster_filename, output_raster_filename, band_count=8, fire_val_threshold=7):
    driver = gdal.GetDriverByName('GTiff')
    source_ds = gdal.Open(input_raster_filename, GA_ReadOnly)
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


def merge_to_vrt(hdf_files):
    pass

