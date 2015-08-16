from functools import partial
from utils.tiles_lookup import *
import urllib
import os.path
from multiprocessing import Pool
from utils.tiles_processing import simplify_raster_fire_file, merge_to_vrt

FIRE_DIRECTORY_URL = 'http://e4ftl01.cr.usgs.gov/MOLT/MOD14A1.005/'
TILE_TMP_DIR = 'tmp/'
ORIGINAL_TILE_SUFFIX = 'hdf'


def get_last_added_tile_urls(satellite_directory_url):
    tiles_dirs = get_tiles_directories_urls(satellite_directory_url)
    return get_tiles_files_urls(tiles_dirs[max(tiles_dirs.keys())])

def transform_fire_file(tile_filename):
    transformed_filename = tile_filename.rstrip(ORIGINAL_TILE_SUFFIX) + 'tiff'
    simplify_raster_fire_file(tile_filename, transformed_filename)
    os.remove(tile_filename)
    return transformed_filename

def fetch_fire_tile(tile_url, tile_filename, download_dir):
    fetched_file_path = os.path.join(download_dir, tile_filename)
    urllib.urlretrieve(tile_url, fetched_file_path)
    return fetched_file_path

def process_one_fire_tile(tile, download_dir):
    tile_filename, tile_url = tile
    try:
        return transform_fire_file(fetch_fire_tile(tile_url, tile_filename, download_dir))
    except IOError:
        pass

def fetch_last_added_satellite_data(satellite_fire_directory_ulr, tmp_dir):

    if not (os.path.exists(tmp_dir) and os.path.isdir(tmp_dir)):
        os.mkdir(tmp_dir)

    p = Pool(4)
    fetched_files = filter(lambda f: f is None,
                           p.map(partial(process_one_fire_tile, download_dir=tmp_dir),
                                 get_last_added_tile_urls(satellite_fire_directory_ulr).items()))

    intermediate_vrt_filename = '%s.vrt' % str(datetime.date.today())
    merge_to_vrt(intermediate_vrt_filename, fetched_files)


def fetch_last_added_data():
    fetch_last_added_satellite_data(FIRE_DIRECTORY_URL, 'satellite-data-tmp1')
    # TODO fetch_last_added_satellite_data('url-to-another-satellite', 'satellite-data-tmp2')


if __name__ == '__main__':
    fetch_last_added_data()


