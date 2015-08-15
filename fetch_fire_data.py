
from utils.tiles_lookup import *
import urllib
import os.path
from multiprocessing import Pool
from utils.tiles_processing import simplify_raster_fire_file, merge_to_vrt

FIRE_DIRECTORY_URL = 'http://e4ftl01.cr.usgs.gov/MOLT/MOD14A1.005/'
TILE_TMP_DIR = 'tmp/'
ORIGINAL_TILE_SUFFIX = 'hdf'


def get_last_added_tile_urls():
    tiles_dirs = get_tiles_directories_urls(FIRE_DIRECTORY_URL)
    return get_tiles_files_urls(tiles_dirs[max(tiles_dirs.keys())])

def transform_fire_file(tile_filename):
    transformed_filename = tile_filename.rstrip(ORIGINAL_TILE_SUFFIX) + 'tiff'
    simplify_raster_fire_file(tile_filename, transformed_filename)
    os.remove(tile_filename)
    return transformed_filename

def fetch_fire_tile(tile_url, tile_filename):
    fetched_file_path = os.path.join(TILE_TMP_DIR, tile_filename)
    urllib.urlretrieve(tile_url, fetched_file_path)
    return fetched_file_path

def process_one_fire_tile(tile):
    tile_filename, tile_url = tile
    try:
        return transform_fire_file(fetch_fire_tile(tile_url, tile_filename))
    except IOError:
        pass

def fetch_last_added_data():

    if not (os.path.exists(TILE_TMP_DIR) and os.path.isdir(TILE_TMP_DIR)):
        os.mkdir(TILE_TMP_DIR)

    p = Pool(4)
    fetched_files = filter(lambda f: f is None,
                           p.map(process_one_fire_tile, get_last_added_tile_urls().items()))

    intermediate_vrt_filename = '%s.vrt' % str(datetime.date.today())
    merge_to_vrt(intermediate_vrt_filename, fetched_files)


if __name__ == '__main__':
    fetch_last_added_data()


