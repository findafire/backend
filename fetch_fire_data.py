from functools import partial
import time
from utils.tiles_lookup import *
from multiprocessing import Pool
from utils.tiles_processing import simplify_raster_fire_file
from utils.common import date_from_str, TimePeriod
from utils.fire_db_access import insert_fire_polygons

import urllib
import os.path

ORIGINAL_TILE_SUFFIX = 'hdf'
SATELLITES = [
    ('http://e4ftl01.cr.usgs.gov/MOLT/MOD14A1.005/', 'tmp-terra'),
    # ('http://e4ftl01.cr.usgs.gov/MOLA/MYD14A1.005/', 'tmp-aqua')
]


def get_last_added_tile_urls(satellite_directory_url):
    tiles_dirs = get_tiles_directories_urls(satellite_directory_url)
    last_date = max(tiles_dirs.keys())
    return {last_date: tiles_dirs[last_date]}


def get_tiles_dirs_by_date_filter(satellite_fire_directory_ulr, date_filter_func):
    return get_tiles_directories_urls(satellite_fire_directory_ulr, date_filter_func)


def transform_fire_file(tile_filename):
    transformed_filename = tile_filename.rstrip(ORIGINAL_TILE_SUFFIX) + 'tiff'
    simplify_raster_fire_file(tile_filename, transformed_filename)
    return transformed_filename


def download_fire_tile(tile_url, tile_filename, download_dir):
    fetched_file_path = os.path.join(download_dir, tile_filename)
    urllib.urlretrieve(tile_url, fetched_file_path)
    return fetched_file_path


def process_one_fire_tile(tile, download_dir, tile_date):
    tile_filename, tile_url = tile
    try:
        hdf_tile = download_fire_tile(tile_url, tile_filename, download_dir)
        tiff_tile = transform_fire_file(hdf_tile)
        insert_fire_polygons(tiff_tile, tile_date)
        os.remove(hdf_tile)
        os.remove(tiff_tile)
    except IOError:
        pass


def fetch_satellite_data(satellite_fire_directory_ulr, tmp_dir, tiles_dirs_getter):
    if not (os.path.exists(tmp_dir) and os.path.isdir(tmp_dir)):
        os.mkdir(tmp_dir)

    p = Pool(4)
    for date, tiles_directory_url in tiles_dirs_getter(satellite_fire_directory_ulr).items():
        p.map(partial(process_one_fire_tile, download_dir=tmp_dir, tile_date=date),
              get_tiles_files_urls(tiles_directory_url).items())


def fetch_data(tiles_dirs_getter=partial(get_tiles_dirs_by_date_filter, date_filter_func=lambda date: True)):
    for satellite_fire_directory_url, tmp_dir in SATELLITES:
        fetch_satellite_data(satellite_fire_directory_url, tmp_dir, tiles_dirs_getter)


if __name__ == '__main__':
    import argparse
    import sys

    args_parser = argparse.ArgumentParser(
        version='1.0',
        add_help=True,
        description='Stores satellite data into fire db.'
    )
    args_parser.add_argument(
        '-a', '--all',
        action='store_true',
        dest='all',
        help='Fetch, process and store all available satellite data'
    )
    args_parser.add_argument(
        '-l', '--last',
        action='store_true',
        dest='last',
        help='Fetch, process and store latest satellite data'
    )
    args_parser.add_argument(
        '-e', '--exact',
        action='store',
        type=date_from_str,
        help='Fetch, process and store satellite data with exact date'
    )
    args_parser.add_argument(
        '-p', '--period',
        action='store',
        type=TimePeriod.from_str,
        help=''
    )

    parsed_args = args_parser.parse_args(sys.argv[1:])
    if parsed_args.all:
        print 'Fetching of all data is begun'
        fetch_data()
    elif parsed_args.exact:
        print 'Fetching of data with exact date %s is begun' % parsed_args.exact
        fetch_data(partial(get_tiles_dirs_by_date_filter,
                           date_filter_func=lambda date: date == parsed_args.exact))
    elif parsed_args.last:
        print 'Fetching of fresh data is begun'
        fetch_data(get_last_added_tile_urls)
    elif parsed_args.period:
        print 'Fetching data for the period %s id begun' % parsed_args.period
        fetch_data(
            partial(
                get_tiles_dirs_by_date_filter,
                date_filter_func=lambda date: parsed_args.period.from_date <= date <= parsed_args.period.to_date
            )
        )
    else:
        print 'There is no job'
        sys.exit(0)
    print 'Job is finished'
