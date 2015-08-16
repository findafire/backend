import ogr
import utils.fire_db_access as db


def clear_country_table():
    with db.get_fire_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM countries')


def insert_country(cur, country_feature):
    feature_items = country_feature.items()
    country = dict(
        iso2=feature_items['ISO2'],
        name=feature_items['NAME'].replace("'", "''"),
        shape=country_feature.GetGeometryRef().ExportToWkt()
    )
    cur.execute("""INSERT INTO countries (iso_2_chars, name, shape)
                   VALUES ('%(iso2)s', '%(name)s', ST_Multi(ST_GeomFromText('%(shape)s')))""" % country)


# Dataset from http://thematicmapping.org/downloads/world_borders.php is expected
def fill_country_table(country_shp_file):
    with db.get_fire_db_connection() as conn:
        with conn.cursor() as cur:
            for county in traverse_countries(country_shp_file):
                insert_country(cur, county)


def traverse_countries(country_shp_file):
    ds = ogr.Open(country_shp_file)
    try:
        layer = ds.GetLayer()
        feature_iter = layer.GetNextFeature()

        while feature_iter:
            yield feature_iter
            feature_iter = layer.GetNextFeature()

    finally:
        ds.Destroy()


if __name__ == '__main__':
    import argparse
    import sys

    args_parser = argparse.ArgumentParser(
        version='1.0',
        add_help=True,
        description='Inserts countries data into findafire db.'
    )
    args_parser.add_argument(
        'data_source',
        metavar='shp-file',
        type=str,
        help='Shape file with countries.'
    )
    args_parser.add_argument(
        '-d', '--delete',
        action='store_true',
        dest='delete',
        help='Removes all previous data about countries.'
    )
    args = args_parser.parse_args(sys.argv[1:])
    if args.delete:
        clear_country_table()
    fill_country_table(args.data_source)
