import os
import gdal
import ogr
import psycopg2
import re

from psycopg2.extras import DictCursor

DB_HOST = 'api.findafire.org'
DB_NAME = 'findafire'


def get_fire_db_connection():
    return psycopg2.connect(
        database=DB_NAME,
        host=DB_HOST,
        user=os.getenv('FINDAFIRE_DB_USER'),
        password=os.getenv('FINDAFIRE_DB_PASSWORD')
    )


def get_date_from_metadata(input_filename, band_number):
    source_ds = gdal.Open(input_filename)
    source_metadata = source_ds.GetMetadata()
    get_date_as_string = source_metadata.get("DAYSOFYEAR")
    date_list = re.findall(r"\d{4}\W\d{2}\W\d{2}", get_date_as_string)
    return date_list[band_number - 1]

# TODO only for testing
DATE = "2015-01-01"
AREA = 0
DANGER_LEVEL = 0


def insert_fire_polygons(input_filename):
    source_ds = ogr.Open(input_filename)
    source_layer = source_ds.GetLayer()

    with get_fire_db_connection() as conn:
        with conn.cursor() as cur:
            #  reading all geometries
            source_feature = source_layer.GetNextFeature()
            while source_feature:
                geom = source_feature.GetGeometryRef()
                geom_center_point = geom.Centroid().ExportToWkt()
                geom_export_wkt = geom.ExportToWkt()
                geom_export_json = geom.ExportToJson()

                insert_com = """INSERT INTO fire_places (date_of_fire, area, danger_level, center_point, shape, geojson)
                                VALUES ('%(date)s', %(area)s, %(danger_level)s, ST_GeomFromText('%(center_point)s'),
                                        ST_GeomFromText('%(shape)s'), '%(geojson)s' );
                             """ % dict(
                    date=DATE,
                    area=AREA,
                    danger_level=DANGER_LEVEL,
                    center_point=geom_center_point,
                    shape=geom_export_wkt,
                    geojson=geom_export_json
                )

                cur.execute(insert_com)
                source_feature.Destroy()
                source_feature = source_layer.GetNextFeature()

    source_layer = None
    source_ds.Destroy()


def select_fires(from_date, to_date, country=None):
    with get_fire_db_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            data = dict(from_date=from_date, to_date=to_date)
            if country:
                sql_query = '''SELECT id, date_of_fire, geojson, area, danger_level
                           FROM fire_places
                           WHERE (date_of_fire BETWEEN %(from_date)s AND %(to_date)s)
                           AND ST_Intersects((SELECT shape FROM countries WHERE iso_2_chars = %(country)s),
                                             fire_places.shape)'''
                data['country'] = country
            else:
                sql_query = '''SELECT id, date_of_fire, geojson, area, danger_level
                           FROM fire_places
                           WHERE date_of_fire BETWEEN %(from_date)s AND %(to_date)s '''

            cur.execute(sql_query, data)
            return cur.fetchall()
