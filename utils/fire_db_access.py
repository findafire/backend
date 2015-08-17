import datetime
import os
import gdal
import ogr
import osr
import psycopg2


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

# TODO only for testing
AREA = 0
DANGER_LEVEL = 0

OGR_MEMORY_DRIVER = ogr.GetDriverByName("Memory")

MODIS_SIN_PROJECTION_WKT = 'PROJCS["unnamed", GEOGCS["Unknown datum based upon the custom spheroid", DATUM["Not_specified_based_on_custom_spheroid", SPHEROID["Custom spheroid",6371007.181,0]], PRIMEM["Greenwich",0], UNIT["degree",0.0174532925199433]], PROJECTION["Sinusoidal"], PARAMETER["longitude_of_center",0], PARAMETER["false_easting",0], PARAMETER["false_northing",0], UNIT["metre",1, AUTHORITY["EPSG","9001"]]]'
OSR_MODIS_SIN_PROJECTION_REF = osr.SpatialReference()
OSR_MODIS_SIN_PROJECTION_REF.ImportFromWkt(MODIS_SIN_PROJECTION_WKT)


def insert_fire_polygons(tiff_file, first_band_date, band_number=8):
    insert_query = """
      INSERT INTO fire_places (date_of_fire, area, danger_level, center_point, shape, geojson)
      VALUES (%(date)s, %(area)s, %(danger_level)s, ST_GeomFromText(%(center_point)s),
              ST_GeomFromText(%(shape)s), %(geojson)s );
    """

    source_ds = gdal.Open(tiff_file)
    shape_dataset = OGR_MEMORY_DRIVER.CreateDataSource('shapemask')

    with get_fire_db_connection() as conn:
        with conn.cursor() as cur:

            date_iter = first_band_date
            for band_index in xrange(1, band_number + 1):
                shape_layer = shape_dataset.CreateLayer('shapemask', OSR_MODIS_SIN_PROJECTION_REF)

                source_band = source_ds.GetRasterBand(band_index)
                gdal.Polygonize(source_band, None, shape_layer, -1, [], callback=None)

                shape_feature = shape_layer.GetNextFeature()
                while shape_feature:
                    geom = shape_feature.GetGeometryRef()

                    query_data = dict(
                        date=date_iter,
                        area=AREA,
                        danger_level=DANGER_LEVEL,
                        center_point=geom.Centroid().ExportToWkt(),
                        shape=geom.ExportToWkt(),
                        geojson=geom.ExportToJson()
                    )

                    cur.execute(insert_query, query_data)

                    shape_feature.Destroy()
                    shape_feature = shape_layer.GetNextFeature()

                date_iter = date_iter + datetime.timedelta(days=1)

    shape_dataset.Destroy()


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
