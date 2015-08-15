import gdal
import ogr
import psycopg2
import re

HOST_NAME = 'api.findafire.org'
USER_NAME = 'findafire'
DB_NAME = 'findafire'
PASSWORD = 'Co6BapdDpUrFnBFPr8TuuaFQTHCCHK'

DATE = "2015-01-01"
AREA = 0
DANGER_LEVEL = 0


def connect_to_db ():
    try:
        conn = psycopg2.connect("dbname="+DB_NAME+" user="+USER_NAME+" host="+HOST_NAME+" password="+PASSWORD)
        conn.autocommit = True
    except:
        print "Cannot connect to the database"
    return conn


def get_date_from_metadata (input_filename, band_number):
    source_ds = gdal.Open(input_filename)
    source_metadata = source_ds.GetMetadata()
    get_date_as_string = source_metadata.get("DAYSOFYEAR")
    date_list = re.findall(r"\d{4}\W\d{2}\W\d{2}", get_date_as_string)

    return date_list[band_number-1]
   
 
def insert_fire_polygons (input_filename):
    source_ds = ogr.Open(input_filename)
    source_layer = source_ds.GetLayer()
    
    #  setting connection 
    new_conn = connect_to_db()
    cur = new_conn.cursor()

    #  reading all geometries
    source_feature = source_layer.GetFeature(0)
    while source_feature:
        geom = source_feature.GetGeometryRef()
        geom_center_point = geom.Centroid().ExportToWkt()
        geom_export_wkt = geom.ExportToWkt()
        geom_export_json = geom.ExportToJson()

        #  command creation for insert
        #   
        try :
            insert_com = """INSERT INTO fire_places (date_of_fire, area, danger_level, center_point, shape, geojson) 
                         VALUES ('""" + DATE + "', " + str(AREA) + ", " + str(DANGER_LEVEL) + ", " \
                         + "ST_GeomFromText('" + geom_center_point + "'), " + "ST_GeomFromText('" \
                         + geom_export_wkt+"'), " + "'" + geom_export_json+ "'" + ');'
        except:
            print "Cannot insert"
            
        cur.execute(insert_com)  
        source_feature.Destroy
        source_feature = source_layer.GetNextFeature()
    source_layer = None
    source_ds.Destroy()
"""      
input_filename = "/home/vlad/Satellite/Projections/Fire_WGS84.shp"
insert_fire_polygons (input_filename)
print(get_date_from_metadata ("/home/vlad/Satellite/Sat_img/MOD14A1.A2015209.h20v03.005.2015219195200.hdf",1))
"""
