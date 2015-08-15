
import psycopg2
import os

DB_HOST = 'api.findafire.org'
DB_NAME = 'findafire'


def get_fire_db_connection():
    return psycopg2.connect(
        database=DB_NAME,
        host=DB_HOST,
        user=os.getenv('FINDAFIRE_DB_USER'),
        password=os.getenv('FINDAFIRE_DB_PASSWORD')
    )

