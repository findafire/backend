from flask import Flask, request
from werkzeug.exceptions import BadRequestKeyError
from utils.fire_db_access import get_fire_db_connection
from psycopg2.extras import DictCursor
import datetime
import json

app = Flask('findafire')


def parse_date_param_from_request(request, param_name, std=None):
    try:
        date = datetime.datetime.strptime(request.args[param_name], '%Y-%m-%d').date()
    except (ValueError, BadRequestKeyError):
        if std is not None:
            date = std
        else:
            raise Exception('Date can not be parsed from request param %s' % param_name)
    return date


class DangerLevel(object):
    DANGER_LEVEL = {
        0: 'low',
        1: 'medium',
        2: 'high'
    }

    @classmethod
    def level_description(cls, danger_level):
        return cls.DANGER_LEVEL[danger_level]


@app.route('/v1/fire-places', methods=['GET'])
def get_fire():
    to_date = parse_date_param_from_request(request, 'to-date', datetime.date.today())
    from_date = parse_date_param_from_request(request, 'from-date', datetime.date.min)
    country_code = None  # TODO will be available later
    fire_places = {}
    number_of_records = 0

    with get_fire_db_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute('''SELECT id, date_of_fire, geojson, area, danger_level
                           FROM fire_places
                           WHERE date_of_fire BETWEEN '%s' AND '%s' ''' % (from_date, to_date))
            for fire in cur.fetchall():
                number_of_records += 1
                date_of_fire = str(fire['date_of_fire'])

                if date_of_fire not in fire_places:
                    fire_places[date_of_fire] = {}

                fire_places[date_of_fire][fire['id']] = {
                    'type': 'Feature',
                    'geometry': fire['geojson'],
                    'properties': {
                        'id': fire['id'],
                        'area': fire['area'],
                        'danger_level': DangerLevel.level_description(fire['danger_level'])
                    }
                }

    return json.dumps(
        dict(
            fire_places=fire_places,
            meta=dict(
                from_date=str(from_date),
                to_date=str(to_date),
                country_code=country_code,
                number_of_records=number_of_records
            )
        )
    )


if __name__ == '__main__':
    app.run(debug=True)
