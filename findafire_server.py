from flask import Flask, request
from werkzeug.exceptions import BadRequestKeyError
from utils.fire_db_access import select_fires
from utils.common import date_from_str

import datetime
import json

app = Flask(__name__)


def parse_date_param_from_request(request, param_name, std=None):
    try:
        date = date_from_str(request.args[param_name])
    except (ValueError, BadRequestKeyError):
        if std is not None:
            date = std
        else:
            raise Exception('Date can not be parsed from request param %s' % param_name)
    return date

def param_value_or_none(request, param_name):
    if param_name in request.args:
        return request.args[param_name]
    return None


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
    country_code = param_value_or_none(request, 'country-code')
    fire_places = {}
    number_of_records = 0

    for fire in select_fires(from_date=from_date, to_date=to_date, country=country_code):
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
