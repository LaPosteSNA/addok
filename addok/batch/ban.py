"""Import from BAN database."""

import psycopg2
import psycopg2.extras

from addok.utils import yielder
from .psql import connection


@yielder
def get_context(row):
    if "context" not in row:
        row['context'] = []
    #add_parent(row, row)
    return row


@yielder
def get_housenumbers(row):
    #print(row)

    # if row['class'] == 'highway':
    #     sql = """SELECT housenumber, ST_X(ST_Centroid(geometry)) as lon,
    #         ST_Y(ST_Centroid(geometry)) as lat
    #         FROM placex
    #         WHERE housenumber IS NOT NULL
    #         AND parent_place_id=%(place_id)s"""
    #     cur = connection.cursor(str(row['place_id']),
    #                             cursor_factory=psycopg2.extras.DictCursor)
    #     cur.execute(sql, {'place_id': row['place_id']})
    #     housenumbers = cur.fetchall()
    #     cur.close()
    row['housenumber'] = {'lat': row['lat'], 'lon': row['lon']
                           }
    return row


@yielder
def row_to_doc(row):
    doc = {
        "id": row["id"],
        "lat": row['lat'],
        "lon": row['lon'],
        "name":  ' '.join([row['number'], row['street'], row['municipality']]),
        # All HouseNumbers have the same importance
        "importance": 1 * 0.1
    }
    municipality = row.get('municipality')
    if municipality:
        doc['municipality'] = municipality
        doc['type'] = 'municipality'

    street = row.get('street')
    if street:
        doc['street'] = street
        doc['type'] = 'street'

    locality = row.get('locality')
    if street:
        doc['locality'] = locality
        doc['type'] = 'locality'

    context = row.get('context')
    if context:
        doc['context'] = context
    number = row.get('number')
    if number:
        doc['number'] = number
        doc['type'] = 'housenumber'

    row['source'] = 'BAN'
    # See https://wiki.osm.org/wiki/Nominatim/Development_overview#Country_to_street_level  # noqa
    doc['importance'] = (30 / 30) * 0.1
    return doc

def last_update():
    from addok.db import DB
    pipe = DB.pipeline()
    pipe.sadd("last_update", key)

@yielder
def ban_to_row(row):
    print(row)

    return row