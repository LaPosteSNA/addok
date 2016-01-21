import json
import os

import requests
from urllib.parse import urlparse

from pathlib import Path

import sys

from addok.batch.utils import process, batch

BAN_SERVER = "http://localhost:5959"  # "http://ban-dev.data.gouv.fr:80"
PATH = 'C:\\Users\\mhx157'


def addok_register_command(subparsers):
    parser = subparsers.add_parser('update', help='Import documents update')
    parser.add_argument('filepath', nargs='*',
                        help='Path to file to process')
    parser.set_defaults(func=update_by_file)


def update_by_file(add):
    path = Path(add)
    with path.open() as f:
        print('initialize...')
        bddban = {}
        for line in f:
            jr = json.loads(line)
            if jr['resource'] != 'housenumber' or jr['number'] == '0':
                if jr['resource'] not in bddban:
                    bddban[jr['resource']] = {}
                bddban[jr['resource']][jr['id']] = jr

        print('correlate...')
        bddban = correlate_resource(bddban)

        gen = response_generator(bddban, f)
        batch(gen)
        print('finish')


def update_by_diff():
    diffs = get_diff()
    diff_generator = generator_by_diffs(diffs)
    batch(diff_generator)


def response_generator(bddban, f):
    f.seek(0)
    for line in f:
        jr = json.loads(line)
        if jr['resource'] == 'housenumber' and jr['number'] != '0':
            hns = get_housenumbers_in_file_for(bddban, jr)
            response = make_a_way(None, hns, jr['resource'], find_way_in_resource(bddban, jr))
            if response:
                yield response


def find_way_in_resource(bddban, jr):
    if jr['locality']:
        return jr['locality']
    if jr['street']:
        return jr['street']


def get_housenumbers_in_file_for(bddban, res):
    if res['street']:
        res['street'] = bddban['street'][res['street']]
    if res['locality']:
        res['locality'] = bddban['locality'][res['locality']]
    if res['postcode']:
        res['postcode'] = bddban['postcode'][res['postcode']]
    return make_a_housenumber(res)





def correlate_resource(dict_ban):
    for resource in dict_ban:
        if resource in ('locality', 'street'):
            dict_entity = dict_ban[resource]
            for entity in dict_entity:
                # remplacement ID par entité
                dict_entity[entity]['municipality'] = dict_ban['municipality'][dict_entity[entity]['municipality']]
        if resource in ('housenumber'):
            dict_entity = dict_ban[resource]
            for hn in dict_entity:
                if dict_entity[hn]['street']:
                    dict_ban['street'][dict_entity[hn]['street']]['lon'] = dict_entity[hn]['center']['coordinates'][0]
                    dict_ban['street'][dict_entity[hn]['street']]['lat'] = dict_entity[hn]['center']['coordinates'][1]
                if dict_entity[hn]['locality']:
                    dict_ban['locality'][dict_entity[hn]['locality']]['lon'] = dict_entity[hn]['center']['coordinates'][
                        0]
                    dict_ban['locality'][dict_entity[hn]['locality']]['lat'] = dict_entity[hn]['center']['coordinates'][
                        1]
    return dict_ban


def generator_by_diffs(diffs):
    for diff in diffs['collection']:
        if diff['resource'] != 'municipality':
            if diff['resource'] == 'housenumber':
                if diff['new'].get('locality'):
                    hns, resource_name, way = get_a_way('locality', diff['new']['locality'])
                elif diff['new'].get('street'):
                    hns, resource_name, way = get_a_way('street', diff['new']['street'])
                elif diff['new'].get('districts'):
                    hns, resource_name, way = get_a_way('districts', diff['new']['districts'])

            else:
                hns, resource_name, way = get_a_way(diff.get('resource'), diff.get('resource_id'))

            yield make_a_way('update', hns, resource_name, way)


def get_housenumber_way(housenumber_id):
    uri = BAN_SERVER + "/housenumber/{}".format(housenumber_id)
    return request_call(uri)


def get_diff(increment=''):
    uri = BAN_SERVER + "/diff/?increment={}".format(increment)
    return request_call(uri)


def extract_ban_to_file(municipality_id, f):
    response = make_municipality(None, municipality_id)
    # print(response)
    if response:
        f.write(str(response) + '\n')
        ways = get_municipality_ways_by_id(municipality_id, 'localities')
        for response in ways_generator(None, ways):
            f.write(str(response) + '\n')

        ways = get_municipality_ways_by_id(municipality_id, 'streets')
        for response in ways_generator(None, ways):
            f.write(str(response) + '\n')
        print('ok')


def ways_generator(update, response):
    for loc in response:
        yield get_a_way(update, loc.get('resource'), loc.get('id'))



def extract_ban_process(municipality_id):
    municipality = get_municipality_by_id(municipality_id)
    response = make_municipality(None, municipality)
    print(response)
    if response:
        process(response)
        ways = get_municipality_ways_by_id(municipality_id, 'localities')
        for response in ways_generator(ways):
            batch(response)

        ways = get_municipality_ways_by_id(municipality_id, 'streets')
        print(ways)
        for response in ways_generator(ways):
            batch(response)

        # if resp:
        #     process(resp)
        print('ok')


def get_municipality_ways_by_id(municipality_id, way_type):
    uri = BAN_SERVER + "/municipality/{}/{}".format(municipality_id, way_type)
    ways = request_call(uri)
    return get_ways_by_municipality(ways)


def get_ways_by_municipality(ways):
    if ways is None:
        return None
    response = []
    while True:
        for way in ways['collection']:
            response.append(way)
        try:
            ways = request_call(BAN_SERVER + urlparse(ways.get('next')).get('path'))
        except:
            break
    return response


def make_municipality(action, municipality):
    importance = 1
    orig_center = {'lon': 0, 'lat': 0, 'postcode': None}
    if municipality is None:
        return None
    if municipality.get('postcodes'):
        orig_center['postcode'] = municipality.get('postcodes').get(0)

    response = {'id': municipality.get('insee'), 'type': 'municipality', 'name': municipality.get('name'),
                'postcode': orig_center.get('postcode'), 'lon': orig_center.get('lon'), 'lat': orig_center.get('lat'),
                'city': municipality.get('name'), 'importance': importance}
    if action is 'update':
        response['_action'] = 'update'
    return response


def get_municipality_by_id(municipality_id):
    uri = BAN_SERVER + "/{}/{}".format('municipality', municipality_id)
    municipality = request_call(uri)
    return municipality


def get_a_way(resource_name, resource_id):
    uri = BAN_SERVER + "/{}/{}".format(resource_name, resource_id)
    way = request_call(uri)
    hns = get_housenumbers(uri)
    return hns, resource_name, way


def make_a_way(action, housenumbers, resource_name, way):
    json_resp = {'id': way.get('municipality').get('insee') + '_' + way.get('fantoir'), 'type': resource_name,
                 'name': way.get('name'), 'insee': way.get('municipality').get('insee'),
                 'postcode': way.get('postcode', ''), 'lon': way.get('lon', 0), 'lat': way.get('lat', 0),
                 'city': way.get('municipality').get('name'),
                 'context': way.get('name') + ',' + way.get('municipality').get('name'),
                 'importance': get_importance(way.get('name'))}
    if housenumbers:
        json_resp['housenumbers'] = housenumbers

    if action:
        json_resp['_action'] = action

    return json_resp


def get_housenumbers(uri):
    hns = {}
    housenumbers = request_call(uri + '/housenumbers/')
    while True:
        for hn in housenumbers['collection']:
            hns.update(make_a_housenumber(hn))
        try:
            housenumbers = request_call(BAN_SERVER + urlparse(housenumbers.get('next')).get('path'))
        except:
            break
    return hns


def make_a_housenumber(hn):
    if hn['number'] != 0:
        return {(hn['number'] + ' ' + hn['ordinal']).strip(): {'lat': hn['center']['coordinates'][0],
                                                               'lon': hn['center']['coordinates'][1], 'id': hn['cia'],
                                                               'cea': hn['laposte'],
                                                               'postcode': hn['postcode']}}


def request_call(url):
    try:
        os.environ['NO_PROXY'] = 'localhost'
        headers = {'Authorization': 'Bearer token'}
        response = requests.get(url, headers=headers)
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        return None
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        return None
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        return None
    except:
        return None
    if response.ok:
        return response.json()
    else:
        return None


def write_responce(response, in_file):
    in_file.write(response + '\n')


def get_importance(street_name):
    importance = 1 / 4
    if not street_name.find('Boulevard') == -1 or not street_name.find('Place') == -1 or not street_name.find(
            'Espl') == -1:
        importance = 4 / 4
    elif not street_name.find('Av') == -1:
        importance = 3 / 4
    elif not street_name.find('Rue') == -1:
        importance = 2 / 4
    return importance
