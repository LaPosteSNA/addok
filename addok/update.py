import json
import os
import requests

from urllib.parse import urlparse
from pathlib import Path
import logging

from addok.batch.utils import process, batch, get_increment, set_increm
from addok.config import default as config

BAN_SERVER = config.BAN_SERVER


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
            json_line = json.loads(line)
            if json_line['resource'] != 'housenumber' or json_line['number'] == '0':
                if json_line['resource'] not in bddban:
                    bddban[json_line['resource']] = {}
                bddban[json_line['resource']][json_line['id']] = json_line

        print('correlate...')
        bddban = correlate_resource(bddban)

        print('update redis with unitary resources...')
        for municipality in bddban['municipality']:
            response = make_municipality(None, bddban['municipality'][municipality])
            if response:
                process(response)

        print('update redis with housenumber resources...')
        gen = response_generator(bddban, f)
        batch(gen)
        print('finish')


def update_by_diff(increment=0):
    # if increment == '':
    #     increment = 0
    if get_increment() < int(increment):
        set_increm(increment)
    diffs = request_diff(increment)
    if diffs:
        diff_generator = generator_by_diffs(diffs)
        batch(diff_generator)


def response_generator(bddban, f):
    f.seek(0)
    for line in f:
        json_line = json.loads(line)
        if json_line['resource'] == 'housenumber' and json_line['number'] != '0':  # a voir pour les vrai hn à Null ou 0
            hns = get_housenumbers_in_file_for(bddban, json_line)
            json_resource = find_way_in_resource(json_line)
            response = make_a_way(None, hns, json_resource['resource'], json_resource)
            if response:
                yield response


def find_way_in_resource(json_resource):
    if json_resource['locality']:
        return json_resource['locality']
    if json_resource['street']:
        return json_resource['street']


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
        if resource in 'housenumber':
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
    for diff in diffs:
        if diff['resource'] != 'municipality':
            resource_name, way = get_a_way(diff['resource'], diff['new']['id'])

            if diff['resource'] == 'housenumber':
                hns = make_a_housenumber(way)
                if way.get('street'):
                    resource_name, way = get_a_way('street', way['street']['id'])
                elif way.get('locality'):
                    resource_name, way = get_a_way('locality', way['locality']['id'])
            else:
                hns = request_housenumbers(diff['resource'], diff['new']['id'])
            yield make_a_way('update', hns, resource_name, way)


def extract_ban_to_file(municipality_id, f):
    response = make_municipality(None, municipality_id)
    # print(response)
    if response:
        f.write(str(response) + '\n')
        ways = request_municipality_ways_by_id(municipality_id, 'localities')
        [f.write(str(response) + '\n') for response in ways_generator(ways)]

        ways = request_municipality_ways_by_id(municipality_id, 'streets')
        [f.write(str(response) + '\n') for response in ways_generator(ways)]
        print('ok')


def ways_generator(response):
    for loc in response:
        yield get_a_way(loc.get('resource'), loc.get('id'))


def extract_ban_process(municipality_id):
    municipality = request_municipality_by_id(municipality_id)
    response = make_municipality(None, municipality)
    print(response)
    if response:
        process(response)
        ways = request_municipality_ways_by_id(municipality_id, 'localities')
        [batch(response) for response in ways_generator(ways)]

        ways = request_municipality_ways_by_id(municipality_id, 'streets')
        [batch(response) for response in ways_generator(ways)]
        print('ok')


def request_municipality_ways_by_id(municipality_id, way_type):
    uri = BAN_SERVER + "/municipality/{}/{}".format(municipality_id, way_type)
    ways = request_call(uri)
    return ways


def request_housenumber_way(housenumber_id):
    uri = BAN_SERVER + "/housenumber/{}".format(housenumber_id)
    return request_call(uri)


def request_diff(increment=''):
    uri = BAN_SERVER + "/diff/?increment={}".format(increment)
    return request_call(uri)


def request_municipality_by_id(municipality_id):
    uri = BAN_SERVER + "/{}/{}".format('municipality', municipality_id)
    municipality = request_call(uri)
    return municipality


def get_a_way(resource_name, resource_id):
    uri = BAN_SERVER + "/{}/{}".format(resource_name, resource_id)
    way = request_call(uri)

    return resource_name, way


def request_housenumbers(resource_name, resource_id):
    uri = BAN_SERVER + "/{}/{}".format(resource_name, resource_id)
    hns = {}
    housenumbers = request_call(uri + '/housenumbers/')
    [hns.update(make_a_housenumber(hn)) for hn in housenumbers]
    return hns


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


def make_a_way(action, housenumbers, resource_name, way):
    json_resp = {'id': way.get('municipality').get('insee') + '_' + way.get('fantoir'), 'type': resource_name,
                 'name': way.get('name'), 'insee': way.get('municipality').get('insee'),
                 'postcode': way.get('postcode', ''), 'lon': way.get('lon', 0), 'lat': way.get('lat', 0),
                 'city': way.get('municipality').get('name'),
                 'context': way.get('name') + ',' + way.get('municipality').get('name'),
                 'importance': compute_importance(way.get('name'))}
    if housenumbers:
        json_resp['housenumbers'] = housenumbers

    if action:
        json_resp['_action'] = action

    return json_resp


def make_a_housenumber(hn):
    if hn['number'] != 0:
        return {(hn['number'] + ' ' + hn['ordinal']).strip(): {'lat': hn['center']['coordinates'][0],
                                                               'lon': hn['center']['coordinates'][1], 'id': hn['cia'],
                                                               'cea': hn['laposte'],
                                                               'postcode': hn['postcode']}}


def compute_importance(name):
    importance = 1 / 4
    if 'Boulevard' or 'Place' or 'Espl' or 'Esplanade' or 'Paris' in name:
        importance = 4 / 4
    elif 'Av' or 'Avenue' in name:
        importance = 3 / 4
    elif 'Rue' in name:
        importance = 2 / 4
    return importance


def request_call(url):
    resp = []
    transition = url_call(url)
    if transition.get('collection'):
        while True:
            [resp.append(item) for item in transition['collection']]
            try:
                q = urlparse(transition.get('next'))
                transition = url_call(BAN_SERVER + q.path + '?' + q.query)
            except Exception as e:
                logging.log(logging.DEBUG, e)
                break
        return resp
    else:
        return transition


def url_call(url):
    os.environ['NO_PROXY'] = 'localhost'
    headers = {'Authorization': 'Bearer token'}
    try:
        response = requests.get(url, headers=headers)
    except Exception as e:
        logging.exception(e)
        return None
    if response.ok:
        return response.json()
