import json
import os

import requests
from urllib.parse import urlparse

from pathlib import Path

import sys

from addok import hooks
from . import batch

BAN_SERVER = "http://localhost:5959"  # "http://ban-dev.data.gouv.fr:80"
PATH = 'C:\\Users\\mhx157'


@hooks.register
def addok_register_command(subparsers):
    parser = subparsers.add_parser('update', help='Import documents update')
    parser.add_argument('filepath', nargs='*',
                        help='Path to file to process')
    parser.set_defaults(func=get_response)


def get_response(args):
    add = 'C:\\Users\\mhx157\\AllResourcesBAN.json'
    path = Path(add)
    with path.open() as f:
        bddban = {}
        for line in f:

            jr = json.loads(line)
            if jr['resource'] != 'housenumber':
                if jr['resource'] not in bddban:
                    bddban[jr['resource']] = {}
                bddban[jr['resource']][jr['id']] = jr

        for municipality in bddban['municipality']:
            response = make_municipality(None, bddban['municipality'][municipality])
            print(response)
            if response:
                batch.process(response)

        if jr['resource'] == 'locality':
            hns = get_housenumbers_in_file_for(f, jr)
            response = make_a_way(None, hns, jr['resource'], jr['id'])
            print(response)
            if response:
                batch.process(response)

        print('finish')


        # Replace with the correct URL
        # url = "http://ban-dev.data.gouv.fr/municipality/15076"
        # resp = make_municipality(None, request_call(url))
        # print(resp)

        # resp = make_a_way(None, 'street', 1)
        # print(resp)


def get_housenumbers_in_file_for(f, res):
    hns = {}
    for line in f:
        jr = json.loads(line)
        if jr['resource'] == 'housenumber' and jr[res['resource']] == res['id']:
            hns.update(make_a_housenumber(jr))
    return hns


def update_by_diff():
    diffs = get_diff()
    for diff in diffs['collection']:
        if diff['resource'] == 'municipality':
            print('resource: municipality')
        elif diff['resource'] == 'housenumber':
            if diff['new'].get('locality'):
                hns, resource_name, way = get_a_way('update', 'locality', diff['new']['locality'])
            elif diff['new'].get('street'):
                hns, resource_name, way = get_a_way('update', 'street', diff['new']['street'])
            elif diff['new'].get('districts'):
                hns, resource_name, way = get_a_way('update', 'districts', diff['new']['districts'])

        else:
            hns, resource_name, way = get_a_way('update', diff.get('resource'), diff.get('resource_id'))
        batch.process(make_a_way('update', hns, resource_name, way))
        print('process ok')

        # with Path(os.path.join(PATH, 'allBAN_addok.json')).open(mode='w') as f:
        #     for insee in range(33000, 34000):
        #         _insee = 'insee:' + str(insee)
        #         # extract_ban_process(_insee)
        #
        #         extract_ban_to_file(_insee, f)

        # url = "http://ban-dev.data.gouv.fr/municipality/15076/localities"
        # resp = make_ways(None, request_call(url))
        # print(resp)


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
        resp = get_municipality_ways_by_id(municipality_id, 'localities')
        # print(resp)
        for loc in resp:
            rep = get_a_way(None, loc.get('resource'), loc.get('id'))
            f.write(str(rep) + '\n')
        resp = get_municipality_ways_by_id(municipality_id, 'streets')
        # print(resp)
        for loc in resp:
            rep = get_a_way(None, loc.get('resource'), loc.get('id'))
            f.write(str(rep) + '\n')
        print('ok')


def extract_ban_process(municipality_id):
    municipality = get_municipality_by_id(municipality_id)
    response = make_municipality(None, municipality)
    print(response)
    if response:
        batch.process(response)
        resp = get_municipality_ways_by_id(municipality_id, 'localities')
        print(resp)
        for loc in resp:
            rep = get_a_way(None, loc.get('resource'), loc.get('id'))
            print(rep)
            batch.process(rep)
        resp = get_municipality_ways_by_id(municipality_id, 'streets')
        print(resp)
        for loc in resp:
            rep = get_a_way(None, loc.get('resource'), loc.get('id'))
            print(rep)
            batch.process(rep)
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


def make_a_way(action, hns, resource_name, way):
    json_resp = {'id': way.get('municipality').get('insee') + '_' + way.get('fantoir'), 'type': resource_name,
                 'name': way.get('name'), 'insee': way.get('municipality').get('insee'),
                 'postcode': way.get('postcode', ''), 'lon': way.get('lon', 0), 'lat': way.get('lat', 0),
                 'city': way.get('municipality').get('name'),
                 'context': way.get('name') + ',' + way.get('municipality').get('name'),
                 'importance': get_importance(way.get('name')),
                 'housenumbers': hns}
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


def iter_file(path, formatter=lambda x: x):
    path = Path(path)
    if not path.exists():
        abort('Path does not exist: {}'.format(path))
    with path.open() as f:
        for l in f:
            yield formatter(l)


def abort(msg):
    sys.stderr.write("\n" + msg)
    sys.exit(1)
