import json
import os
from pathlib import Path
from collections import deque
from pip._vendor.distlib.compat import raw_input

import requests

from addok.batch.utils import process

BAN_SERVER = "http://ban-dev.data.gouv.fr:80"
PATH = 'C:\\Users\\mhx157'


def get_response():
    # Replace with the correct URL
    # url = "http://ban-dev.data.gouv.fr/municipality/15076"
    # resp = make_municipality(None, request_call(url))
    # print(resp)

    # resp = make_a_way(None, 'street', 1)
    # print(resp)
    # process(resp)
    with Path(os.path.join(PATH, 'allBAN_addok.json')).open(mode='w') as f:
        for insee in range(33000, 34000):
            _insee = 'insee:' + str(insee)
            # extract_ban_process(_insee)

            extract_ban_to_file(_insee, f)



            # url = "http://ban-dev.data.gouv.fr/municipality/15076/localities"
            # resp = make_ways(None, request_call(url))
            # print(resp)


def extract_ban_to_file(municipality_id, f):
    response = make_municipality(None, municipality_id)
    # print(response)
    if response:
        f.write(str(response) + '\n')
        resp = get_municipality_ways(municipality_id, 'localities')
        # print(resp)
        for loc in resp:
            rep = make_a_way(None, loc.get('resource'), loc.get('id'))
            f.write(str(rep) + '\n')
        resp = get_municipality_ways(municipality_id, 'streets')
        # print(resp)
        for loc in resp:
            rep = make_a_way(None, loc.get('resource'), loc.get('id'))
            f.write(str(rep) + '\n')
        print('ok')


def extract_ban_process(municipality_id):
    response = make_municipality(None, municipality_id)
    print(response)
    if response:
        process(response)
        resp = get_municipality_ways(municipality_id, 'localities')
        print(resp)
        for loc in resp:
            rep = make_a_way(None, loc.get('resource'), loc.get('id'))
            print(rep)
            process(rep)
        resp = get_municipality_ways(municipality_id, 'streets')
        print(resp)
        for loc in resp:
            rep = make_a_way(None, loc.get('resource'), loc.get('id'))
            print(rep)
            process(rep)
        # if resp:
        #     process(resp)
        print('ok')


def get_municipality_ways(municipality_id, way_type):
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
            ways = request_call(ways['next'])
        except:
            break
    return response


def make_municipality(action, municipality_id):
    importance = 1
    orig_center = {'lon': 0, 'lat': 0, 'postcode': None}
    uri = BAN_SERVER + "/{}/{}".format('municipality', municipality_id)
    munic = request_call(uri)
    if munic is None:
        return None
    if munic.get('postcodes'):
        orig_center['postcode'] = munic.get('postcodes').get(0)

    response = {'id': munic.get('insee'), 'type': 'municipality', 'name': munic.get('name'),
                'postcode': orig_center.get('postcode'), 'lon': orig_center.get('lon'), 'lat': orig_center.get('lat'),
                'city': munic.get('name'), 'importance': importance}
    if action is 'update':
        response['_action'] = 'update'
    return response


def make_a_way(action, resource_name, resource_id):
    uri = BAN_SERVER + "/{}/{}".format(resource_name, resource_id)
    way = request_call(uri)
    housenumbers = request_call(uri + '/housenumbers/')
    importance = 1
    hns = {}
    orig_center = {'lon': 0, 'lat': 0, 'postcode': None}

    while True:
        for hn in housenumbers['collection']:
            hns.update(make_a_housenumber(hn, orig_center))
        try:
            housenumbers = request_call(housenumbers['next'])
        except:
            break

    json_resp = {'id': way.get('municipality').get('insee') + '_' + way.get('fantoir'), 'type': resource_name,
                 'name': way.get('name'), 'insee': way.get('municipality').get('insee'),
                 'postcode': orig_center.get('postcode'), 'lon': orig_center.get('lon'), 'lat': orig_center.get('lat'),
                 'city': way.get('municipality').get('name'),
                 'context': way.get('name') + ',' + way.get('municipality').get('name'),
                 'importance': get_importance(way.get('name')),
                 'housenumbers': hns}
    if action:
        json_resp['_action'] = action

    return json_resp


def make_a_housenumber(hn, orig_center):
    if hn['number'] != 0:
        return {(hn['number'] + ' ' + hn['ordinal']).strip(): {'lat': hn['center']['coordinates'][0],
                                                               'lon': hn['center']['coordinates'][1], 'id': hn['cia'],
                                                               'cea': hn['laposte'],
                                                               'postcode': orig_center['postcode']}}


def request_call(url):
    try:
        response = requests.get(url)
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


def write_responce(action, response, in_file):
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
