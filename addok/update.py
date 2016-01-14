import os

import requests
from urllib.parse import urlparse

from addok.batch.utils import process

BAN_SERVER = "http://localhost:5959"  # "http://ban-dev.data.gouv.fr:80"
PATH = 'C:\\Users\\mhx157'


def get_response():
    # Replace with the correct URL
    # url = "http://ban-dev.data.gouv.fr/municipality/15076"
    # resp = make_municipality(None, request_call(url))
    # print(resp)

    # resp = make_a_way(None, 'street', 1)
    # print(resp)
    diffs = get_diff()
    for diff in diffs['collection']:
        if diff['resource'] == 'municipality':
            print('resource: municipality')
        elif diff['resource'] == 'housenumber':
            if diff['new'].get('locality'):
                process(make_a_way('update', 'locality', diff['new']['locality']))
            elif diff['new'].get('street'):
                process(make_a_way('update', 'locality', diff['new']['street']))
            elif diff['new'].get('districts'):
                process(make_a_way('update', 'locality', diff['new']['districts']))
        else:
            process(make_a_way('update', diff.get('resource'), diff.get('resource_id')))
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
            ways = request_call(BAN_SERVER + urlparse(ways.get('next')).get('path'))
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

    importance = 1
    hns = get_housenumbers(uri)
    default_orig_center = {'lon': 0, 'lat': 0, 'postcode': None}

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
