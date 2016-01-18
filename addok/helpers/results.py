from addok import config
from addok.helpers import haversine_distance, km_to_score
from addok.helpers.text import (ascii, compare_ngrams, contains, equals,
                                startswith)


def make_labels(helper, result):
    if not result.labels:
        # Make your own for better scoring (see addok-france for inspiration).
        result.labels = result._rawattr(config.NAME_FIELD)[:]
        label = result.labels[0]
        city = getattr(result, 'city', None)
        if city and city != label:
            postcode = getattr(result, 'postcode', None)
            if postcode:
                label = '{} {}'.format(label, postcode)
            label = '{} {}'.format(label, city)
            result.labels.insert(0, label)
        housenumber = getattr(result, 'housenumber', None)
        if housenumber:
            label = '{} {}'.format(housenumber, label)
            result.labels.insert(0, label)


def match_housenumber(helper, result):
    if not helper.check_housenumber:
        return
    originals = [t.original for t in helper.tokens]
    name_tokens = result.name.split()
    for original in originals:
        if original in result.housenumbers:
            raw, lat, lon, *extra = result.housenumbers[original].split('|')
            if raw in name_tokens and originals.count(original) != 2:
                # Consider that user is not requesting a housenumber if
                # token is also in name (ex. rue du 8 mai), unless this
                # token is twice in the query (8 rue du 8 mai).
                continue
            result.housenumber = raw
            result.lat = lat
            result.lon = lon
            result.type = 'housenumber'
            if extra:
                extra = zip(config.HOUSENUMBERS_PAYLOAD_FIELDS, extra)
                result._cache.update(extra)
            break


def score_by_importance(helper, result):
    importance = getattr(result, 'importance', None)
    importance = importance or 0.0
    result.add_score('importance',
                     float(importance) * config.IMPORTANCE_WEIGHT,
                     config.IMPORTANCE_WEIGHT)


def score_by_autocomplete_distance(helper, result):
    if not helper._autocomplete:
        return
    score = 0
    query = ascii(helper.query)
    for idx, label in enumerate(result.labels):
        label = ascii(label)
        result.labels[idx] = label  # Cache ascii folding.
        if equals(query, label):
            score = 1.0
        elif startswith(query, label):
            score = 0.9
        elif contains(query, label):
            score = 0.7
        if score:
            result.add_score('str_distance', score, ceiling=1.0)
            if score >= config.MATCH_THRESHOLD:
                break
    if not score:
        _score_by_ngram_distance(helper, result)


def _score_by_ngram_distance(helper, result):
    for label in result.labels:
        label = ascii(label)
        score = compare_ngrams(label, helper.query)
        result.add_score('str_distance', score, ceiling=1.0)
        if score >= config.MATCH_THRESHOLD:
            break


def score_by_ngram_distance(helper, result):
    if helper._autocomplete:
        return
    _score_by_ngram_distance(helper, result)


def score_by_geo_distance(helper, result):
    if not helper.lat or not helper.lon:
        return
    km = haversine_distance((float(result.lat), float(result.lon)),
                            (helper.lat, helper.lon))
    result.distance = km * 1000
    result.add_score('geo_distance', km_to_score(km), ceiling=0.1)


def load_closer(helper, result):
    if not helper.check_housenumber:
        return

    def sort(h):
        return haversine_distance((float(h[1]), float(h[2])),
                                  (helper.lat, helper.lon))

    candidates = [v.split('|') for v in result.housenumbers.values()]
    candidates.append((None, result.lat, result.lon))
    candidates.sort(key=sort)
    closer = candidates[0]
    if closer[0]:  # Means a housenumber is closer than street centerpoint.
        result.housenumber = closer[0]
        result.lat = closer[1]
        result.lon = closer[2]
        result.type = "housenumber"
