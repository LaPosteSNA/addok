import os
from pathlib import Path

REDIS = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Max number of results to be retrieved from db and scored.
BUCKET_LIMIT = 100

# Above this treshold, terms are considered commons.
COMMON_THRESHOLD = 10000

# Above this treshold, we avoid intersecting sets.
INTERSECT_LIMIT = 100000

# Min score considered matching the query.
MATCH_THRESHOLD = 0.9

GEOHASH_PRECISION = 7

MIN_EDGE_NGRAMS = 3
MAX_EDGE_NGRAMS = 20

RESOURCES_ROOT = Path(__file__).parent.parent.joinpath('resources')
SYNONYMS_PATH = RESOURCES_ROOT.joinpath('synonyms').joinpath('fr.txt')

# Pipeline stream to be used.
PROCESSORS = [
    'addok.textutils.default.pipeline.tokenize',
    'addok.textutils.default.pipeline.normalize',
    'addok.textutils.default.pipeline.synonymize',
    'addok.textutils.fr.phonemicize',
]
QUERY_PROCESSORS = (
    'addok.textutils.fr_FR.extract_address',
    'addok.textutils.fr_FR.clean_query',
    'addok.textutils.fr_FR.glue_ordinal',
    'addok.textutils.fr_FR.fold_ordinal',
)
HOUSENUMBER_PROCESSORS = [
    'addok.textutils.fr_FR.glue_ordinal',
    'addok.textutils.fr_FR.fold_ordinal',
]
BATCH_PROCESSORS = (
    'addok.batch.default.to_json',
    'addok.batch.ban.ban_to_row',
)
API_ENDPOINTS = [
    ('/get/<doc_id>/', 'get'),
    ('/search/', 'search'),
    ('/reverse/', 'reverse'),
    ('/search/csv/', 'search.csv'),
    ('/reverse/csv/', 'reverse.csv'),
    ('/csv/', 'search.csv'),  # Retrocompat.
]
URL_MAP = None

# Fields to be indexed
# If you want a housenumbers field but need to name it differently, just add
# type="housenumbers" to your field.
FIELDS = [
    {'key': 'name', 'boost': 4, 'null': False},
    #{'key': 'name'},
    {'key': 'locality'},
    {'key': 'id'},
    {'key': 'street'},
    {'key': 'municipality'},
    {'key': 'number'},
    {'key': 'ordinal'},
    {'key': 'context'},
    {'key': 'resource'},
    {'key': 'housenumbers', type: 'house'},

]

# Sometimes you only want to add some fields keeping the default ones.
EXTRA_FIELDS = []

# Weight of a document own importance:
IMPORTANCE_WEIGHT = 0.1

# Default score for the relation token => document
DEFAULT_BOOST = 1.0

# Data attribution
# Can also be an object {source: attribution}
ATTRIBUTION = "BAN"

# Data licence
# Can also be an object {source: licence}
LICENCE = "ODbL"

# Available filters (remember that every filter means bigger index)
FILTERS = ["type", "municipality", "street"]

LOG_DIR = os.environ.get("ADDOK_LOG_DIR", Path(__file__).parent.parent.parent)

LOG_QUERIES = False
LOG_NOT_FOUND = False

PSQL = {
    # 'dbname': 'nominatim'
    'dbname': 'banapi',
    'database': 'banapi',
    'password': 'postgres',
    'user': 'postgres',
    'host': 'localhost'


}

PSQL_PROCESSORS = (
    'addok.batch.psql.query',
    'addok.batch.ban.get_context',
    'addok.batch.ban.get_housenumbers',
    'addok.batch.ban.row_to_doc',
)

PSQL_QUERY = """SELECT
  hn.id,
  ST_X(ST_Centroid(p.center)) lon,
  ST_Y(ST_Centroid(p.center)) lat,
  s.name as street,
  m.name as municipality,
  hn."number",
  hn.ordinal,
  l.name as locality
FROM
  public.housenumber hn
    left outer join  public.locality l on hn.locality_id = l.id
    left outer join  public.street s on hn.street_id = s.id,
  public."position" p,
  public.municipality m

WHERE
  hn.id = p.housenumber_id AND
  s.municipality_id = m.id;
             {limit}
             """
PSQL_EXTRAWHERE = ''
# If you only want addresses
# PSQL_EXTRAWHERE = "AND class='highway' AND osm_type='W'"
# If you don't want any address
# PSQL_EXTRAWHERE = ("AND (class!='highway' OR osm_type='W') "
#                    "AND class!='place'")

PSQL_LIMIT = None

PSQL_ITERSIZE = 1000

UPDATING_THREAD_STATE = True

UPDATING_THREAD_JOBS = (
    'addok.thread.searchDiff'
)

UPDATING_DELAY = 5  # secondes
