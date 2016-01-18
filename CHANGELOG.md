## 0.5.0
- Expose housenumber parent name in result geojson
- add support for housenumber payload ([#134](https://github.com/etalab/addok/issues/134))
- Fix clean_query being too much greedy for "cs" ([#125](https://github.com/etalab/addok/issues/125)
- also accept long for longitude
- replace "s/s" in French preprocessing
- fix autocomplete querystring casting to boolean
- Always add housenumber in label candidates if set ([#120](https://github.com/etalab/addok/issues/120))
- make CSVView more hackable by plugins ([#116][https://github.com/etalab/addok/issues/116))


## 0.4.0
- fix filters not taken into account in manual scan ([#105](https://github.com/etalab/addok/issues/105))
- added experimental list support for document values
- Added MIN_EDGE_NGRAMS and MAX_EDGE_NGRAMS settings ([#102](https://github.com/etalab/addok/issues/102))
- documented MAKE_LABELS setting
- Allow to pass functions as PROCESSORS, instead of path
- remove raw housenumbers returned in result properties
- do not consider filter if column is empty, in csv ([#109](https://github.com/etalab/addok/issues/109))
- allow to pass lat and lon to define columns to be used for geo preference, in csv ([#110](https://github.com/etalab/addok/issues/110))
- replace "s/" by "sur" in French preprocessing ([#107](https://github.com/etalab/addok/issues/107))
- fix server failing when document was missing `importance` value
- refuse to load if `ADDOK_CONFIG_MODULE` is given but not found
- allow to set ADDOK_CONFIG_MODULE with command line parameter `--config`
- mention request parameters in geojson ([#113](https://github.com/etalab/addok/issues/113))


## 0.3.1

- fix single character wrongly glued to housenumber ([#99](https://github.com/etalab/addok/issues/99))

## 0.3.0

- use housenumber id as result id, when given ([#38](https://github.com/etalab/addok/issues/38))
- shell: warn when requested id does not exist ([#75](https://github.com/etalab/addok/issues/75))
- print filters in debug mode
- added filters to CSV endpoint ([#67](https://github.com/etalab/addok/issues/67))
- also accept `lng` as parameter ([#88](https://github.com/etalab/addok/issues/88))
- add `/get/` endpoint ([#87](https://github.com/etalab/addok/issues/87))
- display distance in meters (not kilometers)
- add distance in single `/reverse/` call
- workaround python badly sniffing csv file with only one column ([#90](https://github.com/etalab/addok/issues/90))
- add housenumber in csv results ([#91](https://github.com/etalab/addok/issues/91))
- CSV: renamed "result_address" to "result_label" ([#92](https://github.com/etalab/addok/issues/92))
- no BOM by default in UTF-8
