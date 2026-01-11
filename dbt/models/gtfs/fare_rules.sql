SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||fare_id as fare_id,
  gtfs_id||route_id as route_id,
  gtfs_id||origin_id as origin_id,
  gtfs_id||destination_id as destination_id,
  contains_id
FROM {{ ref('row_gtfs__fare_rules_all') }}