SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||route_id as route_id,
  gtfs_id||service_id as service_id,
  gtfs_id||trip_id as trip_id,
  trip_headsign,
  trip_short_name,
  gtfs_id||direction_id as direction_id,
  gtfs_id||block_id as block_id,
  gtfs_id||shape_id as shape_id,
  wheelchair_accessible,
  bikes_allowed
FROM {{ ref('row_gtfs__trips_all') }}