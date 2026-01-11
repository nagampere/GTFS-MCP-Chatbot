
SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||stop_id as stop_id,
  stop_code,
  stop_name,
  stop_desc,
  stop_lat,
  stop_lon,
  zone_id,
  stop_url,
  location_type,
  parent_station,
  stop_timezone,
  wheelchair_boarding,
  gtfs_id||level_id as level_id,
  platform_code
FROM {{ ref('row_gtfs__stops_all') }}