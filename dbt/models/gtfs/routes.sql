SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||route_id as route_id,
  agency_id,
  route_short_name,
  route_long_name,
  route_desc,
  route_type,
  route_url,
  route_color,
  route_text_color,
  route_sort_order,
  continuous_pickup,
  continuous_drop_off,
  network_id
FROM {{ ref('row_gtfs__routes_all') }}