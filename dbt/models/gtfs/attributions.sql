SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||attribution_id as attribution_id,
  agency_id,
  gtfs_id||route_id as route_id,
  gtfs_id||trip_id as trip_id,
  organization_name,
  is_producer,
  is_operator,
  is_authority,
  attribution_url,
  attribution_email,
  attribution_phone
FROM {{ ref('row_gtfs__attributions_all') }}