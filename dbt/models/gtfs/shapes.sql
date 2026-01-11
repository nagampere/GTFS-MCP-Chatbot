
SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||shape_id as shape_id,
  shape_pt_lat,
  shape_pt_lon,
  shape_pt_sequence,
  shape_dist_traveled
FROM {{ ref('row_gtfs__shapes_all') }}