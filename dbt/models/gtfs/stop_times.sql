SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||trip_id as trip_id,
  arrival_time,
  departure_time,
  gtfs_id||stop_id as stop_id,
  stop_sequence,
  stop_headsign,
  pickup_type,
  drop_off_type,
  continuous_pickup,
  continuous_drop_off,
  shape_dist_traveled,
  timepoint
FROM {{ ref('row_gtfs__stop_times_all') }}
