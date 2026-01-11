
SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||trip_id as trip_id,
  start_time,
  end_time,
  headway_secs,
  exact_times
FROM {{ ref('row_gtfs__frequencies_all') }}