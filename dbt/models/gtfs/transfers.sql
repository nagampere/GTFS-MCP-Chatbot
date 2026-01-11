SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||from_stop_id as from_stop_id,
  gtfs_id||to_stop_id as to_stop_id,
  transfer_type,
  min_transfer_time
FROM {{ ref('row_gtfs__transfers_all') }}