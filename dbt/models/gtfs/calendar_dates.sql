SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||service_id as service_id,
  date,
  exception_type
FROM {{ ref('row_gtfs__calendar_dates_all') }}