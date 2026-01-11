SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||service_id as service_id,
  monday,
  tuesday,
  wednesday,
  thursday,
  friday,
  saturday,
  sunday,
  start_date,
  end_date
FROM {{ ref('row_gtfs__calendar_all') }}