SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||fare_id as fare_id,
  price,
  currency_type,
  payment_method,
  transfers,
  agency_id,
  transfer_duration
FROM {{ref('row_gtfs__fare_attributes_all')}}