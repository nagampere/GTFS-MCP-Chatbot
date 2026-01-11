SELECT
  gtfs_id,
  _path,
  _missing,
  agency_id,
  agency_name,
  agency_url,
  agency_timezone,
  agency_lang,
  agency_phone,
  agency_fare_url,
  agency_email
FROM {{ ref('row_gtfs__agency_all') }}

