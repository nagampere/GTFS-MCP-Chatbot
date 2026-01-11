SELECT
  gtfs_id,
  _path,
  _missing,
  table_name,
  field_name,
  language,
  translation,
  gtfs_id||record_id as record_id,
  gtfs_id||record_sub_id as record_sub_id,
  field_value
FROM {{ ref('row_gtfs__translations_all') }}