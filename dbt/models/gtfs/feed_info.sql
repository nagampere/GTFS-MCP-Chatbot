SELECT
  gtfs_id,
  _path,
  _missing,
  feed_publisher_name,
  feed_publisher_url,
  feed_lang,
  feed_start_date,
  feed_end_date,
  feed_version,
  feed_contact_email,
  feed_contact_url
FROM {{ ref('row_gtfs__feed_info_all') }}