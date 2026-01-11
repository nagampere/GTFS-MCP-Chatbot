{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/feed_info.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/feed_info.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/feed_info.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/feed_info.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/feed_info.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/feed_info.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/feed_info.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/feed_info.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/feed_info.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/feed_info.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/feed_info.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/feed_info.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/feed_info.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/feed_info.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/feed_info.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/feed_info.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/feed_info.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/feed_info.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/feed_info.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/feed_info.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/feed_info.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/feed_info.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/feed_info.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/feed_info.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/feed_info.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/feed_info.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/feed_info.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/feed_info.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/feed_info.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/feed_info.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/feed_info.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/feed_info.txt',
  's3://georoost/sandbox/tokyo_missing_bus/feed_info.txt',
  's3://georoost/sandbox/tokyo_rail/feed_info.txt'
] %}

{% set missing_ids = [
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS feed_publisher_name,
    NULL::VARCHAR AS feed_publisher_url,
    NULL::VARCHAR AS feed_lang,
    NULL::DATE    AS feed_start_date,
    NULL::DATE    AS feed_end_date,
    NULL::VARCHAR AS feed_version,
    NULL::VARCHAR AS feed_contact_email,
    NULL::VARCHAR AS feed_contact_url
  WHERE FALSE
{% else %}

{% for p in paths %}
  {%- if not loop.first %} UNION ALL {%- endif %}
  {# stable 配下は var(source_path) から、s3:// はそのまま読む #}
  {% set full_path = p if p.startswith('s3://') else (var('source_path') ~ '/' ~ p) %}
  {% set src_id = regexp_extract(p, '^(?:s3://[^/]+/)?(?:stable/)?(?:gtfs|sandbox)/([^/]+)/', 1) %}

  SELECT
    '{{ src_id }}' AS gtfs_id,
    '{{ p }}' AS _path,
    FALSE AS _missing,
    t.feed_publisher_name,
    t.feed_publisher_url,
    t.feed_lang,
    TRY_CAST(try_strptime(t.feed_start_date, '%Y%m%d') AS DATE) AS feed_start_date,
    TRY_CAST(try_strptime(t.feed_end_date, '%Y%m%d') AS DATE) AS feed_end_date,
    t.feed_version,
    t.feed_contact_email,
    t.feed_contact_url
  FROM read_csv(
    '{{ full_path }}',
    delim = ',',
    quote = '"',
    escape = '"',
    header = true,
    auto_detect = false,
    null_padding = true,
    null_padding = true,
    strict_mode = false,
    columns = {
      'feed_publisher_name':'VARCHAR',
      'feed_publisher_url':'VARCHAR',
      'feed_lang':'VARCHAR',
      'feed_start_date':'VARCHAR',
      'feed_end_date':'VARCHAR',
      'feed_version':'VARCHAR',
      'feed_contact_email':'VARCHAR',
      'feed_contact_url':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS feed_publisher_name,
    NULL::VARCHAR AS feed_publisher_url,
    NULL::VARCHAR AS feed_lang,
    NULL::DATE    AS feed_start_date,
    NULL::DATE    AS feed_end_date,
    NULL::VARCHAR AS feed_version,
    NULL::VARCHAR AS feed_contact_email,
    NULL::VARCHAR AS feed_contact_url
{% endfor %}

{% endif %}
)
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
FROM source

