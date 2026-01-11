{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/stops.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/stops.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/stops.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/stops.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/stops.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/stops.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/stops.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/stops.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/stops.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/stops.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/stops.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/stops.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/stops.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/stops.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/stops.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/stops.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/stops.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/stops.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/stops.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/stops.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/stops.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/stops.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/stops.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/stops.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/stops.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/stops.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/stops.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/stops.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/stops.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/stops.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/stops.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/stops.txt',
  's3://georoost/sandbox/tokyo_missing_bus/stops.txt',
  's3://georoost/sandbox/tokyo_rail/stops.txt'
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

    NULL::VARCHAR AS stop_id,
    NULL::VARCHAR AS stop_code,
    NULL::VARCHAR AS stop_name,
    NULL::VARCHAR AS stop_desc,
    NULL::DOUBLE  AS stop_lat,
    NULL::DOUBLE  AS stop_lon,
    NULL::VARCHAR AS zone_id,
    NULL::VARCHAR AS stop_url,
    NULL::INTEGER AS location_type,
    NULL::VARCHAR AS parent_station,
    NULL::VARCHAR AS stop_timezone,
    NULL::INTEGER AS wheelchair_boarding,
    NULL::VARCHAR AS level_id,
    NULL::VARCHAR AS platform_code
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

    t.stop_id,
    t.stop_code,
    t.stop_name,
    t.stop_desc,
    TRY_CAST(t.stop_lat AS DOUBLE) AS stop_lat,
    TRY_CAST(t.stop_lon AS DOUBLE) AS stop_lon,
    t.zone_id,
    t.stop_url,
    TRY_CAST(t.location_type AS INTEGER) AS location_type,
    t.parent_station,
    t.stop_timezone,
    TRY_CAST(t.wheelchair_boarding AS INTEGER) AS wheelchair_boarding,
    t.level_id,
    t.platform_code
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
      'stop_id':'VARCHAR',
      'stop_code':'VARCHAR',
      'stop_name':'VARCHAR',
      'stop_desc':'VARCHAR',
      'stop_lat':'VARCHAR',
      'stop_lon':'VARCHAR',
      'zone_id':'VARCHAR',
      'stop_url':'VARCHAR',
      'location_type':'VARCHAR',
      'parent_station':'VARCHAR',
      'stop_timezone':'VARCHAR',
      'wheelchair_boarding':'VARCHAR',
      'level_id':'VARCHAR',
      'platform_code':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,

    NULL::VARCHAR AS stop_id,
    NULL::VARCHAR AS stop_code,
    NULL::VARCHAR AS stop_name,
    NULL::VARCHAR AS stop_desc,
    NULL::DOUBLE  AS stop_lat,
    NULL::DOUBLE  AS stop_lon,
    NULL::VARCHAR AS zone_id,
    NULL::VARCHAR AS stop_url,
    NULL::INTEGER AS location_type,
    NULL::VARCHAR AS parent_station,
    NULL::VARCHAR AS stop_timezone,
    NULL::INTEGER AS wheelchair_boarding,
    NULL::VARCHAR AS level_id,
    NULL::VARCHAR AS platform_code
{% endfor %}

{% endif %}
)
SELECT * FROM source

