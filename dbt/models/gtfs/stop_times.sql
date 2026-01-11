{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/stop_times.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/stop_times.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/stop_times.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/stop_times.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/stop_times.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/stop_times.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/stop_times.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/stop_times.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/stop_times.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/stop_times.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/stop_times.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/stop_times.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/stop_times.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/stop_times.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/stop_times.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/stop_times.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/stop_times.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/stop_times.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/stop_times.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/stop_times.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/stop_times.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/stop_times.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/stop_times.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/stop_times.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/stop_times.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/stop_times.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/stop_times.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/stop_times.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/stop_times.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/stop_times.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/stop_times.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/stop_times.txt',
  's3://georoost/sandbox/tokyo_missing_bus/stop_times.txt',
  's3://georoost/sandbox/tokyo_rail/stop_times.txt'
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
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS arrival_time,
    NULL::VARCHAR AS departure_time,
    NULL::VARCHAR AS stop_id,
    NULL::INTEGER AS stop_sequence,
    NULL::VARCHAR AS stop_headsign,
    NULL::INTEGER AS pickup_type,
    NULL::INTEGER AS drop_off_type,
    NULL::INTEGER AS continuous_pickup,
    NULL::INTEGER AS continuous_drop_off,
    NULL::DOUBLE  AS shape_dist_traveled,
    NULL::INTEGER AS timepoint
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
    t.trip_id,
    t.arrival_time,
    t.departure_time,
    t.stop_id,
    TRY_CAST(t.stop_sequence AS INTEGER) AS stop_sequence,
    t.stop_headsign,
    TRY_CAST(t.pickup_type AS INTEGER) AS pickup_type,
    TRY_CAST(t.drop_off_type AS INTEGER) AS drop_off_type,
    TRY_CAST(t.continuous_pickup AS INTEGER) AS continuous_pickup,
    TRY_CAST(t.continuous_drop_off AS INTEGER) AS continuous_drop_off,
    TRY_CAST(t.shape_dist_traveled AS DOUBLE) AS shape_dist_traveled,
    TRY_CAST(t.timepoint AS INTEGER) AS timepoint
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
      'trip_id':'VARCHAR',
      'arrival_time':'VARCHAR',
      'departure_time':'VARCHAR',
      'stop_id':'VARCHAR',
      'stop_sequence':'VARCHAR',
      'stop_headsign':'VARCHAR',
      'pickup_type':'VARCHAR',
      'drop_off_type':'VARCHAR',
      'continuous_pickup':'VARCHAR',
      'continuous_drop_off':'VARCHAR',
      'shape_dist_traveled':'VARCHAR',
      'timepoint':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS arrival_time,
    NULL::VARCHAR AS departure_time,
    NULL::VARCHAR AS stop_id,
    NULL::INTEGER AS stop_sequence,
    NULL::VARCHAR AS stop_headsign,
    NULL::INTEGER AS pickup_type,
    NULL::INTEGER AS drop_off_type,
    NULL::INTEGER AS continuous_pickup,
    NULL::INTEGER AS continuous_drop_off,
    NULL::DOUBLE  AS shape_dist_traveled,
    NULL::INTEGER AS timepoint
{% endfor %}

{% endif %}
)
SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||trip_id as trip_id,
  arrival_time,
  departure_time,
  gtfs_id||stop_id as stop_id,
  stop_sequence,
  stop_headsign,
  pickup_type,
  drop_off_type,
  continuous_pickup,
  continuous_drop_off,
  shape_dist_traveled,
  timepoint
FROM source

