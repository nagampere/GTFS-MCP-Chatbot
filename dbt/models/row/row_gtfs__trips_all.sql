{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/trips.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/trips.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/trips.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/trips.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/trips.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/trips.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/trips.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/trips.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/trips.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/trips.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/trips.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/trips.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/trips.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/trips.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/trips.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/trips.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/trips.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/trips.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/trips.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/trips.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/trips.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/trips.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/trips.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/trips.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/trips.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/trips.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/trips.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/trips.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/trips.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/trips.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/trips.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/trips.txt',
  's3://georoost/sandbox/tokyo_missing_bus/trips.txt',
  's3://georoost/sandbox/tokyo_rail/trips.txt'
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
    NULL::VARCHAR AS route_id,
    NULL::VARCHAR AS service_id,
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS trip_headsign,
    NULL::VARCHAR AS trip_short_name,
    NULL::INTEGER AS direction_id,
    NULL::VARCHAR AS block_id,
    NULL::VARCHAR AS shape_id,
    NULL::INTEGER AS wheelchair_accessible,
    NULL::INTEGER AS bikes_allowed
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
    t.route_id,
    t.service_id,
    t.trip_id,
    t.trip_headsign,
    t.trip_short_name,
    TRY_CAST(t.direction_id AS INTEGER) AS direction_id,
    t.block_id,
    t.shape_id,
    TRY_CAST(t.wheelchair_accessible AS INTEGER) AS wheelchair_accessible,
    TRY_CAST(t.bikes_allowed AS INTEGER) AS bikes_allowed
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
      'route_id':'VARCHAR',
      'service_id':'VARCHAR',
      'trip_id':'VARCHAR',
      'trip_headsign':'VARCHAR',
      'trip_short_name':'VARCHAR',
      'direction_id':'VARCHAR',
      'block_id':'VARCHAR',
      'shape_id':'VARCHAR',
      'wheelchair_accessible':'VARCHAR',
      'bikes_allowed':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS route_id,
    NULL::VARCHAR AS service_id,
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS trip_headsign,
    NULL::VARCHAR AS trip_short_name,
    NULL::INTEGER AS direction_id,
    NULL::VARCHAR AS block_id,
    NULL::VARCHAR AS shape_id,
    NULL::INTEGER AS wheelchair_accessible,
    NULL::INTEGER AS bikes_allowed
{% endfor %}

{% endif %}
)
SELECT * FROM source

