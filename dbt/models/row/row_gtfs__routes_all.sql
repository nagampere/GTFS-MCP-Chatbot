{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/routes.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/routes.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/routes.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/routes.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/routes.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/routes.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/routes.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/routes.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/routes.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/routes.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/routes.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/routes.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/routes.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/routes.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/routes.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/routes.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/routes.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/routes.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/routes.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/routes.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/routes.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/routes.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/routes.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/routes.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/routes.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/routes.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/routes.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/routes.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/routes.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/routes.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/routes.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/routes.txt',
  's3://georoost/sandbox/tokyo_missing_bus/routes.txt',
  's3://georoost/sandbox/tokyo_rail/routes.txt'
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
    NULL::VARCHAR AS agency_id,
    NULL::VARCHAR AS route_short_name,
    NULL::VARCHAR AS route_long_name,
    NULL::VARCHAR AS route_desc,
    NULL::INTEGER AS route_type,
    NULL::VARCHAR AS route_url,
    NULL::VARCHAR AS route_color,
    NULL::VARCHAR AS route_text_color,
    NULL::INTEGER AS route_sort_order,
    NULL::INTEGER AS continuous_pickup,
    NULL::INTEGER AS continuous_drop_off,
    NULL::VARCHAR AS network_id
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
    t.agency_id,
    t.route_short_name,
    t.route_long_name,
    t.route_desc,
    TRY_CAST(t.route_type AS INTEGER) AS route_type,
    t.route_url,
    t.route_color,
    t.route_text_color,
    TRY_CAST(t.route_sort_order AS INTEGER) AS route_sort_order,
    TRY_CAST(t.continuous_pickup AS INTEGER) AS continuous_pickup,
    TRY_CAST(t.continuous_drop_off AS INTEGER) AS continuous_drop_off,
    t.network_id
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
      'agency_id':'VARCHAR',
      'route_short_name':'VARCHAR',
      'route_long_name':'VARCHAR',
      'route_desc':'VARCHAR',
      'route_type':'VARCHAR',
      'route_url':'VARCHAR',
      'route_color':'VARCHAR',
      'route_text_color':'VARCHAR',
      'route_sort_order':'VARCHAR',
      'continuous_pickup':'VARCHAR',
      'continuous_drop_off':'VARCHAR',
      'network_id':'VARCHAR'
    }
  ) AS t
{% endfor %}

-- {% for id in missing_ids %}
--   {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
--   SELECT
--     '{{ id }}' AS gtfs_id,
--     NULL::VARCHAR AS _path,
--     TRUE AS _missing,
--     NULL::VARCHAR AS route_id,
--     NULL::VARCHAR AS agency_id,
--     NULL::VARCHAR AS route_short_name,
--     NULL::VARCHAR AS route_long_name,
--     NULL::VARCHAR AS route_desc,
--     NULL::INTEGER AS route_type,
--     NULL::VARCHAR AS route_url,
--     NULL::VARCHAR AS route_color,
--     NULL::VARCHAR AS route_text_color,
--     NULL::INTEGER AS route_sort_order,
--     NULL::INTEGER AS continuous_pickup,
--     NULL::INTEGER AS continuous_drop_off,
--     NULL::VARCHAR AS network_id
-- {% endfor %}

{% endif %}
)
SELECT * FROM source

