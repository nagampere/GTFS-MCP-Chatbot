{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/shapes.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/shapes.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/shapes.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/shapes.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/shapes.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/shapes.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/shapes.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/shapes.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/shapes.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/shapes.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/shapes.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/shapes.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/shapes.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/shapes.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/shapes.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/shapes.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/shapes.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/shapes.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/shapes.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/shapes.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/shapes.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/shapes.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/shapes.txt',
  's3://georoost/sandbox/tokyo_rail/shapes.txt'
] %}

{% set missing_ids = [
  'miyakemura_bus',
  'kiyoseshi_bus',
  'nishitokyoshi_bus',
  'higashimurayamashi_com',
  'shichigahamacho_com',
  'keio_bus',
  'kanto_bus',
  'matsueshi_bus',
  'kawasaki_bus',
  'tokyubus_com',
  'odakyu_bus',
  'shimoden_bus',
  'juo_bus',
  'seibu_bus'
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS shape_id,
    NULL::DOUBLE  AS shape_pt_lat,
    NULL::DOUBLE  AS shape_pt_lon,
    NULL::INTEGER AS shape_pt_sequence,
    NULL::DOUBLE  AS shape_dist_traveled
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
    t.shape_id,
    TRY_CAST(t.shape_pt_lat AS DOUBLE) AS shape_pt_lat,
    TRY_CAST(t.shape_pt_lon AS DOUBLE) AS shape_pt_lon,
    TRY_CAST(t.shape_pt_sequence AS INTEGER) AS shape_pt_sequence,
    TRY_CAST(t.shape_dist_traveled AS DOUBLE) AS shape_dist_traveled
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
      'shape_id':'VARCHAR',
      'shape_pt_lat':'VARCHAR',
      'shape_pt_lon':'VARCHAR',
      'shape_pt_sequence':'VARCHAR',
      'shape_dist_traveled':'VARCHAR'
    }
  ) AS t
{% endfor %}

-- {% for id in missing_ids %}
--   {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
--   SELECT
--     '{{ id }}' AS gtfs_id,
--     NULL::VARCHAR AS _path,
--     TRUE AS _missing,
--     NULL::VARCHAR AS shape_id,
--     NULL::DOUBLE  AS shape_pt_lat,
--     NULL::DOUBLE  AS shape_pt_lon,
--     NULL::INTEGER AS shape_pt_sequence,
--     NULL::DOUBLE  AS shape_dist_traveled
-- {% endfor %}

{% endif %}
)
SELECT * FROM source

