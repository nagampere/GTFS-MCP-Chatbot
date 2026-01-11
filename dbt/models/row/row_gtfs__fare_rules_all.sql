{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/fare_rules.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/fare_rules.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/fare_rules.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/fare_rules.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/fare_rules.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/fare_rules.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/fare_rules.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/fare_rules.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/fare_rules.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/fare_rules.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/fare_rules.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/fare_rules.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/fare_rules.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/fare_rules.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/fare_rules.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/fare_rules.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/fare_rules.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/fare_rules.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/fare_rules.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/fare_rules.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/fare_rules.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/fare_rules.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/fare_rules.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/fare_rules.txt'
] %}

{% set missing_ids = [
  'bunkyoku_bguru',
  'taitoku_com',
  'kitaku_com',
  'shimoden_bus',
  'juo_bus'
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS fare_id,
    NULL::VARCHAR AS route_id,
    NULL::VARCHAR AS origin_id,
    NULL::VARCHAR AS destination_id,
    NULL::VARCHAR AS contains_id
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
    t.fare_id,
    t.route_id,
    t.origin_id,
    t.destination_id,
    t.contains_id
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
      'fare_id':'VARCHAR',
      'route_id':'VARCHAR',
      'origin_id':'VARCHAR',
      'destination_id':'VARCHAR',
      'contains_id':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS fare_id,
    NULL::VARCHAR AS route_id,
    NULL::VARCHAR AS origin_id,
    NULL::VARCHAR AS destination_id,
    NULL::VARCHAR AS contains_id
{% endfor %}

{% endif %}
)
SELECT * FROM source

