{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/translations.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/translations.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/translations.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/translations.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/translations.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/translations.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/translations.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/translations.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/translations.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/translations.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/translations.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/translations.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/translations.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/translations.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/translations.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/translations.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/translations.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/translations.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/translations.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/translations.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/translations.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/translations.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/translations.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/translations.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/translations.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/translations.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/translations.txt',
  's3://georoost/sandbox/tokyo_rail/translations.txt'
] %}

{% set missing_ids = [
  'bunkyoku_bguru',
  'chiyodaku_kazaguruma',
  'taitoku_com',
  'suginamiku_greenslow',
  'kitaku_com'
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS table_name,
    NULL::VARCHAR AS field_name,
    NULL::VARCHAR AS language,
    NULL::VARCHAR AS translation,
    NULL::VARCHAR AS record_id,
    NULL::VARCHAR AS record_sub_id,
    NULL::VARCHAR AS field_value
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
    t.table_name,
    t.field_name,
    t.language,
    t.translation,
    t.record_id,
    t.record_sub_id,
    t.field_value
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
      'table_name':'VARCHAR',
      'field_name':'VARCHAR',
      'language':'VARCHAR',
      'translation':'VARCHAR',
      'record_id':'VARCHAR',
      'record_sub_id':'VARCHAR',
      'field_value':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS table_name,
    NULL::VARCHAR AS field_name,
    NULL::VARCHAR AS language,
    NULL::VARCHAR AS translation,
    NULL::VARCHAR AS record_id,
    NULL::VARCHAR AS record_sub_id,
    NULL::VARCHAR AS field_value
{% endfor %}

{% endif %}
)
SELECT * FROM source

