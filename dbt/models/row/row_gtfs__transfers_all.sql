{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/transfers.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/transfers.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/transfers.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/transfers.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/transfers.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/transfers.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/transfers.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/transfers.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/transfers.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/transfers.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/transfers.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/transfers.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/transfers.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/transfers.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/transfers.txt',
  's3://georoost/sandbox/tokyo_missing_bus/transfers.txt',
  's3://georoost/sandbox/tokyo_rail/transfers.txt'
] %}

{% set missing_ids = [
  'keifuku_bus',
  'akitashi_bus',
  'aomorishi_com',
  'nagai_transportation',
  'nippon_chuo_bus',
  'kiyoseshi_bus',
  'aomorishi_bus',
  'keisei_transit',
  'gumma_bus',
  'kunitachishi_com',
  'suginamiku_greenslow',
  'otone_bus',
  'gummachuo_bus',
  'tokyoto_bus',
  'kyoto_bus',
  'keio_bus',
  'kanto_bus',
  'kawasakitsurumirinko_bus',
  'matsueshi_bus',
  'yokohamashi_bus',
  'kawasaki_bus',
  'funakitetsudo_bus',
  'tokyubus_com',
  'akiha_bus',
  'odakyu_bus',
  'takushoku_bus',
  'shimoden_bus',
  'juo_bus',
  'seibu_bus',
  'joetsushi_bus'
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS from_stop_id,
    NULL::VARCHAR AS to_stop_id,
    NULL::INTEGER AS transfer_type,
    NULL::INTEGER AS min_transfer_time
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
    t.from_stop_id,
    t.to_stop_id,
    TRY_CAST(t.transfer_type AS INTEGER) AS transfer_type,
    TRY_CAST(t.min_transfer_time AS INTEGER) AS min_transfer_time
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
      'from_stop_id':'VARCHAR',
      'to_stop_id':'VARCHAR',
      'transfer_type':'VARCHAR',
      'min_transfer_time':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS from_stop_id,
    NULL::VARCHAR AS to_stop_id,
    NULL::INTEGER AS transfer_type,
    NULL::INTEGER AS min_transfer_time
{% endfor %}

{% endif %}
)
SELECT * FROM source

