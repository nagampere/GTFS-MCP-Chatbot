{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/fare_attributes.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/fare_attributes.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/fare_attributes.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/fare_attributes.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/fare_attributes.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/fare_attributes.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/fare_attributes.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/fare_attributes.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/fare_attributes.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/fare_attributes.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/fare_attributes.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/fare_attributes.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/fare_attributes.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/fare_attributes.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/fare_attributes.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/fare_attributes.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/fare_attributes.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/fare_attributes.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/fare_attributes.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/fare_attributes.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/fare_attributes.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/fare_attributes.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/fare_attributes.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/fare_attributes.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/fare_attributes.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/fare_attributes.txt'
] %}

{% set missing_ids = [
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
    NULL::DOUBLE  AS price,
    NULL::VARCHAR AS currency_type,
    NULL::INTEGER AS payment_method,
    NULL::INTEGER AS transfers,
    NULL::VARCHAR AS agency_id,
    NULL::INTEGER AS transfer_duration
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
    TRY_CAST(t.price AS DOUBLE) AS price,
    t.currency_type,
    TRY_CAST(t.payment_method AS INTEGER) AS payment_method,
    TRY_CAST(t.transfers AS INTEGER) AS transfers,
    t.agency_id,
    TRY_CAST(t.transfer_duration AS INTEGER) AS transfer_duration
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
      'price':'VARCHAR',
      'currency_type':'VARCHAR',
      'payment_method':'VARCHAR',
      'transfers':'VARCHAR',
      'agency_id':'VARCHAR',
      'transfer_duration':'VARCHAR'
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
    NULL::DOUBLE  AS price,
    NULL::VARCHAR AS currency_type,
    NULL::INTEGER AS payment_method,
    NULL::INTEGER AS transfers,
    NULL::VARCHAR AS agency_id,
    NULL::INTEGER AS transfer_duration
{% endfor %}

{% endif %}
)
SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||fare_id as fare_id,
  price,
  currency_type,
  payment_method,
  transfers,
  agency_id,
  transfer_duration
FROM source

