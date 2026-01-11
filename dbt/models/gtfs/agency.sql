{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/agency.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/agency.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/agency.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/agency.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/agency.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/agency.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/agency.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/agency.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/agency.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/agency.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/agency.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/agency.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/agency.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/agency.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/agency.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/agency.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/agency.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/agency.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/agency.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/agency.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/agency.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/agency.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/agency.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/agency.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/agency.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/agency.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/agency.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/agency.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/agency.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/agency.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/agency.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/agency.txt',
  's3://georoost/sandbox/tokyo_missing_bus/agency.txt',
  's3://georoost/sandbox/tokyo_rail/agency.txt'
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
    NULL::VARCHAR AS agency_id,
    NULL::VARCHAR AS agency_name,
    NULL::VARCHAR AS agency_url,
    NULL::VARCHAR AS agency_timezone,
    NULL::VARCHAR AS agency_lang,
    NULL::VARCHAR AS agency_phone,
    NULL::VARCHAR AS agency_fare_url,
    NULL::VARCHAR AS agency_email
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
    t.agency_id AS agency_id,
    t.agency_name,
    t.agency_url,
    t.agency_timezone,
    t.agency_lang,
    t.agency_phone,
    t.agency_fare_url,
    t.agency_email
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
      'agency_id':'VARCHAR',
      'agency_name':'VARCHAR',
      'agency_url':'VARCHAR',
      'agency_timezone':'VARCHAR',
      'agency_lang':'VARCHAR',
      'agency_phone':'VARCHAR',
      'agency_fare_url':'VARCHAR',
      'agency_email':'VARCHAR'
    }
  ) AS t
{% endfor %}

-- {% for id in missing_ids %}
--   {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
--   SELECT
--     '{{ id }}' AS gtfs_id,
--     NULL::VARCHAR AS _path,
--     TRUE AS _missing,
--     '{{ id }}' AS agency_id,
--     NULL::VARCHAR AS agency_name,
--     NULL::VARCHAR AS agency_url,
--     NULL::VARCHAR AS agency_timezone,
--     NULL::VARCHAR AS agency_lang,
--     NULL::VARCHAR AS agency_phone,
--     NULL::VARCHAR AS agency_fare_url,
--     NULL::VARCHAR AS agency_email
-- {% endfor %}

{% endif %}
)
SELECT
  gtfs_id,
  _path,
  _missing,
  agency_id,
  agency_name,
  agency_url,
  agency_timezone,
  agency_lang,
  agency_phone,
  agency_fare_url,
  agency_email
FROM source

