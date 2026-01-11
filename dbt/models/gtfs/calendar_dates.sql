{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/calendar_dates.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/calendar_dates.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/calendar_dates.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/calendar_dates.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/calendar_dates.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/calendar_dates.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/calendar_dates.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/calendar_dates.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/calendar_dates.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/calendar_dates.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/calendar_dates.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/calendar_dates.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/calendar_dates.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/calendar_dates.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/calendar_dates.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/calendar_dates.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/calendar_dates.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/calendar_dates.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/odakyu_bus/latest/AIILines_extracted/calendar_dates.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/calendar_dates.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/calendar_dates.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/calendar_dates.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/calendar_dates.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/calendar_dates.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/calendar_dates.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/calendar_dates.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/calendar_dates.txt',
  's3://georoost/sandbox/tokyo_missing_bus/calendar_dates.txt',
  's3://georoost/sandbox/tokyo_rail/calendar_dates.txt'
] %}

{% set missing_ids = [
  'suginamiku_greenslow'
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS service_id,
    NULL::DATE    AS date,
    NULL::INTEGER AS exception_type
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
    t.service_id,
    TRY_CAST(try_strptime(t.date, '%Y%m%d') AS DATE) AS date,
    TRY_CAST(t.exception_type AS INTEGER) AS exception_type
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
      'service_id':'VARCHAR',
      'date':'VARCHAR',
      'exception_type':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS service_id,
    NULL::DATE    AS date,
    NULL::INTEGER AS exception_type
{% endfor %}

{% endif %}
)
SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||service_id as service_id,
  date,
  exception_type
FROM source

