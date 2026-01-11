{% set paths = [
  'gtfs/nagoyashi_srt/latest/NagoyaSRT_AllLines_extracted/calendar.txt',
  'gtfs/bunkyoku_bguru/latest/AllLines_extracted/calendar.txt',
  'gtfs/chiyodaku_kazaguruma/latest/Chiyoda_ALLLINES_extracted/calendar.txt',
  'gtfs/taitoku_com/latest/megurinCCBY40_extracted/calendar.txt',
  'gtfs/kanetsu_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/keifuku_bus/latest/keifuku_rosen_extracted/calendar.txt',
  'gtfs/akitashi_bus/latest/AkitaCityBus_extracted/calendar.txt',
  'gtfs/aomorishi_com/latest/AllLines_extracted/calendar.txt',
  'gtfs/miyakemura_bus/latest/AllLine_extracted/calendar.txt',
  'gtfs/nagai_transportation/latest/AllLines_extracted/calendar.txt',
  'gtfs/nippon_chuo_bus/latest/Maebashi_Area_extracted/calendar.txt',
  'gtfs/kiyoseshi_bus/latest/KiyoBus_extracted/calendar.txt',
  'gtfs/aomorishi_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/keisei_transit/latest/AllLines_extracted/calendar.txt',
  'gtfs/gumma_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/kunitachishi_com/latest/kunitachi_city_kunikko_extracted/calendar.txt',
  'gtfs/suginamiku_greenslow/latest/GreenSlowMobility_extracted/calendar.txt',
  'gtfs/otone_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/nishitokyoshi_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/kitaku_com/latest/KitaAllLines_extracted/calendar.txt',
  'gtfs/gummachuo_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/higashiyamatoshi_com/latest/AllLines_CCBY4_extracted/calendar.txt',
  'gtfs/higashimurayamashi_com/latest/Alllines_extracted/calendar.txt',
  'gtfs/shichigahamacho_com/latest/All_Gururinko_extracted/calendar.txt',
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/calendar.txt',
  'gtfs/kyoto_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/keio_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/kanto_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/kawasakitsurumirinko_bus/latest/allrinko_extracted/calendar.txt',
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/calendar.txt',
  'gtfs/matsueshi_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/yokohamashi_bus/latest/Bus_extracted/calendar.txt',
  'gtfs/kawasaki_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/funakitetsudo_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/tokyubus_com/latest/tokyubus_community_extracted/calendar.txt',
  'gtfs/akiha_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/oshima_bus/latest/AllLines_extracted/calendar.txt',
  'gtfs/takushoku_bus/latest/Takusyoku_highway_line_extracted/calendar.txt',
  'gtfs/shimoden_bus/latest/Shimoden_BUS_GTFS_Realtime_extracted/calendar.txt',
  'gtfs/juo_bus/latest/Isesaki_Honjo_Line_extracted/calendar.txt',
  'gtfs/daishinto_bus/latest/Radiantcity_Yokohama_extracted/calendar.txt',
  'gtfs/higashiyamatoshi_bus/latest/AllLines_ODPT_extracted/calendar.txt',
  'gtfs/seibu_bus/latest/SeibuBus-GTFS_extracted/calendar.txt',
  'gtfs/joetsushi_bus/latest/data-1_extracted/calendar.txt',
  's3://georoost/sandbox/tokyo_rail/calendar.txt'
] %}

{% set missing_ids = [
  'odakyu_bus'
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS service_id,
    NULL::INTEGER AS monday,
    NULL::INTEGER AS tuesday,
    NULL::INTEGER AS wednesday,
    NULL::INTEGER AS thursday,
    NULL::INTEGER AS friday,
    NULL::INTEGER AS saturday,
    NULL::INTEGER AS sunday,
    NULL::DATE    AS start_date,
    NULL::DATE    AS end_date
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
    TRY_CAST(t.monday AS INTEGER) AS monday,
    TRY_CAST(t.tuesday AS INTEGER) AS tuesday,
    TRY_CAST(t.wednesday AS INTEGER) AS wednesday,
    TRY_CAST(t.thursday AS INTEGER) AS thursday,
    TRY_CAST(t.friday AS INTEGER) AS friday,
    TRY_CAST(t.saturday AS INTEGER) AS saturday,
    TRY_CAST(t.sunday AS INTEGER) AS sunday,

    TRY_CAST(try_strptime(t.start_date, '%Y%m%d') AS DATE) AS start_date,
    TRY_CAST(try_strptime(t.end_date, '%Y%m%d') AS DATE) AS end_date
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
      'monday':'VARCHAR',
      'tuesday':'VARCHAR',
      'wednesday':'VARCHAR',
      'thursday':'VARCHAR',
      'friday':'VARCHAR',
      'saturday':'VARCHAR',
      'sunday':'VARCHAR',
      'start_date':'VARCHAR',
      'end_date':'VARCHAR'
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
    NULL::INTEGER AS monday,
    NULL::INTEGER AS tuesday,
    NULL::INTEGER AS wednesday,
    NULL::INTEGER AS thursday,
    NULL::INTEGER AS friday,
    NULL::INTEGER AS saturday,
    NULL::INTEGER AS sunday,
    NULL::DATE    AS start_date,
    NULL::DATE    AS end_date
{% endfor %}

{% endif %}
)
SELECT * FROM source

