{% set paths = [
  'gtfs/kyotoshi_bus/latest/Kyoto_City_Bus_GTFS_extracted/frequencies.txt'
] %}

{% set missing_ids = [
  'nagoyashi_srt',
  'bunkyoku_bguru',
  'chiyodaku_kazaguruma',
  'taitoku_com',
  'kanetsu_bus',
  'keifuku_bus',
  'akitashi_bus',
  'aomorishi_com',
  'miyakemura_bus',
  'nagai_transportation',
  'nippon_chuo_bus',
  'kiyoseshi_bus',
  'aomorishi_bus',
  'keisei_transit',
  'gumma_bus',
  'kunitachishi_com',
  'suginamiku_greenslow',
  'otone_bus',
  'nishitokyoshi_bus',
  'kitaku_com',
  'gummachuo_bus',
  'higashiyamatoshi_com',
  'higashimurayamashi_com',
  'shichigahamacho_com',
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
  'oshima_bus',
  'takushoku_bus',
  'shimoden_bus',
  'juo_bus',
  'daishinto_bus',
  'higashiyamatoshi_bus',
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
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS start_time,
    NULL::VARCHAR AS end_time,
    NULL::INTEGER AS headway_secs,
    NULL::INTEGER AS exact_times
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
    t.trip_id,
    t.start_time,
    t.end_time,
    TRY_CAST(t.headway_secs AS INTEGER) AS headway_secs,
    TRY_CAST(t.exact_times AS INTEGER) AS exact_times
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
      'trip_id':'VARCHAR',
      'start_time':'VARCHAR',
      'end_time':'VARCHAR',
      'headway_secs':'VARCHAR',
      'exact_times':'VARCHAR'
    }
  ) AS t
{% endfor %}

-- {% for id in missing_ids %}
--   {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
--   SELECT
--     '{{ id }}' AS gtfs_id,
--     NULL::VARCHAR AS _path,
--     TRUE AS _missing,
--     NULL::VARCHAR AS trip_id,
--     NULL::VARCHAR AS start_time,
--     NULL::VARCHAR AS end_time,
--     NULL::INTEGER AS headway_secs,
--     NULL::INTEGER AS exact_times
-- {% endfor %}

{% endif %}
)
SELECT * FROM source

