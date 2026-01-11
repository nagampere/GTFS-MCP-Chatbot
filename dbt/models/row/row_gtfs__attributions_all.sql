{% set paths = [
  'gtfs/tokyoto_bus/latest/ToeiBus-GTFS_extracted/attributions.txt',
  's3://georoost/sandbox/tokyo_missing_bus/attributions.txt',
  's3://georoost/sandbox/tokyo_rail/attributions.txt'
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
  'kyoto_bus',
  'keio_bus',
  'kanto_bus',
  'kawasakitsurumirinko_bus',
  'kyotoshi_bus',
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
    NULL::VARCHAR AS attribution_id,
    NULL::VARCHAR AS agency_id,
    NULL::VARCHAR AS route_id,
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS organization_name,
    NULL::VARCHAR AS is_producer,
    NULL::VARCHAR AS is_operator,
    NULL::VARCHAR AS is_authority,
    NULL::VARCHAR AS attribution_url,
    NULL::VARCHAR AS attribution_email,
    NULL::VARCHAR AS attribution_phone
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
    t.attribution_id,
    t.agency_id,
    t.route_id,
    t.trip_id,
    t.organization_name,
    t.is_producer,
    t.is_operator,
    t.is_authority,
    t.attribution_url,
    t.attribution_email,
    t.attribution_phone
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
      'attribution_id':'VARCHAR',
      'agency_id':'VARCHAR',
      'route_id':'VARCHAR',
      'trip_id':'VARCHAR',
      'organization_name':'VARCHAR',
      'is_producer':'VARCHAR',
      'is_operator':'VARCHAR',
      'is_authority':'VARCHAR',
      'attribution_url':'VARCHAR',
      'attribution_email':'VARCHAR',
      'attribution_phone':'VARCHAR'
    }
  ) AS t
{% endfor %}

-- {% for id in missing_ids %}
--   {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
--   SELECT
--     '{{ id }}' AS gtfs_id,
--     NULL::VARCHAR AS _path,
--     TRUE AS _missing,
--     NULL::VARCHAR AS attribution_id,
--     NULL::VARCHAR AS agency_id,
--     NULL::VARCHAR AS route_id,
--     NULL::VARCHAR AS trip_id,
--     NULL::VARCHAR AS organization_name,
--     NULL::VARCHAR AS is_producer,
--     NULL::VARCHAR AS is_operator,
--     NULL::VARCHAR AS is_authority,
--     NULL::VARCHAR AS attribution_url,
--     NULL::VARCHAR AS attribution_email,
--     NULL::VARCHAR AS attribution_phone
-- {% endfor %}

{% endif %}
)
SELECT * FROM source

