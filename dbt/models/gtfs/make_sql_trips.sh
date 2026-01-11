#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
manifest_file="$script_dir/../../seeds/gtfs.csv"
dir="$script_dir"

# trips （必要なら `--file routes` のように変更可能）
fle="trips"
out="$dir/${fle}.sql"

FORCE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --force|-f) FORCE=1; shift ;;
    --file) fle="$2"; out="$dir/${fle}.sql"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [ -e "$out" ] && [ "$FORCE" -ne 1 ]; then
  echo "File already exists: $out"
  exit 0
fi
if [ -e "$out" ] && [ "$FORCE" -eq 1 ]; then
  echo "Overwriting existing file: $out"
fi

if [ ! -f "$manifest_file" ]; then
  echo "manifest.csv not found: $manifest_file" >&2
  exit 1
fi

# 1) manifest.csv から source=gtfs & active=true を抽出（id, url）
manifest_rows="$(mktemp)"
python3 - "$manifest_file" <<'PY' > "$manifest_rows"
import csv, sys

manifest = sys.argv[1]

def is_true(v: str) -> bool:
    v = (v or "").strip().lower()
    return v in ("true", "1", "yes", "y")

with open(manifest, newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        if (row.get("source") or "").strip() != "gtfs":
            continue
        if not is_true(row.get("active")):
            continue
        _id = (row.get("id") or "").strip()
        url = (row.get("url") or "").strip()
        if not _id:
            continue
        print(f"{_id}\t{url}")
PY

if [ ! -s "$manifest_rows" ]; then
  echo "No active gtfs rows found in manifest.csv"
  rm -f "$manifest_rows"
  exit 0
fi

echo "Found active gtfs rows in manifest.csv: $(wc -l < "$manifest_rows")"

# 2) 各 id の stable から feed_info.(txt|csv) を探してパスを集める
paths_file="$(mktemp)"
missing_ids_file="$(mktemp)"

while IFS=$'\t' read -r id url || [ -n "$id" ]; do
  key="$(
    { aws s3 ls "s3://georoost/stable/gtfs/${id}/latest/" --recursive 2>/dev/null || true; } \
      | awk -v f="${fle}" '$4 ~ "_extracted/" && $4 ~ ("/" f "\\.(txt|csv)$") {print $4; exit}'
  )"

  if [ -z "${key:-}" ]; then
    echo "No ${fle} found for gtfs id=${id}. Will UNION a NULL row." >&2
    echo "$id" >> "$missing_ids_file"
    continue
  fi

  echo "${key#stable/}" >> "$paths_file"
done < "$manifest_rows"

rm -f "$manifest_rows"

# 追加: sandbox の tokyo_missing_bus / tokyo_rail は「存在する場合のみ」UNION 対象にする
for sandbox_id in tokyo_missing_bus tokyo_rail; do
  for ext in txt csv; do
    extra="s3://georoost/sandbox/${sandbox_id}/${fle}.${ext}"
    if aws s3 ls "$extra" >/dev/null 2>&1; then
      grep -qxF "$extra" "$paths_file" 2>/dev/null || echo "$extra" >> "$paths_file"
      break
    fi
  done
done

# 3) SQL 生成（paths と missing_ids を Jinja 配列にして UNION ALL）
sql=""
first=true
if [ -s "$paths_file" ]; then
  while IFS= read -r p; do
    if $first; then
      sql="{% set paths = [
  '$p'"
      first=false
    else
      sql="${sql},
  '$p'"
    fi
  done < "$paths_file"
else
  sql="{% set paths = []"
fi
rm -f "$paths_file"

sql="${sql}
] %}

"

first=true
if [ -s "$missing_ids_file" ]; then
  while IFS= read -r mid; do
    if $first; then
      sql="${sql}{% set missing_ids = [
  '$mid'"
      first=false
    else
      sql="${sql},
  '$mid'"
    fi
  done < "$missing_ids_file"
else
  sql="${sql}{% set missing_ids = ["
fi
rm -f "$missing_ids_file"

sql="${sql}
] %}

WITH source AS (
{% set has_any = (paths | length) + (missing_ids | length) %}
{% if has_any == 0 %}
  SELECT
    NULL::VARCHAR AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS route_id,
    NULL::VARCHAR AS service_id,
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS trip_headsign,
    NULL::VARCHAR AS trip_short_name,
    NULL::INTEGER AS direction_id,
    NULL::VARCHAR AS block_id,
    NULL::VARCHAR AS shape_id,
    NULL::INTEGER AS wheelchair_accessible,
    NULL::INTEGER AS bikes_allowed
  WHERE FALSE
{% else %}

{% for p in paths %}
  {%- if not loop.first %} UNION ALL {%- endif %}
  {# stable 配下は var("source_path") から、s3:// はそのまま読む #}
  {% set full_path = p if p.startswith('s3://') else (var('source_path') ~ '/' ~ p) %}
  {% set src_id = regexp_extract(p, '^(?:s3://[^/]+/)?(?:stable/)?(?:gtfs|sandbox)/([^/]+)/', 1) %}

  SELECT
    '{{ src_id }}' AS gtfs_id,
    '{{ p }}' AS _path,
    FALSE AS _missing,
    t.route_id,
    t.service_id,
    t.trip_id,
    t.trip_headsign,
    t.trip_short_name,
    TRY_CAST(t.direction_id AS INTEGER) AS direction_id,
    t.block_id,
    t.shape_id,
    TRY_CAST(t.wheelchair_accessible AS INTEGER) AS wheelchair_accessible,
    TRY_CAST(t.bikes_allowed AS INTEGER) AS bikes_allowed
  FROM read_csv(
    '{{ full_path }}',
    delim = ',',
    quote = '\"',
    escape = '\"',
    header = true,
    auto_detect = false,
    null_padding = true,
    null_padding = true,
    strict_mode = false,
    columns = {
      'route_id':'VARCHAR',
      'service_id':'VARCHAR',
      'trip_id':'VARCHAR',
      'trip_headsign':'VARCHAR',
      'trip_short_name':'VARCHAR',
      'direction_id':'VARCHAR',
      'block_id':'VARCHAR',
      'shape_id':'VARCHAR',
      'wheelchair_accessible':'VARCHAR',
      'bikes_allowed':'VARCHAR'
    }
  ) AS t
{% endfor %}

{% for id in missing_ids %}
  {%- if (paths | length) > 0 or not loop.first %} UNION ALL {%- endif %}
  SELECT
    '{{ id }}' AS gtfs_id,
    NULL::VARCHAR AS _path,
    TRUE AS _missing,
    NULL::VARCHAR AS route_id,
    NULL::VARCHAR AS service_id,
    NULL::VARCHAR AS trip_id,
    NULL::VARCHAR AS trip_headsign,
    NULL::VARCHAR AS trip_short_name,
    NULL::INTEGER AS direction_id,
    NULL::VARCHAR AS block_id,
    NULL::VARCHAR AS shape_id,
    NULL::INTEGER AS wheelchair_accessible,
    NULL::INTEGER AS bikes_allowed
{% endfor %}

{% endif %}
)
SELECT
  gtfs_id,
  _path,
  _missing,
  gtfs_id||route_id as route_id,
  gtfs_id||service_id as service_id,
  gtfs_id||trip_id as trip_id,
  trip_headsign,
  trip_short_name,
  gtfs_id||direction_id as direction_id,
  gtfs_id||block_id as block_id,
  gtfs_id||shape_id as shape_id,
  wheelchair_accessible,
  bikes_allowed
FROM source
"

printf '%s\n' "$sql" > "$out"
echo "Wrote $out"