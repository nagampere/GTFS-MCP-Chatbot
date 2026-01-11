#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
manifest_file="$script_dir/../../../../file_loader/manifest.csv"
dir="$script_dir"

# translations （必要なら `--file routes` のように変更可能）
fle="translations"
out="$dir/row_gtfs__${fle}_all.sql"

FORCE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --force|-f) FORCE=1; shift ;;
    --file) fle="$2"; out="$dir/row_gtfs__${fle}_all.sql"; shift 2 ;;
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

# 追加: sandbox の tokyo_missing_bus と tokyo_rail も常に UNION 対象にする（存在しなくてもOK）
extra1="s3://georoost/sandbox/tokyo_missing_bus/${fle}.txt"
extra2="s3://georoost/sandbox/tokyo_rail/${fle}.txt"
grep -qxF "$extra1" "$paths_file" 2>/dev/null || echo "$extra1" >> "$paths_file"
grep -qxF "$extra2" "$paths_file" 2>/dev/null || echo "$extra2" >> "$paths_file"

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
  {# stable 配下は var("source_path") から、s3:// はそのまま読む #}
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
"

printf '%s\n' "$sql" > "$out"
echo "Wrote $out"