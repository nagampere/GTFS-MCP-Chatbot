{% macro regexp_extract(text, pattern, group_num=1) %}
  {#
    Jinja(compile-time) helper.

    Many models do: {% set src_id = regexp_extract(p, '...regex...', 1) %}
    where `p` is a Jinja string.

    DuckDB also has a SQL function named regexp_extract, but that is NOT available
    inside `{% set %}` unless we provide a macro.
  #}

  {%- set group_i = group_num | int -%}
  {%- set match = modules.re.search(pattern, text) -%}
  {%- if match is not none and group_i <= match.groups()|length -%}
    {{ return(match.group(group_i)) }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}
{% endmacro %}
