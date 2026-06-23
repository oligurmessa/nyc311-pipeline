{# Socrata timestamps arrive as text like '2026-09-01T10:00:00.000' (NYC local, no offset) or '...Z'. #}
{% macro parse_ts(col) -%}
    try_cast(replace({{ col }}, 'Z', '') as timestamp)
{%- endmacro %}
