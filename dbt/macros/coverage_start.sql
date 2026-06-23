{# First :updated_at the pipeline has ever covered. Requests created before this date are only present
   if they happened to be updated since, so cohort-style marts must exclude them. #}
{% macro coverage_start() -%}
    (select min(try_cast(watermark_from as timestamp)) from {{ source('meta', 'ingest_runs') }} where status = 'succeeded')
{%- endmacro %}
