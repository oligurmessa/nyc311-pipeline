{{
  config(
    materialized='incremental',
    unique_key=['unique_key', 'sys_updated_at'],
    incremental_strategy='delete+insert'
  )
}}

/*
  One row per observed version of a request, with the previous observed status.
  This is what makes "late-arriving updates" visible: a request that closes weeks after creation gets a
  second row here. Versions before the pipeline started are of course not observable.
*/

with v as (
    select *
    from {{ ref('stg_311__request_versions') }}
    {% if is_incremental() %}
    -- recompute history for any request with a new version (LAG needs the full sequence)
    where unique_key in (
        select unique_key from {{ ref('stg_311__request_versions') }}
        where _ingested_at > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
    )
    {% endif %}
)

select
    unique_key,
    sys_updated_at,
    source_updated_at,
    status_raw                                                        as status,
    closed_at,
    agency,
    row_number() over (partition by unique_key order by source_updated_at)      as version_no,
    lag(status_raw) over (partition by unique_key order by source_updated_at)   as previous_status,
    lag(source_updated_at) over (partition by unique_key order by source_updated_at) as previous_updated_at,
    _ingested_at
from v
