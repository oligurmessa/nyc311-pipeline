{{
  config(
    materialized='incremental',
    unique_key=['unique_key', 'sys_updated_at'],
    incremental_strategy='delete+insert'
  )
}}

/*
  Every distinct version of every request ever landed (the replay log), typed.
  Duplicate copies of the same version (from overlap windows or re-run failed windows) collapse here:
  one row per (unique_key, source_updated_at). Feeds fct_request_status_history.
*/

select
    unique_key,
    sys_updated_at,
    {{ parse_ts('sys_updated_at') }}                                 as source_updated_at,
    trim(status)                                                     as status_raw,
    {{ parse_ts('closed_date') }}                                    as closed_at,
    trim(resolution_description)                                     as resolution_description,
    upper(trim(agency))                                              as agency,
    _ingested_at
from {{ source('raw', 'service_requests') }}
{% if is_incremental() %}
where _ingested_at > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
{% endif %}
qualify row_number() over (partition by unique_key, sys_updated_at order by _ingested_at) = 1
