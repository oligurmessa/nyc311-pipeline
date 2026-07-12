{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='delete+insert'
  )
}}

/*
  Fact: one row per service request, current state. Grain = request.
  Incremental: requests whose staging row was (re)loaded since the last build are replaced.
*/

with s as (
    select *
    from {{ ref('stg_311__service_requests') }}
    {% if is_incremental() %}
    where _ingested_at > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    unique_key,
    created_at,
    closed_at,
    created_at::date                                                 as created_date,
    closed_at::date                                                  as closed_date,
    agency                                                           as agency_key,
    md5(coalesce(complaint_type, '') || '|' || coalesce(descriptor, ''))  as complaint_type_key,
    incident_zip                                                     as location_key,
    borough,
    status,
    channel,
    location_type,
    address_type,
    community_board,
    council_district,
    police_precinct,
    latitude,
    longitude,
    latitude is not null and longitude is not null                   as has_geo,
    status = 'Closed'                                                as is_closed,
    closed_at is not null and closed_at < created_at                 as has_invalid_close_time,
    case when closed_at >= created_at
         then date_diff('minute', created_at, closed_at) / 60.0 end  as hours_to_close,
    case when closed_at is null
         then date_diff('minute', created_at, now()::timestamp) / 60.0 / 24.0 end as open_age_days,
    resolution_description,
    source_updated_at,
    _ingested_at
from s
