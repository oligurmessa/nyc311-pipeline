{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Staging: one row per service request = the LATEST version seen in raw.
  - typed and cleaned columns; raw values preserved in *_raw where cleaning can lose information
  - incremental on _ingested_at: only parquet batches newer than the last build are scanned,
    and any request they contain is replaced with its newest version (delete+insert on unique_key)
*/

with new_raw as (

    select *
    from {{ source('raw', 'service_requests') }}
    {% if is_incremental() %}
    where _ingested_at > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
    {% endif %}

),

latest as (

    select *
    from new_raw
    qualify row_number() over (
        partition by unique_key
        order by sys_updated_at desc, _ingested_at desc, sys_id desc
    ) = 1

)

select
    unique_key,
    {{ parse_ts('created_date') }}                                   as created_at,
    {{ parse_ts('closed_date') }}                                    as closed_at,
    {{ parse_ts('due_date') }}                                       as due_at,
    {{ parse_ts('resolution_action_updated_date') }}                 as resolution_updated_at,

    upper(trim(agency))                                              as agency,
    trim(agency_name)                                                as agency_name,
    trim(complaint_type)                                             as complaint_type,
    trim(descriptor)                                                 as descriptor,
    trim(location_type)                                              as location_type,
    trim(status)                                                     as status_raw,
    case
        when upper(trim(status)) in ('CLOSED')                       then 'Closed'
        when upper(trim(status)) in ('OPEN')                         then 'Open'
        when upper(trim(status)) in ('IN PROGRESS', 'STARTED')       then 'In Progress'
        when upper(trim(status)) in ('ASSIGNED')                     then 'Assigned'
        when upper(trim(status)) in ('PENDING')                      then 'Pending'
        else 'Unspecified'
    end                                                              as status,
    trim(resolution_description)                                     as resolution_description,
    trim(open_data_channel_type)                                     as channel,

    -- location: zip must be 5 digits; borough is upper-cased, 'Unspecified' kept as null
    incident_zip                                                     as incident_zip_raw,
    case when regexp_matches(trim(incident_zip), '^[0-9]{5}$') then trim(incident_zip) end as incident_zip,
    case when upper(trim(borough)) in ('BRONX','BROOKLYN','MANHATTAN','QUEENS','STATEN ISLAND')
         then upper(trim(borough)) end                               as borough,
    trim(city)                                                       as city,
    trim(community_board)                                            as community_board,
    trim(council_district)                                           as council_district,
    trim(police_precinct)                                            as police_precinct,
    trim(incident_address)                                           as incident_address,
    trim(address_type)                                               as address_type,
    try_cast(latitude as double)                                     as latitude,
    try_cast(longitude as double)                                    as longitude,
    bbl,

    -- lineage
    sys_id,
    {{ parse_ts('sys_updated_at') }}                                 as source_updated_at,
    _run_id                                                          as ingest_run_id,
    _batch_id                                                        as ingest_batch_id,
    _ingested_at,
    _extra                                                           as extra_json

from latest
