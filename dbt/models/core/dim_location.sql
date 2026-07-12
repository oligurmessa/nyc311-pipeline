-- Zip-level location dimension. Borough is the modal borough for the zip (a few zips straddle boroughs).
select
    incident_zip                                                     as location_key,
    incident_zip,
    arg_max(borough, n)                                              as borough,
    arg_max(city, n)                                                 as city,
    sum(n)                                                           as n_requests_seen
from (
    select incident_zip, borough, city, count(*) as n
    from {{ ref('stg_311__service_requests') }}
    where incident_zip is not null
    group by 1, 2, 3
)
group by 1, 2
