-- One row per agency code. agency_name varies slightly across rows in the source; take the most common spelling.
select
    agency                                                           as agency_key,
    agency,
    arg_max(agency_name, n)                                          as agency_name,
    sum(n)                                                           as n_requests_seen
from (
    select agency, agency_name, count(*) as n
    from {{ ref('stg_311__service_requests') }}
    where agency is not null
    group by 1, 2
)
group by 1, 2
