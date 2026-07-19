-- Open (not closed) requests as of the last build, bucketed by age, per agency and borough.
select
    f.agency_key                                                     as agency,
    coalesce(f.borough, 'UNKNOWN')                                   as borough,
    f.status,
    case
        when f.open_age_days < 1   then '0. < 1 day'
        when f.open_age_days < 7   then '1. 1-7 days'
        when f.open_age_days < 30  then '2. 7-30 days'
        when f.open_age_days < 90  then '3. 30-90 days'
        else                            '4. > 90 days'
    end                                                              as age_bucket,
    count(*)                                                         as n_open,
    round(avg(f.open_age_days), 1)                                   as avg_age_days,
    max(f.open_age_days)                                             as max_age_days,
    now()::timestamp                                                 as as_of
from {{ ref('fct_service_requests') }} f
where not f.is_closed
group by 1, 2, 3, 4
