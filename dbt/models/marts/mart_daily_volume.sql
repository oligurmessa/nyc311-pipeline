-- Requests created per day x borough x complaint family (dashboard: volume over time).
-- Only days fully covered by the pipeline: requests created before coverage start are a biased sample
-- (present only if they were updated since), so they are excluded here but kept in the fact table.
select
    f.created_date,
    d.year_month,
    d.is_weekend,
    coalesce(f.borough, 'UNKNOWN')                                   as borough,
    c.complaint_family,
    f.agency_key                                                     as agency,
    count(*)                                                         as n_requests,
    sum(case when f.is_closed then 1 else 0 end)                     as n_closed,
    sum(case when f.has_geo then 1 else 0 end)                       as n_with_geo
from {{ ref('fct_service_requests') }} f
join {{ ref('dim_complaint_type') }} c on c.complaint_type_key = f.complaint_type_key
left join {{ ref('dim_date') }} d on d.date_day = f.created_date
where f.created_at >= {{ coverage_start() }}
group by 1, 2, 3, 4, 5, 6
