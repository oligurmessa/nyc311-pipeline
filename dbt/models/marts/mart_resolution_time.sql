-- Time-to-close by agency and week of creation (closed requests with valid timestamps only).
select
    date_trunc('week', f.created_date)::date                         as week_start,
    f.agency_key                                                     as agency,
    a.agency_name,
    count(*)                                                         as n_closed,
    round(median(f.hours_to_close), 1)                               as median_hours_to_close,
    round(quantile_cont(f.hours_to_close, 0.9), 1)                   as p90_hours_to_close,
    round(avg(f.hours_to_close), 1)                                  as mean_hours_to_close,
    sum(case when f.hours_to_close <= 24 then 1 else 0 end) * 1.0 / count(*)  as share_closed_within_24h
from {{ ref('fct_service_requests') }} f
join {{ ref('dim_agency') }} a on a.agency_key = f.agency_key
where f.is_closed and f.hours_to_close is not null
  and f.created_at >= {{ coverage_start() }}   -- complete cohorts only (see mart_daily_volume)
group by 1, 2, 3
