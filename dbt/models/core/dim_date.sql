-- Calendar dimension covering the data range plus a year ahead (no external package needed).
with bounds as (
    select date '2010-01-01' as start_date, (current_date + interval 1 year)::date as end_date
),
days as (
    select unnest(generate_series(start_date, end_date, interval 1 day))::date as date_day from bounds
)
select
    date_day,
    year(date_day)                               as year,
    quarter(date_day)                            as quarter,
    month(date_day)                              as month,
    strftime(date_day, '%Y-%m')                  as year_month,
    weekofyear(date_day)                         as iso_week,
    date_trunc('week', date_day)::date           as week_start,
    dayofweek(date_day)                          as day_of_week,          -- 0 = Sunday
    strftime(date_day, '%a')                     as day_name,
    dayofweek(date_day) in (0, 6)                as is_weekend
from days
