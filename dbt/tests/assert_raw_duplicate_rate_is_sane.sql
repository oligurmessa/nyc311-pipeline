{{ config(severity='warn') }}

-- Duplicate copies in raw are expected (overlap windows, re-run failed windows) but a duplicate rate
-- above 20% means the watermark logic is re-fetching far too much. Returns one row when it does.
with d as (
    select count(*) as n_rows, count(distinct unique_key || sys_updated_at) as n_versions
    from {{ source('raw', 'service_requests') }}
)
select * from d where n_rows > 0 and (n_rows - n_versions) * 1.0 / n_rows > 0.20
