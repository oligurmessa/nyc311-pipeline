-- One-row health summary for the dashboard banner: freshness, last run, volumes.
with runs as (
    select * from {{ source('meta', 'ingest_runs') }}
),
last_ok as (
    select * from runs where status = 'succeeded' order by started_at desc limit 1
),
last_any as (
    select * from runs order by started_at desc limit 1
),
facts as (
    select
        count(*)                                     as n_requests,
        max(source_updated_at)                       as newest_source_update,
        max(_ingested_at)                            as newest_ingest,
        sum(case when has_invalid_close_time then 1 else 0 end) as n_invalid_close_time
    from {{ ref('fct_service_requests') }}
)
select
    facts.n_requests,
    facts.newest_source_update,
    date_diff('hour', facts.newest_source_update, now()::timestamp)  as hours_since_source_update,
    facts.newest_ingest,
    facts.n_invalid_close_time,
    last_ok.run_id                                                   as last_successful_run_id,
    last_ok.finished_at                                              as last_successful_run_at,
    last_ok.rows_fetched                                             as last_successful_rows,
    last_any.status                                                  as last_run_status,
    last_any.error                                                   as last_run_error,
    (select count(*) from runs where status = 'succeeded')           as n_successful_runs,
    (select count(*) from runs where status = 'failed')              as n_failed_runs
from facts, last_ok, last_any
