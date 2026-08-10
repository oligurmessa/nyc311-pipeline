{{ config(severity='error', warn_if='>0', error_if='>5000') }}

-- Known source anomaly: a few hundred requests per month carry a closed_date earlier than created_date.
-- They are kept (flagged in the fact table, excluded from duration metrics) and monitored here:
-- any count warns; a jump past 5,000 in one build means the source changed and fails the run.
select unique_key, created_at, closed_at
from {{ ref('stg_311__service_requests') }}
where closed_at < created_at
