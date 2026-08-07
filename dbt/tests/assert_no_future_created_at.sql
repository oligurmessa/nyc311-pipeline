-- A request cannot be created in the future (allowing 1 day of timezone slack).
select unique_key, created_at
from {{ ref('stg_311__service_requests') }}
where created_at > now() + interval 1 day
