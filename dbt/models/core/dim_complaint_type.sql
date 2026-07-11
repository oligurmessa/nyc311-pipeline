-- One row per (complaint_type, descriptor). Also exposes a coarse complaint family for dashboards.
select
    md5(coalesce(complaint_type, '') || '|' || coalesce(descriptor, ''))   as complaint_type_key,
    complaint_type,
    descriptor,
    case
        when complaint_type ilike 'noise%'                                            then 'Noise'
        when complaint_type ilike '%heat%' or complaint_type ilike '%hot water%'      then 'Heat / Hot Water'
        when complaint_type ilike '%parking%' or complaint_type ilike '%vehicle%'     then 'Parking / Vehicles'
        when complaint_type ilike '%water%' or complaint_type ilike '%sewer%'         then 'Water / Sewer'
        when complaint_type ilike '%street%' or complaint_type ilike '%sidewalk%'     then 'Street / Sidewalk'
        when complaint_type ilike '%sanitation%' or complaint_type ilike '%dirty%'
          or complaint_type ilike '%litter%' or complaint_type ilike '%graffiti%'    then 'Sanitation'
        when complaint_type ilike '%unsanitary%' or complaint_type ilike '%rodent%'
          or complaint_type ilike '%pest%'                                            then 'Pests / Unsanitary'
        when complaint_type ilike '%plumbing%' or complaint_type ilike '%paint%'
          or complaint_type ilike '%door%' or complaint_type ilike '%electric%'       then 'Housing Maintenance'
        when complaint_type ilike '%homeless%' or complaint_type ilike '%encampment%' then 'Homeless Assistance'
        when complaint_type ilike '%tree%'                                            then 'Trees / Parks'
        else 'Other'
    end                                                                   as complaint_family,
    count(*)                                                              as n_requests_seen
from {{ ref('stg_311__service_requests') }}
group by 1, 2, 3
