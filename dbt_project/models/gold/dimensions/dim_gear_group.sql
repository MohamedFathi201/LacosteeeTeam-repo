{{ config(materialized='table') }}
select
    gear_group
from {{ ref('fct_vehicle_telemetry') }}
group by gear_group
order by gear_group