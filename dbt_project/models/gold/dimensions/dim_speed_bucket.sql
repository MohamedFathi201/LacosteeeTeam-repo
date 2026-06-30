{{ config(materialized='table') }}
select
    speed_bucket
from {{ ref('fct_vehicle_telemetry') }}
group by speed_bucket
order by speed_bucket