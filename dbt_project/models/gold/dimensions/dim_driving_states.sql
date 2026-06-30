select
    drive_state
from {{ ref('fct_vehicle_telemetry') }}
group by drive_state
order by drive_state
