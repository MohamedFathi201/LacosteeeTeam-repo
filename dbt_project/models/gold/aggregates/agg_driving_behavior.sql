{{ config(materialized='table') }}

select
    -- Driving Context
    speed_bucket,
    drive_state,
    engine_load,

    -- Event Statistics
    count(*) as total_events,

    -- Driving Metrics
    round(avg(speed_kmh)::numeric, 2) as avg_speed_kmh,
    round(avg(rpm)::numeric, 2) as avg_rpm,

    -- Engine Metrics
    round(avg(engine_temp)::numeric, 2) as avg_engine_temp,
    round(avg(thermal_delta)::numeric, 2) as avg_thermal_delta,

    -- Fuel Metrics
    round(avg(fuel_level)::numeric, 2) as avg_fuel_level,
    round(sum(fuel_consumed_this_tick)::numeric, 2) as total_fuel_consumed,
   
    -- Vehicle Health
    round(avg(anomaly_count)::numeric, 2) as avg_anomaly_count,

    sum(case when vehicle_health = 'NORMAL' then 1 else 0 end)
        as normal_events,

    sum(case when vehicle_health = 'WARNING' then 1 else 0 end)
        as warning_events,

    sum(case when vehicle_health = 'CRITICAL' then 1 else 0 end)
        as critical_events,
    round(
        (
            100.0 *
            sum(case when vehicle_health <> 'NORMAL' then 1 else 0 end)
            / count(*)
        )::numeric,
        2
    ) as failure_rate,

    -- Operational Readiness
    round(
        (
            100.0 *
            sum(case when ready_for_trip then 1 else 0 end)
            / count(*)
        )::numeric,
        2
    ) as ready_trip_rate

from {{ ref('fct_vehicle_telemetry') }}

group by
    speed_bucket,
    drive_state,
    engine_load

order by
    speed_bucket,
    drive_state,
    engine_load