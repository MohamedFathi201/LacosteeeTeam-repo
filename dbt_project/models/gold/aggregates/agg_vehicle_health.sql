{{ config(materialized='table') }}
select
 -- Vehicle Health
    vehicle_health,

    -- Event Statistics
    count(*) as telemetry_events,

    round(
        count(*) * 100.0
        / sum(count(*)) over (),
        2
    ) as percentage,

    round(
        100.0 *
        sum(case when anomaly_count > 0 then 1 else 0 end)
        / count(*),
        2
    ) as anomaly_rate,

    -- Readiness
    sum(
        case
            when ready_for_trip then 1
            else 0
        end
    ) as ready_trip_events,

    sum(
        case
            when not ready_for_trip then 1
            else 0
        end
    ) as not_ready_trip_events,

    -- Driving Metrics
    -- Driving Metrics
round(avg(speed_kmh)::numeric,2) as avg_speed_kmh,
round(avg(rpm)::numeric,2) as avg_rpm,

-- Engine Metrics
round(avg(engine_temp)::numeric,2) as avg_engine_temp,
round(max(engine_temp)::numeric,2) as max_engine_temp,
round(avg(thermal_delta)::numeric,2) as avg_thermal_delta,

-- Oil Metrics
round(avg(oil_pressure)::numeric,2) as avg_oil_pressure,
round(avg(oil_temp)::numeric,2) as avg_oil_temp,

-- Battery Metrics
round(avg(battery_voltage)::numeric,2) as avg_battery_voltage,

-- Fuel Metrics
round(avg(fuel_level)::numeric,2) as avg_fuel_level,
round(sum(fuel_consumed_this_tick)::numeric,2) as total_fuel_consumed,

-- Tyre Metrics
round(avg(avg_tyre_pressure)::numeric,2) as avg_tyre_pressure,
round(avg(avg_tyre_temp)::numeric,2) as avg_tyre_temperature,
round(avg(tyre_pressure_imbalance)::numeric,2) as avg_tyre_pressure_imbalance,
round(avg(tyre_temp_imbalance)::numeric,2) as avg_tyre_temperature_imbalance,

-- Health Metrics
round(avg(anomaly_count)::numeric,2) as avg_anomaly_count
from {{ ref('fct_vehicle_telemetry') }}

group by vehicle_health

order by
    case vehicle_health
        when 'CRITICAL' then 1
        when 'WARNING' then 2
        when 'NORMAL' then 3
    end