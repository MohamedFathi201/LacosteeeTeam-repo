{{ config(materialized='table') }}

select

    -- Distance
    round(
        (max(odometer_km) - min(odometer_km))::numeric,
        2
    ) as total_distance_km,

    round(
        max(trip_km)::numeric,
        2
    ) as trip_distance_km,

    -- Fuel
    round(
        sum(fuel_consumed_this_tick)::numeric,
        2
    ) as total_fuel_consumed_l,

    round(
        (
            (max(odometer_km) - min(odometer_km))
            /
            nullif(sum(fuel_consumed_this_tick), 0)
        )::numeric,
        2
    ) as km_per_litre,

    -- Driving Performance
    round(
        avg(speed_kmh)::numeric,
        2
    ) as avg_speed_kmh,

    round(
        max(speed_kmh)::numeric,
        2
    ) as max_speed_kmh,

    round(
        avg(rpm)::numeric,
        2
    ) as avg_rpm,

    round(
        avg(throttle)::numeric,
        2
    ) as avg_throttle,

    -- Engine
    round(
        avg(engine_temp)::numeric,
        2
    ) as avg_engine_temp,

    round(
        max(engine_temp)::numeric,
        2
    ) as max_engine_temp,

    round(
        avg(oil_temp)::numeric,
        2
    ) as avg_oil_temp,

    -- Electrical
    round(
        avg(battery_voltage)::numeric,
        2
    ) as avg_battery_voltage,

    -- Fuel Tank
    round(
        avg(fuel_level)::numeric,
        2
    ) as avg_fuel_level,

    round(
        min(fuel_level)::numeric,
        2
    ) as min_fuel_level,

    -- Tyres
    round(
        avg(avg_tyre_pressure)::numeric,
        2
    ) as avg_tyre_pressure,

    round(
        avg(avg_tyre_temp)::numeric,
        2
    ) as avg_tyre_temp,

    -- Vehicle Health
    count(*) as telemetry_events,

    round(
        avg(anomaly_count)::numeric,
        2
    ) as avg_anomaly_count,

    sum(
        case
            when vehicle_health = 'NORMAL' then 1
            else 0
        end
    ) as normal_events,

    sum(
        case
            when vehicle_health = 'WARNING' then 1
            else 0
        end
    ) as warning_events,

    sum(
        case
            when vehicle_health = 'CRITICAL' then 1
            else 0
        end
    ) as critical_events,

    round(
        (
            100.0 *
            sum(
                case
                    when ready_for_trip then 1
                    else 0
                end
            )
            / count(*)
        )::numeric,
        2
    ) as ready_trip_rate

from {{ ref('fct_vehicle_telemetry') }}