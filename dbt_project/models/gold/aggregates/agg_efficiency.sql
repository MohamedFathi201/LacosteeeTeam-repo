{{ config(materialized='table') }}

select
    -- Driving Category
    speed_bucket,

    -- Event Statistics
    count(*) as total_events,

    
  -- Distance Metrics
    round(
        (max(odometer_km) - min(odometer_km))::numeric,
        2
    ) as distance_travelled_km,

    round(
        max(trip_km)::numeric,
        2
    ) as trip_distance_km,

    -- Fuel Metrics
    round(
        sum(fuel_consumed_this_tick)::numeric,
        2
    ) as total_fuel_consumed_l,

    round(
        avg(fuel_consumed_this_tick)::numeric,
        4
    ) as avg_fuel_per_event_l,


    round(
        (
            (max(odometer_km) - min(odometer_km))
            /
            nullif(sum(fuel_consumed_this_tick),0)
        )::numeric,
        2
    ) as km_per_litre,

    round(
        avg(trip_km)::numeric,
        2
    ) as avg_trip_distance_km,

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

    -- Fuel Tank
    round(
        avg(fuel_level)::numeric,
        2
    ) as avg_fuel_level,

    round(
        min(fuel_level)::numeric,
        2
    ) as min_fuel_level,

    round(
        (
            sum(fuel_consumed_this_tick)
            /
            nullif(max(odometer_km)-min(odometer_km),0)
        )::numeric,
        3
    ) as litres_per_km,

    -- Efficiency Indicators
    round(
        avg(throttle)::numeric,
        2
    ) as avg_throttle,

    round(
        (
            avg(
                case
                    when engine_load = 'HIGH' then 1
                    else 0
                end
            ) * 100
        )::numeric,
        2
    ) as high_engine_load_percentage

from {{ ref('fct_vehicle_telemetry') }}

group by
    speed_bucket

order by
    case speed_bucket
        when 'city' then 1
        when 'suburban' then 2
        when 'highway' then 3
    end