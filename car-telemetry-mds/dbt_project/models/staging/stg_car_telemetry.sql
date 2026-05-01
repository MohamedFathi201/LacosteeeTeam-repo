{{
    config(
        materialized='view'
    )
}}

/*
    Staging model: extracts and casts every field from the JSONB raw_data
    column in bronze.raw_car_telemetry.

    Deduplication: keeps only the latest ingested row per (timestamp, tick).
*/

with source as (

    select
        id,
        ingested_at,
        raw_data
    from {{ source('bronze', 'raw_car_telemetry') }}

),

extracted as (

    select
        id,
        ingested_at,

        -- Temporal
        (raw_data->>'timestamp')::timestamptz       as "timestamp",
        (raw_data->>'tick')::integer                 as tick,

        -- Engine
        (raw_data->>'rpm')::numeric                  as rpm,
        (raw_data->>'speed_kmh')::numeric            as speed_kmh,
        (raw_data->>'engine_temp')::numeric          as engine_temp,
        (raw_data->>'oil_temp')::numeric             as oil_temp,
        (raw_data->>'oil_pressure')::numeric         as oil_pressure,
        (raw_data->>'throttle')::numeric             as throttle,

        -- Fuel & Battery
        (raw_data->>'fuel_level')::numeric           as fuel_level,
        (raw_data->>'fuel_flow')::numeric            as fuel_flow,
        (raw_data->>'battery_voltage')::numeric      as battery_voltage,

        -- Brakes
        (raw_data->>'brake_pressure')::numeric       as brake_pressure,

        -- Odometer
        (raw_data->>'odometer_km')::numeric          as odometer_km,
        (raw_data->>'trip_km')::numeric              as trip_km,

        -- Gear
        (raw_data->>'gear')::integer                 as gear,

        -- Boolean flags
        (raw_data->>'ignition_on')::boolean          as ignition_on,
        (raw_data->>'alternator_on')::boolean        as alternator_on,
        (raw_data->>'abs_active')::boolean           as abs_active,
        (raw_data->>'check_engine')::boolean         as check_engine,
        (raw_data->>'low_fuel_warn')::boolean        as low_fuel_warn,

        -- Drive state
        raw_data->>'drive_state'                     as drive_state,

        -- Tyre arrays (kept as JSONB)
        raw_data->'tyre_pressure'                    as tyre_pressure,
        raw_data->'tyre_temp'                        as tyre_temp,

        -- Individual tyre temps unpacked
        (raw_data->'tyre_temp'->>0)::float           as tyre_temp_0,
        (raw_data->'tyre_temp'->>1)::float           as tyre_temp_1,
        (raw_data->'tyre_temp'->>2)::float           as tyre_temp_2,
        (raw_data->'tyre_temp'->>3)::float           as tyre_temp_3,

        -- Individual tyre pressures unpacked
        (raw_data->'tyre_pressure'->>0)::float       as tyre_pressure_0,
        (raw_data->'tyre_pressure'->>1)::float       as tyre_pressure_1,
        (raw_data->'tyre_pressure'->>2)::float       as tyre_pressure_2,
        (raw_data->'tyre_pressure'->>3)::float       as tyre_pressure_3,

        -- Deduplication ranking
        row_number() over (
            partition by
                (raw_data->>'timestamp')::timestamptz,
                (raw_data->>'tick')::integer
            order by ingested_at desc
        ) as _row_num

    from source

)

select
    "timestamp",
    tick,
    rpm,
    speed_kmh,
    engine_temp,
    oil_temp,
    oil_pressure,
    throttle,
    fuel_level,
    fuel_flow,
    battery_voltage,
    brake_pressure,
    odometer_km,
    trip_km,
    gear,
    ignition_on,
    alternator_on,
    abs_active,
    check_engine,
    low_fuel_warn,
    drive_state,
    tyre_pressure,
    tyre_temp,
    tyre_temp_0,
    tyre_temp_1,
    tyre_temp_2,
    tyre_temp_3,
    tyre_pressure_0,
    tyre_pressure_1,
    tyre_pressure_2,
    tyre_pressure_3,
    ingested_at
from extracted
where _row_num = 1
