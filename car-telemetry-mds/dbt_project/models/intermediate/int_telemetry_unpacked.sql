{{
    config(
        materialized='view'
    )
}}

/*
    Intermediate model: enriches the staging telemetry with derived columns
    including Fahrenheit conversions, fuel consumption, and speed buckets.
*/

select
    -- Pass through all staging columns
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
    ingested_at,

    -- ── Derived: Fahrenheit conversions ─────────────────────
    {{ celsius_to_fahrenheit('engine_temp') }}    as engine_temp_f,
    {{ celsius_to_fahrenheit('oil_temp') }}       as oil_temp_f,
    {{ celsius_to_fahrenheit('tyre_temp_0') }}    as tyre_temp_0_f,
    {{ celsius_to_fahrenheit('tyre_temp_1') }}    as tyre_temp_1_f,
    {{ celsius_to_fahrenheit('tyre_temp_2') }}    as tyre_temp_2_f,
    {{ celsius_to_fahrenheit('tyre_temp_3') }}    as tyre_temp_3_f,

    -- ── Derived: fuel consumed per tick (dt = 0.5s) ─────────
    {{ fuel_consumed_litres('fuel_flow', 0.5) }}  as fuel_consumed_l,

    -- ── Derived: speed bucket classification ────────────────
    case
        when speed_kmh < 30  then 'city'
        when speed_kmh < 90  then 'suburban'
        else 'highway'
    end as speed_bucket

from {{ ref('stg_car_telemetry') }}
