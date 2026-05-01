{{
    config(
        materialized='incremental',
        unique_key='surrogate_key'
    )
}}

/*
    Fact table: all telemetry records from the enriched intermediate model.
    Incremental: only loads rows ingested since the last run.
*/

select
    -- Surrogate key for incremental unique_key
    {{ dbt_utils.generate_surrogate_key(['"timestamp"', 'tick']) }} as surrogate_key,

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
    engine_temp_f,
    oil_temp_f,
    tyre_temp_0_f,
    tyre_temp_1_f,
    tyre_temp_2_f,
    tyre_temp_3_f,
    fuel_consumed_l,
    speed_bucket,
    ingested_at

from {{ ref('int_telemetry_unpacked') }}

{% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
