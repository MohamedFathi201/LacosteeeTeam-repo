{{
    config(
        materialized='table'
    )
}}

/*
    Anomalies fact table: captures telemetry ticks where at least one
    anomaly condition was triggered (check engine, overheating tyres,
    or low battery voltage).
*/

with anomalous as (

    select
        *,
        array_remove(
            array[
                case when check_engine         then 'check_engine'   end,
                case when tyre_temp_0 > 100    then 'tyre_temp_fl'   end,
                case when tyre_temp_1 > 100    then 'tyre_temp_fr'   end,
                case when tyre_temp_2 > 100    then 'tyre_temp_rl'   end,
                case when tyre_temp_3 > 100    then 'tyre_temp_rr'   end,
                case when battery_voltage < 12 then 'low_battery'    end
            ],
            null
        ) as anomaly_flags

    from {{ ref('int_telemetry_unpacked') }}

    where
        check_engine = true
        or tyre_temp_0 > 100
        or tyre_temp_1 > 100
        or tyre_temp_2 > 100
        or tyre_temp_3 > 100
        or battery_voltage < 12.0

)

select
    "timestamp",
    tick,
    rpm,
    speed_kmh,
    engine_temp,
    oil_temp,
    battery_voltage,
    drive_state,
    tyre_temp_0,
    tyre_temp_1,
    tyre_temp_2,
    tyre_temp_3,
    check_engine,
    anomaly_flags,
    ingested_at

from anomalous
