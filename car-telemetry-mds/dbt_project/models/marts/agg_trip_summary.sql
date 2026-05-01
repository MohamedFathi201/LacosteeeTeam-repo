{{
    config(
        materialized='incremental',
        unique_key='surrogate_key'
    )
}}

/*
    Aggregated trip summary: groups telemetry by drive_state and speed_bucket
    to produce summary metrics per segment. Incremental to append new data.
*/

with base as (

    select
        drive_state,
        speed_bucket,
        rpm,
        speed_kmh,
        engine_temp_f,
        fuel_consumed_l,
        battery_voltage,
        check_engine,
        ingested_at
    from {{ ref('int_telemetry_unpacked') }}

    {% if is_incremental() %}
        -- الطريقة دي بتضمن إن الـ Subquery تتنفذ صح بعيد عن الـ WHERE المباشرة
        where ingested_at > (
            select coalesce(max(t.ingested_at), '1900-01-01') 
            from {{ this }} as t
        )
    {% endif %}

)

select
    {{ dbt_utils.generate_surrogate_key(['drive_state', 'speed_bucket']) }} as surrogate_key,

    drive_state,
    speed_bucket,

    count(*)                                                    as record_count,
    max(speed_kmh)                                              as max_speed_kmh,
    avg(rpm)                                                    as avg_rpm,
    avg(engine_temp_f)                                          as avg_engine_temp_f,
    sum(fuel_consumed_l)                                        as total_fuel_consumed_l,
    min(battery_voltage)                                        as min_battery_voltage,
    sum(
        case
            when check_engine or battery_voltage < 12
            then 1
            else 0
        end
    )                                                           as anomaly_count

from base

group by drive_state, speed_bucket
