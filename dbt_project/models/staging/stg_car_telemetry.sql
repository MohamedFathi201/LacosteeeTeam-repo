with source_data as (
    select
        id,
        ingested_at,
        raw_data
    from {{ source('bronze', 'raw_car_telemetry') }}
),
parsed as (
    select
        id,
        ingested_at,
        (raw_data->>'timestamp')::timestamptz as timestamp,
        (raw_data->>'tick')::integer as tick,
        nullif(raw_data->>'rpm', '')::double precision as rpm,
        nullif(raw_data->>'speed_kmh', '')::double precision as speed_kmh,
        nullif(raw_data->>'engine_temp', '')::double precision as engine_temp,
        nullif(raw_data->>'oil_temp', '')::double precision as oil_temp,
        nullif(raw_data->>'oil_pressure', '')::double precision as oil_pressure,
        nullif(raw_data->>'throttle', '')::double precision as throttle,
        nullif(raw_data->>'fuel_level', '')::double precision as fuel_level,
        nullif(raw_data->>'fuel_flow', '')::double precision as fuel_flow,
        nullif(raw_data->>'battery_voltage', '')::double precision as battery_voltage,
        nullif(raw_data->>'brake_pressure', '')::double precision as brake_pressure,
        nullif(raw_data->>'odometer_km', '')::double precision as odometer_km,
        nullif(raw_data->>'trip_km', '')::double precision as trip_km,
        nullif(raw_data->>'gear', '')::integer as gear,
        nullif(raw_data->>'ignition_on', '')::boolean as ignition_on,
        nullif(raw_data->>'alternator_on', '')::boolean as alternator_on,
        nullif(raw_data->>'abs_active', '')::boolean as abs_active,
        nullif(raw_data->>'check_engine', '')::boolean as check_engine,
        nullif(raw_data->>'low_fuel_warn', '')::boolean as low_fuel_warn,
        raw_data->>'drive_state' as drive_state,
        nullif(raw_data->>'fault_active', '')::boolean as fault_active,
        raw_data->>'fault_type' as fault_type,
        raw_data->'tyre_pressure' as tyre_pressure,
        raw_data->'tyre_temp' as tyre_temp,
        nullif(raw_data->'tyre_temp'->>0, '')::double precision as tyre_temp_FL,
        nullif(raw_data->'tyre_temp'->>1, '')::double precision as tyre_temp_FR,
        nullif(raw_data->'tyre_temp'->>2, '')::double precision as tyre_temp_RL,
        nullif(raw_data->'tyre_temp'->>3, '')::double precision as tyre_temp_RR,
        nullif(raw_data->'tyre_pressure'->>0, '')::double precision as tyre_pressure_FL,
        nullif(raw_data->'tyre_pressure'->>1, '')::double precision as tyre_pressure_FR,
        nullif(raw_data->'tyre_pressure'->>2, '')::double precision as tyre_pressure_RL,
        nullif(raw_data->'tyre_pressure'->>3, '')::double precision as tyre_pressure_RR
    from source_data
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by timestamp, tick
            order by ingested_at desc, id desc
        ) as row_num
    from parsed
)
select
    id,
    ingested_at,
    timestamp,
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
    fault_active,
    fault_type,
    drive_state,
    tyre_pressure,
    tyre_temp,
    tyre_temp_FL,
    tyre_temp_FR,
    tyre_temp_RL,
    tyre_temp_RR,
    tyre_pressure_FL,
    tyre_pressure_FR,
    tyre_pressure_RL,
    tyre_pressure_RR
from deduplicated
where row_num = 1
