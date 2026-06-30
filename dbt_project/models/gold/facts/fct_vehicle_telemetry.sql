select 
    -- Event    
    timestamp,
    tick,    

    -- Driving Context    
    drive_state,
    speed_bucket,
    gear,
    gear_group,
    is_moving,
   
    -- Vehicle Measurements    
    speed_kmh,
    rpm,
    throttle,
    engine_temp,
    oil_temp,
    oil_pressure,
    battery_voltage,
    fuel_level,
    fuel_flow,
    trip_km,
    odometer_km,

    -- Engineering Metrics 
    avg_tyre_pressure,
    avg_tyre_temp,
    tyre_pressure_imbalance,
    tyre_temp_imbalance,
    thermal_delta,
    thermal_stress,
    engine_load,
    fuel_consumed_this_tick,
    
   
    -- Health Assessment   
    engine_status,
    oil_status,
    battery_status,
    fuel_status,
    tyre_status,
    anomaly_count,
    vehicle_health,
    ready_for_trip

from {{ ref('int_telemetry_unpacked') }}