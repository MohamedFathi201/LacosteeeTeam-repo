with telemetry as(
    select
        *,
        --UNIT CONVERSION
        {{ celsius_to_fahrenheit('engine_temp') }} as engine_temp_f,
        {{ celsius_to_fahrenheit('oil_temp') }} as oil_temp_f,
        {{ celsius_to_fahrenheit('tyre_temp_FL') }} as tyre_temp_FL_f,
        {{ celsius_to_fahrenheit('tyre_temp_FR') }} as tyre_temp_FR_f,
        {{ celsius_to_fahrenheit('tyre_temp_RL') }} as tyre_temp_RL_f,
        {{ celsius_to_fahrenheit('tyre_temp_RR') }} as tyre_temp_RR_f,
        {{ fuel_consumed_litres('fuel_flow', 0.5) }} as fuel_consumed_this_tick,
        
        --DRIVING BEHAVIOUR
        case
            when speed_kmh < 30 then 'city'
            when speed_kmh < 90 then 'suburban'
            else 'highway'
        end as speed_bucket,
        
        case
            when speed_kmh > 0 then true
            else false
        end as is_moving,

        engine_temp - oil_temp as thermal_delta
   
    from {{ ref('stg_car_telemetry') }}     
),    
metrics as(
    select
        *,  
        --DERIVED METRICS  
        (
            tyre_pressure_FL +
            tyre_pressure_FR +
            tyre_pressure_RL +
            tyre_pressure_RR
        ) / 4.0 as avg_tyre_pressure,

        (
            tyre_temp_FL +
            tyre_temp_FR +
            tyre_temp_RL +
            tyre_temp_RR
        ) / 4.0 as avg_tyre_temp,


        greatest(
            tyre_pressure_FL,
            tyre_pressure_FR,
            tyre_pressure_RL,
            tyre_pressure_RR
        ) -
        least(
            tyre_pressure_FL,
            tyre_pressure_FR,
            tyre_pressure_RL,
            tyre_pressure_RR
        ) as tyre_pressure_imbalance,

        greatest(
                tyre_temp_FL,
                tyre_temp_FR,
                tyre_temp_RL,
                tyre_temp_RR
            ) -
        least(
                tyre_temp_FL,
                tyre_temp_FR,
                tyre_temp_RL,
                tyre_temp_RR
            ) as tyre_temp_imbalance,
    
        
        case
            when thermal_delta < 8 then 'LOW'
            when thermal_delta < 18 then 'NORMAL'
            else 'HIGH'
            end as thermal_stress,
        
        case
            when rpm < 1500 then 'LOW_LOAD'
            when rpm < 5000 then 'NORMAL_LOAD'
            else 'HIGH_LOAD'
        end as engine_load,

        case
            when gear = 0 then 'PARKED'
            when gear <= 2 then 'LOW_GEAR'
            when gear <= 4 then 'MID_GEAR'
            else 'HIGH_GEAR'
        end as gear_group
   
    from telemetry
),
health as(
    select
        *,
        --vehicle health 
        case
            when engine_temp >= 110 
            OR rpm >=6500
            OR thermal_stress = 'HIGH' 
            then 'CRITICAL'
            when engine_temp >= 100 
            OR engine_load = 'HIGH_LOAD'
            then 'WARNING'
           else 'NORMAL'

        end as engine_status,

        case
            when oil_pressure <= 1.2 
            OR oil_temp>=120
            then 'CRITICAL'
            when oil_pressure <= 1.5 
            OR oil_temp>=105
            then 'WARNING'
            else 'NORMAL'
        end as oil_status,

        case
            when battery_voltage <= 10.5 then 'CRITICAL'
            when battery_voltage <= 11.8 then 'WARNING'
            else 'NORMAL'
        end as battery_status,

        case
            when fuel_level <= 5 then 'CRITICAL'
            when fuel_level <= 10 then 'WARNING'
            else 'NORMAL'
        end as fuel_status,

        case
            when(
                avg_tyre_pressure < 26
                or avg_tyre_temp > 90
                or tyre_pressure_imbalance > 5
                or tyre_temp_imbalance > 15
            )then 'CRITICAL'
            

            when(
                avg_tyre_pressure < 28
                or avg_tyre_temp > 80
                or tyre_pressure_imbalance > 3
                or tyre_temp_imbalance > 10               
            )then 'WARNING'
            
            else 'NORMAL'
        end as tyre_status

    from metrics
),

anomaly as(
   select
        *,
        --ANOMALY COUNT
            (
                (engine_status <> 'NORMAL')::int +
                (oil_status <> 'NORMAL')::int +
                (battery_status <> 'NORMAL')::int +
                (fuel_status <> 'NORMAL')::int +
                (tyre_status <> 'NORMAL')::int

            ) as anomaly_count
    from health        
   
),
final as(
    select
        *,
        --OVERALL VEHICLE HEALTH SCORE
        case
            when
                engine_status = 'CRITICAL'
                or oil_status = 'CRITICAL'
                or battery_status = 'CRITICAL'
                or fuel_status = 'CRITICAL'
                or tyre_status = 'CRITICAL'
            then 'CRITICAL'
            
            when anomaly_count >= 3 then 'CRITICAL'
            when anomaly_count >= 1 then 'WARNING'
            else 'NORMAL'
        end as vehicle_health,
        
        -- ready_for_trip
        case
            when
                engine_status='NORMAL'
                and oil_status='NORMAL'
                and battery_status='NORMAL'
                and fuel_status='NORMAL'
                and tyre_status='NORMAL'
            then true
            else false
        end as ready_for_trip

    from anomaly
)

select *
from final   

