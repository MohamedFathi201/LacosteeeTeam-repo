{{ config(materialized='table') }}

with subsystem_status as (  
    
     -- Engine
    select
        'ENGINE' as subsystem,
        engine_status as status
    from {{ ref('fct_vehicle_telemetry') }}
    
     union all   
    
     -- Oil
    
    select
        'OIL',
        oil_status
    from {{ ref('fct_vehicle_telemetry') }}

    union all   
     -- Battery
     select
        'BATTERY',
        battery_status
    from {{ ref('fct_vehicle_telemetry') }}
    
    union all

     -- Fuel
     select
        'FUEL',
        fuel_status
    from {{ ref('fct_vehicle_telemetry') }}

    union all   
    
     -- Tyres
     select
        'TYRES',
        tyre_status
    from {{ ref('fct_vehicle_telemetry') }}

)

select
    -- Subsystem
    subsystem,

    -- Event counts
    count(*) as total_events,

    sum(case when status='NORMAL' then 1 else 0 end)
        as normal_events,

    sum(case when status='WARNING' then 1 else 0 end)
        as warning_events,

    sum(case when status='CRITICAL' then 1 else 0 end)
        as critical_events,

    -- Rates
    round(

        100.0 *

        sum(case when status<>'NORMAL' then 1 else 0 end) / count(*),2) 
            
    as overall_failure_rate,

    round(
        100.0 *
        sum(
            case
                when status = 'WARNING' then 1 else 0 end ) / count(*),2) 
    as warning_rate,

    round(
        100.0 *
        sum(
            case
                when status = 'CRITICAL' then 1 else 0 end) / count(*),2 ) 
    as critical_rate,

    (
        sum(case when status='WARNING' then 1 else 0 end)
        +
        (
            sum(case when status='CRITICAL' then 1 else 0 end) * 2
        )
    ) as severity_score
    

from subsystem_status

group by subsystem
order by
    critical_rate desc,
    severity_score desc,
    overall_failure_rate desc