{{
    config(
        materialized='table'
    )
}}

/*
    Static dimension table for the 5 driving states.
    Uses the seed file as the source of truth.
*/

select
    drive_state_key,
    drive_state,
    description,
    is_moving

from {{ ref('drive_state_labels') }}
