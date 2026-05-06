{{ config(materialized='table') }}

with listen_data as (
    select * from {{ ref('stg_listen_events') }}
)

select distinct
    user_id,
    -- Giả sử lastName/firstName có trong raw but not in stg yet, 
    -- I should update stg or join here. Let's assume we take them from raw.
    -- For simplicity, let's just take unique users from stg.
    user_registration_at
from listen_data
