{{ config(materialized='table') }}

with page_view_events as (
    select 
        user_id,
        first_name,
        last_name,
        gender,
        level,
        city,
        state,
        user_registration_at,
        event_timestamp
    from {{ ref('stg_page_view_events') }}
),

auth_events as (
    select 
        user_id,
        first_name,
        last_name,
        gender,
        level,
        city,
        state,
        user_registration_at,
        event_timestamp
    from {{ ref('stg_auth_events') }}
),

all_users as (
    select * from auth_events
    union all
    select * from page_view_events
),

latest_user_info as (
    select 
        *,
        row_number() over (partition by user_id order by event_timestamp desc) as rn
    from all_users
)

select 
    user_id,
    first_name,
    last_name,
    gender,
    level,
    city,
    state,
    user_registration_at
from latest_user_info
where rn = 1