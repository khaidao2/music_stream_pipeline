

with listen_events as (
    select * from `realestate-492305`.`music_stream_warehouse`.`stg_listen_events`
),

page_view_events as (
    select * from `realestate-492305`.`music_stream_warehouse`.`stg_page_view_events`
),

auth_events as (
    select * from `realestate-492305`.`music_stream_warehouse`.`stg_auth_events`
),

all_users as (
    select user_id, first_name, last_name, gender, level, city, state, user_registration_at, event_timestamp
    from listen_events
    union all
    select user_id, first_name, last_name, gender, level, city, state, user_registration_at, event_timestamp
    from page_view_events
    union all
    select user_id, first_name, last_name, gender, level, city, state, user_registration_at, event_timestamp
    from auth_events
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