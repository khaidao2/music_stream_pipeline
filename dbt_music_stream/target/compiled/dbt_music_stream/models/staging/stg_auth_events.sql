

with source as (
    select * from `realestate-492305`.`music_stream_warehouse`.`auth_events`
)

select
    event_timestamp,
    userId as user_id,
    sessionId as session_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level,
    success,
    itemInSession as item_in_session,
    city,
    zip,
    state,
    userAgent as user_agent,
    lon,
    lat,
    TIMESTAMP_MILLIS(registration) as user_registration_at
from source
where userId is not null