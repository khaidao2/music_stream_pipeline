

  create or replace view `realestate-492305`.`music_stream_warehouse`.`stg_page_view_events`
  OPTIONS()
  as 

with source as (
    select * from `realestate-492305`.`music_stream_warehouse`.`page_view_events`
)

select
    event_timestamp,
    userId as user_id,
    sessionId as session_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    page,
    auth,
    method,
    status,
    level,
    itemInSession as item_in_session,
    city,
    zip,
    state,
    userAgent as user_agent,
    lon,
    lat,
    TIMESTAMP_MILLIS(registration) as user_registration_at
from source
where userId is not null;

