{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_gcs', 'page_view_events') }}
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
where userId is not null