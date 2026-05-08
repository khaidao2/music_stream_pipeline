{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_gcs', 'listen_events') }}
)

select
    TIMESTAMP_MILLIS(ts) as event_timestamp,
    cast(userId as string) as user_id,
    cast(sessionId as string) as session_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    artist,
    song,
    cast(duration as float64) as duration,
    auth,
    level,
    city,
    state,
    userAgent as user_agent,
    lon,
    lat,
    TIMESTAMP_MILLIS(registration) as user_registration_at,
    extract(year from TIMESTAMP_MILLIS(ts)) as event_year,
    extract(month from TIMESTAMP_MILLIS(ts)) as event_month,
    extract(day from TIMESTAMP_MILLIS(ts)) as event_day
from source
where userId is not null
