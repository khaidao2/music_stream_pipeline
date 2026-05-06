{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_gcs', 'listen_events') }}
)

select
    -- Dùng generate_uuid() hoặc băm (hash) nếu cần surrogate key, 
    -- ở đây chúng ta giữ nguyên các thông tin định danh
    cast(ts as timestamp) as event_timestamp,
    cast(userId as string) as user_id,
    cast(sessionId as string) as session_id,
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
    cast(registration as timestamp) as user_registration_at,
    -- Trích xuất thời gian để tiện phân vùng sau này
    extract(year from cast(ts as timestamp)) as event_year,
    extract(month from cast(ts as timestamp)) as event_month,
    extract(day from cast(ts as timestamp)) as event_day
from source
where userId is not null
