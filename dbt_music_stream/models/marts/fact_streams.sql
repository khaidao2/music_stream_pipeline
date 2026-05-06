{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'event_timestamp', 'session_id']) }} as stream_id,
    event_timestamp,
    user_id,
    session_id,
    artist,
    song,
    duration,
    city,
    state
from {{ ref('stg_listen_events') }}
