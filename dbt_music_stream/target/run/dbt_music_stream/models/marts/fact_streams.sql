
  
    

    create or replace table `realestate-492305`.`music_stream_warehouse`.`fact_streams`
      
    
    

    OPTIONS()
    as (
      

select
    to_hex(md5(cast(coalesce(cast(user_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_timestamp as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(session_id as string), '_dbt_utils_surrogate_key_null_') as string))) as stream_id,
    event_timestamp,
    user_id,
    session_id,
    artist,
    song,
    duration,
    city,
    state
from `realestate-492305`.`music_stream_warehouse`.`stg_listen_events`
where user_id is not null
    );
  