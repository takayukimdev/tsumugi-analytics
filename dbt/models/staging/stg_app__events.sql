with source as (
    select * from {{ source('app', 'events') }}
)

select
    event_id,
    event_name,
    event_ts,
    date(event_ts, 'Asia/Tokyo') as event_date,
    received_ts,
    org_id,
    user_id,
    session_id,
    platform,
    app_version,
    schema_version,
    properties,
    _loaded_at
from source
