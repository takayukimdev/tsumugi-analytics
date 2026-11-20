with source as (
    select * from {{ source('api', 'inference_requests') }}
)

select
    request_id,
    event_ts,
    date(event_ts, 'Asia/Tokyo') as event_date,
    received_ts,
    org_id,
    user_id,
    api_key_id,
    channel,
    conversation_id,
    message_id,
    model,
    input_tokens,
    output_tokens,
    cached_tokens,
    latency_ms,
    ttft_ms,
    status,
    error_code,
    finish_reason,
    zdr as is_zdr,
    schema_version,
    _loaded_at
from source
