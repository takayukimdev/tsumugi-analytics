-- One row per message, with the user's rating if feedback was submitted.
with events as (
    select * from {{ ref('int_app_events_deduped') }}
),

messages as (
    select
        json_value(properties, '$.message_id') as message_id,
        json_value(properties, '$.conversation_id') as conversation_id,
        json_value(properties, '$.request_id') as request_id,
        session_id,
        org_id,
        user_id,
        platform,
        event_ts,
        event_date,
        cast(json_value(properties, '$.prompt_chars') as int64) as prompt_chars,
        cast(json_value(properties, '$.has_attachment') as bool) as has_attachment
    from events
    where event_name = 'message_sent'
),

feedback as (
    select
        json_value(properties, '$.message_id') as message_id,
        json_value(properties, '$.rating') as rating,
        json_value(properties, '$.reason') as feedback_reason,
        event_ts as feedback_ts
    from events
    where event_name = 'feedback_submitted'
    -- A user may rate twice; the latest rating wins.
    qualify row_number() over (partition by message_id order by event_ts desc) = 1
)

select
    messages.*,
    feedback.rating,
    feedback.feedback_reason,
    feedback.feedback_ts
from messages
left join feedback on messages.message_id = feedback.message_id
