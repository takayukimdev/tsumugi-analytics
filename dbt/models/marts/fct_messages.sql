{{
    config(
        partition_by={"field": "event_date", "data_type": "date", "granularity": "day"},
        cluster_by=["org_id"],
    )
}}

-- Full rebuild (table), not incremental: feedback can arrive after its message was loaded,
-- and the table is small. Revisit with a lookback window if volume grows (see ADR).
select
    message_id,
    conversation_id,
    request_id,
    session_id,
    org_id,
    user_id,
    platform,
    event_ts,
    event_date,
    prompt_chars,
    has_attachment,
    rating,
    feedback_reason,
    feedback_ts
from {{ ref('int_messages') }}
