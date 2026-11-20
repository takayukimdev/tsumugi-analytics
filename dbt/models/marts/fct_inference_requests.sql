{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="request_id",
        partition_by={"field": "event_date", "data_type": "date", "granularity": "day"},
        cluster_by=["org_id", "model"],
        require_partition_filter=true,
        incremental_predicates=[
            "DBT_INTERNAL_DEST.event_date >= date_sub(current_date('Asia/Tokyo'), interval 3 day)"
        ],
        on_schema_change="fail",
    )
}}

-- Watermark = latest _loaded_at already merged, over the WHOLE table (a constant date filter
-- satisfies require_partition_filter; only one column is scanned). A recent-partitions-only
-- watermark would turn NULL after a >3-day pause and silently stop all further loads.
--
-- MERGE only touches target partitions from the last 3 days (incremental_predicates). That
-- is safe because the generator bounds late arrivals to 2 days, and the singular test
-- assert_late_arrivals_within_lookback fails the build if that assumption ever breaks.
with requests as (
    select * from {{ ref('int_inference_requests_priced') }}
    {% if is_incremental() %}
        where _loaded_at > (
            select coalesce(max(existing._loaded_at), timestamp('1970-01-01'))
            from {{ this }} as existing
            where existing.event_date >= date('2000-01-01')
        )
    {% endif %}
)

select
    request_id,
    event_ts,
    event_date,
    org_id,
    user_id,
    api_key_id,
    channel,
    conversation_id,
    message_id,
    model,
    org_plan,
    input_tokens,
    output_tokens,
    cached_tokens,
    latency_ms,
    ttft_ms,
    status,
    error_code,
    finish_reason,
    is_zdr,
    cost_usd,
    _loaded_at
from requests
