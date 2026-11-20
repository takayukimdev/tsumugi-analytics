-- fct_inference_requests merges only into the last 3 days of partitions. A record arriving
-- 3+ days after its event would be inserted as a duplicate instead of merged, so fail the
-- build before that can happen.
select
    request_id,
    timestamp_diff(received_ts, event_ts, hour) as lateness_hours
from {{ ref('stg_api__inference_requests') }}
where timestamp_diff(received_ts, event_ts, hour) >= 72
