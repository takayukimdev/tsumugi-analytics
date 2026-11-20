-- One row per pseudonymous user. API-only users (key owners) appear only in inference logs,
-- so app activity and inference activity are combined with a full outer join.
with app_activity as (
    select
        user_id,
        min(event_ts) as first_seen_at,
        array_agg(struct(org_id, platform) order by event_ts desc limit 1)[offset(0)] as latest
    from {{ ref('int_app_events_deduped') }}
    where event_name != 'org_changed'
    group by user_id
),

inference_activity as (
    select
        user_id,
        any_value(org_id) as org_id,
        min(event_ts) as first_inference_at,
        min(event_date) as first_inference_date
    from {{ ref('int_inference_requests_deduped') }}
    group by user_id
)

select
    coalesce(app_activity.user_id, inference_activity.user_id) as user_id,
    coalesce(app_activity.latest.org_id, inference_activity.org_id) as org_id,
    app_activity.latest.platform as latest_platform,
    least(
        coalesce(app_activity.first_seen_at, inference_activity.first_inference_at),
        coalesce(inference_activity.first_inference_at, app_activity.first_seen_at)
    ) as first_seen_at,
    inference_activity.first_inference_date
from app_activity
full outer join inference_activity on app_activity.user_id = inference_activity.user_id
