-- APPI k-anonymity invariant: no cell with fewer than k = 5 distinct users may exist in
-- anon_weekly_model_benchmarks (see docs/privacy.md).
select
    event_week,
    org_plan,
    model,
    channel,
    distinct_users
from {{ ref('anon_weekly_model_benchmarks') }}
where distinct_users < 5
