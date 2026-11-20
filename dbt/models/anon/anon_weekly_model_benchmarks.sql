-- APPI Anonymously Processed Information (匿名加工情報) mart for external/partner benchmarking.
-- Quasi-identifiers generalized to (event_week, org_plan, model, channel); no linkage keys
-- (no user_id, org_id, or request_id) and no free text. Cells with k < 5 distinct users are
-- suppressed before materialization (see docs/privacy.md).
select
    date_trunc(event_date, isoweek) as event_week,
    org_plan,
    model,
    channel,
    count(distinct user_id) as distinct_users,
    count(*) as requests,
    countif(status = 'error') as error_requests,
    sum(input_tokens) as input_tokens,
    sum(output_tokens) as output_tokens,
    approx_quantiles(latency_ms, 100)[offset(95)] as p95_latency_ms
from {{ ref('fct_inference_requests') }}
where event_date >= date('2000-01-01')
group by
    date_trunc(event_date, isoweek),
    org_plan,
    model,
    channel
having count(distinct user_id) >= 5
