-- Small, pre-aggregated table for Looker Studio, which cannot add partition filters itself.
-- Distinct counts (active_users, active_orgs) are NOT additive across rows; use MetricFlow
-- metrics for any roll-up beyond this grain.
select
    event_date,
    org_plan,
    model,
    channel,
    count(*) as requests,
    countif(status = 'error') as error_requests,
    sum(input_tokens) as input_tokens,
    sum(output_tokens) as output_tokens,
    sum(cached_tokens) as cached_tokens,
    sum(cost_usd) as cost_usd,
    count(distinct user_id) as active_users,
    count(distinct org_id) as active_orgs,
    approx_quantiles(latency_ms, 100)[offset(95)] as p95_latency_ms
from {{ ref('fct_inference_requests') }}
where event_date >= date('2000-01-01')
group by event_date, org_plan, model, channel
