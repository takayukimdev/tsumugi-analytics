-- Adds cost (effective-dated price list) and the org's plan at request time (SCD2 join).
-- Cost is NUMERIC: integer tokens x NUMERIC prices / integer, so no float rounding.
with requests as (
    select * from {{ ref('int_inference_requests_deduped') }}
),

pricing as (
    select * from {{ ref('model_pricing') }}
),

org_history as (
    select * from {{ ref('int_org_history') }}
)

select
    -- requests.* is intentional: this enrichment layer passes every request column through.
    requests.*,  -- noqa: AM04
    org_history.plan as org_plan,
    case
        when requests.status = 'error' then cast(0 as numeric)
        else (
            (requests.input_tokens - requests.cached_tokens) * pricing.input_usd_per_mtok
            + requests.cached_tokens * pricing.cached_input_usd_per_mtok
            + requests.output_tokens * pricing.output_usd_per_mtok
        ) / 1000000
    end as cost_usd
from requests
left join pricing
    on
        requests.model = pricing.model
        and requests.event_ts >= pricing.valid_from
        and (pricing.valid_to is null or requests.event_ts < pricing.valid_to)
left join org_history
    on
        requests.org_id = org_history.org_id
        and requests.event_ts >= org_history.valid_from
        and (org_history.valid_to is null or requests.event_ts < org_history.valid_to)
