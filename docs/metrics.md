# Metrics

Metrics are defined once in the MetricFlow semantic layer
([`dbt/models/semantic/metrics.yml`](../dbt/models/semantic/metrics.yml)). Dashboards and LLM
agents query them by name, never with ad-hoc SQL, so every consumer gets the same number.

## Metric tree

```
weekly_active_orgs  (north star: are customers getting ongoing value?)
├── active_users
│   └── d7_retention                 do new users come back?
├── requests
│   ├── error_rate          GUARDRAIL reliability
│   └── p95_latency_ms      GUARDRAIL user-perceived speed
├── thumbs_up_rate                   response quality
└── total_cost_usd
    └── cost_per_active_user GUARDRAIL unit economics
```

**Why weekly active orgs?** Tsumugi sells to organizations (free / pro / enterprise), and
revenue follows org adoption, not individual sessions. Weekly smooths the weekday/weekend
cycle seen in both chat and API traffic.

**Why these guardrails?** Growth that costs more per user, or that ships with more errors or
slower responses, is not healthy growth. A launch is judged on the north star *without*
regressing any guardrail.

## Definitions

| Metric | Definition | Notes |
|---|---|---|
| `weekly_active_orgs` | Distinct orgs with ≥1 inference request | Query with `metric_time__week` (ISO weeks, Monday start) |
| `active_users` | Distinct pseudonymous users with ≥1 inference request | IDs are HMAC pseudonyms; counts are exact |
| `requests` | Inference requests (chat app + API) | Slice by `request__channel` |
| `error_rate` | `error_requests / requests` | 0–1 |
| `p95_latency_ms` | Approximate 95th percentile end-to-end latency | `APPROX_QUANTILES` on BigQuery |
| `total_cost_usd` | Tokens × effective-dated list price | Failed requests cost 0; cached input tokens priced separately |
| `cost_per_active_user` | `total_cost_usd / active_users` | Compare periods of equal length |
| `thumbs_up_rate` | `thumbs_up / rated messages` | Unrated messages excluded |
| `d7_retention` | Users active exactly 7 days after their first request ÷ eligible users | Cohort = first inference date (JST). Excludes cohorts younger than 7 days and the left-censored first day of data |

All dates are JST. Currency is USD.

## Querying

```bash
make metrics-explain METRICS=cost_per_active_user,error_rate GROUP_BY=metric_time__week,request__model
```

Run `dbt parse` (included in the target) after editing YAML: `mf` reads the cached
`target/semantic_manifest.json`.
