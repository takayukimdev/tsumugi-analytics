# MetricFlow Sample Queries (BigQuery `prod`)

Live `mf query` outputs against `<project>.marts` (`asia-northeast1`) across the 11-day
synthetic window `2026-11-08`–`2026-11-18` (JST).

## Partition Pruning & `require_partition_filter` Verification

`marts.fct_inference_requests` is partitioned by `event_date` (`DATE`) with
`require_partition_filter = true`. MetricFlow compiles `--start-time` / `--end-time` to
`TIMESTAMP_TRUNC(event_date, day) BETWEEN ...`, which BigQuery accepts as a valid partition
filter and prunes proportionally:

| Filter | Dry-run bytes (`org_id`) | Partitions scanned |
|---|---:|---:|
| `TIMESTAMP_TRUNC(event_date, day) BETWEEN '2026-11-16' AND '2026-11-18'` | 1,412,156 B (1.35 MiB) | 3 / 11 |
| `event_date >= DATE('2000-01-01')` (full table) | 4,792,180 B (4.57 MiB) | 11 / 11 |

---

## Q1. How are the north star (`weekly_active_orgs`) and guardrails trending week over week?

```bash
mf query \
  --metrics weekly_active_orgs,active_users,requests,error_rate,p95_latency_ms,total_cost_usd,cost_per_active_user \
  --group-by metric_time__week \
  --start-time 2026-11-08 --end-time 2026-11-18 \
  --order metric_time__week
```

```text
metric_time__week      weekly_active_orgs    active_users    requests    p95_latency_ms    total_cost_usd    error_rate    cost_per_active_user
-------------------  --------------------  --------------  ----------  ----------------  ----------------  ------------  ----------------------
2026-11-02T00:00:00                    51             281        3963             18547           26.0303     0.0259904               0.0926344
2026-11-09T00:00:00                    60            1373       40511             18547          285.19       0.0236973               0.207713
2026-11-16T00:00:00                    60             874       18581             18623          133.545      0.0224961               0.152797
```

<details>
<summary>Compiled SQL (<code>mf query --explain</code>)</summary>

```sql
WITH sma_10001_cte AS (
  SELECT
    TIMESTAMP_TRUNC(event_date, day) AS metric_time__day
    , TIMESTAMP_TRUNC(event_date, isoweek) AS metric_time__week
    , org_id AS __weekly_active_orgs
    , user_id AS __active_users
    , CASE WHEN request_id IS NOT NULL THEN 1 ELSE 0 END AS __requests
    , cost_usd AS __total_cost_usd
    , latency_ms AS __p95_latency_ms
    , case when status = 'error' then 1 else 0 end AS __error_requests
  FROM `<project>`.`marts`.`fct_inference_requests` inference_requests_src_10000
)

SELECT
  COALESCE(subq_12.metric_time__week, subq_18.metric_time__week) AS metric_time__week
  , MAX(subq_12.weekly_active_orgs) AS weekly_active_orgs
  , MAX(subq_12.active_users) AS active_users
  , MAX(subq_12.requests) AS requests
  , MAX(subq_12.p95_latency_ms) AS p95_latency_ms
  , MAX(subq_12.total_cost_usd) AS total_cost_usd
  , MAX(CAST(subq_18.error_requests AS FLOAT64) / CAST(NULLIF(subq_18.requests, 0) AS FLOAT64)) AS error_rate
  , MAX(CAST(subq_18.total_cost_usd AS FLOAT64) / CAST(NULLIF(subq_18.active_users, 0) AS FLOAT64)) AS cost_per_active_user
FROM (
  SELECT
    metric_time__week
    , COUNT(DISTINCT __weekly_active_orgs) AS weekly_active_orgs
    , COUNT(DISTINCT __active_users) AS active_users
    , SUM(__requests) AS requests
    , APPROX_QUANTILES(__p95_latency_ms, 20)[OFFSET(19)] AS p95_latency_ms
    , SUM(__total_cost_usd) AS total_cost_usd
  FROM sma_10001_cte
  WHERE metric_time__day BETWEEN '2026-11-02' AND '2026-11-22'
  GROUP BY
    metric_time__week
) subq_12
FULL OUTER JOIN (
  SELECT
    metric_time__week
    , SUM(__error_requests) AS error_requests
    , SUM(__requests) AS requests
    , SUM(__total_cost_usd) AS total_cost_usd
    , COUNT(DISTINCT __active_users) AS active_users
  FROM sma_10001_cte
  WHERE metric_time__day BETWEEN '2026-11-02' AND '2026-11-22'
  GROUP BY
    metric_time__week
) subq_18
ON
  subq_12.metric_time__week = subq_18.metric_time__week
GROUP BY
  metric_time__week
```

</details>

---

## Q2. How do cost, latency, and error rate compare by model (`tsumugi-mini` vs `tsumugi-pro`) and channel (`app` vs `api`)?

```bash
mf query \
  --metrics requests,total_cost_usd,error_rate,p95_latency_ms \
  --group-by request__model,request__channel \
  --start-time 2026-11-08 --end-time 2026-11-18 \
  --order request__model,request__channel
```

```text
request__model    request__channel      requests    total_cost_usd    p95_latency_ms    error_rate
----------------  ------------------  ----------  ----------------  ----------------  ------------
tsumugi-mini      api                      13371          14.6554               5618     0.024082
tsumugi-mini      app                      14692           8.87043              5597     0.0236183
tsumugi-pro       api                      22776         314.046               22071     0.0230067
tsumugi-pro       app                      12180         106.847               22114     0.0235632
```

---

## Q3. How does usage and unit cost split across org plan tiers (`free`, `pro`, `enterprise`) and Zero Data Retention (`is_zdr`)?

```bash
mf query \
  --metrics active_users,requests,total_cost_usd,cost_per_active_user \
  --group-by request__org_plan,request__is_zdr \
  --start-time 2026-11-08 --end-time 2026-11-18 \
  --order request__org_plan,request__is_zdr
```

```text
request__org_plan    request__is_zdr      active_users    requests    total_cost_usd    cost_per_active_user
-------------------  -----------------  --------------  ----------  ----------------  ----------------------
enterprise           False                         550       23283         196.379                 0.357053
enterprise           True                          391       14476         118.454                 0.302951
free                 False                         276        3975           5.57235               0.0201897
pro                  False                         611       21285         124.014                 0.202968
```

---

## Q4. What is the chat response thumbs-up rate across client platforms (`web`, `ios`, `android`)?

```bash
mf query \
  --metrics rated_message_count,thumbs_up_count,thumbs_up_rate \
  --group-by message__platform \
  --start-time 2026-11-08 --end-time 2026-11-18 \
  --order message__platform
```

```text
message__platform      rated_message_count    thumbs_up_count    thumbs_up_rate
-------------------  ---------------------  -----------------  ----------------
android                                476                352          0.739496
ios                                    677                511          0.754801
web                                   1863               1432          0.768653
```

---

## Q5. What is D7 retention by new-user cohort date?

Cohorts on `2026-11-12` or later have `< 7` days of follow-up by `2026-11-18` and are excluded by
`is_d7_eligible = false` (yielding `NULL` ratio rather than an artificially low retention rate).

```bash
mf query \
  --metrics d7_eligible_users,d7_retained_users,d7_retention \
  --group-by metric_time__day \
  --start-time 2026-11-08 --end-time 2026-11-12 \
  --order metric_time__day
```

```text
metric_time__day       d7_eligible_users    d7_retained_users    d7_retention
-------------------  -------------------  -------------------  --------------
2026-11-08T00:00:00                  258                   42        0.162791
2026-11-09T00:00:00                  261                   45        0.172414
2026-11-10T00:00:00                  233                   37        0.158798
2026-11-11T00:00:00                  189                   28        0.148148
2026-11-12T00:00:00                    0                    0
```
