# Runbook: BigQuery `MERGE` Duplicate Source Row Error

**Alert / Symptom**: Cloud Run Job `dbt` (or Composer task `dbt_build`) fails on an incremental
fact model (`fct_inference_requests`) with:

```text
Database Error in model fct_inference_requests
  UPDATE/MERGE must match at most one source row for each target row
```

---

## 1. Impact

- `marts.fct_inference_requests` is **unchanged** for the failed run (BigQuery `MERGE` is atomic).
- Downstream marts (`agg_daily_usage`, `fct_user_retention`) are skipped by dbt for this run.
- `source_freshness` and `load_run_stats` still run (`TriggerRule.ALL_DONE`), and the failure row is
  recorded in `raw.dbt_run_results` (`status = 'error'`).

---

## 2. Root Cause Possibilities

1. **Duplicate natural key in the staging/intermediate view**:
   `int_inference_requests_deduped` deduplicates on `request_id` via
   `QUALIFY ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY _loaded_at DESC) = 1`. If a join
   in `int_inference_requests_priced` (to `model_pricing` or `int_org_history`) fans out because of
   overlapping `[valid_from, valid_to)` intervals, one `request_id` produces $>1$ row.
2. **Arrival outside the 3-day `incremental_predicates` window**:
   If a duplicate or late copy of an older `request_id` (`event_date < CURRENT_DATE('Asia/Tokyo') - 3`)
   arrives with a new `_loaded_at` watermark, the `MERGE` `ON` clause does not see the older target
   partition and attempts to `INSERT` a second copy, or two late copies collide.

---

## 3. Triage Steps (BigQuery)

1. **Check whether `int_inference_requests_priced` has duplicate `request_id`s** (fan-out from
   pricing or SCD2 org history):
   ```sql
   SELECT
     request_id,
     COUNT(*) AS row_count,
     ARRAY_AGG(STRUCT(org_plan, cost_usd, _loaded_at)) AS copies
   FROM `marts`.int_inference_requests_priced
   GROUP BY request_id
   HAVING COUNT(*) > 1
   LIMIT 20;
   ```
   *(Note: in `prod`, intermediate views live in dataset `int`: `int.int_inference_requests_priced`.)*

2. **Check for overlapping SCD2 intervals in `int.int_org_history` or `stg.model_pricing`**:
   ```sql
   SELECT org_id, valid_from, valid_to, COUNT(*) AS n
   FROM `int.int_org_history`
   GROUP BY org_id, valid_from, valid_to
   HAVING COUNT(*) > 1;
   ```

3. **Check whether any record violated the 3-day late-arrival bound**:
   ```sql
   SELECT
     request_id,
     event_ts,
     received_ts,
     TIMESTAMP_DIFF(received_ts, event_ts, HOUR) AS lateness_hours
   FROM `stg.stg_api__inference_requests`
   WHERE TIMESTAMP_DIFF(received_ts, event_ts, HOUR) >= 72;
   ```

---

## 4. Resolution & Recovery

- **Case A — Overlapping SCD2 or `model_pricing` intervals**:
  1. Fix the overlapping `valid_from` / `valid_to` rows in `dbt/seeds/model_pricing.csv` (or fix
     tie-breaking in `int_org_history.sql` when two `org_changed` events share the exact millisecond
     `changed_at`).
  2. Run `make test-bq` to verify `unique_int_inference_requests_priced_request_id` passes.
  3. Trigger a rerun of the `dbt` Cloud Run Job:
     ```bash
     make run-dbt TARGET_DATE=<YYYY-MM-DD>
     ```

- **Case B — Late arrival $\ge 3$ days (`assert_late_arrivals_within_lookback` failed)**:
  1. Rebuild `fct_inference_requests` with `--full-refresh` once (or widen `incremental_predicates`
     temporarily to cover the affected `event_date` range):
     ```bash
     gcloud run jobs execute dbt \
       --project=$GCP_PROJECT_ID --region=asia-northeast1 \
       --args="dbt,build,--select,fct_inference_requests+,--full-refresh,--target,prod" \
       --wait
     ```
