# Runbook: Cloud DLP `429 RESOURCE_EXHAUSTED` Rate Limit in `ingest`

**Alert / Symptom**: Cloud Run Job `ingest` (or Composer task `ingest`) takes longer than usual or
fails after exhausting retries with `google.api_core.exceptions.ResourceExhausted: 429` from
`projects/<project>/locations/asia-northeast1/content:deidentify`.

---

## 1. Impact

- **Zero partial or unredacted data lands in BigQuery**: `ingest` processes each partition in
  memory (`ZDR strip -> validate -> DLP de-identify -> HMAC`) before uploading `clean/` NDJSON and
  submitting `CREATE_NEVER` load jobs. If DLP fails after all retries, no load job is executed for
  that slice.
- **No ZDR or raw PII in logs**: Exception handlers log only the exception class name and batch
  metadata (cell/byte counts), never prompt or response text.

---

## 2. Built-in Mitigations (`ingest/dlp.py`)

1. **Table batching within DLP limits**: Non-ZDR `prompt_text` and `response_text` cells are
   packed into `Table` requests capped at **$\le 0.5\text{ MB}$ UTF-8 and $\le 50,000$ cells** per
   RPC (`MAX_BATCH_BYTES = 500_000`, `MAX_BATCH_CELLS = 50_000`), minimizing request count per
   partition (~1–2 RPCs per JST day at current volume).
2. **Exponential backoff on `429` / `503`**: `DlpRedactor` retries transient rate-limit responses
   with exponential backoff before surfacing an error, and Cloud Run Job `ingest` (`max_retries=1`)
    plus Composer (`retries=2, retry_delay=5m`) provide task-level retries.

---

## 3. Triage Steps

1. **Inspect Cloud Run Job execution logs** (confirm whether the failure is quota/rate-limit vs.
   request payload size):
   ```bash
   gcloud logging read \
     'resource.type="cloud_run_job" AND resource.labels.job_name="ingest" AND severity>=WARNING' \
     --project=$GCP_PROJECT_ID --limit=30 --format="table(timestamp,severity,textPayload)"
   ```

2. **Check Cloud DLP quota utilization in `asia-northeast1`**:
   - In Cloud Monitoring, check metric
     `serviceruntime.googleapis.com/quota/rate/net_usage` for service `dlp.googleapis.com`
     (`ContentDeidentifyRequestsPerMinutePerProjectAndRegion` and
     `ContentBytesProcessedPerMinutePerProjectAndRegion`).

---

## 4. Resolution & Recovery

1. **If triggered by a multi-day backfill (`INGEST_DAYS > 10`)**:
   - Split the backfill into smaller date windows (e.g., `INGEST_DAYS=3` or `1`) so per-minute
     DLP byte throughput stays below the regional per-minute quota.
   - Because `raw` tables are deduplicated downstream in `int_*` (`QUALIFY ROW_NUMBER() OVER
     (PARTITION BY <id> ORDER BY _loaded_at DESC) = 1`), re-running `make run-ingest` for the
     failed `TARGET_DATE` is safe and idempotent for downstream marts:
     ```bash
     make run-ingest TARGET_DATE=<YYYY-MM-DD> INGEST_DAYS=1
     make run-dbt TARGET_DATE=<YYYY-MM-DD>
     ```

2. **If daily steady-state traffic outgrows default regional DLP quota**:
   - Request a quota increase for `dlp.googleapis.com` in `asia-northeast1` via IAM & Admin $\rightarrow$
     Quotas, or lower `MAX_BATCH_BYTES` / add inter-batch pacing in `ingest/dlp.py`.
