# AGENTS.md

Instructions for coding agents working in this repo. Antigravity CLI reads this file directly;
Claude Code reads it through `CLAUDE.md` (`@AGENTS.md`). Personal preferences live in a
gitignored local file, never here.

## Purpose

This repo is a portfolio project. Optimize for a reader skimming it in 5 minutes, not for
feature count. Priorities, in order: data modeling, LLM usage/cost data, privacy (APPI/ZDR),
reproducible infra, data quality, semantic layer.

It builds the analytics platform for **Tsumugi**, a *fictional* LLM chat app plus developer API
(models `tsumugi-mini`, `tsumugi-pro`), **fully on Google Cloud managed services**.
Never use real companies' product names or branding for it.

## Working in this repo

- Everything in the repo (code, comments, docs, commits) is **English only**.
- Show design, operations, and cost evidence, not only troubleshooting.
- Ask before starting work outside **Scope**. If time runs short, follow **Priority** below.

## Status (update when a step lands)

Done (offline + live GCP verified): tracking plan + JSON/BigQuery raw schema codegen + drift
check, generator, ingest (local + Cloud Run Job `ingest` on GCP with real DLP recall in
`docs/sample_outputs/ingest_stats_dlp.json`), shared Cloud Run Job entrypoint (`runner`),
`Dockerfile`, `infra/` Terraform applied on GCP (APIs, GCS, Artifact Registry, Secret Manager,
IAM/SAs/WIF, BigQuery datasets + raw tables + `raw_content` policy tag/`ALWAYS_NULL` masking,
Cloud Run Jobs `ingest` & `dbt`, alert policy, Composer 3 gated behind `enable_composer`),
`Makefile` GCP/Terraform targets, dbt stg/int/marts + `fct_pipeline_runs` +
`anon_weekly_model_benchmarks` ($k \ge 5$) + contracts (`make run-dbt` and `make test-bq` 64/64
green), MetricFlow metrics + 5 live query outputs in `docs/sample_outputs/metricflow_queries.md`
(partition pruning verified), Composer 3 DAG (`tsumugi_daily`) deployed and verified end-to-end on
GCP (`ingest >> dbt_build >> source_freshness >> load_run_stats` all green; the `ONE_FAILED`
leaf watcher skipped, as designed on a successful run), CI `offline` job (`ci.yml`; the WIF
`test-bq` job is written but not yet verified on a PR), and full documentation (`README.md`,
`docs/privacy.md`, `docs/architecture.md`, `docs/metrics.md`,
`docs/adr/0001-what-id-do-at-scale.md`, and two runbooks in `docs/runbooks/`).

Next, in order:
1. User destroys the Composer 3 environment (`terraform -chdir=infra apply -var="enable_composer=false"`)
   to stay within the $50/month budget. That apply also narrows `sa-composer` on `raw` to
   `raw.dbt_run_results` (plus a `bigquery.tables.create`-only custom role); `load_run_stats`
   under the narrowed grant is not yet verified on GCP.
2. Looker Studio screenshot on `marts.agg_daily_usage` and verify CI `test-bq` job via WIF on PR.

## Architecture

```
Cloud Run Job "ingest": generate → ZDR strip → validate → DLP de-identify → HMAC   (in memory)
  → GCS clean/ (NDJSON) → BigQuery load jobs → raw
Cloud Run Job "dbt": staging → intermediate → marts / anon → MetricFlow metrics
Composer 3 DAG: ingest → dbt build → source freshness → load run stats
```
- Region: `asia-northeast1` for everything. Ingest and dbt share **one Docker image** (Artifact Registry).
- Datasets: `raw`, `stg`, `int`, `marts`, `anon`. Dev datasets use a `dev_` prefix and expire after 7 days.
- Service accounts: `sa-ingest`, `sa-dbt`, `sa-composer`, `sa-ci-wif` (`account_id` requires ≥6 chars).
- Bootstrap order: state bucket (user) → Artifact Registry → `make image` → Cloud Run Jobs → Composer.

## Scope

- **Scaffolding**: `Makefile`, `pyproject.toml` + committed `uv.lock` (Python 3.12), `Dockerfile`,
  `README.md`, `.env.example`, `.github/workflows/ci.yml`. Run `git init` first if the repo isn't one yet.
- **`tracking_plan/events.yaml`**: source of truth. Ingest JSON Schemas and BigQuery raw schemas are
  generated from it, and CI fails on drift.
- **`generator/`**: seeded, ≤ ~1M events.
  - Defects: ~1% total. Duplicates, and late arrivals at most 2 days late. Prompt text contains
    Japanese/English PII, labeled as ground truth.
  - Inference fields: `request_id`, `org_id`, `user_id`, `model`, input/output/cached tokens, `latency_ms`,
    `ttft_ms`, `status`, `error_code`, `finish_reason`, `zdr`.
  - Product events include `feedback_submitted` and `org_changed` (plan / ZDR changes with `changed_at`).
- **`ingest/`**: see **Ingest contract**.
- **`dbt/`**: see **dbt conventions**. MetricFlow metrics have descriptions written for LLM consumers.
  Commit `mf query` outputs for ~5 product questions to `docs/sample_outputs/`.
- **`orchestration/`**: one DAG (`tsumugi_daily`) that runs **only on Composer 3**, never on the Mac.
  Locally, `make dag-test` checks imports, the task graph, trigger rules, rendered templates, and
  the Slack callback.
  - Separate uv project pinned to the Composer image (`composer-3-airflow-3.3.1`: Airflow 3.3.1,
    google provider 22.3.0 from `constraints-3.3.1`). Airflow and dbt pin conflicting dependencies.
  - Cloud Run Jobs via `CloudRunExecuteJobOperator` with container-arg overrides. Config comes
    from Composer env vars `TSUMUGI_PROJECT_ID`, `TSUMUGI_REGION`, `TSUMUGI_BUCKET`.
  - Schedule `0 3 * * *` JST, no catchup, retries=2. Airflow 3 cron schedules make `logical_date`
    the trigger time, so the processed day is the `jst_target_date` macro (the previous JST day),
    never `ds`.
  - Order: ingest → dbt build → source freshness (`ALL_DONE`) → load run stats (`ALL_DONE`). A
    leaf watcher (`ONE_FAILED`) fails the DAG run, which the `ALL_DONE` leaf would otherwise hide.
  - Container args contract (the shared image's entrypoint must accept these):
    `ingest --date <YYYY-MM-DD>`, `dbt <dbt args> --target prod --artifacts-uri gs://<bucket>/run_artifacts/dt=<date>`.
    The dbt entrypoint writes one NDJSON file per step there, even when dbt fails.
  - `load_run_stats` loads them into `raw.dbt_run_results$YYYYMMDD` (`WRITE_TRUNCATE`,
    `CREATE_NEVER`; Terraform owns the table).
  - `on_failure_callback` posts identifiers and the log URL to Slack, never exception text. The
    webhook is Airflow Variable `slack_webhook_url` via the Secret Manager backend; without it,
    the callback logs a stub line.
- **`infra/`** (Terraform, GCS backend): datasets, GCS, Artifact Registry, Cloud Run Jobs, Composer
  behind `enable_composer` (default false), and service accounts / IAM.
  - `sa-composer` gets `roles/run.jobsExecutorWithOverrides` plus `roles/run.viewer`
    (`CloudRunExecuteJobOperator` polls the execution LRO via `run.operations.get`, which is not in
    `jobsExecutorWithOverrides`).
  - Terraform owns `raw.raw_content`: schema, policy tag, `partition_expiration_days=30`.
  - It also owns the taxonomy (asia-northeast1), a data policy (`ALWAYS_NULL`), and `maskedReader` /
    `fineGrainedReader` grants.
  - Secret Manager holds `hmac-key` and `airflow-variables-slack_webhook_url` (Composer's
    secrets backend naming).
  - WIF pool for GitHub Actions (condition on `repository_id`).
  - One alert policy: Cloud Run Job failed executions (built-in metric).
  - The budget is created manually in the console.
- **Dashboard**: one Looker Studio screenshot on an aggregated mart.
- **`docs/`**: `architecture.md`, `privacy.md`, `metrics.md`, `ai-usage.md`, `adr/`, `runbooks/` (two
  runbooks: MERGE duplicate error and DLP 429), `sample_outputs/`.

**Out of scope.** Cover these in **one** ADR, "What I'd do at scale"; don't build them:
- MCP semantic agent + evals (the next step for the semantic layer), Dataplex scans/lineage
- Pub/Sub + SMT streaming, Dataflow, Dataform, VPC-SC, CMEK, Looker
- key rotation, partner delivery of `anon_*` data

## Priority (cut from the bottom if behind)

1. Tracking plan, generator, ingest (ZDR/DLP/HMAC) + privacy tests
2. dbt core: inference/cost facts, SCD2 `dim_orgs`, tests, contracts
3. Terraform + Docker + CI/WIF
4. Composer DAG running on GCP
5. MetricFlow metrics + `sample_outputs/`, `fct_pipeline_runs`
6. `anon_*` (one mart), docs/ADRs/runbooks, Looker screenshot

## Commands

`make help` lists all targets; every new target gets a `## description` comment.
- **Offline** (no GCP):
  - `make demo-offline` (= `gen` + `ingest-local`): regex redactor instead of DLP, demo HMAC key;
    writes `data/ingest/_stats.json` (copy to `docs/sample_outputs/` when it changes).
  - `make lint`: ruff, sqlfluff (jinja templater), `terraform fmt -check`
  - `make test`: pytest, `make dag-test`, `dbt parse` + `mf validate-configs --skip-dw` with `ci/profiles.yml` (a dummy
    profile that never connects), `terraform init -backend=false && terraform validate`, gitleaks,
    schema-drift check
  - `make metrics-explain METRICS=... GROUP_BY=...`: SQL that MetricFlow generates. `mf` reads the cached
    `target/semantic_manifest.json`, so always `dbt parse` first (the target does).
- **GCP**: `make image`, `make run-ingest`, `make run-dbt`, `make test-bq`, `make composer-deploy-dags`,
  `make tf-plan`
- **Never run `terraform apply` / `destroy`.** The user runs them. When a Composer task is finished,
  remind the user to destroy Composer (`-var enable_composer=false`).

CI:
- Every push runs `make lint test`.
- PRs also run `make test-bq` via WIF. Skip this job for fork PRs, which get no OIDC token.
- `test-bq` first runs `bq mk --default_table_expiration 86400 ci_<sha>`, and reads `raw` read-only.
  `sa-ci` gets `bigquery.resourceViewer` for `INFORMATION_SCHEMA.JOBS`.

## Ingest contract

All in memory inside the Cloud Run Job: **ZDR strip → JSON-Schema validate → DLP de-identify → HMAC** →
GCS `clean/` → BigQuery load jobs.
- Free text is split out of metadata rows into a separate `raw_content` stream (→ `raw.raw_content`).
  Metadata tables never hold text; ZDR rows produce no content row. dbt never reads `raw_content`.
- Local ingest reads only `part-NNNNN.jsonl` files (cloud-synced folders such as iCloud `~/Documents`
  create `part-00000 2.jsonl` conflict copies in `data/`; keep clones outside them).
- ZDR strip runs first. Raw text never lands in GCS, logs, dead letters, exception messages, or DLP
  requests for ZDR rows.
- `zdr` is on each inference record: the org's setting at request time.
- Dead letters go to `gs://<bucket>/dead_letter/` with metadata and an error code only, never the payload.
- DLP:
  - `content.deidentify` on the regional endpoint (`locations/asia-northeast1`).
  - Free-text columns only, batched as table items of **≤0.5 MB and ≤50,000 cells** per request.
  - Exponential backoff on 429.
  - InfoTypes: `PERSON_NAME`, `EMAIL_ADDRESS`, `PHONE_NUMBER`, `STREET_ADDRESS`, `JAPAN_*`, plus custom
    regex for Japanese phone numbers and 〒 postal codes.
  - Measure recall against the generator's ground truth and commit it to `sample_outputs/`.
- HMAC-SHA256 on `user_id`, `org_id`, `api_key_id`. The key comes from Secret Manager (`HMAC_KEY` in
  `.env` offline).
- Each NDJSON row includes `_loaded_at`. Reconciliation: generated = loaded + dead-lettered, or the job
  fails.

## Data & metric definitions

- JST for `event_date` and reporting. Weeks: `DATE_TRUNC(d, ISOWEEK)`. Currency: USD.
- Cost = input/output/cached tokens × effective-dated seed `model_pricing` (fictional prices).
  Failed requests cost 0.
- Active user = a pseudonymous user with ≥1 inference request in the period.
- D7 retention: cohort = first inference date (JST). Retained = ≥1 request on day 7. Exclude cohorts
  younger than 7 days.
- `docs/metrics.md`: the metric tree. North star: weekly active orgs. Guardrails: cost per active user,
  error rate, p95 latency.

## dbt conventions

- Sources: `app` and `api` (`stg_api__inference_requests`). `stg_*` are views, rename/cast only.
- `int_*` dedupe **before** any merge:
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY <natural key> ORDER BY _loaded_at DESC) = 1`.
  BigQuery MERGE fails if more than one source row matches a target row.
- `fct_*`: incremental `merge` on the natural key (`event_id` / `request_id`). Partition by `event_date`,
  cluster by `org_id` (plus `model`), `require_partition_filter=true`.
  - Source filter: `_loaded_at > (select coalesce(max(existing._loaded_at), timestamp('1970-01-01')) from {{ this }} as existing where existing.event_date >= date('2000-01-01'))`.
    The watermark scans the whole table (one column): a last-3-days watermark turns NULL after a
    >3-day pause and silently stops all loads.
  - `incremental_predicates: ["DBT_INTERNAL_DEST.event_date >= date_sub(current_date('Asia/Tokyo'), interval 3 day)"]`.
    Use the bare partition column against a constant. 3 days is longer than the maximum late arrival.
- `dim_orgs`: SCD2 from `org_changed` (`valid_from = changed_at`, `valid_to = LEAD(changed_at)`).
- Exception: `fct_messages` is a full-rebuild `table` (feedback arrives after its message; small).
- Every model has a YAML entry with a description, **documented grain**, and PK tests (unique + not_null).
  Marts have enforced contracts. Incremental contracted models use `on_schema_change: fail`.
- Partition-required tables: tests need `config: {where: "event_date >= date('2000-01-01')"}` (a
  constant filter satisfies the requirement and still covers the whole table). Raw tables are
  partitioned by `_loaded_at`; source freshness uses a 3-day `filter:`.
- No dbt packages (keeps `dbt parse` offline); generic test args go under `arguments:`.
- MetricFlow:
  - Semantic models use `agg_time_dimension: event_date`.
  - The time spine is configured with `time_spine:` YAML.
  - Queries use `--start-time`/`--end-time`. Confirmed on GCP: MetricFlow's
    `TIMESTAMP_TRUNC(event_date, day) BETWEEN ...` satisfies `require_partition_filter=true` on
    `fct_inference_requests` and prunes partitions (see `docs/sample_outputs/metricflow_queries.md`).
  - Ratio metrics reference metrics, not measures. Never reuse one metric with a filter as both
    numerator and denominator (it compiles to `x / x`); define a separate numerator metric.
- sqlfluff jinja templater mocks `is_incremental()` as True and `var()` as the string `"item"`. Never use
  `var()` where a number is required.
- `fct_pipeline_runs`: `run_results.json` joined to `region-asia-northeast1`.INFORMATION_SCHEMA.JOBS
  (`total_bytes_billed`, `total_slot_ms`, `error_result`). Estimated $/run is a SELECT-list constant.
- New event → update `tracking_plan/events.yaml` first, then the generator, then staging.

## Privacy rules (non-negotiable)

- Synthetic data only, fixed seed. No real PII anywhere, including fixtures and git history.
- A pytest test asserts that ZDR text never reaches any sink (see **Ingest contract**).
- Never use plain or unsalted SHA-256, or BigQuery's SHA-256 masking rule (it has no key), as
  pseudonymization.
- `anon_*` marts:
  - Aggregated only. No linkage keys (including the HMAC ID) and no free text.
  - Before building one, write its quasi-identifiers and generalization rules in `docs/privacy.md`
    and get the user's confirmation.
  - Suppress cells with k<5 in SQL; a test asserts none remain.
  - `privacy.md` lists the items APPI requires to be published (公表).

## Cost & secrets

- Budget: $50/month (manual). Composer exists only during demo windows.
- Use BigQuery load jobs only. dbt and ad-hoc queries use `maximum_bytes_billed: 1000000000` and
  `job_execution_timeout_seconds: 300`.
- Never commit credential JSON, `.env`, `terraform.tfstate*`, `profiles.yml` with secrets, or `data/`.
  No key files: use ADC, SA impersonation, or WIF.

## Definition of done (per change)

- `make lint test` pass. For dbt changes, `make test-bq` passes too.
- If the change affects architecture, privacy, metrics, or operations, update the matching doc, ADR,
  or runbook in the same commit.
- The agent commits after each passing change: small commits, Conventional Commits format.
  **Never push without asking.**
- When the agent proposes something the user rejects or rewrites, add one line to `docs/ai-usage.md`.
- README: lead with a 30-second summary, an architecture diagram, and "Review in 5 minutes" links.
