# ADR 0001: What I Would Do at Scale (Out-of-Scope Production Roadmap)

- **Status**: Accepted
- **Date**: 2026-11-20

## Context

This portfolio builds a complete, privacy-safe batch analytics stack for **Tsumugi** within a
10-working-day scope and a $50/month GCP budget. Several production capabilities are deliberately
kept out of scope so the core data modeling, privacy controls, reproducible Terraform, data quality
contracts, and semantic layer remain small and auditable. This ADR documents how each out-of-scope
component would be introduced in production and at what scale trigger.

---

## 1. MCP Semantic Agent + Evals (Next Step for the Semantic Layer)

- **Why deferred**: MetricFlow semantic models and LLM-oriented descriptions are in place
  (`dbt/models/semantic/`), and `mf query` is verified against BigQuery. Wrapping `mf` in an MCP
  server without an evaluation harness adds surface area without proving accuracy.
- **Production design**:
  - Expose a read-only MCP server with two tools: `list_metrics` (reads `semantic_manifest.json`)
    and `query_metrics(metrics, group_by, start_time, end_time, where)` (invokes MetricFlow, never
    raw LLM-generated SQL).
  - Build a deterministic evaluation suite (~50 natural-language product questions mapped to
    canonical MetricFlow arguments and expected result hashes) run in CI whenever metric YAML
    changes.

## 2. Streaming Ingest (Pub/Sub + Single Message Transforms / Dataflow)

- **Why deferred**: Daily Cloud Run batch ingest (`0 3 * * *` JST) processes 10 days (~100k rows)
  in ~2 minutes for pennies and uses free BigQuery load jobs.
- **Scale trigger**: Sub-hour freshness requirements (e.g., live token-spend circuit breakers or
  real-time abuse detection) or daily volume exceeding single-container memory (>10M events/day).
- **Production design**:
  - Collector publishes to regional **Cloud Pub/Sub** (`asia-northeast1`) with schema validation
    attached to the topic.
  - **Apache Beam on Dataflow Streaming** performs stateful ZDR stripping, micro-batched regional
    DLP de-identification (using DLP's `500 KB / 50,000 cells` RPC windowing), and keyed HMAC
    pseudonymization before writing to BigQuery via the **Storage Write API (`EXACTLY_ONCE`)**,
    routing validation failures to a dead-letter topic.

## 3. Dataplex Data Quality Scans & Automated Lineage

- **Why deferred**: All 44 data tests, schema contracts, and source freshness checks already run in
  `dbt build` / `dbt source freshness` and land in `marts.fct_pipeline_runs`.
- **Production design**:
  - Enable **BigQuery Data Lineage API** and **Dataplex Universal Catalog** to track column-level
    lineage from `raw` through `marts` to Looker.
  - Use **Dataplex Auto Data Quality** for cross-system reconciliation (e.g., comparing billing
    stripe invoice totals against `marts.fct_inference_requests.cost_usd`).

## 4. Enterprise Perimeter Security (VPC Service Controls & CMEK)

- **Why deferred**: VPC-SC requires an Organization-level Access Context Manager policy, and CMEK
  adds per-key KMS cost without changing the functional data flow in a single-project demo.
- **Production design**:
  - Place BigQuery, Cloud Storage, Cloud DLP, Secret Manager, Artifact Registry, Cloud Run, and
    Cloud Composer inside a **VPC Service Controls** perimeter in `asia-northeast1` with ingress
    rules limited to the GitHub Actions WIF principal and corporate IAP.
  - Encrypt GCS buckets, BigQuery datasets, Secret Manager secrets, and Artifact Registry with
    regional **Cloud KMS CMEK** keys (`asia-northeast1`) with 90-day automatic rotation.

## 5. Cryptographic Key Rotation for HMAC Pseudonyms

- **Why deferred**: A single `hmac-key` version in Secret Manager suffices for a 28-day synthetic
  history.
- **Production design**:
  - Dual-key epoch rotation: store `hmac_key_version` on each row; during a rotation window,
    compute an encrypted mapping table (`old_hmac -> new_hmac`) inside a restricted confidential
    job, re-key historical `raw` / `marts` partitions in a maintenance window, and destroy the
    mapping table and retired key version so pseudonyms remain unlinkable to raw IDs.

## 6. Governed BI (Looker vs. Looker Studio) & Partner Delivery of `anon_*`

- **Why deferred**: Looker Studio is free and connects directly to `marts.agg_daily_usage`.
- **Production design**:
  - Connect **Looker** (or Looker via MetricFlow / Semantic Layer integration) so explores are
    generated from the same metric definitions as LLM agents.
  - Deliver `anon.anon_weekly_model_benchmarks` to external research or model partners via
    **BigQuery Analytics Hub (Data Exchanges)** with subscriber logging and automated pre-publish
    $k$-anonymity verification (`k >= 5`).
