# Privacy & APPI Compliance (`Tsumugi` Analytics)

This platform processes product telemetry and LLM inference logs under Japan's **Act on the
Protection of Personal Information (APPI / 個人情報保護法)** and enterprise **Zero Data Retention
(ZDR)** commitments. All data in this repository is 100% synthetic (fixed seed `42`).

---

## 1. Defense-in-Depth Architecture

```
Generator / Collector
  │
  ▼  (in-memory inside Cloud Run Job `ingest`, asia-northeast1)
[Step 1: ZDR Strip] ──► If `zdr = true`: drop prompt_text & response_text before validation,
  │                     logging, dead-lettering, or DLP calls. No row emitted to raw_content.
  ▼
[Step 2: Schema Validation] ──► Invalid rows go to gs://<bucket>/dead_letter/ with
  │                             {source, line_no, error_code, _loaded_at} ONLY (never payload).
  ▼
[Step 3: Regional Cloud DLP] ──► Locations/asia-northeast1 `content.deidentify` on non-ZDR
  │                              free text only (<= 0.5 MB and <= 50,000 cells per batch).
  ▼
[Step 4: Keyed HMAC-SHA256] ──► Pseudonymize `user_id`, `org_id`, `api_key_id` using 256-bit
  │                             secret `hmac-key` from Secret Manager.
  ├──► Metadata NDJSON ──► `raw.app_events`, `raw.inference_requests` (no free-text columns)
  └──► Redacted text NDJSON ──► `raw.raw_content` (30-day partition expiry + `ALWAYS_NULL`
                                Data Catalog policy tag; dbt never reads this table)
```

### Why four separate controls?
1. **ZDR strip runs first in memory**: A customer enabling ZDR expects prompt/response text to
   never touch persistent storage or third-party processors. Stripping before validation and DLP
   ensures ZDR text never appears in dead-letter files, exception messages, or DLP API payloads
   (enforced by `tests/test_ingest_privacy.py` and `tests/test_runner.py`).
2. **Metadata/content physical split**: `raw.inference_requests` holds only token counts, latency,
   cost dimensions, and `zdr`. Redacted non-ZDR text is isolated in `raw.raw_content`.
3. **Measured DLP recall motivates storage controls**: Against the generator's ground-truth PII
   labels (`docs/sample_outputs/ingest_stats_dlp.json`), regional Cloud DLP + custom Japanese
   regexes achieved:
   - `email_address`: **100.0%** recall (`0` leaked)
   - `person_name`: **100.0%** recall (`0` leaked, vs. `0.0%` with offline regex alone)
   - `phone_number`: **100.0%** recall (`0` leaked)
   - `postal_code`: **100.0%** recall (`0` leaked)
   - `street_address`: **78.7%** recall (`380 / 483` non-ZDR addresses masked; `103` partial leaks)
   Because unstructured text de-identification is probabilistic (`78.7%` on Japanese street
   addresses), `raw.raw_content` is additionally protected by a **30-day partition expiration**
   (matching the GCS `clean/raw_content/` lifecycle delete rule), a **Data Catalog policy tag
   with an `ALWAYS_NULL` masking policy**, and **complete exclusion from dbt (`sa-dbt` and
   `sa-ci-wif` only have `maskedReader`, never `fineGrainedReader`)**.
4. **Keyed HMAC-SHA256 pseudonymization**: Plain or unsalted SHA-256 (including BigQuery's
   built-in SHA-256 masking rule, which uses no secret key) is vulnerable to rainbow-table
   reversal across predictable ID formats. We use `HMAC-SHA256(secret_key, value)` with a key
   stored in Secret Manager (`hmac-key`).

---

## 2. APPI Classification & Mandatory Public Disclosure Items (公表事項)

| Dataset layer | APPI category | Rationale |
|---|---|---|
| `raw.raw_content` | **Personal Information (個人情報) — safety-net treated** | DLP-redacted, but residual false negatives (e.g., street addresses) are possible; access restricted + auto-deleted after 30 days. |
| `raw.app_events`, `raw.inference_requests`, `stg.*`, `int.*`, `marts.*` | **Pseudonymously Processed Information (仮名加工情報, APPI Art. 41–42)** | Direct identifiers replaced with keyed HMAC-SHA256 pseudonyms; free text removed; used strictly for internal product analytics. Key is held in Secret Manager with separate IAM (`sa-ingest` only). |
| `anon.*` | **Anonymously Processed Information (匿名加工情報, APPI Art. 43–46)** | Aggregated only, no linkage keys (no `user_id`, `org_id`, `request_id`, or HMAC pseudonyms), generalized quasi-identifiers, and cell suppression (`k >= 5`). |

### Items Required to be Published under APPI (公表事項)

1. **For Pseudonymously Processed Information (仮名加工情報 — APPI Art. 41(4) / Art. 17)**:
   - **Purpose of Utilization (利用目的)**:
     1. Internal product analytics and feature evaluation for the Tsumugi chat application and
        developer API (active organizations, retention, thumbs-up quality, token consumption).
     2. Infrastructure reliability, latency, error-rate monitoring, and unit-cost accounting.
     3. Model routing and capacity planning across `tsumugi-mini` and `tsumugi-pro`.
   - **Prohibitions enforced**: Internal use only (`stg`, `int`, `marts`); no third-party
     provision (第三者提供の禁止); no re-identification or contact with individuals (本人への連絡等の禁止);
     deletion Rules (`hmac-key` separated from analytics readers in Secret Manager IAM).

2. **For Anonymously Processed Information (匿名加工情報 — APPI Art. 43(3) & Art. 43(4))**:
   - **Categories of Personal Information Included in Anonymously Processed Information
     (匿名加工情報に含まれる個人に関する情報の項目)**:
     - Week of inference request (`event_week`, ISO week in JST; exact timestamps generalized to week)
     - Customer plan tier at request time (`org_plan`: `free`, `pro`, `enterprise`)
     - Model identifier (`model`: `tsumugi-mini`, `tsumugi-pro`)
     - Traffic channel (`channel`: `app`, `api`)
     - Aggregated token counts (`input_tokens`, `output_tokens`) and aggregated request latency statistics
   - **When Provided to Third Parties / Partners (第三者提供時の公表事項 — APPI Art. 43(4))**:
     - Items provided: the aggregated categories listed above (no user, org, or request identifiers;
       all cells with fewer than `k = 5` distinct users suppressed).
     - Method of provision (提供の方法): Controlled export from the `anon` BigQuery dataset over
       TLS with explicit marking that the dataset constitutes Anonymously Processed Information
       (匿名加工情報).

---

## 3. Specification for `anon.anon_weekly_model_benchmarks`

The quasi-identifiers, generalization rules, and $k$-anonymity threshold for
`dbt/models/anon/anon_weekly_model_benchmarks.sql` are:

- **Target relation**: `anon.anon_weekly_model_benchmarks` (configured via `+schema: anon` in
  `dbt/dbt_project.yml`)
- **Excluded columns (direct identifiers & linkage keys)**:
  - `request_id`, `user_id`, `org_id`, `api_key_id`, `conversation_id`, `message_id`, `session_id`
    (even HMAC-pseudonymized IDs are excluded so records cannot be linked across periods or tables).
  - All free text (`prompt_text`, `response_text`).
- **Quasi-identifiers (QIs) & Generalization rules**:
  1. `event_ts` / `event_date` $\rightarrow$ generalized to **ISO week** (`event_week = date_trunc(event_date, isoweek)`) so sparse daily/hourly activity cannot isolate a single user or org.
  2. `org_plan` $\rightarrow$ kept at coarse 3-tier grain (`free`, `pro`, `enterprise`).
  3. `model` $\rightarrow$ coarse 2-value grain (`tsumugi-mini`, `tsumugi-pro`).
  4. `channel` $\rightarrow$ coarse 2-value grain (`app`, `api`).
- **Suppression rule ($k \ge 5$)**:
  - Each cell `(event_week, org_plan, model, channel)` computes `distinct_users = count(distinct user_id)`
    during aggregation and filters `having count(distinct user_id) >= 5`.
  - Any cell with `distinct_users < 5` is suppressed (dropped in SQL before materialization).
  - A dbt test (`assert_anon_k_anonymity_enforced`) asserts `min(distinct_users) >= 5` and zero
    rows with `distinct_users < 5`.
