"""Daily Tsumugi pipeline on Cloud Composer 3 (Airflow 3).

ingest -> dbt build -> dbt source freshness -> load run stats, plus a watcher.

Every task runs in a Cloud Run Job or a BigQuery load job; Composer only orchestrates. This
DAG never runs locally: offline, `make test` checks that it imports and that its graph,
schedule, templates, and callbacks are right.

Config comes from Composer environment variables (set by Terraform):
TSUMUGI_PROJECT_ID, TSUMUGI_REGION, TSUMUGI_BUCKET.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

import pendulum
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import DAG, TriggerRule, Variable, task

log = logging.getLogger(__name__)

JST = pendulum.timezone("Asia/Tokyo")
PROJECT_ID = os.environ.get("TSUMUGI_PROJECT_ID", "unset-project")
REGION = os.environ.get("TSUMUGI_REGION", "asia-northeast1")
BUCKET = os.environ.get("TSUMUGI_BUCKET", "unset-bucket")

INGEST_JOB = "ingest"
DBT_JOB = "dbt"
RUN_STATS_TABLE = "raw.dbt_run_results"
SLACK_WEBHOOK_VARIABLE = "slack_webhook_url"  # Secret Manager: airflow-variables-slack_webhook_url
SLACK_TIMEOUT_SECONDS = 10

# The JST day this run processes. Airflow 3 cron schedules use CronTriggerTimetable, so
# logical_date is the trigger time (03:00 JST = 18:00 UTC the day before), not the start of a
# data interval. `ds` would give the UTC date, which is the right day only by coincidence.
TARGET_DATE = "{{ jst_target_date(logical_date) }}"
TARGET_PARTITION = "{{ jst_target_date(logical_date) | replace('-', '') }}"  # YYYYMMDD
ARTIFACT_PREFIX = f"run_artifacts/dt={TARGET_DATE}"


def jst_target_date(logical_date: datetime) -> str:
    """Return the JST calendar day before the run's JST trigger time (YYYY-MM-DD)."""
    trigger_day = pendulum.instance(logical_date).in_timezone(JST).date()
    return (trigger_day - timedelta(days=1)).isoformat()


def notify_slack(context: dict) -> None:
    """Post a task failure to Slack. Without a webhook, log a stub line instead.

    The message carries only identifiers and the log URL, never the exception text: a failing
    task might have prompt-derived data in its error. This callback never raises, and it logs
    only exception class names: the webhook URL is a secret, and Airflow does not mask it
    because `slack_webhook_url` is not a sensitive key name.
    """
    ti = context["task_instance"]
    text = (
        f":red_circle: {ti.dag_id}.{ti.task_id} failed "
        f"(run {context['run_id']}, try {ti.try_number}). Logs: {ti.log_url}"
    )
    try:
        # Raises on secrets-backend errors other than "not found" (e.g. permission denied).
        webhook = Variable.get(SLACK_WEBHOOK_VARIABLE, default=None)
        if not webhook:
            log.warning("Slack webhook not configured; would have posted: %s", text)
            return
        # https only: urllib would also accept http:// (cleartext) and file://, and a value
        # without a scheme raises ValueError with the URL in its message.
        if not webhook.startswith("https://"):
            log.error("Slack webhook is not an https:// URL; not posting.")
            return
        request = urllib.request.Request(
            webhook,
            data=json.dumps({"text": text}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=SLACK_TIMEOUT_SECONDS):
            pass
    except Exception as exc:  # never mask the task's own failure with a callback error
        log.error("Slack notification failed: %s", type(exc).__name__)


def cloud_run_task(task_id: str, job_name: str, args: list[str], **kwargs):
    """Execute a Cloud Run Job, overriding the container args (shared image, per-step args)."""
    return CloudRunExecuteJobOperator(
        task_id=task_id,
        project_id=PROJECT_ID,
        region=REGION,
        job_name=job_name,
        overrides={"container_overrides": [{"args": args}]},
        **kwargs,
    )


with DAG(
    dag_id="tsumugi_daily",
    description="Ingest one JST day, build dbt, check freshness, record run stats.",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=JST),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    user_defined_macros={"jst_target_date": jst_target_date},
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_slack,
    },
    tags=["tsumugi"],
) as dag:
    ingest = cloud_run_task("ingest", INGEST_JOB, ["ingest", "--date", TARGET_DATE])

    dbt_build = cloud_run_task(
        "dbt_build",
        DBT_JOB,
        ["dbt", "build", "--target", "prod", "--artifacts-uri", f"gs://{BUCKET}/{ARTIFACT_PREFIX}"],
    )

    # ALL_DONE: freshness is most useful exactly when ingest or the build failed.
    source_freshness = cloud_run_task(
        "source_freshness",
        DBT_JOB,
        [
            "dbt",
            "source",
            "freshness",
            "--target",
            "prod",
            "--artifacts-uri",
            f"gs://{BUCKET}/{ARTIFACT_PREFIX}",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # The dbt job writes one NDJSON file per step (BigQuery cannot load run_results.json, a
    # single JSON document). WRITE_TRUNCATE on the day's partition makes reruns idempotent.
    load_run_stats = GCSToBigQueryOperator(
        task_id="load_run_stats",
        bucket=BUCKET,
        source_objects=[f"{ARTIFACT_PREFIX}/*.ndjson"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RUN_STATS_TABLE}${TARGET_PARTITION}",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_NEVER",  # Terraform owns the table and its schema
        write_disposition="WRITE_TRUNCATE",
        autodetect=None,  # None = rely on the existing table's schema without autodetect
        time_partitioning={"type": "DAY", "field": "target_date"},
        location=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0, on_failure_callback=None)
    def fail_run_if_any_task_failed() -> None:
        """Leaf watcher. load_run_stats (ALL_DONE) would otherwise mark failed runs successful."""
        raise RuntimeError("An upstream task failed; see its logs and Slack alert.")

    watcher = fail_run_if_any_task_failed()

    ingest >> dbt_build >> source_freshness >> load_run_stats
    [ingest, dbt_build, source_freshness, load_run_stats] >> watcher
