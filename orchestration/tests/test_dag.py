"""Offline checks for the Composer DAG: it imports, and its wiring is what the docs claim.

The DAG never runs locally; these tests are the local safety net before
`make composer-deploy-dags`.
"""

import io
import json
import urllib.error
from datetime import timedelta
from types import SimpleNamespace

import pendulum
import pytest
import tsumugi_daily
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk import TriggerRule
from conftest import DAGS_DIR

DAG_ID = "tsumugi_daily"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS_DIR))
    assert bag.import_errors == {}
    return bag.dags[DAG_ID]


def downstream(dag, task_id: str) -> set[str]:
    return set(dag.get_task(task_id).downstream_task_ids)


def render(dag, template: str, logical_date: pendulum.DateTime) -> str:
    env = dag.get_template_env()
    return env.from_string(template).render(logical_date=logical_date, **dag.user_defined_macros)


def test_dag_folder_imports_without_errors(dag):
    assert dag.dag_id == DAG_ID


def test_tasks_run_in_pipeline_order_with_watcher_as_leaf(dag):
    assert downstream(dag, "ingest") == {"dbt_build", "fail_run_if_any_task_failed"}
    assert downstream(dag, "dbt_build") == {"source_freshness", "fail_run_if_any_task_failed"}
    assert downstream(dag, "source_freshness") == {
        "load_run_stats",
        "fail_run_if_any_task_failed",
    }
    assert downstream(dag, "load_run_stats") == {"fail_run_if_any_task_failed"}
    assert {t.task_id for t in dag.leaves} == {"fail_run_if_any_task_failed"}


def test_schedule_is_daily_jst_without_catchup(dag):
    assert type(dag.timetable).__name__ == "CronTriggerTimetable"
    assert dag.timetable.expression == "0 3 * * *"
    assert str(dag.timetable.timezone) == "Asia/Tokyo"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_trigger_rules_keep_freshness_and_stats_after_failures(dag):
    rules = {t.task_id: t.trigger_rule for t in dag.tasks}
    assert rules == {
        "ingest": TriggerRule.ALL_SUCCESS,
        "dbt_build": TriggerRule.ALL_SUCCESS,
        "source_freshness": TriggerRule.ALL_DONE,
        "load_run_stats": TriggerRule.ALL_DONE,
        "fail_run_if_any_task_failed": TriggerRule.ONE_FAILED,
    }


def test_work_tasks_retry_twice_and_alert_but_watcher_does_not(dag):
    for t in dag.tasks:
        if t.task_id == "fail_run_if_any_task_failed":
            assert t.retries == 0
            assert not t.on_failure_callback
        else:
            assert t.retries == 2
            assert t.retry_delay == timedelta(minutes=5)
            # DagBag loads its own copy of the module, so compare by name, not identity.
            assert [cb.__name__ for cb in t.on_failure_callback] == ["notify_slack"]


@pytest.mark.parametrize(
    ("logical_date", "expected"),
    [
        # Scheduled run: 03:00 JST on 11-20 is 18:00 UTC on 11-19 -> process JST 11-19.
        (pendulum.datetime(2026, 11, 19, 18, 0, tz="UTC"), "2026-11-19"),
        # Manual run at 08:30 JST on 11-20 (23:30 UTC 11-19) -> still JST 11-19.
        (pendulum.datetime(2026, 11, 19, 23, 30, tz="UTC"), "2026-11-19"),
        # Manual run at 10:00 JST on 11-20 (01:00 UTC 11-20) -> JST 11-19.
        (pendulum.datetime(2026, 11, 20, 1, 0, tz="UTC"), "2026-11-19"),
    ],
)
def test_target_date_is_previous_jst_day(logical_date, expected):
    assert tsumugi_daily.jst_target_date(logical_date) == expected


def test_cloud_run_overrides_render_target_date(dag):
    when = pendulum.datetime(2026, 11, 19, 18, 0, tz="UTC")
    ingest = dag.get_task("ingest")
    args = ingest.overrides["container_overrides"][0]["args"]
    assert ingest.job_name == "ingest"
    assert "overrides" in ingest.template_fields
    assert [render(dag, a, when) for a in args] == ["ingest", "--date", "2026-11-19"]

    for task_id in ("dbt_build", "source_freshness"):
        t = dag.get_task(task_id)
        rendered = [render(dag, a, when) for a in t.overrides["container_overrides"][0]["args"]]
        assert t.job_name == "dbt"
        assert rendered[-1].endswith("/run_artifacts/dt=2026-11-19")


def test_run_stats_load_truncates_only_the_target_partition(dag):
    when = pendulum.datetime(2026, 11, 19, 18, 0, tz="UTC")
    load = dag.get_task("load_run_stats")
    assert render(dag, load.destination_project_dataset_table, when).endswith(
        "raw.dbt_run_results$20261119"
    )
    assert [render(dag, o, when) for o in load.source_objects] == [
        "run_artifacts/dt=2026-11-19/*.ndjson"
    ]
    assert load.write_disposition == "WRITE_TRUNCATE"
    assert load.create_disposition == "CREATE_NEVER"


def failure_context():
    ti = SimpleNamespace(
        dag_id=DAG_ID, task_id="ingest", try_number=3, log_url="https://example.invalid/log"
    )
    return {
        "task_instance": ti,
        "run_id": "scheduled__2026-11-19T18:00:00+00:00",
        "exception": RuntimeError("PII-CANARY should never be sent"),
    }


def test_slack_callback_logs_stub_without_webhook(monkeypatch, caplog):
    monkeypatch.setattr(tsumugi_daily.Variable, "get", lambda *a, **k: None)
    sent = []
    monkeypatch.setattr(tsumugi_daily.urllib.request, "urlopen", lambda *a, **k: sent.append(a))

    tsumugi_daily.notify_slack(failure_context())

    assert sent == []
    assert "tsumugi_daily.ingest failed" in caplog.text


def test_slack_callback_posts_identifiers_but_never_exception_text(monkeypatch):
    monkeypatch.setattr(tsumugi_daily.Variable, "get", lambda *a, **k: "https://hooks.invalid/x")
    sent = []

    def fake_urlopen(request, timeout):
        sent.append((request, timeout))
        return io.BytesIO(b"ok")

    monkeypatch.setattr(tsumugi_daily.urllib.request, "urlopen", fake_urlopen)

    tsumugi_daily.notify_slack(failure_context())

    ((request, timeout),) = sent
    text = json.loads(request.data)["text"]
    assert "tsumugi_daily.ingest" in text and "try 3" in text
    assert "PII-CANARY" not in text
    assert timeout == tsumugi_daily.SLACK_TIMEOUT_SECONDS


def test_slack_callback_never_raises_on_network_error(monkeypatch, caplog):
    monkeypatch.setattr(tsumugi_daily.Variable, "get", lambda *a, **k: "https://hooks.invalid/x")

    def broken_urlopen(request, timeout):
        raise urllib.error.URLError("unreachable")

    monkeypatch.setattr(tsumugi_daily.urllib.request, "urlopen", broken_urlopen)

    tsumugi_daily.notify_slack(failure_context())

    assert "Slack notification failed: URLError" in caplog.text


def test_slack_callback_never_raises_on_secrets_backend_error(monkeypatch, caplog):
    def denied(*args, **kwargs):
        raise PermissionError("403 on airflow-variables-slack_webhook_url")

    monkeypatch.setattr(tsumugi_daily.Variable, "get", denied)

    tsumugi_daily.notify_slack(failure_context())

    assert "Slack notification failed: PermissionError" in caplog.text
    assert "403 on" not in caplog.text


@pytest.mark.parametrize(
    "webhook",
    [
        "hooks.slack.com/services/T000/B000/SECRET-TOKEN",  # no scheme: ValueError quotes it
        "http://hooks.slack.com/services/T000/B000/SECRET-TOKEN",
        "file:///etc/SECRET-TOKEN",
    ],
)
def test_slack_callback_rejects_non_https_webhook_without_logging_it(monkeypatch, caplog, webhook):
    monkeypatch.setattr(tsumugi_daily.Variable, "get", lambda *a, **k: webhook)
    sent = []
    monkeypatch.setattr(tsumugi_daily.urllib.request, "urlopen", lambda *a, **k: sent.append(a))

    tsumugi_daily.notify_slack(failure_context())

    assert sent == []
    assert "not an https:// URL" in caplog.text
    assert "SECRET-TOKEN" not in caplog.text
