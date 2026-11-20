"""Offline tests for the Cloud Run Job entrypoint (`ingest --date` and `dbt` wrapper)."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ingest.gcp import GeneratedSlice, generate_slice, run_gcp_slice
from ingest.pipeline import ReconciliationError, SourceLine
from ingest.redact import RegexRedactor
from runner.__main__ import main as runner_main
from runner.__main__ import run_ingest_cli
from runner.dbt_wrapper import extract_artifacts_uri, run_dbt, step_name, target_date_from_uri
from tracking_plan import codegen

KEY = b"test-gcp-hmac-key-" + b"z" * 32
LOADED_AT = "2026-11-19T18:05:00.000Z"
CANARY = "GCP-ZDR-CANARY"
PII_CANARY = "山田 太郎 090-0000-2222"


class FakeBlob:
    def __init__(self, bucket: FakeBucket, name: str) -> None:
        self.bucket = bucket
        self.name = name

    def upload_from_string(self, data: bytes | str, content_type: str | None = None) -> None:
        raw = data.encode("utf-8") if isinstance(data, str) else data
        self.bucket.objects[self.name] = (raw, content_type)


class FakeBucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: dict[str, tuple[bytes, str | None]] = {}

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)


class FakeStorageClient:
    def __init__(self) -> None:
        self.buckets: dict[str, FakeBucket] = {}

    def bucket(self, name: str) -> FakeBucket:
        return self.buckets.setdefault(name, FakeBucket(name))


class FakeBqClient:
    def __init__(self, storage: FakeStorageClient, short_read_table: str | None = None) -> None:
        self.storage = storage
        self.short_read_table = short_read_table
        self.loads: list[tuple[list[str], str, Any, str]] = []

    def load_table_from_uri(
        self, uris: list[str], destination: str, job_config: Any, location: str
    ) -> Any:
        self.loads.append((uris, destination, job_config, location))
        rows = 0
        for uri in uris:
            without = uri.removeprefix("gs://")
            bkt, _, obj = without.partition("/")
            payload, _ = self.storage.buckets[bkt].objects[obj]
            rows += len(payload.decode("utf-8").splitlines())
        if self.short_read_table and destination.endswith(f".{self.short_read_table}"):
            rows -= 1
        return SimpleNamespace(result=lambda: None, output_rows=rows)


class RecordingRedactor(RegexRedactor):
    def __init__(self) -> None:
        self.seen: list[str] = []

    def redact(self, texts):
        self.seen.extend(texts)
        return super().redact(texts)


def _decode_rows(bucket: FakeBucket, prefix: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, (payload, _) in sorted(bucket.objects.items()):
        if name.startswith(prefix):
            rows.extend(json.loads(line) for line in payload.decode("utf-8").splitlines())
    return rows


def test_single_day_slice_shares_orgs_across_days_and_bootstraps_org_history():
    anchor_start, anchor_end = date(2026, 11, 1), date(2026, 11, 5)
    day2 = generate_slice(
        date(2026, 11, 2),
        days=1,
        seed=9,
        anchor_start=anchor_start,
        anchor_end=anchor_end,
        n_orgs=8,
        n_users=60,
    )
    day3 = generate_slice(
        date(2026, 11, 3),
        days=1,
        seed=9,
        anchor_start=anchor_start,
        anchor_end=anchor_end,
        n_orgs=8,
        n_users=60,
    )

    def created_orgs(sl: GeneratedSlice) -> set[str]:
        return {
            json.loads(line.text)["org_id"]
            for lines in sl.partitions["app_events"].values()
            for line in lines
            if json.loads(line.text)["event_name"] == "org_changed"
            and json.loads(line.text)["properties"]["change_type"] == "created"
        }

    orgs_day2 = created_orgs(day2)
    orgs_day3 = created_orgs(day3)
    assert len(orgs_day2) == 8
    assert orgs_day2 == orgs_day3
    assert list(day2.partitions["inference_requests"]) == ["2026-11-02"]
    assert list(day3.partitions["inference_requests"]) == ["2026-11-03"]


def test_gcp_ingest_enforces_zdr_bq_schemas_and_reconciliation():
    base = generate_slice(
        date(2026, 11, 2),
        days=1,
        seed=42,
        anchor_start=date(2026, 11, 1),
        anchor_end=date(2026, 11, 4),
        n_orgs=30,
        n_users=250,
    )

    # Plant ZDR canary strings and corrupt lines to exercise dead-letter + ZDR isolation.
    inf_lines: list[SourceLine] = []
    planted = 0
    for line in base.partitions["inference_requests"]["2026-11-02"]:
        row = json.loads(line.text)
        if row["zdr"]:
            planted += 1
            row["prompt_text"] = f"{CANARY}-{row['request_id']} {PII_CANARY}"
            row["response_text"] = f"{CANARY}-resp-{row['request_id']}"
        inf_lines.append(SourceLine(line.source, line.line_no, json.dumps(row, ensure_ascii=False)))
    assert planted > 0
    inf_lines.append(SourceLine("inference_requests/dt=2026-11-02/part-00000.jsonl", 99999, "{bad"))

    tampered = GeneratedSlice(
        partitions={
            "app_events": base.partitions["app_events"],
            "inference_requests": {"2026-11-02": tuple(inf_lines)},
        },
        pii_labels=base.pii_labels,
    )

    storage = FakeStorageClient()
    bq = FakeBqClient(storage)
    redactor = RecordingRedactor()
    root_logger = logging.getLogger()
    log_records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = log_records.append
    root_logger.addHandler(handler)
    try:
        report = run_gcp_slice(
            tampered,
            project="proj-tsumugi",
            region="asia-northeast1",
            bucket_name="bkt-tsumugi",
            dataset="raw",
            redactor=redactor,
            redactor_name="regex",
            key=KEY,
            loaded_at=LOADED_AT,
            storage_client=storage,
            bq_client=bq,
        )
    finally:
        root_logger.removeHandler(handler)

    bucket = storage.buckets["bkt-tsumugi"]
    all_gcs_text = "\n".join(p.decode("utf-8") for p, _ in bucket.objects.values())
    assert CANARY not in all_gcs_text
    assert not any(CANARY in t for t in redactor.seen)
    assert not any(CANARY in r.getMessage() for r in log_records)

    # Every clean stream's columns match its generated BigQuery TableFieldSchema.
    bq_schemas = codegen.build_bq_schemas(codegen.load_plan())
    for table in ("app_events", "inference_requests", "raw_content"):
        expected_cols = {f["name"] for f in bq_schemas[f"{table}.json"]}
        rows = _decode_rows(bucket, f"clean/{table}/")
        assert rows, table
        assert all(set(r) == expected_cols for r in rows)

    dead = _decode_rows(bucket, "dead_letter/")
    assert len(dead) == 1 and dead[0]["error_code"] == "invalid_json"
    assert report["streams"]["inference_requests"]["dead_letters"] == 1
    assert [dest for _, dest, _, _ in bq.loads] == [
        "proj-tsumugi.raw.app_events",
        "proj-tsumugi.raw.inference_requests",
        "proj-tsumugi.raw.raw_content",
    ]
    for _, _, cfg, loc in bq.loads:
        assert cfg.write_disposition == "WRITE_APPEND"
        assert cfg.create_disposition == "CREATE_NEVER"
        assert loc == "asia-northeast1"


def test_gcp_ingest_fails_closed_when_bq_load_count_diverges():
    sl = generate_slice(
        date(2026, 11, 2),
        days=1,
        seed=5,
        anchor_start=date(2026, 11, 1),
        anchor_end=date(2026, 11, 3),
        n_orgs=6,
        n_users=40,
    )
    storage = FakeStorageClient()
    bq = FakeBqClient(storage, short_read_table="inference_requests")
    with pytest.raises(ReconciliationError, match="inference_requests"):
        run_gcp_slice(
            sl,
            project="proj",
            region="asia-northeast1",
            bucket_name="bkt",
            dataset="raw",
            redactor=RegexRedactor(),
            redactor_name="regex",
            key=KEY,
            loaded_at=LOADED_AT,
            storage_client=storage,
            bq_client=bq,
        )


def test_ingest_cli_accepts_dag_container_args_with_env_defaults(monkeypatch):
    monkeypatch.setattr(
        "runner.__main__.generate_slice",
        lambda target_date, *, days, seed: generate_slice(
            target_date,
            days=days,
            seed=seed,
            anchor_start=date(2026, 11, 18),
            anchor_end=date(2026, 11, 20),
            n_orgs=6,
            n_users=40,
        ),
    )
    storage = FakeStorageClient()
    bq = FakeBqClient(storage)
    rc = run_ingest_cli(
        ["--date", "2026-11-19", "--stats-uri", "gs://bkt/run_artifacts/dt=2026-11-19/ingest.json"],
        env={"TSUMUGI_PROJECT_ID": "proj-1", "TSUMUGI_BUCKET": "bkt"},
        key_loader=lambda project, secret: KEY,
        redactor_factory=lambda name, project, region: RegexRedactor(),
        storage_client=storage,
        bq_client=bq,
    )
    assert rc == 0
    stats_raw, _ = storage.buckets["bkt"].objects["run_artifacts/dt=2026-11-19/ingest.json"]
    parsed = json.loads(stats_raw.decode("utf-8"))
    assert parsed["date"] == "2026-11-19" and parsed["days"] == 1
    assert parsed["bq_loaded_rows"]["inference_requests"] > 0


# --- dbt wrapper -------------------------------------------------------------


def _target_path_from_cmd(cmd: list[str]) -> Path:
    idx = cmd.index("--target-path")
    return Path(cmd[idx + 1])


def test_dbt_wrapper_helpers_parse_dag_args():
    dbt_args, uri = extract_artifacts_uri(
        [
            "source",
            "freshness",
            "--target",
            "prod",
            "--artifacts-uri",
            "gs://b/run_artifacts/dt=2026-11-19",
        ]
    )
    assert dbt_args == ["source", "freshness", "--target", "prod"]
    assert uri == "gs://b/run_artifacts/dt=2026-11-19"
    assert step_name(dbt_args) == "source_freshness"
    assert target_date_from_uri(uri) == "2026-11-19"


def test_dbt_wrapper_flattens_run_results_and_omits_error_messages():
    storage = FakeStorageClient()
    expected_cols = {f["name"] for f in codegen.DBT_RUN_RESULTS_SCHEMA}

    def fake_dbt(cmd: list[str]) -> int:
        target = _target_path_from_cmd(list(cmd))
        artifact = {
            "metadata": {
                "invocation_id": "inv-123",
                "generated_at": "2026-11-19T18:10:00Z",
            },
            "results": [
                {
                    "unique_id": "model.tsumugi.fct_inference_requests",
                    "status": "success",
                    "execution_time": 4.25,
                    "failures": None,
                    "message": "MERGE (120.0 rows, 1.2 MiB processed)",
                    "adapter_response": {
                        "job_id": "bq-job-1",
                        "bytes_processed": 1258291,
                        "bytes_billed": 10485760,
                        "slot_ms": 840,
                    },
                },
                {
                    "unique_id": "test.tsumugi.unique_dim_orgs_org_version_id",
                    "status": "fail",
                    "execution_time": 1.1,
                    "failures": 2,
                    "message": f"Sensitive exception text {PII_CANARY}",
                    "adapter_response": {"job_id": "bq-job-2"},
                },
            ],
        }
        (target / "run_results.json").write_text(json.dumps(artifact), encoding="utf-8")
        return 1  # failing test returns exit code 1, yet NDJSON must still be uploaded

    rc = run_dbt(
        ["build", "--target", "prod", "--artifacts-uri", "gs://bkt/run_artifacts/dt=2026-11-19"],
        runner_fn=fake_dbt,
        storage_client=storage,
        loaded_at=LOADED_AT,
    )
    assert rc == 1
    raw, _ = storage.buckets["bkt"].objects["run_artifacts/dt=2026-11-19/build.ndjson"]
    text = raw.decode("utf-8")
    assert PII_CANARY not in text

    rows = [json.loads(line) for line in text.splitlines()]
    assert len(rows) == 2
    assert all(set(r) == expected_cols for r in rows)
    assert rows[0]["job_id"] == "bq-job-1" and rows[0]["resource_type"] == "model"
    assert rows[1]["status"] == "fail" and rows[1]["failures"] == 2


def test_dbt_wrapper_handles_source_freshness_and_startup_crash():
    storage = FakeStorageClient()
    expected_cols = {f["name"] for f in codegen.DBT_RUN_RESULTS_SCHEMA}

    def fake_freshness(cmd: list[str]) -> int:
        target = _target_path_from_cmd(list(cmd))
        sources = {
            "metadata": {"invocation_id": "inv-fresh", "generated_at": "2026-11-19T18:12:00Z"},
            "results": [
                {
                    "unique_id": "source.tsumugi.app.events",
                    "status": "pass",
                    "execution_time": 0.9,
                    "max_loaded_at": "2026-11-19T18:04:00Z",
                    "age": 480.0,
                    "adapter_response": {"job_id": "bq-fresh-1", "bytes_billed": 10485760},
                }
            ],
        }
        (target / "sources.json").write_text(json.dumps(sources), encoding="utf-8")
        return 0

    rc_ok = run_dbt(
        [
            "source",
            "freshness",
            "--target",
            "prod",
            "--artifacts-uri",
            "gs://bkt/run_artifacts/dt=2026-11-19",
        ],
        runner_fn=fake_freshness,
        storage_client=storage,
        loaded_at=LOADED_AT,
    )
    assert rc_ok == 0
    fresh_raw, _ = storage.buckets["bkt"].objects[
        "run_artifacts/dt=2026-11-19/source_freshness.ndjson"
    ]
    fresh_row = json.loads(fresh_raw.decode("utf-8").strip())
    assert set(fresh_row) == expected_cols
    assert fresh_row["step"] == "source_freshness"
    assert fresh_row["max_loaded_at"] == "2026-11-19T18:04:00Z" and fresh_row["age_s"] == 480.0

    # Crash before dbt writes any file: wrapper still writes invocation_error NDJSON.
    rc_crash = run_dbt(
        ["build", "--target", "prod", "--artifacts-uri", "gs://bkt/run_artifacts/dt=2026-11-19"],
        runner_fn=lambda cmd: 2,
        storage_client=storage,
        loaded_at=LOADED_AT,
    )
    assert rc_crash == 2
    crash_raw, _ = storage.buckets["bkt"].objects["run_artifacts/dt=2026-11-19/build.ndjson"]
    crash_row = json.loads(crash_raw.decode("utf-8").strip())
    assert set(crash_row) == expected_cols
    assert crash_row["status"] == "invocation_error"
    assert crash_row["unique_id"] == "invocation.build"


def test_runner_main_rejects_unknown_subcommand():
    assert runner_main(["--help"]) == 0
    assert runner_main([]) == 2
    assert runner_main(["unknown"]) == 2
