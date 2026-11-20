"""End-to-end privacy guarantees of the ingest pipeline on generated data.

These are the tests a reviewer should read first: ZDR text never reaches any sink, dead
letters carry no payload, identifiers leave only as keyed hashes, and every row reconciles.
"""

import json
import logging
from datetime import date
from pathlib import Path

import pytest

from generator.__main__ import generate
from generator.config import GeneratorConfig
from ingest.local import run_local
from ingest.recall import measure_recall
from ingest.redact import RegexRedactor
from ingest.specs import load_specs

KEY = b"test-key-" + b"x" * 32
LOADED_AT = "2026-11-02T00:00:00.000Z"
CFG = GeneratorConfig(end_date=date(2026, 11, 1), days=7, seed=11, n_orgs=15, n_users=200)
PII_LIKE_GARBAGE = "山田 花子 090-0000-9999"
CANARY = "ZDR-CANARY"


class RecordingRedactor(RegexRedactor):
    """Regex redaction that also records every text it was asked to process (like DLP would)."""

    def __init__(self) -> None:
        self.seen: list[str] = []

    def redact(self, texts):
        self.seen.extend(texts)
        return super().redact(texts)


def read_rows(root: Path, pattern: str) -> list[dict]:
    return [
        json.loads(line)
        for p in sorted(root.glob(pattern))
        for line in p.read_text("utf-8").splitlines()
    ]


def plant_zdr_canaries(landing: Path) -> int:
    """Replace every ZDR record's text with a unique canary string. Returns ZDR records seen."""
    n = 0
    for path in landing.glob("inference_requests/dt=*/*.jsonl"):
        rows = [json.loads(line) for line in path.read_text("utf-8").splitlines()]
        for row in rows:
            if row["zdr"]:
                n += 1
                row["prompt_text"] = f"{CANARY}-{row['request_id']} {PII_LIKE_GARBAGE}"
                if row["response_text"] is not None:
                    row["response_text"] = f"{CANARY}-response-{row['request_id']}"
        path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), "utf-8")
    return n


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    landing = tmp_path_factory.mktemp("landing")
    out = tmp_path_factory.mktemp("ingest")
    generate(CFG, landing)
    assert plant_zdr_canaries(landing) > 0, "fixture must contain ZDR records"
    raw = read_rows(landing, "inference_requests/dt=*/*.jsonl")

    # Corrupt a partition: a non-JSON line, and a ZDR record that fails validation.
    zdr_row = next(r for r in raw if r["zdr"])
    bad = {**zdr_row, "model": "gpt-unknown"}
    target = sorted(landing.glob("inference_requests/dt=*/*.jsonl"))[0]
    with target.open("a", encoding="utf-8") as f:
        f.write(json.dumps(bad, ensure_ascii=False) + "\n")
        f.write(f"{{not json {PII_LIKE_GARBAGE}\n")

    redactor = RecordingRedactor()
    logger = logging.getLogger()
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append
    logger.addHandler(handler)
    try:
        stats = run_local(landing, out, load_specs(), redactor, KEY, LOADED_AT)
    finally:
        logger.removeHandler(handler)
    return {
        "landing": landing,
        "out": out,
        "raw": raw,
        "stats": stats,
        "redactor": redactor,
        "logs": [r.getMessage() for r in records],
    }


def all_output_text(out: Path) -> str:
    return "\n".join(p.read_text("utf-8") for p in out.rglob("*.jsonl"))


def test_metadata_rows_never_hold_text(run):
    meta = read_rows(run["out"], "clean/inference_requests/**/*.jsonl")
    assert meta
    assert not any("prompt_text" in r or "response_text" in r for r in meta)


def test_zdr_records_have_no_content_rows(run):
    meta = read_rows(run["out"], "clean/inference_requests/**/*.jsonl")
    content_ids = {r["request_id"] for r in read_rows(run["out"], "clean/raw_content/**/*.jsonl")}
    zdr_ids = {r["request_id"] for r in meta if r["zdr"]}
    assert zdr_ids and content_ids
    assert not zdr_ids & content_ids


def test_zdr_text_never_reaches_any_sink(run):
    # Clean output, dead letters, the redaction (DLP) request stream, and logs.
    assert CANARY not in all_output_text(run["out"])
    assert not any(CANARY in text for text in run["redactor"].seen)
    assert not any(CANARY in message for message in run["logs"])


def test_dead_letters_carry_no_payload(run):
    dead = read_rows(run["out"], "dead_letter/**/*.jsonl")
    assert {d["error_code"] for d in dead} >= {"invalid_json", "enum:model"}
    for d in dead:
        assert set(d) == {"stream", "record_id", "error_code", "source", "line_no", "_loaded_at"}
    dead_text = json.dumps(dead, ensure_ascii=False)
    assert PII_LIKE_GARBAGE not in dead_text and "山田" not in dead_text


def test_identifiers_leave_only_as_keyed_hashes(run):
    clean = read_rows(run["out"], "clean/inference_requests/**/*.jsonl")
    clean += read_rows(run["out"], "clean/app_events/**/*.jsonl")
    raw_ids = {r[f] for r in run["raw"] for f in ("org_id", "user_id", "api_key_id") if r[f]}
    output = all_output_text(run["out"] / "clean")
    assert not any(i in output for i in raw_ids)
    assert all(r["user_id"].startswith("h_") and r["org_id"].startswith("h_") for r in clean)


def test_every_row_is_loaded_or_dead_lettered(run):
    for name, s in run["stats"].items():
        assert s.lines_read == s.clean + s.dead_letters, name
        assert s.lines_read > 0
    clean = read_rows(run["out"], "clean/**/*.jsonl")
    assert all(r["_loaded_at"] == LOADED_AT for r in clean)


def test_regex_recall_is_high_for_structured_pii_and_reports_name_gap(run):
    recall = measure_recall(run["landing"] / "_labels" / "pii.jsonl", run["out"] / "clean")
    for pii_type in ("email_address", "phone_number", "postal_code", "street_address"):
        assert recall[pii_type].recall == 1.0, pii_type
    assert recall["person_name"].recall < 0.1  # regex cannot find names; DLP must
    assert all(r.zdr_removed > 0 for r in recall.values())
