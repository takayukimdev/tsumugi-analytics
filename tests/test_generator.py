import json
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from generator.__main__ import generate
from generator.config import GeneratorConfig
from generator.defects import MAX_LATENESS
from tracking_plan import codegen

SMALL = GeneratorConfig(end_date=date(2026, 11, 1), days=10, seed=7, n_orgs=12, n_users=200)


def read_jsonl(paths: list[Path]) -> list[dict]:
    return [json.loads(line) for p in sorted(paths) for line in p.read_text("utf-8").splitlines()]


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@pytest.fixture(scope="module")
def landing(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("landing")
    generate(SMALL, out)
    return out


@pytest.fixture(scope="module")
def app_events(landing) -> list[dict]:
    return read_jsonl(list(landing.glob("app_events/dt=*/*.jsonl")))


@pytest.fixture(scope="module")
def requests(landing) -> list[dict]:
    return read_jsonl(list(landing.glob("inference_requests/dt=*/*.jsonl")))


@pytest.fixture(scope="module")
def labels(landing) -> dict[str, list[dict]]:
    return {
        name: read_jsonl([landing / "_labels" / f"{name}.jsonl"]) for name in ("pii", "defects")
    }


def test_same_seed_produces_identical_files(tmp_path, landing):
    generate(SMALL, tmp_path)
    produced = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*.jsonl"))
    expected = sorted(p.relative_to(landing) for p in landing.rglob("*.jsonl"))
    assert produced == expected
    assert all((tmp_path / p).read_bytes() == (landing / p).read_bytes() for p in produced)


def test_every_record_matches_its_tracking_plan_schema(app_events, requests):
    schemas = codegen.build_schemas(codegen.load_plan())
    for event in app_events:
        schema = schemas[f"app__{event['event_name']}.json"]
        assert not list(Draft202012Validator(schema).iter_errors(event)), event
    validator = Draft202012Validator(schemas["api__inference_requests.json"])
    for request in requests:
        assert not list(validator.iter_errors(request)), request


def test_files_are_partitioned_by_jst_received_date(landing):
    for path in landing.glob("*/dt=*/*.jsonl"):
        dt = path.parent.name.removeprefix("dt=")
        for row in read_jsonl([path]):
            jst = parse_ts(row["received_ts"]) + timedelta(hours=9)
            assert jst.date().isoformat() == dt


def test_defect_rate_is_about_one_percent(app_events, requests, labels):
    total = len(app_events) + len(requests)
    assert 0.005 <= len(labels["defects"]) / total <= 0.015
    assert {d["defect"] for d in labels["defects"]} == {"duplicate", "late_arrival"}


def test_duplicates_share_id_and_late_arrivals_are_bounded(app_events, requests, labels):
    rows_by_id = defaultdict(list)
    for row in app_events:
        rows_by_id[row["event_id"]].append(row)
    for row in requests:
        rows_by_id[row["request_id"]].append(row)
    for defect in labels["defects"]:
        rows = rows_by_id[defect["record_id"]]
        if defect["defect"] == "duplicate":
            assert len(rows) >= 2
        else:
            lag = parse_ts(rows[0]["received_ts"]) - parse_ts(rows[0]["event_ts"])
            assert timedelta(hours=1) <= lag <= MAX_LATENESS


def test_pii_labels_point_at_real_substrings(requests, labels):
    by_id = {r["request_id"]: r for r in requests}
    assert labels["pii"], "expected some injected PII"
    for label in labels["pii"]:
        assert label["value"] in by_id[label["record_id"]][label["field"]]
    assert {lbl["field"] for lbl in labels["pii"]} == {"prompt_text", "response_text"}


def test_zdr_orgs_send_text_that_ingest_must_strip(requests):
    zdr_with_text = [r for r in requests if r["zdr"] and r["prompt_text"]]
    assert zdr_with_text, "generator must exercise the ZDR strip path"


def test_error_requests_have_no_output(requests):
    errors = [r for r in requests if r["status"] == "error"]
    assert errors
    for r in errors:
        assert r["output_tokens"] == 0
        assert r["response_text"] is None and r["ttft_ms"] is None and r["error_code"]


def test_cached_tokens_never_exceed_input(requests):
    assert all(0 <= r["cached_tokens"] <= r["input_tokens"] for r in requests)


def test_each_org_starts_with_created_then_changes_in_order(app_events):
    changes = defaultdict(list)
    for e in app_events:
        if e["event_name"] == "org_changed":
            changes[e["org_id"]].append(e["properties"])
    assert len(changes) == SMALL.n_orgs
    for history in changes.values():
        ordered = sorted(
            {p["changed_at"]: p for p in history}.values(), key=lambda p: p["changed_at"]
        )
        assert ordered[0]["change_type"] == "created"
        assert Counter(p["change_type"] for p in ordered)["created"] == 1


def test_request_zdr_matches_org_state_at_request_time(app_events, requests):
    history = defaultdict(list)
    for e in app_events:
        if e["event_name"] == "org_changed":
            history[e["org_id"]].append(
                (parse_ts(e["properties"]["changed_at"]), e["properties"]["zdr"])
            )
    for r in requests:
        ts = parse_ts(r["event_ts"])
        state = max((h for h in history[r["org_id"]] if h[0] <= ts), key=lambda h: h[0])
        assert r["zdr"] == state[1]


@pytest.mark.parametrize(
    "kwargs",
    [{"days": 0}, {"n_orgs": 0}, {"n_orgs": 10, "n_users": 5}, {"defect_rate": 0.5}],
)
def test_invalid_config_is_rejected(kwargs):
    with pytest.raises(ValueError):
        GeneratorConfig(end_date=date(2026, 11, 1), **kwargs)
