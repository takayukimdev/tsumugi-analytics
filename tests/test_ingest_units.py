import hashlib
from types import SimpleNamespace

import pytest
from google.api_core import exceptions

from ingest import dlp, pseudonymize
from ingest.dlp import DlpRedactor, plan_batches
from ingest.redact import RegexRedactor
from ingest.specs import load_specs
from ingest.validate import validation_error
from ingest.zdr import strip_zdr

KEY = b"k" * 32
FREE_TEXT = ("prompt_text", "response_text")


# --- ZDR strip ---------------------------------------------------------------


def test_zdr_record_loses_free_text_and_input_is_not_mutated():
    record = {"zdr": True, "prompt_text": "secret", "response_text": "also", "model": "m"}
    stripped = strip_zdr(record, FREE_TEXT)
    assert stripped == {"zdr": True, "prompt_text": None, "response_text": None, "model": "m"}
    assert record["prompt_text"] == "secret"


def test_non_zdr_record_is_returned_unchanged():
    record = {"zdr": False, "prompt_text": "keep"}
    assert strip_zdr(record, FREE_TEXT) is record


@pytest.mark.parametrize("flag", [None, "false", 0, "no"])
def test_zdr_fails_closed_on_missing_or_malformed_flag(flag):
    record = {"prompt_text": "text"} if flag is None else {"zdr": flag, "prompt_text": "text"}
    assert strip_zdr(record, FREE_TEXT)["prompt_text"] is None


# --- validation --------------------------------------------------------------


def test_validation_errors_never_contain_record_values():
    spec = load_specs()["inference_requests"]
    pii = "山田 花子 090-0000-1234"
    error = validation_error(spec, {"request_id": pii, "model": pii})
    assert error and pii not in error and "山田" not in error


def test_unknown_app_event_is_rejected():
    spec = load_specs()["app_events"]
    assert validation_error(spec, {"event_name": "nope"}) == "unknown_event_name"
    assert validation_error(spec, ["not", "a", "dict"]) == "not_an_object"


def test_specs_read_pii_classes_from_tracking_plan():
    specs = load_specs()
    assert specs["inference_requests"].free_text_fields == FREE_TEXT
    assert set(specs["inference_requests"].identifier_fields) == {"org_id", "user_id", "api_key_id"}
    assert specs["app_events"].free_text_fields == ()


# --- regex redactor ----------------------------------------------------------


@pytest.mark.parametrize(
    "text, value",
    [
        ("連絡先はhanako12@example.com です。", "hanako12@example.com"),
        ("電話番号は090-0000-1234です。", "090-0000-1234"),
        ("住所は〒123-4567 東京都架空区1-2-3です。", "東京都架空区1-2-3"),
        ("住所は〒123-4567 東京都架空区1-2-3です。", "123-4567"),
        ("Please ship it to 3-12-4 Kasou-ku, Tokyo, 〒100-0001.", "3-12-4 Kasou-ku, Tokyo"),
    ],
)
def test_regex_redactor_removes_structured_pii(text, value):
    assert value not in RegexRedactor().redact([text])[0]


def test_regex_redactor_misses_person_names_by_design():
    # Names need NER: this gap is what DLP closes, and recall reporting makes it visible.
    assert "山田 花子" in RegexRedactor().redact(["私の名前は山田 花子です。"])[0]


# --- HMAC pseudonymization ---------------------------------------------------


def test_hmac_is_deterministic_keyed_and_not_plain_sha256():
    a = pseudonymize.pseudonymize(KEY, "user_1")
    assert a == pseudonymize.pseudonymize(KEY, "user_1")
    assert a != pseudonymize.pseudonymize(b"j" * 32, "user_1")
    assert a.removeprefix("h_") != hashlib.sha256(b"user_1").hexdigest()


def test_pseudonymize_fields_skips_nulls_and_copies():
    record = {"user_id": "u", "api_key_id": None, "model": "m"}
    out = pseudonymize.pseudonymize_fields(record, ("user_id", "api_key_id"), KEY)
    assert out["user_id"].startswith("h_") and out["api_key_id"] is None
    assert record["user_id"] == "u"


@pytest.mark.parametrize(
    "env, message",
    [({}, "not set"), ({"HMAC_KEY": "zz"}, "hex"), ({"HMAC_KEY": "ab" * 8}, "at least")],
)
def test_bad_hmac_key_fails_fast(env, message):
    with pytest.raises(pseudonymize.HmacKeyError, match=message):
        pseudonymize.load_key_from_env(env)


# --- DLP batching and backoff (fake client, no GCP) --------------------------


def test_batches_respect_byte_and_cell_limits():
    texts = ["x" * 100] * 25
    batches = plan_batches(texts, max_bytes=500, max_cells=3)
    assert [len(b) for b in batches] == [3] * 8 + [1]
    assert batches[0].start == 0 and batches[-1].stop == 25
    assert all(len(b) * (100 + dlp.PER_CELL_OVERHEAD_BYTES) <= 500 for b in batches)


def test_oversized_text_gets_its_own_batch():
    batches = plan_batches(["a", "b" * 1000, "c"], max_bytes=100, max_cells=10)
    assert [list(b) for b in batches] == [[0], [1], [2]]


class FakeDlp:
    def __init__(self, failures: int = 0):
        self.requests: list[dict] = []
        self.failures = failures

    def deidentify_content(self, request):
        if self.failures:
            self.failures -= 1
            raise exceptions.ResourceExhausted("quota")
        self.requests.append(request)
        rows = [
            SimpleNamespace(
                values=[SimpleNamespace(string_value=f"<{r['values'][0]['string_value']}>")]
            )
            for r in request["item"]["table"]["rows"]
        ]
        return SimpleNamespace(item=SimpleNamespace(table=SimpleNamespace(rows=rows)))


def test_dlp_redactor_preserves_order_and_uses_regional_parent():
    client = FakeDlp()
    out = DlpRedactor("proj", client=client).redact(["a", "b"])
    assert out == ["<a>", "<b>"]
    assert client.requests[0]["parent"] == "projects/proj/locations/asia-northeast1"


def test_dlp_redactor_backs_off_on_quota_errors():
    sleeps: list[float] = []
    client = FakeDlp(failures=2)
    out = DlpRedactor("proj", client=client, sleep=sleeps.append).redact(["a"])
    assert out == ["<a>"] and sleeps == [1.0, 2.0]


def test_dlp_redactor_does_not_retry_other_errors():
    class Broken(FakeDlp):
        def deidentify_content(self, request):
            raise exceptions.InvalidArgument("bad")

    with pytest.raises(exceptions.InvalidArgument):
        DlpRedactor("proj", client=Broken(), sleep=lambda _: None).redact(["a"])


def test_local_runner_skips_unexpected_files(tmp_path, caplog):
    from ingest.local import iter_partitions

    part = tmp_path / "app_events" / "dt=2026-11-01"
    part.mkdir(parents=True)
    (part / "part-00000.jsonl").write_text('{"a": 1}\n', "utf-8")
    (part / "part-00000 2.jsonl").write_text('{"a": 1}\n', "utf-8")  # sync-conflict copy
    [(dt, lines)] = list(iter_partitions(tmp_path, "app_events"))
    assert dt == "dt=2026-11-01" and len(lines) == 1
    assert "unexpected" in caplog.text
