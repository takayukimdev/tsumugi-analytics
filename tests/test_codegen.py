import pytest
from google.cloud.bigquery import SchemaField
from jsonschema import Draft202012Validator

from tracking_plan import codegen


@pytest.fixture(scope="module")
def schemas():
    return codegen.build_schemas(codegen.load_plan())


@pytest.fixture(scope="module")
def bq_schemas():
    return codegen.build_bq_schemas(codegen.load_plan())


def test_committed_schemas_match_tracking_plan(schemas):
    assert codegen.stale_files(schemas, codegen.SCHEMA_DIR) == []


def test_every_schema_is_valid_json_schema(schemas):
    for schema in schemas.values():
        Draft202012Validator.check_schema(schema)


def test_one_schema_per_app_event_plus_inference_stream(schemas):
    plan = codegen.load_plan()
    expected = {f"app__{e}.json" for e in plan["sources"]["app"]["events"]}
    expected.add("api__inference_requests.json")
    assert set(schemas) == expected


def test_optional_fields_are_nullable_and_enum_allows_null():
    schema = codegen.property_schema(
        "x", {"type": "string", "required": False, "pii": "none", "enum": ["a"]}
    )
    assert schema["type"] == ["string", "null"]
    assert schema["enum"] == ["a", None]


def test_unknown_fields_are_rejected(schemas):
    validator = Draft202012Validator(schemas["api__inference_requests.json"])
    errors = list(validator.iter_errors({"surprise": 1}))
    assert any("Additional properties" in e.message for e in errors)


def test_event_name_is_pinned_per_schema(schemas):
    schema = schemas["app__message_sent.json"]
    assert schema["properties"]["event_name"]["const"] == "message_sent"


def test_free_text_fields_are_tagged_for_ingest(schemas):
    props = schemas["api__inference_requests.json"]["properties"]
    free_text = {name for name, p in props.items() if p["x-pii"] == "free_text"}
    assert free_text == {"prompt_text", "response_text"}


@pytest.mark.parametrize(
    "spec, message",
    [
        ({"type": "uuid", "pii": "none"}, "unknown type"),
        ({"type": "string", "pii": "secret"}, "pii must be one of"),
    ],
)
def test_malformed_plan_raises(spec, message):
    with pytest.raises(codegen.TrackingPlanError, match=message):
        codegen.property_schema("bad", spec)
    with pytest.raises(codegen.TrackingPlanError, match=message):
        codegen.bq_field("bad", spec, "test")


def test_committed_bq_schemas_match_tracking_plan(bq_schemas):
    assert codegen.stale_files(bq_schemas, codegen.BQ_SCHEMA_DIR) == []
    assert codegen.main(["--check"]) == 0


def test_every_bq_schema_is_valid_bigquery_schema_field_list(bq_schemas):
    assert set(bq_schemas) == {
        "app_events.json",
        "inference_requests.json",
        "raw_content.json",
        "dbt_run_results.json",
    }
    for name, fields in bq_schemas.items():
        parsed = [SchemaField.from_api_repr(f) for f in fields]
        assert parsed, name
        assert parsed[-1].name == "_loaded_at" and parsed[-1].field_type == "TIMESTAMP"


def test_bq_schemas_split_free_text_and_type_app_properties_as_json(bq_schemas):
    inf_names = {f["name"] for f in bq_schemas["inference_requests.json"]}
    assert not (inf_names & {"prompt_text", "response_text"})

    content_names = [f["name"] for f in bq_schemas["raw_content.json"]]
    assert content_names == ["request_id", "event_ts", "prompt_text", "response_text", "_loaded_at"]

    app_by_name = {f["name"]: f for f in bq_schemas["app_events.json"]}
    assert app_by_name["properties"]["type"] == "JSON"
    assert app_by_name["properties"]["mode"] == "REQUIRED"


def test_drift_check_detects_modified_and_orphan_bq_schema(bq_schemas, tmp_path):
    codegen.write_schemas(bq_schemas, tmp_path)
    assert codegen.stale_files(bq_schemas, tmp_path) == []

    (tmp_path / "raw_content.json").write_text("[]\n", encoding="utf-8")
    (tmp_path / "orphan.json").write_text("[]\n", encoding="utf-8")
    assert codegen.stale_files(bq_schemas, tmp_path) == ["orphan.json", "raw_content.json"]
