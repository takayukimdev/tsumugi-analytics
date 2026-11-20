"""Generate JSON Schemas and BigQuery raw table schemas from tracking_plan/events.yaml.

Usage:
    python -m tracking_plan.codegen          # write schemas/*.json and bq_schemas/*.json
    python -m tracking_plan.codegen --check  # exit 1 if committed schemas are out of date
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

PLAN_PATH = Path(__file__).parent / "events.yaml"
SCHEMA_DIR = Path(__file__).parent / "schemas"
BQ_SCHEMA_DIR = Path(__file__).parent / "bq_schemas"
JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"

TYPE_MAP: dict[str, dict[str, str]] = {
    "string": {"type": "string"},
    "integer": {"type": "integer"},
    "number": {"type": "number"},
    "boolean": {"type": "boolean"},
    "timestamp": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
}
BQ_TYPE_MAP: dict[str, str] = {
    "string": "STRING",
    "integer": "INTEGER",
    "number": "FLOAT",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
}
PII_CLASSES = frozenset({"none", "identifier", "free_text"})

LOADED_AT_FIELD: dict[str, str] = {
    "name": "_loaded_at",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "UTC timestamp when the ingest job processed the batch",
}

DBT_RUN_RESULTS_SCHEMA: list[dict[str, str]] = [
    {
        "name": "target_date",
        "type": "DATE",
        "mode": "REQUIRED",
        "description": "JST date processed by the pipeline run",
    },
    {
        "name": "step",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "dbt subcommand executed (e.g. build, source_freshness)",
    },
    {
        "name": "invocation_id",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "dbt invocation UUID",
    },
    {
        "name": "unique_id",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "dbt node unique_id, or invocation.<step> on startup failure",
    },
    {
        "name": "resource_type",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "model, test, seed, source, or invocation",
    },
    {
        "name": "status",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Outcome: success, pass, error, fail, warn, skipped, or invocation_error",
    },
    {
        "name": "execution_time_s",
        "type": "FLOAT",
        "mode": "NULLABLE",
        "description": "Wall-clock node runtime in seconds",
    },
    {
        "name": "failures",
        "type": "INTEGER",
        "mode": "NULLABLE",
        "description": "Failing row count for tests",
    },
    {
        "name": "job_id",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "BigQuery job_id from adapter_response for joining INFORMATION_SCHEMA.JOBS",
    },
    {
        "name": "bytes_processed",
        "type": "INTEGER",
        "mode": "NULLABLE",
        "description": "Bytes processed reported by the BigQuery adapter",
    },
    {
        "name": "bytes_billed",
        "type": "INTEGER",
        "mode": "NULLABLE",
        "description": "Bytes billed reported by the BigQuery adapter",
    },
    {
        "name": "slot_ms",
        "type": "INTEGER",
        "mode": "NULLABLE",
        "description": "Slot milliseconds reported by the BigQuery adapter",
    },
    {
        "name": "max_loaded_at",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "description": "Latest _loaded_at seen by source freshness",
    },
    {
        "name": "age_s",
        "type": "FLOAT",
        "mode": "NULLABLE",
        "description": "Source age in seconds at freshness check time",
    },
    {
        "name": "executed_at",
        "type": "TIMESTAMP",
        "mode": "REQUIRED",
        "description": "UTC timestamp when dbt generated the artifact",
    },
    {
        "name": "_loaded_at",
        "type": "TIMESTAMP",
        "mode": "REQUIRED",
        "description": "UTC timestamp when the dbt wrapper wrote the NDJSON artifact",
    },
]


class TrackingPlanError(ValueError):
    """Raised when the tracking plan is malformed."""


def load_plan(path: Path = PLAN_PATH) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def property_schema(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    """Translate one tracking-plan property into a JSON Schema fragment."""
    ptype = spec.get("type")
    if ptype not in TYPE_MAP:
        raise TrackingPlanError(f"{name}: unknown type {ptype!r}")
    pii = spec.get("pii")
    if pii not in PII_CLASSES:
        raise TrackingPlanError(f"{name}: pii must be one of {sorted(PII_CLASSES)}, got {pii!r}")

    base = dict(TYPE_MAP[ptype])
    nullable = not spec.get("required", False)
    schema: dict[str, Any] = {**base, "type": [base["type"], "null"]} if nullable else base
    if "enum" in spec:
        schema["enum"] = [*spec["enum"], None] if nullable else list(spec["enum"])
    if "description" in spec:
        schema["description"] = spec["description"]
    schema["x-pii"] = pii
    return schema


def object_schema(fields: dict[str, dict[str, Any]], prefix: str) -> dict[str, Any]:
    """Closed object schema: every field present, unknown fields rejected (catches drift)."""
    props = {name: property_schema(f"{prefix}.{name}", spec) for name, spec in fields.items()}
    return {
        "type": "object",
        "properties": props,
        "required": list(fields),
        "additionalProperties": False,
    }


def app_event_schema(source: dict[str, Any], event_name: str, event: dict[str, Any]) -> dict:
    envelope = object_schema(source["common"], f"app.{event_name}")
    envelope["properties"]["event_name"] = {
        **envelope["properties"]["event_name"],
        "const": event_name,
    }
    envelope["properties"]["properties"] = object_schema(
        event.get("properties") or {}, f"app.{event_name}.properties"
    )
    envelope["required"] = [*envelope["required"], "properties"]
    return {
        "$schema": JSON_SCHEMA_DRAFT,
        "$id": f"tsumugi/app/{event_name}",
        "title": event_name,
        "description": event["trigger"],
        **envelope,
    }


def build_schemas(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return {filename: schema} for every app event and the inference log stream."""
    app = plan["sources"]["app"]
    schemas = {
        f"app__{name}.json": app_event_schema(app, name, event)
        for name, event in app["events"].items()
    }
    api = plan["sources"]["api"]
    schemas[f"api__{api['stream']}.json"] = {
        "$schema": JSON_SCHEMA_DRAFT,
        "$id": f"tsumugi/api/{api['stream']}",
        "title": api["stream"],
        "description": api["description"],
        **object_schema(api["fields"], f"api.{api['stream']}"),
    }
    return schemas


def bq_field(name: str, spec: dict[str, Any], prefix: str) -> dict[str, Any]:
    """Translate one tracking-plan property into a BigQuery TableFieldSchema dict."""
    ptype = spec.get("type")
    if ptype not in BQ_TYPE_MAP:
        raise TrackingPlanError(f"{prefix}.{name}: unknown type {ptype!r}")
    if spec.get("pii") not in PII_CLASSES:
        raise TrackingPlanError(
            f"{prefix}.{name}: pii must be one of {sorted(PII_CLASSES)}, got {spec.get('pii')!r}"
        )
    field: dict[str, Any] = {
        "name": name,
        "type": BQ_TYPE_MAP[ptype],
        "mode": "REQUIRED" if spec.get("required", False) else "NULLABLE",
    }
    if "description" in spec:
        field["description"] = spec["description"]
    return field


def build_bq_schemas(plan: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return {filename: [TableFieldSchema, ...]} for every table in the `raw` dataset."""
    app = plan["sources"]["app"]
    app_fields = [bq_field(n, s, "app.common") for n, s in app["common"].items()]
    app_fields.append(
        {
            "name": "properties",
            "type": "JSON",
            "mode": "REQUIRED",
            "description": "Event-specific payload validated against the per-event JSON Schema",
        }
    )
    app_fields.append(dict(LOADED_AT_FIELD))

    api = plan["sources"]["api"]
    api_specs = api["fields"]
    inf_fields = [
        bq_field(n, s, "api.inference_requests")
        for n, s in api_specs.items()
        if s.get("pii") != "free_text"
    ]
    inf_fields.append(dict(LOADED_AT_FIELD))

    content_fields = [
        bq_field("request_id", api_specs["request_id"], "api.raw_content"),
        bq_field("event_ts", api_specs["event_ts"], "api.raw_content"),
        *[
            bq_field(n, s, "api.raw_content")
            for n, s in api_specs.items()
            if s.get("pii") == "free_text"
        ],
        dict(LOADED_AT_FIELD),
    ]

    return {
        f"{app['stream']}.json": app_fields,
        f"{api['stream']}.json": inf_fields,
        "raw_content.json": content_fields,
        "dbt_run_results.json": [dict(f) for f in DBT_RUN_RESULTS_SCHEMA],
    }


def render(schema: Any) -> str:
    return json.dumps(schema, indent=2, ensure_ascii=False) + "\n"


def stale_files(schemas: dict[str, Any], schema_dir: Path) -> list[str]:
    """Files that are missing, out of date, or no longer in the plan."""
    expected = {name: render(s) for name, s in schemas.items()}
    existing = {p.name for p in schema_dir.glob("*.json")} if schema_dir.exists() else set()
    changed = [
        name
        for name, text in expected.items()
        if name not in existing or (schema_dir / name).read_text(encoding="utf-8") != text
    ]
    return sorted([*changed, *(existing - expected.keys())])


def write_schemas(schemas: dict[str, Any], schema_dir: Path) -> None:
    schema_dir.mkdir(parents=True, exist_ok=True)
    for orphan in {p.name for p in schema_dir.glob("*.json")} - schemas.keys():
        (schema_dir / orphan).unlink()
    for name, schema in schemas.items():
        (schema_dir / name).write_text(render(schema), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if schemas are stale")
    args = parser.parse_args(argv)

    plan = load_plan()
    schemas = build_schemas(plan)
    bq_schemas = build_bq_schemas(plan)
    if args.check:
        stale = [
            *(f"schemas/{n}" for n in stale_files(schemas, SCHEMA_DIR)),
            *(f"bq_schemas/{n}" for n in stale_files(bq_schemas, BQ_SCHEMA_DIR)),
        ]
        if stale:
            print(f"Schemas out of date: {', '.join(stale)}. Run `make schemas`.", file=sys.stderr)
            return 1
        total = len(schemas) + len(bq_schemas)
        print(f"Schemas up to date ({total} files).")
        return 0
    write_schemas(schemas, SCHEMA_DIR)
    write_schemas(bq_schemas, BQ_SCHEMA_DIR)
    print(f"Wrote {len(schemas)} JSON schemas and {len(bq_schemas)} BigQuery schemas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
