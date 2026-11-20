"""Per-stream processing specs derived from the generated JSON Schemas.

Which fields are identifiers (HMAC) and which are free text (ZDR strip / DLP) comes from the
`x-pii` tag in the schema, so the tracking plan stays the single source of truth.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator

from tracking_plan.codegen import build_schemas, load_plan

ANY_EVENT = "*"


@dataclass(frozen=True)
class StreamSpec:
    name: str
    id_field: str
    identifier_fields: tuple[str, ...]
    free_text_fields: tuple[str, ...]
    # Keyed by event_name for app_events; a single ANY_EVENT entry for log streams.
    validators: Mapping[str, Draft202012Validator]

    def validator_for(self, record: Mapping[str, Any]) -> Draft202012Validator | None:
        if ANY_EVENT in self.validators:
            return self.validators[ANY_EVENT]
        return self.validators.get(record.get("event_name"))


def _fields_with_pii(schema: Mapping[str, Any], pii: str) -> tuple[str, ...]:
    return tuple(n for n, p in schema["properties"].items() if p.get("x-pii") == pii)


def load_specs() -> dict[str, StreamSpec]:
    schemas = build_schemas(load_plan())
    app = {
        name.removeprefix("app__").removesuffix(".json"): s
        for name, s in schemas.items()
        if name.startswith("app__")
    }
    inference = schemas["api__inference_requests.json"]
    any_app_schema = next(iter(app.values()))
    return {
        "app_events": StreamSpec(
            name="app_events",
            id_field="event_id",
            identifier_fields=_fields_with_pii(any_app_schema, "identifier"),
            free_text_fields=_fields_with_pii(any_app_schema, "free_text"),
            validators={event: Draft202012Validator(s) for event, s in app.items()},
        ),
        "inference_requests": StreamSpec(
            name="inference_requests",
            id_field="request_id",
            identifier_fields=_fields_with_pii(inference, "identifier"),
            free_text_fields=_fields_with_pii(inference, "free_text"),
            validators={ANY_EVENT: Draft202012Validator(inference)},
        ),
    }
