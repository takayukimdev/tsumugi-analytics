"""Schema validation that reports errors WITHOUT echoing record values.

jsonschema error messages quote the offending value (which may be PII or ZDR text), so only
the failing keyword and JSON path are kept, e.g. `enum:model` or `required:$`.
"""

from __future__ import annotations

from typing import Any

from jsonschema.exceptions import best_match

from ingest.specs import StreamSpec


def validation_error(spec: StreamSpec, record: Any) -> str | None:
    """None if valid, otherwise a value-free error code."""
    if not isinstance(record, dict):
        return "not_an_object"
    validator = spec.validator_for(record)
    if validator is None:
        return "unknown_event_name"
    if validator.is_valid(record):
        return None
    error = best_match(validator.iter_errors(record))
    path = ".".join(str(p) for p in error.absolute_path) or "$"
    return f"{error.validator}:{path}"
