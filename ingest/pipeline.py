"""Core ingest transform for one partition of one stream.

Order is a privacy invariant, not a style choice:
    parse -> ZDR strip -> validate -> redact free text -> HMAC identifiers -> add _loaded_at
ZDR strip runs first so ZDR text can never reach a dead letter, a log line, or a DLP request.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from ingest.pseudonymize import pseudonymize_fields
from ingest.redact import Redactor
from ingest.specs import StreamSpec
from ingest.validate import validation_error
from ingest.zdr import strip_zdr

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


@dataclass(frozen=True)
class SourceLine:
    source: str  # e.g. "app_events/dt=2026-11-01/part-00000.jsonl"
    line_no: int
    text: str


CONTENT_STREAM = "raw_content"


@dataclass(frozen=True)
class PartitionResult:
    clean: tuple[dict[str, Any], ...]  # metadata only: free-text fields removed
    content: tuple[dict[str, Any], ...]  # redacted non-ZDR text, loaded to raw.raw_content
    dead_letters: tuple[dict[str, Any], ...]
    lines_read: int
    zdr_stripped: int
    cells_redacted: int


class ReconciliationError(RuntimeError):
    """Rows read != rows loaded + rows dead-lettered."""


def _safe_record_id(record: Any, id_field: str) -> str | None:
    """The record ID for a dead letter, only if it is a well-formed UUID (never raw content)."""
    value = record.get(id_field) if isinstance(record, dict) else None
    return value if isinstance(value, str) and UUID_RE.match(value) else None


def _dead_letter(
    spec: StreamSpec, line: SourceLine, record: Any, error: str, loaded_at: str
) -> dict[str, Any]:
    return {
        "stream": spec.name,
        "record_id": _safe_record_id(record, spec.id_field),
        "error_code": error,
        "source": line.source,
        "line_no": line.line_no,
        "_loaded_at": loaded_at,
    }


def _parse(text: str) -> tuple[Any, str | None]:
    try:
        return json.loads(text), None
    except json.JSONDecodeError:
        return None, "invalid_json"


def _redact_free_text(
    records: Sequence[dict[str, Any]], fields: Sequence[str], redactor: Redactor
) -> tuple[list[dict[str, Any]], int]:
    """Send every non-empty free-text cell to the redactor in one batch call."""
    cells = [(i, f) for i, r in enumerate(records) for f in fields if r.get(f)]
    if not cells:
        return list(records), 0
    redacted = redactor.redact([records[i][f] for i, f in cells])
    updates: dict[int, dict[str, str]] = {}
    for (i, f), text in zip(cells, redacted, strict=True):
        updates.setdefault(i, {})[f] = text
    return [{**r, **updates.get(i, {})} for i, r in enumerate(records)], len(cells)


def process_partition(
    spec: StreamSpec,
    lines: Iterable[SourceLine],
    redactor: Redactor,
    key: bytes,
    loaded_at: str,
) -> PartitionResult:
    valid: list[dict[str, Any]] = []
    dead: list[dict[str, Any]] = []
    read = zdr_stripped = 0
    for line in lines:
        read += 1
        raw, error = _parse(line.text)
        record = strip_zdr(raw, spec.free_text_fields)
        if record is not raw:
            zdr_stripped += 1
        error = error or validation_error(spec, record)
        if error:
            dead.append(_dead_letter(spec, line, record, error, loaded_at))
        else:
            valid.append(record)

    redacted, n_cells = _redact_free_text(valid, spec.free_text_fields, redactor)
    hashed = [
        {**pseudonymize_fields(r, spec.identifier_fields, key), "_loaded_at": loaded_at}
        for r in redacted
    ]
    clean, content = split_content(spec, hashed)
    if len(clean) + len(dead) != read:
        raise ReconciliationError(f"{spec.name}: read {read}, clean {len(clean)}, dead {len(dead)}")
    return PartitionResult(clean, content, tuple(dead), read, zdr_stripped, n_cells)


def split_content(
    spec: StreamSpec, records: Sequence[dict[str, Any]]
) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
    """Move free text out of the metadata rows into separate content rows.

    Text lives only in raw.raw_content (policy tags, masking, 30-day partition expiry), so
    the wide analytics tables never hold prompt or response text.
    """
    if not spec.free_text_fields:
        return tuple(records), ()
    meta = tuple({k: v for k, v in r.items() if k not in spec.free_text_fields} for r in records)
    content = tuple(
        {
            spec.id_field: r[spec.id_field],
            "event_ts": r["event_ts"],
            **{f: r.get(f) for f in spec.free_text_fields},
            "_loaded_at": r["_loaded_at"],
        }
        for r in records
        if any(r.get(f) for f in spec.free_text_fields)
    )
    return meta, content
