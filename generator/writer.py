"""Write records as JSONL, partitioned by the JST date they were received."""

from __future__ import annotations

import json
import shutil
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from generator.common import JST, to_iso

LABELS_DIR = "_labels"


def _default(value: Any) -> str:
    if isinstance(value, datetime):
        return to_iso(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _dumps(record: Mapping[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, default=_default)


def _write_lines(path: Path, lines: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def _sort_key(record: Mapping[str, Any]) -> tuple[datetime, str]:
    return record["received_ts"], record.get("event_id") or record["request_id"]


def write_landing(
    out_dir: Path,
    streams: Mapping[str, Sequence[Mapping[str, Any]]],
    labels: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, int]:
    """Replace `out_dir/<stream>/dt=YYYY-MM-DD/part-00000.jsonl` and `out_dir/_labels/*.jsonl`.

    Returns the number of records written per stream.
    """
    for name in [*streams, LABELS_DIR]:
        shutil.rmtree(out_dir / name, ignore_errors=True)

    counts = {}
    for stream, records in streams.items():
        by_date: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for record in sorted(records, key=_sort_key):
            by_date[record["received_ts"].astimezone(JST).date().isoformat()].append(record)
        for dt, rows in by_date.items():
            _write_lines(out_dir / stream / f"dt={dt}" / "part-00000.jsonl", map(_dumps, rows))
        counts[stream] = len(records)

    for name, rows in labels.items():
        _write_lines(out_dir / LABELS_DIR / f"{name}.jsonl", map(_dumps, rows))
    return counts
