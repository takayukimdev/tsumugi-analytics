"""Measure redaction recall against the generator's ground-truth PII labels.

A labeled value counts as caught if it no longer appears in the clean output field. Values
in ZDR records are reported separately: they were removed by the ZDR strip, not by redaction.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TypeRecall:
    labeled: int
    zdr_removed: int
    dead_lettered: int
    leaked: int

    @property
    def recall(self) -> float | None:
        denominator = self.labeled - self.zdr_removed - self.dead_lettered
        return None if denominator == 0 else 1 - self.leaked / denominator


def _index_by_request_id(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    by_id: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_id[row["request_id"]].append(row)
    return by_id


def _iter_clean_rows(clean_dir: Path, stream: str) -> Iterable[dict[str, Any]]:
    for path in clean_dir.glob(f"{stream}/dt=*/*.jsonl"):
        for line in path.read_text("utf-8").splitlines():
            yield json.loads(line)


def measure_recall_rows(
    labels: Iterable[Mapping[str, str]],
    meta_rows: Iterable[Mapping[str, Any]],
    content_rows: Iterable[Mapping[str, Any]],
) -> dict[str, TypeRecall]:
    """Check ground-truth PII labels against raw_content (text) and inference_requests (ZDR)."""
    meta_by_id = _index_by_request_id(meta_rows)
    content_by_id = _index_by_request_id(content_rows)
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for label in labels:
        c = counts[label["pii_type"]]
        c["labeled"] += 1
        meta = meta_by_id.get(label["record_id"])
        content = content_by_id.get(label["record_id"], [])
        if not meta:
            c["dead_lettered"] += 1
        elif meta[0]["zdr"] is not False:
            c["zdr_removed"] += 1
        elif any(label["value"] in (r.get(label["field"]) or "") for r in content):
            c["leaked"] += 1
    return {
        t: TypeRecall(c["labeled"], c["zdr_removed"], c["dead_lettered"], c["leaked"])
        for t, c in sorted(counts.items())
    }


def measure_recall(labels_path: Path, clean_dir: Path) -> dict[str, TypeRecall]:
    """Labels are checked against raw_content (text) and inference_requests (ZDR flag)."""
    labels = (json.loads(line) for line in labels_path.read_text("utf-8").splitlines())
    return measure_recall_rows(
        labels,
        _iter_clean_rows(clean_dir, "inference_requests"),
        _iter_clean_rows(clean_dir, "raw_content"),
    )
