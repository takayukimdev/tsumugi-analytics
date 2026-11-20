"""Local filesystem runner: landing JSONL -> clean/ (incl. clean/raw_content) and dead_letter/.

The Cloud Run Job uses the same `process_partition`; only the I/O differs.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ingest.pipeline import CONTENT_STREAM, PartitionResult, SourceLine, process_partition
from ingest.redact import Redactor
from ingest.specs import StreamSpec

OUTPUT_KINDS = ("clean", "dead_letter")
# Only files the generator / collector wrote. Anything else (e.g. "part-00000 2.jsonl" sync
# conflict copies) is skipped loudly instead of being double-counted.
PART_FILE_RE = re.compile(r"^part-\d{5}\.jsonl$")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamStats:
    lines_read: int = 0
    clean: int = 0
    content_rows: int = 0
    dead_letters: int = 0
    zdr_stripped: int = 0
    cells_redacted: int = 0
    error_codes: Mapping[str, int] = field(default_factory=dict)

    def add(self, result: PartitionResult) -> StreamStats:
        codes = dict(self.error_codes)
        for d in result.dead_letters:
            codes[d["error_code"]] = codes.get(d["error_code"], 0) + 1
        return StreamStats(
            lines_read=self.lines_read + result.lines_read,
            clean=self.clean + len(result.clean),
            content_rows=self.content_rows + len(result.content),
            dead_letters=self.dead_letters + len(result.dead_letters),
            zdr_stripped=self.zdr_stripped + result.zdr_stripped,
            cells_redacted=self.cells_redacted + result.cells_redacted,
            error_codes=codes,
        )


def iter_partitions(landing: Path, stream: str) -> Iterator[tuple[str, list[SourceLine]]]:
    for part_dir in sorted((landing / stream).glob("dt=*")):
        files = sorted(p for p in part_dir.iterdir() if p.is_file())
        unexpected = [p for p in files if not PART_FILE_RE.match(p.name)]
        if unexpected:
            logger.warning(
                "%s/%s: skipping %d unexpected file(s)", stream, part_dir.name, len(unexpected)
            )
        lines = [
            SourceLine(str(path.relative_to(landing)), n, text)
            for path in files
            if PART_FILE_RE.match(path.name)
            for n, text in enumerate(path.read_text("utf-8").splitlines(), start=1)
        ]
        yield part_dir.name, lines


def _write(path: Path, rows: tuple[dict[str, Any], ...]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def run_local(
    landing: Path,
    out: Path,
    specs: Mapping[str, StreamSpec],
    redactor: Redactor,
    key: bytes,
    loaded_at: str,
) -> dict[str, StreamStats]:
    for kind in OUTPUT_KINDS:
        shutil.rmtree(out / kind, ignore_errors=True)
    stats: dict[str, StreamStats] = {}
    for name, spec in specs.items():
        stream_stats = StreamStats()
        for dt, lines in iter_partitions(landing, name):
            result = process_partition(spec, lines, redactor, key, loaded_at)
            _write(out / "clean" / name / dt / "part-00000.jsonl", result.clean)
            _write(out / "clean" / CONTENT_STREAM / dt / "part-00000.jsonl", result.content)
            _write(out / "dead_letter" / name / dt / "part-00000.jsonl", result.dead_letters)
            stream_stats = stream_stats.add(result)
        stats[name] = stream_stats
    return stats
