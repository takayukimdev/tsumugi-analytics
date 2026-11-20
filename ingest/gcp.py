"""Cloud Run ingest path: generate -> in-memory ZDR/validate/redact/HMAC -> GCS -> BigQuery.

Raw text and raw identifiers never leave memory: only clean metadata, redacted non-ZDR content,
and payload-free dead letters are uploaded to GCS and loaded into BigQuery via load jobs.
"""

from __future__ import annotations

import json
import logging
import random
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

from generator.common import JST
from generator.config import GeneratorConfig
from generator.defects import inject_defects
from generator.simulate import simulate
from generator.writer import _dumps, _sort_key
from ingest.local import StreamStats
from ingest.pipeline import (
    CONTENT_STREAM,
    ReconciliationError,
    SourceLine,
    process_partition,
)
from ingest.recall import TypeRecall, measure_recall_rows
from ingest.redact import Redactor
from ingest.specs import StreamSpec, load_specs

logger = logging.getLogger(__name__)

# Fixed simulation anchor so daily incremental runs (`--date YYYY-MM-DD`) and multi-day
# backfills share the exact same deterministic cast of orgs, users, and API keys.
ANCHOR_START = date(2026, 10, 22)
ANCHOR_END = date(2026, 12, 5)


@dataclass(frozen=True)
class GeneratedSlice:
    partitions: Mapping[str, Mapping[str, tuple[SourceLine, ...]]]
    pii_labels: tuple[dict[str, str], ...]


def _received_jst_date(record: Mapping[str, Any]) -> date:
    return record["received_ts"].astimezone(JST).date()


def _to_partitions(
    stream: str, records: Sequence[Mapping[str, Any]]
) -> dict[str, tuple[SourceLine, ...]]:
    by_date: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in sorted(records, key=_sort_key):
        by_date[_received_jst_date(record).isoformat()].append(record)
    return {
        dt: tuple(
            SourceLine(f"{stream}/dt={dt}/part-00000.jsonl", n, _dumps(row))
            for n, row in enumerate(rows, start=1)
        )
        for dt, rows in sorted(by_date.items())
    }


def generate_slice(
    target_date: date,
    *,
    days: int = 1,
    seed: int = 42,
    anchor_start: date = ANCHOR_START,
    anchor_end: date = ANCHOR_END,
    n_orgs: int = 60,
    n_users: int = 3000,
) -> GeneratedSlice:
    """Simulate deterministically and select `[target_date - days + 1 .. target_date]` (JST).

    Prior `org_changed` events (`received_ts` before `window_start`) are included in `app_events`
    so `dim_orgs` has complete SCD2 history even when a single day is ingested into an empty
    `raw` dataset. `int_app_events_deduped` deduplicates re-delivered `event_id`s.
    """
    if days < 1:
        raise ValueError("days must be >= 1")
    window_start = target_date - timedelta(days=days - 1)
    sim_start = min(anchor_start, window_start)
    sim_end = max(anchor_end, target_date)
    cfg = GeneratorConfig(
        end_date=sim_end,
        days=(sim_end - sim_start).days + 1,
        seed=seed,
        n_orgs=n_orgs,
        n_users=n_users,
    )

    result = simulate(cfg)
    rng = random.Random(f"{cfg.seed}-defects")
    app_all, _ = inject_defects(
        rng, list(result.app_events), stream="app_events", id_field="event_id", rate=cfg.defect_rate
    )
    inf_all, _ = inject_defects(
        rng,
        list(result.inference_requests),
        stream="inference_requests",
        id_field="request_id",
        rate=cfg.defect_rate,
    )

    app_selected = [
        r
        for r in app_all
        if window_start <= _received_jst_date(r) <= target_date
        or (r["event_name"] == "org_changed" and _received_jst_date(r) < window_start)
    ]
    inf_selected = [r for r in inf_all if window_start <= _received_jst_date(r) <= target_date]
    inf_ids = {r["request_id"] for r in inf_selected}
    labels = tuple(lbl for lbl in result.pii_labels if lbl["record_id"] in inf_ids)

    return GeneratedSlice(
        partitions={
            "app_events": _to_partitions("app_events", app_selected),
            "inference_requests": _to_partitions("inference_requests", inf_selected),
        },
        pii_labels=labels,
    )


def _ndjson_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")


def _upload_ndjson(bucket_obj: Any, object_name: str, rows: Sequence[Mapping[str, Any]]) -> str:
    blob = bucket_obj.blob(object_name)
    blob.upload_from_string(_ndjson_bytes(rows), content_type="application/x-ndjson")
    return f"gs://{bucket_obj.name}/{object_name}"


def _load_uris_to_bq(
    bq_client: Any,
    uris: Sequence[str],
    destination: str,
    region: str,
) -> int:
    if not uris:
        return 0
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
    )
    job = bq_client.load_table_from_uri(
        list(uris), destination, job_config=job_config, location=region
    )
    job.result()
    return int(job.output_rows or 0)


def run_gcp_slice(
    generated: GeneratedSlice,
    *,
    project: str,
    region: str,
    bucket_name: str,
    dataset: str,
    redactor: Redactor,
    redactor_name: str,
    key: bytes,
    loaded_at: str,
    storage_client: Any | None = None,
    bq_client: Any | None = None,
    specs: Mapping[str, StreamSpec] | None = None,
) -> dict[str, Any]:
    """Process a generated slice in memory, write GCS NDJSON, and load into BigQuery."""
    if storage_client is None:
        from google.cloud import storage

        storage_client = storage.Client(project=project)
    if bq_client is None:
        from google.cloud import bigquery

        bq_client = bigquery.Client(project=project, location=region)
    specs = specs or load_specs()
    bucket_obj = storage_client.bucket(bucket_name)

    stats: dict[str, StreamStats] = {}
    clean_uris: dict[str, list[str]] = defaultdict(list)
    inf_meta_rows: list[dict[str, Any]] = []
    content_rows: list[dict[str, Any]] = []

    for name, spec in specs.items():
        stream_stats = StreamStats()
        for dt, lines in generated.partitions.get(name, {}).items():
            result = process_partition(spec, lines, redactor, key, loaded_at)
            if result.clean:
                uri = _upload_ndjson(
                    bucket_obj, f"clean/{name}/dt={dt}/part-00000.jsonl", result.clean
                )
                clean_uris[name].append(uri)
            if result.content:
                uri = _upload_ndjson(
                    bucket_obj, f"clean/{CONTENT_STREAM}/dt={dt}/part-00000.jsonl", result.content
                )
                clean_uris[CONTENT_STREAM].append(uri)
            if result.dead_letters:
                _upload_ndjson(
                    bucket_obj, f"dead_letter/{name}/dt={dt}/part-00000.jsonl", result.dead_letters
                )
            if name == "inference_requests":
                inf_meta_rows.extend(result.clean)
                content_rows.extend(result.content)
            stream_stats = stream_stats.add(result)
        stats[name] = stream_stats

    bq_loaded: dict[str, int] = {}
    for table in (*specs.keys(), CONTENT_STREAM):
        destination = f"{project}.{dataset}.{table}"
        bq_loaded[table] = _load_uris_to_bq(bq_client, clean_uris[table], destination, region)

    for name, s in stats.items():
        if bq_loaded[name] + s.dead_letters != s.lines_read:
            raise ReconciliationError(
                f"{name}: generated {s.lines_read} != "
                f"loaded {bq_loaded[name]} + dead {s.dead_letters}"
            )
    expected_content = stats["inference_requests"].content_rows
    if bq_loaded[CONTENT_STREAM] != expected_content:
        raise ReconciliationError(
            f"{CONTENT_STREAM}: expected {expected_content} rows, "
            f"loaded {bq_loaded[CONTENT_STREAM]}"
        )

    recall: dict[str, TypeRecall] = measure_recall_rows(
        generated.pii_labels, inf_meta_rows, content_rows
    )
    return {
        "redactor": redactor_name,
        "loaded_at": loaded_at,
        "streams": {name: asdict(s) for name, s in stats.items()},
        "bq_loaded_rows": bq_loaded,
        "redaction_recall": {t: {**asdict(r), "recall": r.recall} for t, r in recall.items()},
    }
