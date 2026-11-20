"""Wrapper around the dbt CLI that always writes per-step NDJSON run stats.

BigQuery cannot load dbt's `run_results.json` or `sources.json` directly (each is a single nested
JSON document). This wrapper flattens node results into one NDJSON row per node—or one
`invocation_error` row when dbt crashes before producing an artifact—and writes `<step>.ndjson`
to `--artifacts-uri` even when dbt exits non-zero. Exception messages are deliberately omitted.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from generator.common import JST

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DT_PARTITION_RE = re.compile(r"(?:^|/)dt=(\d{4}-\d{2}-\d{2})(?:/|$)")
SAFE_STEP_RE = re.compile(r"[^a-z0-9_]+")

SubprocessRunner = Callable[[Sequence[str]], int]


def _utc_iso_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def extract_artifacts_uri(argv: Sequence[str]) -> tuple[list[str], str | None]:
    """Strip `--artifacts-uri <uri>` (or `--artifacts-uri=<uri>`) and return remaining dbt args."""
    dbt_args: list[str] = []
    uri: str | None = None
    it = iter(argv)
    for token in it:
        if token == "--artifacts-uri":
            try:
                uri = next(it)
            except StopIteration as exc:
                raise SystemExit("--artifacts-uri requires a value") from exc
        elif token.startswith("--artifacts-uri="):
            uri = token.split("=", 1)[1]
        else:
            dbt_args.append(token)
    if uri is not None and not uri.strip():
        raise SystemExit("--artifacts-uri must not be empty")
    return dbt_args, uri


def step_name(dbt_args: Sequence[str]) -> str:
    """Derive a clean filename stem (`build`, `source_freshness`, ...) from dbt subcommands."""
    parts: list[str] = []
    for token in dbt_args:
        if token.startswith("-"):
            break
        parts.append(token.lower())
    raw = "_".join(parts) if parts else "dbt"
    cleaned = SAFE_STEP_RE.sub("_", raw).strip("_")
    return cleaned or "dbt"


def target_date_from_uri(uri: str | None, fallback: str | None = None) -> str:
    if uri:
        match = DT_PARTITION_RE.search(uri)
        if match:
            return match.group(1)
    return fallback or datetime.now(JST).date().isoformat()


def _opt_int(value: Any) -> int | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _opt_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _opt_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _resource_type(unique_id: str) -> str:
    return unique_id.split(".", 1)[0] if "." in unique_id else "unknown"


def _invocation_row(
    *,
    target_date: str,
    step: str,
    status: str,
    loaded_at: str,
    invocation_id: str | None = None,
    executed_at: str | None = None,
) -> dict[str, Any]:
    return {
        "target_date": target_date,
        "step": step,
        "invocation_id": invocation_id or str(uuid.uuid4()),
        "unique_id": f"invocation.{step}",
        "resource_type": "invocation",
        "status": status,
        "execution_time_s": None,
        "failures": None,
        "job_id": None,
        "bytes_processed": None,
        "bytes_billed": None,
        "slot_ms": None,
        "max_loaded_at": None,
        "age_s": None,
        "executed_at": executed_at or loaded_at,
        "_loaded_at": loaded_at,
    }


def artifact_to_rows(
    artifact: Mapping[str, Any] | None,
    *,
    target_date: str,
    step: str,
    exit_code: int,
    loaded_at: str,
) -> list[dict[str, Any]]:
    """Flatten `run_results.json` or `sources.json` into schema-compliant NDJSON rows."""
    if not artifact:
        status = "no_op" if exit_code == 0 else "invocation_error"
        return [
            _invocation_row(target_date=target_date, step=step, status=status, loaded_at=loaded_at)
        ]

    meta = artifact.get("metadata") or {}
    invocation_id = _opt_str(meta.get("invocation_id")) or str(uuid.uuid4())
    executed_at = _opt_str(meta.get("generated_at")) or loaded_at
    results = artifact.get("results") or []
    if not results:
        status = "no_op" if exit_code == 0 else "invocation_error"
        return [
            _invocation_row(
                target_date=target_date,
                step=step,
                status=status,
                loaded_at=loaded_at,
                invocation_id=invocation_id,
                executed_at=executed_at,
            )
        ]

    rows: list[dict[str, Any]] = []
    for item in results:
        unique_id = _opt_str(item.get("unique_id")) or f"unknown.{step}"
        adapter = item.get("adapter_response") or {}
        rows.append(
            {
                "target_date": target_date,
                "step": step,
                "invocation_id": invocation_id,
                "unique_id": unique_id,
                "resource_type": _resource_type(unique_id),
                "status": _opt_str(item.get("status")) or ("error" if exit_code else "unknown"),
                "execution_time_s": _opt_float(item.get("execution_time")),
                "failures": _opt_int(item.get("failures")),
                "job_id": _opt_str(adapter.get("job_id")),
                "bytes_processed": _opt_int(adapter.get("bytes_processed")),
                "bytes_billed": _opt_int(adapter.get("bytes_billed")),
                "slot_ms": _opt_int(adapter.get("slot_ms")),
                "max_loaded_at": _opt_str(item.get("max_loaded_at")),
                "age_s": _opt_float(item.get("age")),
                "executed_at": executed_at,
                "_loaded_at": loaded_at,
            }
        )
    return rows


def _read_dbt_artifact(target_dir: Path, step: str) -> dict[str, Any] | None:
    candidates = (
        ("sources.json", "run_results.json")
        if step.startswith("source")
        else ("run_results.json", "sources.json")
    )
    for filename in candidates:
        path = target_dir / filename
        if path.is_file():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.error("Malformed dbt artifact %s; recording invocation_error", filename)
                return None
    return None


def write_uri_bytes(uri: str, payload: bytes, *, storage_client: Any | None = None) -> None:
    """Write bytes to either a `gs://bucket/object` URI or a local filesystem path."""
    if uri.startswith("gs://"):
        without_scheme = uri.removeprefix("gs://")
        bucket_name, sep, object_name = without_scheme.partition("/")
        if not bucket_name or not sep or not object_name:
            raise ValueError(f"Invalid GCS URI: {uri!r}")
        if storage_client is None:
            from google.cloud import storage

            storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(object_name)
        blob.upload_from_string(payload, content_type="application/x-ndjson")
        return

    path = Path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _dbt_executable() -> str:
    sibling = Path(sys.executable).with_name("dbt")
    if sibling.is_file():
        return str(sibling)
    found = shutil.which("dbt")
    if found:
        return found
    raise FileNotFoundError("dbt executable not found in virtualenv or PATH")


def _default_subprocess_runner(cmd: Sequence[str]) -> int:
    return subprocess.run(list(cmd), check=False).returncode


def _has_flag(args: Sequence[str], flag: str) -> bool:
    return any(a == flag or a.startswith(f"{flag}=") for a in args)


def run_dbt(
    argv: Sequence[str],
    *,
    project_dir: Path = DEFAULT_DBT_PROJECT_DIR,
    profiles_dir: Path | None = None,
    runner_fn: SubprocessRunner | None = None,
    storage_client: Any | None = None,
    loaded_at: str | None = None,
) -> int:
    """Invoke dbt and always emit `<step>.ndjson` to `--artifacts-uri` when configured."""
    dbt_args, artifacts_uri = extract_artifacts_uri(argv)
    step = step_name(dbt_args)
    target_date = target_date_from_uri(artifacts_uri)
    run_cmd = runner_fn or _default_subprocess_runner

    resolved_profiles = profiles_dir or Path(os.environ.get("DBT_PROFILES_DIR", str(project_dir)))

    with tempfile.TemporaryDirectory(prefix="tsumugi-dbt-") as tmp:
        tmp_path = Path(tmp)
        target_dir = tmp_path / "target"
        log_dir = tmp_path / "logs"
        target_dir.mkdir()
        log_dir.mkdir()

        cmd = [_dbt_executable(), *dbt_args]
        if not _has_flag(dbt_args, "--project-dir"):
            cmd.extend(["--project-dir", str(project_dir)])
        if not _has_flag(dbt_args, "--profiles-dir"):
            cmd.extend(["--profiles-dir", str(resolved_profiles)])
        if not _has_flag(dbt_args, "--target-path"):
            cmd.extend(["--target-path", str(target_dir)])
        if not _has_flag(dbt_args, "--log-path"):
            cmd.extend(["--log-path", str(log_dir)])

        exit_code = 1
        try:
            exit_code = run_cmd(cmd)
        finally:
            if artifacts_uri:
                stamp = loaded_at or _utc_iso_now()
                artifact = _read_dbt_artifact(target_dir, step)
                rows = artifact_to_rows(
                    artifact,
                    target_date=target_date,
                    step=step,
                    exit_code=exit_code,
                    loaded_at=stamp,
                )
                payload = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode(
                    "utf-8"
                )
                dest = f"{artifacts_uri.rstrip('/')}/{step}.ndjson"
                write_uri_bytes(dest, payload, storage_client=storage_client)
                logger.info("Wrote %d run-result row(s) to %s", len(rows), dest)
        return exit_code
