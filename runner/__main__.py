"""Container entrypoint dispatching `ingest` and `dbt` Cloud Run Job commands.

Contract with `orchestration/dags/tsumugi_daily.py`:
    ingest --date <YYYY-MM-DD>
    dbt <dbt args> --target prod --artifacts-uri gs://<bucket>/run_artifacts/dt=<YYYY-MM-DD>
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any

from ingest.gcp import generate_slice, run_gcp_slice
from ingest.pseudonymize import load_key_from_secret_manager
from ingest.redact import Redactor, RegexRedactor
from runner.dbt_wrapper import run_dbt, write_uri_bytes


def _env_first(env: Mapping[str, str], *names: str, default: str | None = None) -> str | None:
    for name in names:
        val = env.get(name, "").strip()
        if val:
            return val
    return default


def _build_redactor(name: str, project: str, region: str) -> Redactor:
    if name == "regex":
        return RegexRedactor()
    from ingest.dlp import DlpRedactor

    return DlpRedactor(project=project, location=region)


def run_ingest_cli(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] = os.environ,
    key_loader: Callable[[str, str], bytes] = load_key_from_secret_manager,
    redactor_factory: Callable[[str, str, str], Redactor] = _build_redactor,
    storage_client: Any | None = None,
    bq_client: Any | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        prog="runner ingest",
        description="Generate synthetic events for JST date(s), de-identify, and load BigQuery.",
    )
    parser.add_argument(
        "--date",
        required=True,
        type=date.fromisoformat,
        help="target JST date (YYYY-MM-DD)",
    )
    parser.add_argument("--days", type=int, default=1, help="number of JST days ending at --date")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--project", default=_env_first(env, "GCP_PROJECT_ID", "TSUMUGI_PROJECT_ID")
    )
    parser.add_argument(
        "--region",
        default=_env_first(env, "GCP_REGION", "TSUMUGI_REGION", default="asia-northeast1"),
    )
    parser.add_argument("--bucket", default=_env_first(env, "GCS_BUCKET", "TSUMUGI_BUCKET"))
    parser.add_argument("--dataset", default=_env_first(env, "BQ_RAW_DATASET", default="raw"))
    parser.add_argument(
        "--hmac-secret",
        default=_env_first(env, "HMAC_SECRET_ID", default="hmac-key"),
    )
    parser.add_argument("--redactor", choices=("dlp", "regex"), default="dlp")
    parser.add_argument("--stats-uri", help="optional gs:// or local URI for JSON summary stats")
    args = parser.parse_args(list(argv))

    if not args.project:
        parser.error("--project (or GCP_PROJECT_ID / TSUMUGI_PROJECT_ID) is required")
    if not args.bucket:
        parser.error("--bucket (or GCS_BUCKET / TSUMUGI_BUCKET) is required")

    key = key_loader(args.project, args.hmac_secret)
    redactor = redactor_factory(args.redactor, args.project, args.region)
    loaded_at = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    generated = generate_slice(args.date, days=args.days, seed=args.seed)
    report = {
        "date": args.date.isoformat(),
        "days": args.days,
        **run_gcp_slice(
            generated,
            project=args.project,
            region=args.region,
            bucket_name=args.bucket,
            dataset=args.dataset,
            redactor=redactor,
            redactor_name=args.redactor,
            key=key,
            loaded_at=loaded_at,
            storage_client=storage_client,
            bq_client=bq_client,
        ),
    }
    encoded = (json.dumps(report, indent=2) + "\n").encode("utf-8")
    if args.stats_uri:
        write_uri_bytes(args.stats_uri, encoded, storage_client=storage_client)
    sys.stdout.write(encoded.decode("utf-8"))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    if not args or args[0] in ("-h", "--help"):
        print("Usage: python -m runner <ingest|dbt> [args...]")
        return 0 if args else 2

    subcommand, rest = args[0], args[1:]
    if subcommand == "ingest":
        return run_ingest_cli(rest)
    if subcommand == "dbt":
        return run_dbt(rest)

    print(f"Unknown command {subcommand!r}; expected 'ingest' or 'dbt'", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
