"""CLI: python -m ingest --landing data/landing --out data/ingest [--redactor regex|dlp]"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from ingest.local import run_local
from ingest.pseudonymize import DEMO_KEY, load_key_from_env
from ingest.recall import measure_recall
from ingest.redact import Redactor, RegexRedactor
from ingest.specs import load_specs

logger = logging.getLogger("ingest")


def _redactor(name: str, project: str | None) -> Redactor:
    if name == "regex":
        return RegexRedactor()
    if not project:
        raise SystemExit("--project is required for --redactor dlp")
    from ingest.dlp import DlpRedactor

    return DlpRedactor(project=project)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run ingest locally.")
    parser.add_argument("--landing", type=Path, default=Path("data/landing"))
    parser.add_argument("--out", type=Path, default=Path("data/ingest"))
    parser.add_argument("--redactor", choices=("regex", "dlp"), default="regex")
    parser.add_argument("--project", help="GCP project (DLP only)")
    parser.add_argument(
        "--demo-key",
        action="store_true",
        help="use the fixed offline demo HMAC key instead of HMAC_KEY",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    if args.demo_key:
        logger.warning("Using the offline demo HMAC key: not for real data")
    key = DEMO_KEY if args.demo_key else load_key_from_env()
    loaded_at = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    stats = run_local(
        args.landing, args.out, load_specs(), _redactor(args.redactor, args.project), key, loaded_at
    )
    recall = measure_recall(args.landing / "_labels" / "pii.jsonl", args.out / "clean")

    report = {
        "redactor": args.redactor,
        "streams": {name: asdict(s) for name, s in stats.items()},
        "redaction_recall": {t: {**asdict(r), "recall": r.recall} for t, r in recall.items()},
    }
    (args.out / "_stats.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
