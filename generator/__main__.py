"""CLI: python -m generator --days 28 --seed 42 --out data/landing"""

from __future__ import annotations

import argparse
import random
import sys
from datetime import date, datetime
from pathlib import Path

from generator.common import JST
from generator.config import GeneratorConfig
from generator.defects import inject_defects
from generator.simulate import simulate
from generator.writer import write_landing


def generate(cfg: GeneratorConfig, out_dir: Path) -> dict[str, int]:
    """Simulate, inject defects, and write landing files. Returns counts per output."""
    result = simulate(cfg)
    # A separate RNG stream keeps defects stable even if the simulation changes shape.
    rng = random.Random(f"{cfg.seed}-defects")
    app, app_defects = inject_defects(
        rng, list(result.app_events), stream="app_events", id_field="event_id", rate=cfg.defect_rate
    )
    inf, inf_defects = inject_defects(
        rng,
        list(result.inference_requests),
        stream="inference_requests",
        id_field="request_id",
        rate=cfg.defect_rate,
    )
    counts = write_landing(
        out_dir,
        streams={"app_events": app, "inference_requests": inf},
        labels={"pii": result.pii_labels, "defects": app_defects + inf_defects},
    )
    return {
        **counts,
        "pii_labels": len(result.pii_labels),
        "defects": len(app_defects) + len(inf_defects),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic Tsumugi data.")
    parser.add_argument("--days", type=int, default=28)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=datetime.now(JST).date(),
        help="last day (JST), default today",
    )
    parser.add_argument("--out", type=Path, default=Path("data/landing"))
    args = parser.parse_args(argv)

    cfg = GeneratorConfig(end_date=args.end_date, days=args.days, seed=args.seed)
    counts = generate(cfg, args.out)
    print(f"{cfg.start_date}..{cfg.end_date} seed={cfg.seed} -> {args.out}")
    for name, n in counts.items():
        print(f"  {name:<20} {n:>9,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
