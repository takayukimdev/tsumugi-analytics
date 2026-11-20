"""Inject realistic delivery defects: duplicates and late arrivals.

Defects are labeled so the pipeline's detection rate can be reported.
"""

from __future__ import annotations

import random
from datetime import timedelta
from typing import Any

Record = dict[str, Any]

# Upper bound on lateness. dbt incremental lookback (3 days) must stay above this.
MAX_LATENESS = timedelta(days=2)
MIN_LATENESS = timedelta(hours=1)
MAX_DUPLICATE_DELAY_S = 600


def inject_defects(
    rng: random.Random, records: list[Record], *, stream: str, id_field: str, rate: float
) -> tuple[list[Record], list[dict[str, Any]]]:
    """Return records with ~`rate` of them defective (half duplicated, half late), plus labels.

    Inputs are not modified; late records are replaced by shifted copies.
    """
    if not records or rate == 0:
        return list(records), []
    n = max(1, round(len(records) * rate))
    picks = rng.sample(range(len(records)), n)
    dup_idx, late_idx = picks[: n // 2], picks[n // 2 :]

    late = {}
    for i in late_idx:
        lag = rng.uniform(MIN_LATENESS.total_seconds(), MAX_LATENESS.total_seconds())
        late[i] = {**records[i], "received_ts": records[i]["event_ts"] + timedelta(seconds=lag)}
    duplicates = [
        {
            **records[i],
            "received_ts": records[i]["received_ts"]
            + timedelta(seconds=rng.uniform(1, MAX_DUPLICATE_DELAY_S)),
        }
        for i in dup_idx
    ]
    out = [late.get(i, r) for i, r in enumerate(records)] + duplicates
    labels = [
        {"stream": stream, "record_id": records[i][id_field], "defect": "late_arrival"}
        for i in late_idx
    ]
    labels += [
        {"stream": stream, "record_id": records[i][id_field], "defect": "duplicate"}
        for i in dup_idx
    ]
    return out, labels
