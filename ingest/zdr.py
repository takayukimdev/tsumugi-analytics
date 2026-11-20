"""Zero Data Retention: drop free text before anything else touches the record."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def strip_zdr(record: Any, free_text_fields: Iterable[str]) -> Any:
    """Return a copy with free-text fields nulled unless the record is explicitly non-ZDR.

    Fail closed: anything other than `zdr is False` (missing flag, "false" string, malformed
    record) is treated as ZDR, so a bad producer can never leak text past this step.
    """
    if not isinstance(record, dict) or record.get("zdr") is False:
        return record
    present = [f for f in free_text_fields if record.get(f) is not None]
    if not present:
        return record  # nothing to strip; returning the same object means "unchanged"
    return {**record, **dict.fromkeys(present)}
