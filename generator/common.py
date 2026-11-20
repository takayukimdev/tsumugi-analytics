"""Shared helpers: time zones, deterministic IDs, timestamp formatting."""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
SCHEMA_VERSION = 1
APP_VERSION = "1.4.2"


def new_uuid(rng: random.Random) -> str:
    """UUIDv4 drawn from the seeded RNG, so output is reproducible."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def new_id(rng: random.Random, prefix: str) -> str:
    """Short prefixed entity ID, e.g. org_3f9a1c2b7d01."""
    return f"{prefix}_{rng.getrandbits(48):012x}"


def to_iso(ts: datetime) -> str:
    """RFC 3339 UTC with millisecond precision, e.g. 2026-11-01T03:04:05.678Z."""
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
