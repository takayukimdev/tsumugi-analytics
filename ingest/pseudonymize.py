"""Keyed pseudonymization (仮名加工) with HMAC-SHA256.

Plain SHA-256 of an ID is reversible by enumeration; a secret key prevents that. The key is
削除情報等 under APPI and lives in Secret Manager, separate from the analytics data.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Iterable, Mapping
from typing import Any

MIN_KEY_BYTES = 32
PREFIX = "h_"
# Fixed key for `make demo-offline` ONLY (`--demo-key`). GCP runs read Secret Manager.
DEMO_KEY = b"tsumugi-offline-demo-key-not-secret!"


class HmacKeyError(ValueError):
    """The HMAC key is missing or too short."""


def _check(key: bytes) -> bytes:
    if len(key) < MIN_KEY_BYTES:
        raise HmacKeyError(f"HMAC key must be at least {MIN_KEY_BYTES} bytes")
    return key


def load_key_from_env(env: Mapping[str, str] = os.environ) -> bytes:
    raw = env.get("HMAC_KEY", "").strip()
    if not raw:
        raise HmacKeyError("HMAC_KEY is not set (see .env.example)")
    try:
        key = bytes.fromhex(raw)
    except ValueError as exc:
        raise HmacKeyError("HMAC_KEY must be hex-encoded") from exc
    return _check(key)


def load_key_from_secret_manager(project: str, secret: str = "hmac-key") -> bytes:
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret}/versions/latest"
    payload = client.access_secret_version(request={"name": name}).payload.data
    return _check(bytes.fromhex(payload.decode("ascii").strip()))


def pseudonymize(key: bytes, value: str) -> str:
    return PREFIX + hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def pseudonymize_fields(record: Mapping[str, Any], fields: Iterable[str], key: bytes) -> dict:
    """Copy of `record` with each non-null identifier field replaced by its HMAC."""
    return {
        **record,
        **{f: pseudonymize(key, record[f]) for f in fields if record.get(f) is not None},
    }
