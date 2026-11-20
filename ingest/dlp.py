"""Sensitive Data Protection (DLP) redactor for the GCP path.

Texts are sent as rows of a one-column table, batched under the content API limits
(0.5 MB and 50,000 cells per request), with exponential backoff on quota errors (429).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from typing import Any

logger = logging.getLogger(__name__)

MAX_REQUEST_BYTES = 450_000  # headroom under the 0.5 MB limit for protobuf overhead
MAX_CELLS = 50_000
PER_CELL_OVERHEAD_BYTES = 16
MAX_ATTEMPTS = 6
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 32.0

INFO_TYPES = (
    "PERSON_NAME",
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "STREET_ADDRESS",
    "JAPAN_INDIVIDUAL_NUMBER",
    "JAPAN_BANK_ACCOUNT",
    "JAPAN_PASSPORT",
    "JAPAN_DRIVERS_LICENSE_NUMBER",
)
CUSTOM_INFO_TYPES = (
    {"info_type": {"name": "JP_PHONE_NUMBER"}, "regex": {"pattern": r"0\d{1,4}-\d{1,4}-\d{3,4}"}},
    {"info_type": {"name": "JP_POSTAL_CODE"}, "regex": {"pattern": r"〒?\s?\d{3}-\d{4}"}},
)
INSPECT_CONFIG = {
    "info_types": [{"name": n} for n in INFO_TYPES],
    "custom_info_types": list(CUSTOM_INFO_TYPES),
    "min_likelihood": "POSSIBLE",
}
DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [{"primitive_transformation": {"replace_with_info_type_config": {}}}]
    }
}


def plan_batches(
    texts: Sequence[str], max_bytes: int = MAX_REQUEST_BYTES, max_cells: int = MAX_CELLS
) -> list[range]:
    """Split text indexes into consecutive batches that respect both request limits."""
    batches, start, size = [], 0, 0
    for i, text in enumerate(texts):
        n = len(text.encode("utf-8")) + PER_CELL_OVERHEAD_BYTES
        if i > start and (size + n > max_bytes or i - start >= max_cells):
            batches.append(range(start, i))
            start, size = i, 0
        size += n
    if start < len(texts):
        batches.append(range(start, len(texts)))
    return batches


def _is_quota_error(exc: Exception) -> bool:
    from google.api_core import exceptions

    return isinstance(exc, exceptions.ResourceExhausted | exceptions.ServiceUnavailable)


class DlpRedactor:
    def __init__(
        self,
        project: str,
        location: str = "asia-northeast1",
        client: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if client is None:
            from google.cloud import dlp_v2

            client = dlp_v2.DlpServiceClient()
        self._client = client
        self._parent = f"projects/{project}/locations/{location}"
        self._sleep = sleep

    def redact(self, texts: Sequence[str]) -> list[str]:
        out: list[str] = []
        for batch in plan_batches(texts):
            out.extend(self._deidentify([texts[i] for i in batch]))
        return out

    def _deidentify(self, chunk: list[str]) -> list[str]:
        request = {
            "parent": self._parent,
            "inspect_config": INSPECT_CONFIG,
            "deidentify_config": DEIDENTIFY_CONFIG,
            "item": {
                "table": {
                    "headers": [{"name": "text"}],
                    "rows": [{"values": [{"string_value": t}]} for t in chunk],
                }
            },
        }
        response = self._call_with_backoff(request)
        rows = response.item.table.rows
        if len(rows) != len(chunk):
            raise RuntimeError(f"DLP returned {len(rows)} rows for {len(chunk)} inputs")
        return [row.values[0].string_value for row in rows]

    def _call_with_backoff(self, request: dict[str, Any]) -> Any:
        delay = INITIAL_BACKOFF_S
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return self._client.deidentify_content(request=request)
            except Exception as exc:
                if not _is_quota_error(exc) or attempt == MAX_ATTEMPTS:
                    raise
                # Log only the exception type: DLP errors must never echo request content.
                logger.warning("DLP %s; retry %d in %.1fs", type(exc).__name__, attempt, delay)
                self._sleep(delay)
                delay = min(delay * 2, MAX_BACKOFF_S)
        raise AssertionError("unreachable")
