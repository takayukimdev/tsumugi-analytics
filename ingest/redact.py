"""PII redaction backends.

`RegexRedactor` is the offline stand-in for Sensitive Data Protection. It catches structured
identifiers (email, phone, postal code, street address) but NOT person names: measuring that
gap is part of why the GCP path uses DLP.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Protocol


class Redactor(Protocol):
    def redact(self, texts: Sequence[str]) -> list[str]:
        """Return redacted texts, same length and order as the input."""
        ...


# Order matters: addresses before phones, phones before postal codes.
REGEX_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("EMAIL_ADDRESS", re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")),
    (
        "STREET_ADDRESS",
        re.compile(
            r"(?:東京都|北海道|(?:京都|大阪)府|\S{2,3}県)\S{1,8}?[市区町村]\d+(?:-\d+){1,2}"
        ),
    ),
    ("STREET_ADDRESS", re.compile(r"\b\d+-\d+-\d+\s+[A-Z][\w-]*(?:,\s*[A-Z]\w+)*")),
    ("PHONE_NUMBER", re.compile(r"(?<!\d)0\d{1,4}-\d{1,4}-\d{3,4}(?!\d)")),
    ("JP_POSTAL_CODE", re.compile(r"〒?\s?(?<!\d)\d{3}-\d{4}(?!\d)")),
)


class RegexRedactor:
    def redact(self, texts: Sequence[str]) -> list[str]:
        return [self._redact_one(t) for t in texts]

    @staticmethod
    def _redact_one(text: str) -> str:
        for info_type, pattern in REGEX_PATTERNS:
            text = pattern.sub(f"[{info_type}]", text)
        return text
