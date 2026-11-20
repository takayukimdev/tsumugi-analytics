"""Builders for single records: app events and inference requests."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from generator.common import APP_VERSION, SCHEMA_VERSION, new_uuid
from generator.entities import Org, User
from generator.text import make_prompt, make_response

STATUSES = ("success", "error", "cancelled")
STATUS_WEIGHTS = (0.965, 0.025, 0.01)
ERROR_CODES = ("rate_limited", "internal_error", "timeout", "context_length_exceeded")
ERROR_WEIGHTS = (0.4, 0.3, 0.2, 0.1)
FINISH_REASONS = ("stop", "length", "content_filter")
FINISH_WEIGHTS = (0.9, 0.08, 0.02)
# Probability of picking tsumugi-mini, by plan. API traffic skews toward the larger model.
MINI_SHARE = {"free": 0.9, "pro": 0.6, "enterprise": 0.4}
API_PRO_SHIFT = 0.1
# Latency profile per model: (median TTFT ms, ms per output token, median output tokens)
MODEL_PROFILE = {"tsumugi-mini": (300, 8, 250), "tsumugi-pro": (700, 18, 450)}
ECHO_PII_RATE = 0.3
CACHE_HIT_RATE = 0.4
TOKENS_PER_CHAR = {"ja": 0.9, "en": 0.25}


@dataclass(frozen=True)
class RequestContext:
    ts: datetime
    org: Org
    user: User
    channel: str
    api_key_id: str | None = None
    conversation_id: str | None = None
    message_id: str | None = None


def app_event(
    rng: random.Random,
    *,
    name: str,
    ts: datetime,
    user: User,
    session_id: str | None,
    platform: str,
    properties: dict[str, Any],
) -> dict[str, Any]:
    return {
        "event_id": new_uuid(rng),
        "event_name": name,
        "event_ts": ts,
        "received_ts": ts + timedelta(seconds=rng.uniform(0.2, 5.0)),
        "org_id": user.org_id,
        "user_id": user.user_id,
        "session_id": session_id,
        "platform": platform,
        "app_version": APP_VERSION,
        "schema_version": SCHEMA_VERSION,
        "properties": properties,
    }


def _choose_model(rng: random.Random, plan: str, channel: str) -> str:
    mini_share = MINI_SHARE[plan] - (API_PRO_SHIFT if channel == "api" else 0.0)
    return "tsumugi-mini" if rng.random() < mini_share else "tsumugi-pro"


def _input_tokens(rng: random.Random, prompt: str, lang: str, channel: str) -> int:
    context = rng.randint(100, 6000) if channel == "api" else rng.randint(50, 2000)
    return context + int(len(prompt) * TOKENS_PER_CHAR[lang])


def _outcome(rng: random.Random, model: str, status: str) -> dict[str, Any]:
    """Token, latency, and finish fields for one request outcome."""
    median_ttft, ms_per_token, median_output = MODEL_PROFILE[model]
    ttft = int(median_ttft * rng.lognormvariate(0, 0.4))
    if status == "error":
        return {
            "output_tokens": 0,
            "ttft_ms": None,
            "latency_ms": int(ttft * rng.uniform(0.5, 3)),
            "error_code": rng.choices(ERROR_CODES, ERROR_WEIGHTS)[0],
            "finish_reason": None,
        }
    output = max(1, int(median_output * rng.lognormvariate(0, 0.6)))
    if status == "cancelled":
        output = max(1, output // rng.randint(2, 5))
    finish = rng.choices(FINISH_REASONS, FINISH_WEIGHTS)[0] if status == "success" else None
    return {
        "output_tokens": output,
        "ttft_ms": ttft,
        "latency_ms": ttft + output * ms_per_token,
        "error_code": None,
        "finish_reason": finish,
    }


def _response_text(
    rng: random.Random, lang: str, status: str, prompt_pii: list[tuple[str, str]]
) -> tuple[str | None, list[tuple[str, str]]]:
    if status == "error":
        return None, []
    echo = (
        prompt_pii[0]
        if prompt_pii and status == "success" and rng.random() < ECHO_PII_RATE
        else None
    )
    text, items = make_response(rng, lang, echo)
    if status == "cancelled":
        return text[: max(1, len(text) // 2)], []
    return text, items


def _labels(request_id: str, field: str, items: list[tuple[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "stream": "inference_requests",
            "record_id": request_id,
            "field": field,
            "pii_type": pii_type,
            "value": value,
        }
        for pii_type, value in items
    ]


def inference_request(
    rng: random.Random, ctx: RequestContext, pii_prompt_rate: float
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """One inference log record plus ground-truth PII labels for its text fields."""
    request_id = new_uuid(rng)
    lang = ctx.user.lang
    model = _choose_model(rng, ctx.org.plan, ctx.channel)
    status = rng.choices(STATUSES, STATUS_WEIGHTS)[0]
    prompt, prompt_pii = make_prompt(rng, lang, rng.random() < pii_prompt_rate)
    response, response_pii = _response_text(rng, lang, status, prompt_pii)
    input_tokens = _input_tokens(rng, prompt, lang, ctx.channel)
    cached = int(input_tokens * rng.uniform(0.3, 0.8)) if rng.random() < CACHE_HIT_RATE else 0
    outcome = _outcome(rng, model, status)
    received = ctx.ts + timedelta(milliseconds=outcome["latency_ms"] + rng.randint(500, 3000))
    record = {
        "request_id": request_id,
        "event_ts": ctx.ts,
        "received_ts": received,
        "org_id": ctx.org.org_id,
        "user_id": ctx.user.user_id,
        "api_key_id": ctx.api_key_id,
        "channel": ctx.channel,
        "conversation_id": ctx.conversation_id,
        "message_id": ctx.message_id,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": outcome["output_tokens"],
        "cached_tokens": cached,
        "latency_ms": outcome["latency_ms"],
        "ttft_ms": outcome["ttft_ms"],
        "status": status,
        "error_code": outcome["error_code"],
        "finish_reason": outcome["finish_reason"],
        "zdr": ctx.org.zdr,
        "prompt_text": prompt,
        "response_text": response,
        "schema_version": SCHEMA_VERSION,
    }
    labels = _labels(request_id, "prompt_text", prompt_pii)
    labels += _labels(request_id, "response_text", response_pii)
    return record, labels
