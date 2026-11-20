"""Day-by-day simulation of product usage, org changes, and API traffic."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from typing import Any

from generator.common import JST, new_uuid
from generator.config import GeneratorConfig
from generator.entities import ApiKey, Org, User, build_entities
from generator.records import RequestContext, app_event, inference_request

Record = dict[str, Any]

BASE_ACTIVE_PROB = 0.35
RETENTION_FLOOR = 0.25
RETENTION_HALF_LIFE_DAYS = 10
WEEKEND_FACTOR = {"app": 0.7, "api": 0.6}
# Activity hours in JST (index = hour). Hour 0 is excluded so that org changes, which
# take effect in the first minutes of a day, always precede that day's traffic.
HOUR_WEIGHTS = (0, 1, 1, 1, 1, 1, 2, 4, 6, 8, 9, 9, 8, 9, 9, 9, 8, 7, 6, 6, 6, 5, 4, 2)
FEEDBACK_RATE = 0.12
DOWN_RATING_SHARE = 0.25
FEEDBACK_REASONS = ("inaccurate", "unhelpful", "too_slow", "unsafe", "other")
# Daily probability of each org change, checked in this order.
UPGRADE_TO_PRO = 0.01
UPGRADE_TO_ENTERPRISE = 0.005
TOGGLE_ZDR = 0.02
API_DAILY_MEDIAN = {"pro": 30, "enterprise": 90}


@dataclass(frozen=True)
class SimulationResult:
    app_events: tuple[Record, ...]
    inference_requests: tuple[Record, ...]
    pii_labels: tuple[dict[str, str], ...]


def _random_time(rng: random.Random, day_start: datetime) -> datetime:
    hour = rng.choices(range(24), HOUR_WEIGHTS)[0]
    return day_start + timedelta(hours=hour, seconds=rng.uniform(0, 3600))


def _org_changed(rng: random.Random, org: Org, admin: User, ts: datetime, kind: str) -> Record:
    props = {"change_type": kind, "plan": org.plan, "zdr": org.zdr, "changed_at": ts}
    return app_event(
        rng,
        name="org_changed",
        ts=ts,
        user=admin,
        session_id=None,
        platform="server",
        properties=props,
    )


def _next_org_state(rng: random.Random, org: Org) -> tuple[Org, str] | None:
    roll = rng.random()
    if org.plan == "free" and roll < UPGRADE_TO_PRO:
        return replace(org, plan="pro"), "plan_changed"
    if org.plan == "pro" and roll < UPGRADE_TO_ENTERPRISE:
        return replace(org, plan="enterprise"), "plan_changed"
    if org.plan == "enterprise" and roll < TOGGLE_ZDR:
        return replace(org, zdr=not org.zdr), "zdr_changed"
    return None


def evolve_orgs(
    rng: random.Random, orgs: dict[str, Org], day_start: datetime, users: dict[str, User]
) -> tuple[dict[str, Org], list[Record]]:
    """Apply today's plan/ZDR changes. Changes land in the first 5 minutes of the day."""
    updated, events = {}, []
    for org_id, org in orgs.items():
        change = _next_org_state(rng, org)
        if change is None:
            updated[org_id] = org
            continue
        new_org, kind = change
        ts = day_start + timedelta(seconds=rng.uniform(0, 300))
        updated[org_id] = new_org
        events.append(_org_changed(rng, new_org, users[org.admin_user_id], ts, kind))
    return updated, events


def is_active(rng: random.Random, user: User, day: int, weekend: bool) -> bool:
    if day < user.signup_day:
        return False
    if day == user.signup_day:
        return True
    age = day - user.signup_day
    retention = RETENTION_FLOOR + (1 - RETENTION_FLOOR) * math.exp(-age / RETENTION_HALF_LIFE_DAYS)
    factor = WEEKEND_FACTOR["app"] if weekend else 1.0
    return rng.random() < min(0.95, BASE_ACTIVE_PROB * user.engagement * retention * factor)


def _message(
    rng: random.Random, cfg: GeneratorConfig, ctx: RequestContext, session_id: str
) -> tuple[list[Record], Record, list[dict[str, str]]]:
    """A user prompt: message_sent, its inference request, and optional feedback."""
    request, labels = inference_request(rng, ctx, cfg.pii_prompt_rate)
    props = {
        "conversation_id": ctx.conversation_id,
        "message_id": ctx.message_id,
        "request_id": request["request_id"],
        "prompt_chars": len(request["prompt_text"]),
        "has_attachment": rng.random() < 0.08,
    }
    events = [
        app_event(
            rng,
            name="message_sent",
            ts=ctx.ts,
            user=ctx.user,
            session_id=session_id,
            platform=ctx.user.platform,
            properties=props,
        )
    ]
    if request["status"] == "success" and rng.random() < FEEDBACK_RATE:
        down = rng.random() < DOWN_RATING_SHARE
        feedback = {
            "message_id": ctx.message_id,
            "request_id": request["request_id"],
            "rating": "down" if down else "up",
            "reason": rng.choice(FEEDBACK_REASONS) if down else None,
        }
        ts = request["received_ts"] + timedelta(seconds=rng.uniform(5, 60))
        events.append(
            app_event(
                rng,
                name="feedback_submitted",
                ts=ts,
                user=ctx.user,
                session_id=session_id,
                platform=ctx.user.platform,
                properties=feedback,
            )
        )
    return events, request, labels


def user_day(
    rng: random.Random, cfg: GeneratorConfig, user: User, org: Org, day_start: datetime
) -> tuple[list[Record], list[Record], list[dict[str, str]]]:
    """All sessions, conversations, and messages for one active user on one day."""
    events, requests, labels = [], [], []
    n_sessions = 1 + (rng.random() < 0.35) + (rng.random() < 0.1)
    for _ in range(n_sessions):
        ts, session_id = _random_time(rng, day_start), new_uuid(rng)
        events.append(
            app_event(
                rng,
                name="session_started",
                ts=ts,
                user=user,
                session_id=session_id,
                platform=user.platform,
                properties={},
            )
        )
        for _ in range(rng.randint(1, 2)):
            ts += timedelta(seconds=rng.uniform(5, 60))
            conversation_id = new_uuid(rng)
            events.append(
                app_event(
                    rng,
                    name="conversation_created",
                    ts=ts,
                    user=user,
                    session_id=session_id,
                    platform=user.platform,
                    properties={"conversation_id": conversation_id},
                )
            )
            for _ in range(rng.randint(1, 6)):
                ts += timedelta(seconds=rng.uniform(20, 120))
                ctx = RequestContext(
                    ts=ts,
                    org=org,
                    user=user,
                    channel="app",
                    conversation_id=conversation_id,
                    message_id=new_uuid(rng),
                )
                msg_events, request, msg_labels = _message(rng, cfg, ctx, session_id)
                events += msg_events
                requests.append(request)
                labels += msg_labels
    return events, requests, labels


def api_key_day(
    rng: random.Random,
    cfg: GeneratorConfig,
    key: ApiKey,
    org: Org,
    owner: User,
    day_start: datetime,
    weekend: bool,
) -> tuple[list[Record], list[dict[str, str]]]:
    """Developer-API traffic for one key on one day."""
    factor = WEEKEND_FACTOR["api"] if weekend else 1.0
    n = int(API_DAILY_MEDIAN[org.plan] * rng.lognormvariate(0, 0.8) * factor)
    requests, labels = [], []
    for _ in range(n):
        ctx = RequestContext(
            ts=_random_time(rng, day_start),
            org=org,
            user=owner,
            channel="api",
            api_key_id=key.api_key_id,
        )
        request, request_labels = inference_request(rng, ctx, cfg.pii_prompt_rate)
        requests.append(request)
        labels += request_labels
    return requests, labels


def simulate(cfg: GeneratorConfig) -> SimulationResult:
    rng = random.Random(cfg.seed)
    window_start = datetime.combine(cfg.start_date, time(), JST)
    entities = build_entities(rng, cfg, window_start)
    users = {u.user_id: u for u in entities.users}
    orgs = {o.org_id: o for o in entities.orgs}
    app = [
        _org_changed(rng, o, users[o.admin_user_id], o.created_at, "created") for o in entities.orgs
    ]
    # Local accumulators (extend, not concatenate: hundreds of thousands of records).
    requests, labels = [], []
    for day in range(cfg.days):
        day_start = window_start + timedelta(days=day)
        weekend = day_start.weekday() >= 5
        orgs, changes = evolve_orgs(rng, orgs, day_start, users)
        app.extend(changes)
        for user in entities.users:
            if is_active(rng, user, day, weekend):
                e, r, lbl = user_day(rng, cfg, user, orgs[user.org_id], day_start)
                app.extend(e)
                requests.extend(r)
                labels.extend(lbl)
        for key in entities.api_keys:
            org = orgs[key.org_id]
            if org.plan != "free":
                r, lbl = api_key_day(
                    rng, cfg, key, org, users[key.owner_user_id], day_start, weekend
                )
                requests.extend(r)
                labels.extend(lbl)
    return SimulationResult(tuple(app), tuple(requests), tuple(labels))
