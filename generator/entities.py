"""Orgs, users, and API keys: the fixed cast of the simulation."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from generator.common import new_id
from generator.config import GeneratorConfig

PLANS = ("free", "pro", "enterprise")
PLAN_WEIGHTS = (0.6, 0.3, 0.1)
# Relative headcount: enterprise orgs attract far more users than free ones.
ORG_SIZE_WEIGHT = {"free": 1, "pro": 4, "enterprise": 12}
ENTERPRISE_ZDR_SHARE = 0.5
PLATFORMS = ("web", "ios", "android")
PLATFORM_WEIGHTS = (0.6, 0.25, 0.15)
JA_SHARE = 0.7
PRE_WINDOW_USER_SHARE = 0.5


@dataclass(frozen=True)
class Org:
    org_id: str
    plan: str
    zdr: bool
    created_at: datetime
    admin_user_id: str


@dataclass(frozen=True)
class User:
    user_id: str
    org_id: str
    signup_day: int  # day index relative to the window start; negative = before the window
    engagement: float  # multiplier on daily activity probability
    platform: str
    lang: str


@dataclass(frozen=True)
class ApiKey:
    api_key_id: str
    org_id: str
    owner_user_id: str


@dataclass(frozen=True)
class Entities:
    orgs: tuple[Org, ...]
    users: tuple[User, ...]
    api_keys: tuple[ApiKey, ...]


def _make_user(rng: random.Random, org_id: str, signup_day: int) -> User:
    return User(
        user_id=new_id(rng, "user"),
        org_id=org_id,
        signup_day=signup_day,
        engagement=min(3.0, rng.lognormvariate(0, 0.6)),
        platform=rng.choices(PLATFORMS, PLATFORM_WEIGHTS)[0],
        lang="ja" if rng.random() < JA_SHARE else "en",
    )


def _signup_day(rng: random.Random, days: int) -> int:
    if rng.random() < PRE_WINDOW_USER_SHARE:
        return -rng.randint(1, 180)
    return rng.randint(0, days - 1)


def build_entities(rng: random.Random, cfg: GeneratorConfig, window_start: datetime) -> Entities:
    org_ids = [new_id(rng, "org") for _ in range(cfg.n_orgs)]
    plans = rng.choices(PLANS, PLAN_WEIGHTS, k=cfg.n_orgs)

    # The first user of each org is its admin and predates the window.
    admins = [_make_user(rng, oid, -rng.randint(181, 365)) for oid in org_ids]
    weights = [ORG_SIZE_WEIGHT[p] for p in plans]
    members = [
        _make_user(rng, rng.choices(org_ids, weights)[0], _signup_day(rng, cfg.days))
        for _ in range(cfg.n_users - cfg.n_orgs)
    ]

    orgs = tuple(
        Org(
            org_id=oid,
            plan=plan,
            zdr=plan == "enterprise" and rng.random() < ENTERPRISE_ZDR_SHARE,
            created_at=window_start + timedelta(days=admin.signup_day),
            admin_user_id=admin.user_id,
        )
        for oid, plan, admin in zip(org_ids, plans, admins, strict=True)
    )
    api_keys = tuple(
        ApiKey(new_id(rng, "key"), org.org_id, org.admin_user_id)
        for org in orgs
        if org.plan != "free"
        for _ in range(rng.randint(1, 3))
    )
    return Entities(orgs=orgs, users=tuple(admins + members), api_keys=api_keys)
