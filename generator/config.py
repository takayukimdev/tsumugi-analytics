"""Generator configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

MAX_DEFECT_RATE = 0.2


@dataclass(frozen=True)
class GeneratorConfig:
    end_date: date
    days: int = 28
    seed: int = 42
    n_orgs: int = 60
    n_users: int = 3000
    defect_rate: float = 0.01
    pii_prompt_rate: float = 0.03

    def __post_init__(self) -> None:
        if self.days < 1:
            raise ValueError("days must be >= 1")
        if not 1 <= self.n_orgs <= self.n_users:
            raise ValueError("need 1 <= n_orgs <= n_users")
        for name in ("defect_rate", "pii_prompt_rate"):
            value = getattr(self, name)
            if not 0 <= value <= MAX_DEFECT_RATE:
                raise ValueError(f"{name} must be between 0 and {MAX_DEFECT_RATE}")

    @property
    def start_date(self) -> date:
        return self.end_date - timedelta(days=self.days - 1)
