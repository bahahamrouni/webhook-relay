"""Exponential backoff with full jitter.

Full jitter (a uniform random delay between 0 and the exponential ceiling) spreads
retries out so many failing deliveries do not hit a recovering endpoint at once.
"""

from __future__ import annotations

import random
from typing import Protocol


class _Uniform(Protocol):
    def uniform(self, a: float, b: float) -> float: ...


def backoff_ceiling(attempt: int, base: float, cap: float) -> float:
    """Upper bound of the delay after `attempt` failed attempts (attempt >= 1)."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    return min(cap, base * (2 ** (attempt - 1)))


def compute_delay(attempt: int, base: float, cap: float, rng: _Uniform | None = None) -> float:
    source = rng if rng is not None else random
    return source.uniform(0.0, backoff_ceiling(attempt, base, cap))
