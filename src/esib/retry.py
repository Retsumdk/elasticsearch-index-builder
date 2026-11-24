"""Retry decisions: which failures deserve another attempt, and how long to wait."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from .errors import ConflictError, ESHTTPError, RateLimited, TransportError

__all__ = [
    "RETRYABLE_STATUS",
    "RETRYABLE_TYPES",
    "Attempt",
    "Retrier",
    "RetryPolicy",
    "is_retryable",
]

T = TypeVar("T")

RETRYABLE_STATUS = frozenset({408, 429, 502, 503, 504})

RETRYABLE_TYPES = frozenset(
    {
        "es_rejected_execution_exception",
        "circuit_breaking_exception",
        "unavailable_shards_exception",
        "cluster_block_exception",
        "process_cluster_event_timeout_exception",
        "node_not_connected_exception",
        "no_shard_available_action_exception",
    }
)

NEVER_RETRY_TYPES = frozenset(
    {
        "mapper_parsing_exception",
        "document_parsing_exception",
        "strict_dynamic_mapping_exception",
        "illegal_argument_exception",
        "parsing_exception",
        "index_not_found_exception",
        "resource_already_exists_exception",
        "version_conflict_engine_exception",
        "security_exception",
        "authentication_exception",
    }
)


def is_retryable(error: BaseException) -> bool:
    """True when another attempt could plausibly succeed.

    Retrying a mapping error or an auth failure only wastes the budget, so the
    classification is a small closed set rather than "anything not 4xx".
    """

    if isinstance(error, TransportError):
        return True
    if isinstance(error, ConflictError):
        return False
    if isinstance(error, RateLimited):
        return True
    if not isinstance(error, ESHTTPError):
        return False
    if error.error_type in NEVER_RETRY_TYPES:
        return False
    if error.error_type in RETRYABLE_TYPES:
        return True
    return error.status in RETRYABLE_STATUS


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded exponential backoff with full jitter."""

    attempts: int = 4
    base_delay: float = 0.25
    max_delay: float = 8.0
    jitter: bool = True

    def delay_for(
        self,
        attempt: int,
        retry_after: float | None = None,
        *,
        uniform: Callable[[float, float], float] | None = None,
    ) -> float:
        """Seconds to wait before attempt ``attempt + 1``.

        A server ``Retry-After`` always wins: the cluster knows when its queue
        drains better than an exponential curve does.
        """

        if retry_after is not None:
            return max(0.0, retry_after)
        raw = self.base_delay * float(2 ** max(0, attempt - 1))
        ceiling = min(self.max_delay, raw)
        if not self.jitter:
            return ceiling
        pick = uniform or random.uniform
        return float(pick(0.0, ceiling))


@dataclass(frozen=True, slots=True)
class Attempt:
    """A failed attempt, recorded so callers can log exactly what happened."""

    number: int
    error: str
    delay: float


@dataclass
class Retrier:
    """Runs an operation under a :class:`RetryPolicy`.

    The clock and sleep function are injectable, which is what makes the retry
    path testable without a single real second passing.
    """

    policy: RetryPolicy = field(default_factory=RetryPolicy)
    sleep: Callable[[float], None] | None = None
    clock: Callable[[], float] | None = None
    uniform: Callable[[float, float], float] | None = None
    attempts_made: int = field(default=0, init=False)
    history: list[Attempt] = field(default_factory=list, init=False)

    def run(self, operation: Callable[[], T]) -> T:
        self.attempts_made = 0
        self.history.clear()
        while True:
            self.attempts_made += 1
            try:
                return operation()
            except Exception as error:
                if not is_retryable(error) or self.attempts_made >= self.policy.attempts:
                    raise
                retry_after = getattr(error, "retry_after", None)
                delay = self.policy.delay_for(self.attempts_made, retry_after, uniform=self.uniform)
                self.history.append(
                    Attempt(number=self.attempts_made, error=str(error), delay=delay)
                )
                if delay:
                    (self.sleep or time.sleep)(delay)
