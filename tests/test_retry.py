"""Retry policy: what is retried, how long we wait, and when we stop."""

from __future__ import annotations

import pytest

from esib import RateLimited, Retrier, RetryPolicy, TransportError, ValidationError
from esib.errors import ServerError
from esib.retry import is_retryable


def test_policy_delays_grow_and_respect_the_ceiling() -> None:
    policy = RetryPolicy(attempts=5, base_delay=0.5, max_delay=4.0)
    delays = [
        policy.delay_for(attempt, None, uniform=lambda _low, high: high) for attempt in range(1, 5)
    ]
    assert delays == [0.5, 1.0, 2.0, 4.0]


def test_jitter_stays_inside_the_window() -> None:
    policy = RetryPolicy(attempts=5, base_delay=1.0, max_delay=8.0)
    for attempt in range(1, 6):
        delay = policy.delay_for(attempt, None, uniform=lambda low, high: low)
        assert 0.0 <= delay <= min(8.0, 1.0 * 2 ** (attempt - 1))


def test_server_retry_after_wins() -> None:
    policy = RetryPolicy(attempts=4, base_delay=0.1, max_delay=1.0)
    assert policy.delay_for(1, 2.5, uniform=lambda _low, high: high) == 2.5


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (RateLimited("busy", status=429, retry_after=1.0), True),
        (ServerError("gateway", status=503), True),
        (ServerError("boom", status=500), False),
        (ValidationError("bad field", status=400), False),
        (TransportError("connection reset"), True),
        (ValueError("not ours"), False),
    ],
)
def test_retryable_classification(error: Exception, expected: bool) -> None:
    assert is_retryable(error) is expected


def test_rejected_execution_is_retryable_even_with_a_non_retryable_status() -> None:
    error = ServerError("queue full", status=500, error_type="es_rejected_execution_exception")
    assert is_retryable(error) is True


def test_retrier_gives_up_after_the_attempt_budget() -> None:
    slept: list[float] = []
    calls = {"count": 0}

    def operation() -> str:
        calls["count"] += 1
        raise RateLimited("busy", status=429, retry_after=0.0)

    retrier = Retrier(
        RetryPolicy(attempts=3, base_delay=0.0, max_delay=0.0),
        sleep=slept.append,
        uniform=lambda low, _high: low,
    )
    with pytest.raises(RateLimited):
        retrier.run(operation)
    assert calls["count"] == 3
    assert retrier.attempts_made == 3
    assert len(retrier.history) == 2


def test_retrier_returns_the_first_success() -> None:
    calls = {"count": 0}

    def operation() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise TransportError("flaky")
        return "done"

    retrier = Retrier(RetryPolicy(attempts=4), sleep=lambda _d: None, uniform=lambda low, _h: low)
    assert retrier.run(operation) == "done"
    assert retrier.attempts_made == 3


def test_retrier_does_not_retry_non_retryable_errors() -> None:
    calls = {"count": 0}

    def operation() -> str:
        calls["count"] += 1
        raise ValidationError("mapping says no", status=400)

    retrier = Retrier(RetryPolicy(attempts=5), sleep=lambda _d: None)
    with pytest.raises(ValidationError):
        retrier.run(operation)
    assert calls["count"] == 1


def test_retrier_records_what_it_waited_for() -> None:
    retrier = Retrier(
        RetryPolicy(attempts=2, base_delay=0.25, max_delay=1.0),
        sleep=lambda _d: None,
        uniform=lambda _low, high: high,
    )
    with pytest.raises(RateLimited):
        retrier.run(lambda: (_ for _ in ()).throw(RateLimited("busy", status=429)))
    (record,) = retrier.history
    assert record.number == 1
    assert record.delay == 0.25
    assert "busy" in record.error
