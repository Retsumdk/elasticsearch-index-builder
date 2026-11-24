"""Error taxonomy: status mapping, cause chains, and the hint each error carries."""

from __future__ import annotations

import pytest

from esib import (
    AuthenticationError,
    AuthorizationError,
    BulkError,
    BulkFailure,
    ConfigError,
    ConflictError,
    NotFoundError,
    PayloadTooLarge,
    RateLimited,
    ServerError,
    SourceError,
    TransportError,
    ValidationError,
)
from esib.errors import error_from_response


def test_status_400_is_a_validation_error() -> None:
    error = error_from_response(
        400, {"error": {"type": "illegal_argument_exception", "reason": "bad"}}
    )
    assert isinstance(error, ValidationError)
    assert error.error_type == "illegal_argument_exception"
    assert error.reason == "bad"
    assert "illegal_argument_exception" in str(error)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, AuthenticationError),
        (403, AuthorizationError),
        (404, NotFoundError),
        (409, ConflictError),
        (413, PayloadTooLarge),
        (429, RateLimited),
        (500, ServerError),
        (503, ServerError),
        (418, Exception),
    ],
)
def test_status_to_type_mapping(status: int, expected: type[Exception]) -> None:
    error = error_from_response(status, {"error": {"type": "x", "reason": "y"}})
    assert isinstance(error, expected)


def test_caused_by_is_appended_to_the_reason() -> None:
    body = {
        "error": {
            "type": "mapper_parsing_exception",
            "reason": "failed to parse field [price]",
            "caused_by": {"type": "number_format_exception", "reason": 'For input string: "n/a"'},
        }
    }
    error = error_from_response(400, body)
    assert "number_format_exception" not in str(error)
    assert 'For input string: "n/a"' in str(error)
    assert error.reason is not None and error.reason.startswith("failed to parse field [price]")


def test_plain_string_error_and_unknown_body_are_tolerated() -> None:
    assert "cluster shutting down" in str(
        error_from_response(503, {"error": "cluster shutting down"})
    )
    assert "HTTP 502" in str(error_from_response(502, "<html>"))


def test_rate_limited_carries_retry_after() -> None:
    error = error_from_response(
        429, {"error": {"type": "rejected", "reason": "queue"}}, retry_after=1.5
    )
    assert isinstance(error, RateLimited)
    assert error.retry_after == 1.5


def test_every_error_serialises_with_a_hint() -> None:
    payload = error_from_response(
        404, {"error": {"type": "index_not_found_exception", "reason": "no such index"}}
    )
    assert isinstance(payload, NotFoundError)
    rendered = payload.as_dict()
    assert rendered["error"] == "NotFoundError"
    assert rendered["status"] == 404
    assert rendered["hint"]


def test_config_and_source_errors_point_at_the_fix() -> None:
    assert "esib config" in ConfigError("bad", field="base_url").hint()
    assert "line 7" in SourceError("bad json", path="docs.ndjson", line=7).hint()
    assert ".ndjson" in SourceError("bad").hint()
    assert "base_url" in TransportError("refused").hint()


def test_bulk_error_summarises_the_first_rejection() -> None:
    failure = BulkFailure(
        action="index",
        doc_id="42",
        status=400,
        error_type="mapper_parsing_exception",
        reason="bad date",
    )
    error = BulkError("3 of 10 rejected", failures=[failure])
    assert error.failures[0].doc_id == "42"
    assert failure.as_dict()["type"] == "mapper_parsing_exception"
    assert "retry" in error.hint()
    assert "BulkFailure" in repr(failure)
