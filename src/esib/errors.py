"""Typed errors, each carrying what the cluster said and what to do about it."""

from __future__ import annotations

from typing import Any

__all__ = [
    "AuthenticationError",
    "AuthorizationError",
    "BulkError",
    "BulkFailure",
    "ConfigError",
    "ConflictError",
    "ESError",
    "ESHTTPError",
    "EmptyLoadError",
    "NotFoundError",
    "PaginationError",
    "PayloadTooLarge",
    "RateLimited",
    "ServerError",
    "SourceError",
    "TransportError",
    "ValidationError",
    "error_from_response",
]


class ESError(Exception):
    """Base class: something the cluster (or the client) refused to do."""

    hint_text = ""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        error_type: str | None = None,
        reason: str | None = None,
        body: Any = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.error_type = error_type
        self.reason = reason
        self.body = body

    def hint(self) -> str:
        return self.hint_text

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": type(self).__name__,
            "message": str(self),
            "status": self.status,
            "type": self.error_type,
            "reason": self.reason,
            "hint": self.hint(),
        }


class ESHTTPError(ESError):
    """An error the cluster answered with an HTTP status code."""


class AuthenticationError(ESHTTPError):
    """401 — the cluster does not know who you are."""

    hint_text = "Set ESIB_API_KEY (or ESIB_USERNAME/ESIB_PASSWORD) for this cluster."


class AuthorizationError(ESHTTPError):
    """403 — authenticated, but not allowed to touch this index."""

    hint_text = "The credential lacks this index/action; check the role's index patterns."


class NotFoundError(ESHTTPError):
    """404 — usually an index or alias that does not exist."""

    hint_text = "Create the index first, or check the alias name (aliases are not indices)."


class ValidationError(ESHTTPError):
    """400 — the request was rejected as malformed or unmappable."""

    hint_text = "Check field types against the index mapping (see `esib mapping`)."


class PayloadTooLarge(ESHTTPError):
    """413 — the request body exceeded the cluster's HTTP limit."""

    hint_text = "Lower batch_size/batch_bytes; the default cap is http.max_content_length."


class ConflictError(ESHTTPError):
    """409 — a version or existence conflict, not a failure to retry blindly."""

    hint_text = "Use `create` for insert-only loads, or let the indexer retry the item."


class RateLimited(ESHTTPError):
    """429 — the cluster's queue is full; retry after the stated interval."""

    hint_text = "Lower concurrency or batch rate; the client already honours Retry-After."

    def __init__(self, message: str, *, retry_after: float | None = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ServerError(ESHTTPError):
    """5xx — the cluster failed while handling an otherwise valid request."""

    hint_text = "Retryable when the status is 502/503/504; check cluster health otherwise."


class TransportError(ESError):
    """The request never produced a response (DNS, TLS, timeout, connection reset)."""

    hint_text = "Verify base_url, network reachability, and TLS settings."


class ConfigError(ESError):
    """Bad configuration, caught before any request is sent."""

    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field

    def hint(self) -> str:
        return "Run `esib config` to print the resolved (redacted) settings."


class SourceError(ESError):
    """A document file could not be read or parsed."""

    def __init__(self, message: str, *, path: str | None = None, line: int | None = None) -> None:
        super().__init__(message)
        self.path = path
        self.line = line

    def hint(self) -> str:
        if self.line is not None:
            return f"Fix line {self.line} of {self.path} (each line must be one JSON object)."
        return "Sources may be .ndjson/.jsonl, .json, .csv, .tsv, optionally .gz."


class EmptyLoadError(ESError):
    """A build produced fewer documents than its floor, so nothing was swapped."""

    hint_text = "Check the source path and filter, or lower `min_docs` deliberately."


class PaginationError(ESError):
    """A scroll/scan cursor stopped making progress."""

    hint_text = "The sort must include a tiebreaker (or use `esib search --scan` with a PIT)."


class BulkFailure:
    """One document the cluster refused, with the reason it gave."""

    __slots__ = ("action", "doc_id", "error_type", "reason", "status")

    def __init__(
        self,
        *,
        action: str,
        doc_id: str | None,
        status: int | None,
        error_type: str | None,
        reason: str | None,
    ) -> None:
        self.action = action
        self.doc_id = doc_id
        self.status = status
        self.error_type = error_type
        self.reason = reason

    def as_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "id": self.doc_id,
            "status": self.status,
            "type": self.error_type,
            "reason": self.reason,
        }

    def __repr__(self) -> str:
        return f"BulkFailure(action={self.action!r}, id={self.doc_id!r}, type={self.error_type!r})"


class BulkError(ESError):
    """At least one document in a bulk load was permanently rejected."""

    hint_text = "Inspect .failures — mapping mismatches need a mapping change, not a retry."

    def __init__(self, message: str, *, failures: list[BulkFailure] | None = None) -> None:
        super().__init__(message)
        self.failures = failures or []


def _reason_of(body: Any) -> tuple[str | None, str | None]:
    if not isinstance(body, dict):
        return None, None
    error = body.get("error")
    if isinstance(error, dict):
        error_type = error.get("type")
        reason = error.get("reason")
        caused = error.get("caused_by")
        if isinstance(caused, dict) and isinstance(caused.get("reason"), str):
            reason = f"{reason}: {caused['reason']}" if reason else caused["reason"]
        return (
            error_type if isinstance(error_type, str) else None,
            reason if isinstance(reason, str) else None,
        )
    if isinstance(error, str):
        return None, error
    return None, None


def error_from_response(status: int, body: Any, *, retry_after: float | None = None) -> ESError:
    """Map an HTTP status plus Elasticsearch error body onto a typed error."""

    error_type, reason = _reason_of(body)
    detail = reason or f"HTTP {status}"
    if error_type:
        detail = f"{detail} [{error_type}]"

    kwargs: dict[str, Any] = {
        "status": status,
        "error_type": error_type,
        "reason": reason,
        "body": body,
    }
    if status == 400:
        return ValidationError(detail, **kwargs)
    if status == 401:
        return AuthenticationError(detail, **kwargs)
    if status == 403:
        return AuthorizationError(detail, **kwargs)
    if status == 404:
        return NotFoundError(detail, **kwargs)
    if status == 409:
        return ConflictError(detail, **kwargs)
    if status == 413:
        return PayloadTooLarge(detail, **kwargs)
    if status == 429:
        return RateLimited(detail, retry_after=retry_after, **kwargs)
    if 500 <= status < 600:
        return ServerError(detail, **kwargs)
    return ESError(detail, **kwargs)
