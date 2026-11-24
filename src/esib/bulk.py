"""NDJSON bulk loading that survives partial failure.

A bulk request answers HTTP 200 even when half its documents were rejected, and a
rejected document fails for one of two very different reasons: the cluster was
busy (retry it) or the document does not fit the mapping (never retry it). This
module keeps that distinction, retries only the documents that deserve it, and
reports exactly what happened to the rest.

Loading streams: documents are encoded and flushed batch by batch, so a 400 MB
source never has to fit in memory.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .client import Client
from .errors import BulkError, BulkFailure, RateLimited, ServerError, TransportError

__all__ = [
    "RETRYABLE_ITEM_STATUS",
    "RETRYABLE_ITEM_TYPES",
    "BulkIndexer",
    "BulkOutcome",
    "BulkProgress",
    "encode_bulk",
]

RETRYABLE_ITEM_STATUS = frozenset({429, 500, 502, 503, 504})
RETRYABLE_ITEM_TYPES = frozenset(
    {
        "es_rejected_execution_exception",
        "circuit_breaking_exception",
        "unavailable_shards_exception",
        "process_cluster_event_timeout_exception",
        "cluster_block_exception",
        "node_not_connected_exception",
    }
)

_MISSING = object()


@dataclass(slots=True)
class BulkProgress:
    """A snapshot handed to the optional progress callback after each batch."""

    batch: int
    documents: int
    indexed: int
    failed: int
    bytes_sent: int


@dataclass(slots=True)
class BulkOutcome:
    """What the cluster did with a load. Counts always sum to `documents`."""

    documents: int = 0
    indexed: int = 0
    created: int = 0
    updated: int = 0
    deleted: int = 0
    noop: int = 0
    failed: int = 0
    batches: int = 0
    bytes_sent: int = 0
    retried_documents: int = 0
    failures: list[BulkFailure] = field(default_factory=list)
    took_ms: int = 0

    @property
    def ok(self) -> bool:
        return self.failed == 0

    def raise_on_failure(self) -> None:
        """Raise :class:`BulkError` if any document was permanently rejected."""

        if not self.failed:
            return
        first = self.failures[0].error_type if self.failures else "unknown"
        raise BulkError(
            f"{self.failed} of {self.documents} documents were rejected (first: {first})",
            failures=self.failures,
        )

    def summary(self) -> dict[str, Any]:
        return {
            "documents": self.documents,
            "indexed": self.indexed,
            "created": self.created,
            "updated": self.updated,
            "deleted": self.deleted,
            "noop": self.noop,
            "failed": self.failed,
            "batches": self.batches,
            "bytes_sent": self.bytes_sent,
            "retried_documents": self.retried_documents,
            "took_ms": self.took_ms,
            "failures": [failure.as_dict() for failure in self.failures[:20]],
            "failure_count": len(self.failures),
        }


def _value_at(document: Mapping[str, Any], path: str, default: Any = _MISSING) -> Any:
    """Read a dotted path (`user.id`) out of a document."""

    current: Any = document
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return default
    return current


def _document_id(document: Mapping[str, Any], id_field: str | None) -> str | None:
    if id_field is None:
        return None
    value = _value_at(document, id_field)
    if value is _MISSING or value is None:
        return None
    if isinstance(value, bool | float | int):
        return str(value)
    if isinstance(value, str):
        return value.strip() or None
    raise BulkError(f"id field {id_field!r} must be a string or number, got {type(value).__name__}")


def encode_bulk(
    index: str,
    documents: Iterable[Mapping[str, Any]],
    *,
    id_field: str | None = None,
    action: str = "index",
    require_id: bool = False,
) -> bytes:
    """Encode documents as newline-delimited action/source pairs.

    Elasticsearch requires a trailing newline on every NDJSON body, including the
    last line; omitting it is the single most common cause of a 400 on `_bulk`.
    """

    if action not in ("index", "create", "delete", "update"):
        raise BulkError(f"unsupported bulk action {action!r}")
    lines: list[bytes] = []
    for position, document in enumerate(documents):
        if not isinstance(document, Mapping):
            raise BulkError(f"document {position} is not an object: {type(document).__name__}")
        doc_id = _document_id(document, id_field)
        if doc_id is None and require_id:
            raise BulkError(f"document {position} has no value at id field {id_field!r}")
        metadata: dict[str, Any] = {"_index": index}
        if doc_id is not None:
            metadata["_id"] = doc_id
        lines.append(json.dumps({action: metadata}, separators=(",", ":")).encode())
        if action == "update":
            lines.append(json.dumps({"doc": document}, separators=(",", ":"), default=str).encode())
        elif action != "delete":
            lines.append(json.dumps(document, separators=(",", ":"), default=str).encode())
    if not lines:
        return b""
    return b"\n".join(lines) + b"\n"


def _item_error(item: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]] | None:
    """Return the action name and error object for a failed bulk item."""

    for name, detail in item.items():
        if isinstance(detail, Mapping):
            error = detail.get("error")
            if isinstance(error, Mapping):
                return str(name), error
    return None


def _documents_from_body(body: bytes) -> list[dict[str, Any]]:
    """Recover the source documents from an encoded NDJSON body."""

    documents: list[dict[str, Any]] = []
    lines = [line for line in body.split(b"\n") if line]
    position = 0
    while position < len(lines):
        try:
            action = json.loads(lines[position])
        except json.JSONDecodeError:  # pragma: no cover - defensive
            break
        meta = next(iter(action.values())) if isinstance(action, dict) and action else {}
        step = 1 if "delete" in action else 2
        if step == 2 and position + 1 < len(lines):
            try:
                documents.append(json.loads(lines[position + 1]))
            except json.JSONDecodeError:  # pragma: no cover - defensive
                documents.append({})
        elif step == 1 and isinstance(meta, dict):
            documents.append({"_id": meta.get("_id")})
        position += step
    return documents


class BulkIndexer:
    """Send documents in bounded batches, retrying only what can succeed later."""

    def __init__(
        self,
        client: Client,
        *,
        batch_size: int = 500,
        batch_bytes: int = 5 * 1024 * 1024,
        max_item_rounds: int = 3,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        if batch_size < 1:
            raise BulkError("batch_size must be >= 1")
        if batch_bytes < 1024:
            raise BulkError("batch_bytes must be >= 1024")
        self._client = client
        self._batch_size = batch_size
        self._batch_bytes = batch_bytes
        self._max_item_rounds = max(1, max_item_rounds)
        self._sleep = sleep
        self._clock = clock

    def load(
        self,
        index: str,
        documents: Iterable[Mapping[str, Any]],
        *,
        id_field: str | None = None,
        action: str = "index",
        refresh: bool = False,
        on_failure: str = "raise",
        progress: Callable[[BulkProgress], None] | None = None,
    ) -> BulkOutcome:
        """Load `documents` into `index`, streaming in bounded batches.

        `on_failure="raise"` raises :class:`BulkError` once the load finishes if
        any document was permanently rejected; `"collect"` returns the outcome and
        leaves the decision to the caller.
        """

        if on_failure not in ("raise", "collect"):
            raise BulkError(f"on_failure must be 'raise' or 'collect', got {on_failure!r}")

        started = self._clock()
        outcome = BulkOutcome()
        batch: list[Mapping[str, Any]] = []
        batch_bytes = 0

        def flush() -> None:
            nonlocal batch, batch_bytes
            if not batch:
                return
            outcome.batches += 1
            self._load_batch(
                index, batch, outcome, id_field=id_field, action=action, refresh=refresh
            )
            if progress is not None:
                progress(
                    BulkProgress(
                        batch=outcome.batches,
                        documents=outcome.documents,
                        indexed=outcome.indexed,
                        failed=outcome.failed,
                        bytes_sent=outcome.bytes_sent,
                    )
                )
            batch = []
            batch_bytes = 0

        for position, document in enumerate(documents):
            if not isinstance(document, Mapping):
                raise BulkError(f"document {position} is not an object: {type(document).__name__}")
            if id_field is not None and _document_id(document, id_field) is None:
                outcome.documents += 1
                outcome.failed += 1
                outcome.failures.append(
                    BulkFailure(
                        action=action,
                        doc_id=None,
                        status=None,
                        error_type="missing_id",
                        reason=f"no value at id field {id_field!r}; give every document one",
                    )
                )
                continue
            encoded_length = len(encode_bulk(index, [document], id_field=id_field, action=action))
            if batch and (
                len(batch) >= self._batch_size or batch_bytes + encoded_length > self._batch_bytes
            ):
                flush()
            batch.append(document)
            batch_bytes += encoded_length
            outcome.documents += 1
        flush()

        outcome.took_ms = int((self._clock() - started) * 1000)
        if on_failure == "raise":
            outcome.raise_on_failure()
        return outcome

    def _load_batch(
        self,
        index: str,
        documents: list[Mapping[str, Any]],
        outcome: BulkOutcome,
        *,
        id_field: str | None,
        action: str,
        refresh: bool,
    ) -> None:
        pending = list(documents)
        for round_number in range(1, self._max_item_rounds + 1):
            body = encode_bulk(index, pending, id_field=id_field, action=action)
            outcome.bytes_sent += len(body)
            try:
                reply = self._client.bulk(body, refresh=refresh)
            except (RateLimited, ServerError, TransportError):
                if round_number >= self._max_item_rounds:
                    raise
                outcome.retried_documents += len(pending)
                self._sleep(0.05 * 2 ** (round_number - 1))
                continue

            items = reply.get("items")
            if not isinstance(items, list):
                raise TransportError(f"bulk reply had no items array: {str(reply)[:200]}")
            pending = self._account(
                items,
                body,
                outcome,
                allow_retry=round_number < self._max_item_rounds,
            )
            if not pending:
                return
            outcome.retried_documents += len(pending)
            self._sleep(0.05 * 2 ** (round_number - 1))

    def _account(
        self,
        items: list[Any],
        body: bytes,
        outcome: BulkOutcome,
        *,
        allow_retry: bool,
    ) -> list[Mapping[str, Any]]:
        """Count item results; return the documents worth sending again."""

        failed_documents: list[Mapping[str, Any]] = []
        sources = _documents_from_body(body)
        for position, item in enumerate(items):
            if not isinstance(item, Mapping):
                continue
            failure = _item_error(item)
            if failure is None:
                _, detail = next(iter(item.items()))
                if isinstance(detail, Mapping):
                    outcome.indexed += 1
                    result = detail.get("result")
                    if isinstance(result, str) and result in ("created", "updated", "noop"):
                        setattr(outcome, result, getattr(outcome, result) + 1)
                    elif result == "deleted":
                        outcome.deleted += 1
                continue
            name, error = failure
            status = error.get("status")
            error_type = error.get("type")
            retryable = (isinstance(status, int) and status in RETRYABLE_ITEM_STATUS) or (
                isinstance(error_type, str) and error_type in RETRYABLE_ITEM_TYPES
            )
            if retryable and allow_retry and position < len(sources):
                failed_documents.append(sources[position])
                continue
            detail_obj = item.get(name)
            doc_id = None
            if isinstance(detail_obj, Mapping) and isinstance(detail_obj.get("_id"), str):
                doc_id = detail_obj["_id"]
            reason = error.get("reason")
            outcome.failed += 1
            outcome.failures.append(
                BulkFailure(
                    action=name,
                    doc_id=doc_id,
                    status=status if isinstance(status, int) else None,
                    error_type=error_type if isinstance(error_type, str) else None,
                    reason=reason if isinstance(reason, str) else None,
                )
            )
        return failed_documents
