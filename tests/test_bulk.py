"""Bulk loading: NDJSON framing, batching, partial failure, targeted retries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from esib import (
    BulkError,
    BulkIndexer,
    BulkOutcome,
    Client,
    Config,
    FakeCluster,
    IndexManager,
    encode_bulk,
)


def client_for(cluster: FakeCluster, **overrides: Any) -> Client:
    config = Config(base_url="http://cluster.test:9200", **overrides)
    return Client(config, transport=cluster, sleep=lambda _delay: None)


def lines(payload: bytes) -> list[str]:
    return payload.decode().split("\n")[:-1]


def test_encode_pairs_actions_with_sources_and_ends_with_newline() -> None:
    payload = encode_bulk("books", [{"title": "Dune"}, {"title": "Emma"}])
    assert payload.endswith(b"\n")
    body = lines(payload)
    assert body[0] == '{"index":{"_index":"books"}}'
    assert body[1] == '{"title":"Dune"}'
    assert len(body) == 4


def test_encode_uses_id_field_dotted_path() -> None:
    payload = encode_bulk("books", [{"meta": {"id": 42}, "title": "Dune"}], id_field="meta.id")
    assert lines(payload)[0] == '{"index":{"_index":"books","_id":"42"}}'


def test_encode_requires_id_when_asked() -> None:
    with pytest.raises(BulkError) as raised:
        encode_bulk("books", [{"title": "Dune"}], id_field="id", require_id=True)
    assert "no value at id field" in str(raised.value)


def test_encode_rejects_non_objects() -> None:
    with pytest.raises(BulkError):
        encode_bulk("books", ["not a document"])  # type: ignore[list-item]


def test_encode_delete_has_no_source_line() -> None:
    payload = encode_bulk("books", [{"id": 1}], id_field="id", action="delete")
    assert lines(payload) == ['{"delete":{"_index":"books","_id":"1"}}']


def test_encode_unknown_action_rejected() -> None:
    with pytest.raises(BulkError):
        encode_bulk("books", [{"id": 1}], action="upsert")


def test_load_indexes_every_document(
    cluster: FakeCluster, manager: IndexManager, catalog: list[dict[str, Any]]
) -> None:
    manager.create("catalog", mappings={"properties": {"sku": {"type": "keyword"}}})
    outcome = manager.load("catalog", catalog, id_field="sku", batch_size=4)
    assert outcome.ok
    assert outcome.documents == len(catalog)
    assert outcome.indexed == len(catalog)
    assert outcome.created == len(catalog)
    assert outcome.batches >= 3
    assert cluster.document_count("catalog") == len(catalog)


def test_load_is_idempotent_with_an_id_field(cluster: FakeCluster, manager: IndexManager) -> None:
    manager.create("books")
    manager.load("books", [{"id": 1, "title": "Dune"}], id_field="id")
    second = manager.load("books", [{"id": 1, "title": "Dune Messiah"}], id_field="id")
    assert second.updated == 1
    assert cluster.document_count("books") == 1


def test_batch_bytes_splits_on_size_not_only_count(cluster: FakeCluster) -> None:
    client = client_for(cluster, batch_size=1000, batch_bytes=1024)
    client.put("/big", {"mappings": {}})
    documents = [{"id": str(index), "padding": "x" * 200} for index in range(20)]
    outcome = BulkIndexer(client, batch_size=1000, batch_bytes=1024).load(
        "big", documents, id_field="id"
    )
    assert outcome.batches > 1
    assert outcome.ok


def test_empty_input_does_no_requests(cluster: FakeCluster, manager: IndexManager) -> None:
    manager.create("books")
    before = len(cluster.requests)
    outcome = manager.load("books", [])
    assert outcome.documents == 0
    assert outcome.batches == 0
    assert len(cluster.requests) == before


def test_mapping_rejection_is_collected_not_retried(
    cluster: FakeCluster, manager: IndexManager
) -> None:
    manager.create(
        "strict", mappings={"dynamic": "strict", "properties": {"id": {"type": "keyword"}}}
    )
    with pytest.raises(BulkError) as raised:
        manager.load("strict", [{"id": "1", "surprise": True}], id_field="id")
    assert len(raised.value.failures) == 1
    assert raised.value.failures[0].error_type == "strict_dynamic_mapping_exception"


def test_collect_mode_returns_failures_without_raising(
    cluster: FakeCluster, manager: IndexManager
) -> None:
    manager.create(
        "strict", mappings={"dynamic": "strict", "properties": {"id": {"type": "keyword"}}}
    )
    outcome = manager.load(
        "strict",
        [{"id": "1", "surprise": True}, {"id": "2"}],
        id_field="id",
        on_failure="collect",
    )
    assert outcome.failed == 1
    assert outcome.indexed == 1
    assert outcome.ok is False
    assert outcome.failures[0].doc_id == "1"


def test_rejected_document_appears_in_outcome_summary(
    cluster: FakeCluster, manager: IndexManager
) -> None:
    manager.create(
        "strict", mappings={"dynamic": "strict", "properties": {"id": {"type": "keyword"}}}
    )
    outcome = manager.load("strict", [{"id": "9", "nope": 1}], id_field="id", on_failure="collect")
    summary = outcome.summary()
    assert summary["failed"] == 1
    assert summary["failures"][0]["id"] == "9"
    assert summary["documents"] == 1


def _account(
    items: list[Any],
    documents: list[Mapping[str, Any]],
    *,
    allow_retry: bool = True,
    batch_size: int = 10,
) -> tuple[BulkOutcome, list[Mapping[str, Any]]]:
    client = Client(Config(base_url="http://cluster.test:9200"), transport=FakeCluster())
    indexer = BulkIndexer(client, batch_size=batch_size)
    outcome = BulkOutcome()
    outcome.documents = len(documents)
    pending = indexer._account(
        items, encode_bulk("books", documents), outcome, allow_retry=allow_retry
    )
    return outcome, pending


def test_account_counts_successes_and_keeps_only_retryable_failures() -> None:
    outcome, pending = _account(
        [
            {"index": {"_id": "1", "status": 201, "result": "created"}},
            {
                "index": {
                    "_id": "2",
                    "status": 429,
                    "error": {
                        "type": "es_rejected_execution_exception",
                        "reason": "queue full",
                    },
                }
            },
        ],
        [{"id": "1", "title": "Dune"}, {"id": "2", "title": "Emma"}],
    )
    assert outcome.indexed == 1
    assert outcome.created == 1
    assert outcome.failed == 0
    assert outcome.retried_documents == 0
    assert pending == [{"id": "2", "title": "Emma"}]


def test_account_never_retries_a_mapping_error() -> None:
    outcome, pending = _account(
        [
            {
                "index": {
                    "_id": "7",
                    "status": 400,
                    "error": {"type": "mapper_parsing_exception", "reason": "bad date"},
                }
            }
        ],
        [{"id": "7", "created_at": "not a date"}],
    )
    assert pending == []
    assert outcome.failed == 1
    assert outcome.failures[0].error_type == "mapper_parsing_exception"
    assert outcome.failures[0].doc_id == "7"


def test_account_marks_a_retryable_failure_failed_on_the_last_round() -> None:
    outcome, pending = _account(
        [{"index": {"_id": "1", "status": 503, "error": {"type": "unavailable_shards_exception"}}}],
        [{"id": "1", "title": "Dune"}],
        allow_retry=False,
    )
    assert pending == []
    assert outcome.failed == 1


def test_account_ignores_malformed_items() -> None:
    outcome, pending = _account(["not-an-object"], [{"title": "Dune"}])
    assert outcome.indexed == 0
    assert outcome.failed == 0
    assert pending == []


def test_http_429_on_a_batch_is_retried_by_the_client(
    cluster: FakeCluster, manager: IndexManager
) -> None:
    manager.create("books")
    cluster.add_fault(
        429, reason="queue full", error_type="es_rejected_execution_exception", retry_after=0.0
    )
    outcome = manager.load("books", [{"id": 1, "title": "Dune"}], id_field="id")
    assert outcome.ok
    assert manager.client.stats.retries == 1


def test_progress_callback_sees_every_batch(manager: IndexManager) -> None:
    manager.create("books")
    seen: list[int] = []
    manager.load(
        "books",
        [{"id": str(index)} for index in range(10)],
        id_field="id",
        batch_size=3,
        progress=lambda snapshot: seen.append(snapshot.batch),
    )
    assert seen == [1, 2, 3, 4]


def test_batch_size_must_be_positive(client: Client) -> None:
    with pytest.raises(BulkError):
        BulkIndexer(client, batch_size=0)
    with pytest.raises(BulkError):
        BulkIndexer(client, batch_bytes=10)


def test_on_failure_mode_is_validated(manager: IndexManager) -> None:
    manager.create("books")
    with pytest.raises(BulkError):
        manager.load("books", [{"id": 1}], on_failure="ignore")
