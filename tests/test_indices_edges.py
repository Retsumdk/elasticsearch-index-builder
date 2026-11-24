"""Index lifecycle edge cases: naming, idempotency, guarded rebuilds, aliases."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from esib import Client, Config, EmptyLoadError, FakeCluster, IndexManager
from esib.errors import ConfigError, NotFoundError
from esib.indices import resolve_alias_or_index


def _client(cluster: FakeCluster, **config: object) -> Client:
    return Client(
        Config(base_url="http://fake:9200", **config),  # type: ignore[arg-type]
        transport=cluster,
        sleep=lambda _delay: None,
    )


def test_exists_is_false_before_creation_and_true_after() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    assert manager.exists("books") is False
    manager.create("books")
    assert manager.exists("books") is True


def test_delete_reports_whether_the_index_was_there() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    manager.create("books")
    assert manager.delete("books") is True
    assert manager.delete("books") is False
    with pytest.raises(Exception, match="no such index"):
        manager.delete("books", ignore_missing=False)


def test_count_and_mapping_round_trip() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    manager.create("books", mappings={"properties": {"title": {"type": "keyword"}}})
    assert manager.count("books") == 0
    assert manager.mapping("books")["properties"]["title"] == {"type": "keyword"}
    manager.put_mapping("books", {"pages": {"type": "long"}})
    properties = manager.mapping("books")["properties"]
    assert properties["pages"] == {"type": "long"}
    with pytest.raises(NotFoundError):
        manager.mapping("missing")


def test_put_settings_reaches_the_cluster() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    manager.create("books", settings={"number_of_replicas": 2})
    manager.put_settings("books", {"number_of_replicas": 0})
    assert cluster.indices["books"].settings["number_of_replicas"] == 0


def test_indices_for_alias_is_empty_when_the_alias_is_unknown() -> None:
    manager = IndexManager(_client(FakeCluster()))
    assert manager.indices_for_alias("live") == []
    with pytest.raises(NotFoundError, match="no index or alias"):
        resolve_alias_or_index(manager, "live")


def test_swap_alias_sends_one_atomic_request() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    manager.create("books-a")
    manager.create("books-b")
    removed = manager.swap_alias("live", "books-b", remove=["books-a"])
    assert removed == ["books-a"]
    assert cluster.aliases["live"] == {"books-b"}
    alias_requests = [path for method, path in cluster.requests if path == "/_aliases"]
    assert len(alias_requests) == 1
    assert resolve_alias_or_index(manager, "live") == ["books-b"]


def test_swap_alias_can_skip_the_write_index_flag() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    manager.create("books-a")
    manager.create("books-b")
    manager.swap_alias("live", "books-b", remove=["books-a"], write_index=False)
    assert cluster.aliases["live"] == {"books-b"}


def test_staged_name_is_sortable_and_validated() -> None:
    manager = IndexManager(_client(FakeCluster()))
    name = manager.staged_name("books", now=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
    assert name == "books-20260102T030405Z"
    for bad in ("", "_hidden", "-dash", "with/slash"):
        with pytest.raises(ConfigError, match="alias"):
            manager.staged_name(bad)


def test_free_name_appends_a_counter_until_it_is_unused() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    manager.create("books")
    manager.create("books-2")
    assert manager.free_name("books") == "books-3"


def test_build_moves_to_a_free_name_when_the_staged_one_is_taken() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    moment = datetime(2026, 1, 1, tzinfo=UTC)
    taken = manager.staged_name("books", now=moment)
    manager.create(taken)
    report = manager.build("books", [{"a": 1}], now=moment)
    assert report.index != taken
    assert manager.count(report.index) == 1
    assert taken in cluster.indices


def test_build_infers_a_mapping_from_the_stream_when_none_is_given() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    report = manager.build("books", [{"title": "Dune", "pages": 412}], id_field="title")
    assert report.mappings_source == "inferred"
    properties = manager.mapping("books")["properties"]
    assert properties["pages"] == {"type": "long"}


def test_build_keeps_explicit_mappings_and_reports_it() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    report = manager.build(
        "books",
        [{"title": "Dune", "pages": "412"}],
        mappings={"properties": {"title": {"type": "keyword"}}},
        id_field="title",
    )
    assert report.mappings_source == "explicit"
    assert "pages" not in manager.mapping("books")["properties"]


def test_build_refuses_a_thin_load_and_removes_the_staged_index() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    manager.create("books-v1")
    manager.swap_alias("books", "books-v1")
    with pytest.raises(EmptyLoadError, match="no documents"):
        manager.build("books", [], min_docs=1)
    assert cluster.aliases["books"] == {"books-v1"}
    with pytest.raises(EmptyLoadError, match="refusing"):
        manager.build("books", [{"title": "Dune"}], min_docs=2)
    assert cluster.aliases["books"] == {"books-v1"}
    assert sorted(cluster.indices) == ["books-v1"]


def test_build_dry_run_touches_nothing() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    report = manager.build("books", [{"title": "Dune"}], dry_run=True)
    assert report.ok is True
    assert report.swapped is False
    assert report.documents == 1
    writes = [request for request in cluster.requests if request[0] != "HEAD"]
    assert writes == []


def test_build_warns_when_ids_collapse_the_count() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    report = manager.build("books", [{"id": "1"}, {"id": "1"}], id_field="id")
    assert report.loaded == 2
    assert any("overwrite" in warning for warning in report.warnings)


def test_build_can_keep_the_previous_index() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    first = manager.build("live", [{"id": "1"}], id_field="id")
    second = manager.build("live", [{"id": "1"}], id_field="id", delete_previous=False)
    assert second.deleted == ()
    assert first.index in cluster.indices
    assert cluster.aliases["live"] == {second.index}


def test_build_reports_failures_without_raising() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster))
    report = manager.build(
        "strict",
        [{"id": "1"}, {"id": "2", "unknown": True}],
        mappings={"dynamic": "strict", "properties": {"id": {"type": "keyword"}}},
        id_field="id",
    )
    assert report.failed == 1
    assert report.ok is False
    assert report.failure_count == 1


def test_load_streams_with_progress() -> None:
    cluster = FakeCluster()
    manager = IndexManager(_client(cluster, batch_size=2))
    manager.create("books")
    seen: list[int] = []
    outcome = manager.load(
        "books",
        [{"id": str(index)} for index in range(5)],
        id_field="id",
        progress=lambda progress: seen.append(progress.batch),
    )
    assert outcome.indexed == 5
    assert seen == [1, 2, 3]
