"""Pagination edge cases: the guards that stop a scan from lying or looping."""

from __future__ import annotations

import json
from collections.abc import Generator
from typing import Any, cast

import pytest

from esib import Client, Config, FakeCluster, IndexManager
from esib.bulk import BulkIndexer
from esib.errors import PaginationError
from esib.search import Page, count, scan, search_after_scan
from esib.transport import Response


def _client(cluster: FakeCluster) -> Client:
    return Client(
        Config(base_url="http://cluster.test:9200"),
        transport=cluster,
        sleep=lambda _delay: None,
    )


def _seeded(count_of: int = 7) -> tuple[Client, FakeCluster]:
    cluster = FakeCluster()
    client = _client(cluster)
    IndexManager(client).create("books")
    BulkIndexer(client, batch_size=100).load(
        "books", [{"i": index} for index in range(count_of)], id_field=None, on_failure="collect"
    )
    return client, cluster


def test_page_helpers_expose_ids_sources_and_length() -> None:
    page = Page(
        number=1,
        hits=[{"_id": "a", "_source": {"x": 1}}, {"_id": "b"}],
        total=2,
    )
    assert page.ids == ["a", "b"]
    assert page.sources == [{"x": 1}, {}]
    assert len(page) == 2


def test_page_accepts_a_plain_integer_total() -> None:
    client = _client(FakeCluster())
    client.put("/books", {})
    client.bulk(b'{"index":{"_index":"books","_id":"1"}}\n{"a":1}\n')
    reply = client.post("/books/_search", {"size": 1})
    reply["hits"]["total"] = 5
    from esib.search import _page

    assert _page(reply, 1).total == 5


def test_a_reply_without_a_hits_block_is_a_pagination_error() -> None:
    from esib.search import _page

    with pytest.raises(PaginationError):
        _page({"aggregations": {}}, 1)


def test_scan_walks_every_page_and_stops_cleanly() -> None:
    client, _ = _seeded(7)
    pages = list(scan(client, "books", size=3))
    assert [len(page) for page in pages] == [3, 3, 1]
    assert sum(len(page) for page in pages) == 7
    assert pages[0].total == 7


def test_scan_forwards_query_and_source() -> None:
    client, cluster = _seeded(2)
    for _page in scan(client, "books", size=1, query={"match_all": {}}, source=["i"]):
        body = cluster.last_json_body
        assert body["query"] == {"match_all": {}}
        assert body["_source"] == ["i"]
        assert "pit" in body
        break
    assert body["_source"] == ["i"]


def test_scan_rejects_a_non_positive_size() -> None:
    client, _ = _seeded(1)
    with pytest.raises(PaginationError):
        next(scan(client, "books", size=0))


def test_scan_requires_a_point_in_time_id() -> None:
    stub = FakeCluster()
    client = _client(stub)
    client.put("/books", {})

    class NoPit(FakeCluster):
        def request(self, method: str, path: str, **kwargs: Any) -> Response:
            if path.endswith("/_pit"):
                return Response(status=200, headers={}, body=b'{"no": "id"}')
            return super().request(method, path, **kwargs)

    client = _client(NoPit())
    with pytest.raises(PaginationError):
        next(scan(client, "books"))


def test_scan_guards_against_a_cursor_that_does_not_advance() -> None:
    class Frozen(FakeCluster):
        def _search(self, index: str | None, payload: Any, *, pit_search: bool) -> Response:
            body = {
                "hits": {
                    "total": {"value": 100, "relation": "eq"},
                    "hits": [
                        {"_id": "1", "_source": {}, "sort": [0, "1"]},
                        {"_id": "2", "_source": {}, "sort": [0, "2"]},
                    ],
                },
                "pit_id": "pit-1",
            }
            return Response(status=200, headers={}, body=json.dumps(body).encode())

    cluster = Frozen()
    client = _client(cluster)
    client.put("/books", {})
    with pytest.raises(PaginationError, match="cursor"):
        list(scan(client, "books", size=2))


def test_scan_stops_at_max_pages_instead_of_running_forever() -> None:
    class Endless(FakeCluster):
        served = 0

        def _search(self, index: str | None, payload: Any, *, pit_search: bool) -> Response:
            Endless.served += 1
            body = {
                "hits": {
                    "total": {"value": 1000, "relation": "eq"},
                    "hits": [
                        {"_id": f"doc-{Endless.served}", "_source": {}, "sort": [Endless.served]}
                    ],
                },
                "pit_id": "pit-1",
            }
            return Response(status=200, headers={}, body=json.dumps(body).encode())

    client = _client(Endless())
    client.put("/books", {})
    with pytest.raises(PaginationError, match="max_pages"):
        list(scan(client, "books", size=1, max_pages=3))


def test_scan_closes_the_pit_when_the_caller_stops_early() -> None:
    client, cluster = _seeded(10)
    pages = cast(Generator[Page, None, None], scan(client, "books", size=2))
    next(pages)
    pages.close()
    assert ("DELETE", "/_pit") in cluster.requests


def test_search_after_scan_needs_a_sort() -> None:
    client, _ = _seeded(2)
    with pytest.raises(PaginationError):
        next(search_after_scan(client, "books", sort=[]))


def test_search_after_scan_requires_sort_values_on_every_hit() -> None:
    class NoSort(FakeCluster):
        def _search(self, index: str | None, payload: Any, *, pit_search: bool) -> Response:
            body = {
                "hits": {
                    "total": {"value": 1, "relation": "eq"},
                    "hits": [{"_id": "1", "_source": {}}],
                }
            }
            return Response(status=200, headers={}, body=json.dumps(body).encode())

    cluster = NoSort()
    client = _client(cluster)
    client.put("/books", {})
    with pytest.raises(PaginationError, match="sort"):
        list(search_after_scan(client, "books", sort=[{"i": "asc"}]))


def test_search_after_scan_pages_with_the_last_sort_value() -> None:
    client, cluster = _seeded(5)
    pages = list(search_after_scan(client, "books", sort=[{"_shard_doc": "asc"}], size=2))
    assert sum(len(page) for page in pages) == 5
    assert cluster.bodies[0].get("search_after") is None
    assert cluster.bodies[-1].get("search_after") is not None


def test_search_after_scan_refuses_a_frozen_cursor() -> None:
    class Frozen(FakeCluster):
        def _search(self, index: str | None, payload: Any, *, pit_search: bool) -> Response:
            body = {
                "hits": {
                    "total": {"value": 100, "relation": "eq"},
                    "hits": [
                        {"_id": "1", "_source": {}, "sort": [1]},
                        {"_id": "2", "_source": {}, "sort": [1]},
                    ],
                }
            }
            return Response(status=200, headers={}, body=json.dumps(body).encode())

    client = _client(Frozen())
    with pytest.raises(PaginationError):
        list(search_after_scan(client, "books", sort=[{"i": "asc"}], size=2))


def test_count_reads_the_count_endpoint() -> None:
    client, _ = _seeded(4)
    assert count(client, "books") == 4
