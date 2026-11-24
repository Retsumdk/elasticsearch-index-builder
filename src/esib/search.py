"""Reading an index back out, without the classic pagination traps.

Elasticsearch refuses `from + size` beyond `index.max_result_window`, and
`search_after` is stable only if the sort is unique. Both helpers here use a
point-in-time (PIT) so the result set cannot shift under the reader, and both
refuse to loop: a cursor that does not advance is an error, not an infinite
stream.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .client import Client
from .errors import PaginationError

__all__ = ["Page", "count", "scan", "search_after_scan"]


@dataclass(frozen=True)
class Page:
    """One page of hits plus what the cluster said around it."""

    number: int
    hits: list[dict[str, Any]]
    raw: dict[str, Any] = field(default_factory=dict)
    total: int | None = None
    cursor: list[Any] | None = None

    @property
    def ids(self) -> list[str]:
        return [str(hit.get("_id")) for hit in self.hits]

    @property
    def sources(self) -> list[dict[str, Any]]:
        return [dict(hit.get("_source") or {}) for hit in self.hits]

    def __len__(self) -> int:
        return len(self.hits)


def _page(reply: Mapping[str, Any], number: int) -> Page:
    hits_block = reply.get("hits")
    if not isinstance(hits_block, Mapping):
        raise PaginationError("search reply had no hits block")
    hits = hits_block.get("hits")
    hit_list = [dict(hit) for hit in hits] if isinstance(hits, list) else []
    total_block = hits_block.get("total")
    total = None
    if isinstance(total_block, Mapping):
        value = total_block.get("value")
        total = int(value) if isinstance(value, int) else None
    elif isinstance(total_block, int):
        total = total_block
    cursor: list[Any] | None = None
    if hit_list:
        last_sort = hit_list[-1].get("sort")
        if isinstance(last_sort, list):
            cursor = list(last_sort)
    return Page(number=number, hits=hit_list, raw=dict(reply), total=total, cursor=cursor)


def count(client: Client, index: str) -> int:
    """Document count of an index or alias."""

    reply = client.get(f"/{index}/_count")
    value = reply.get("count") if isinstance(reply, Mapping) else None
    return int(value) if isinstance(value, int) else 0


def scan(
    client: Client,
    index: str,
    *,
    size: int = 1000,
    query: Mapping[str, Any] | None = None,
    source: Sequence[str] | bool | None = None,
    keep_alive: str = "1m",
    max_pages: int = 1000,
    use_pit: bool = True,
) -> Iterator[Page]:
    """Yield every matching document, page by page, from a fixed snapshot.

    Uses a point-in-time plus `_shard_doc` ordering, so concurrent indexing
    cannot reorder the stream or make a page repeat. The PIT is always closed,
    including when the caller stops iterating early.
    """

    if size < 1:
        raise PaginationError("size must be >= 1")
    body: dict[str, Any] = {"size": size, "sort": [{"_shard_doc": "asc"}]}
    if query is not None:
        body["query"] = dict(query)
    if source is not None:
        body["_source"] = source

    pit_id: str | None = None
    if use_pit:
        opened = client.post(f"/{index}/_pit", params={"keep_alive": keep_alive})
        pit_id = opened.get("id") if isinstance(opened, Mapping) else None
        if not isinstance(pit_id, str):
            raise PaginationError("cluster did not return a point-in-time id")
        body["pit"] = {"id": pit_id, "keep_alive": keep_alive}

    seen: list[Any] = []
    try:
        for number in range(1, max_pages + 1):
            reply = client.post("/_search", body)
            if not isinstance(reply, Mapping):
                raise PaginationError("search reply was not an object")
            if pit_id is not None and isinstance(reply.get("pit_id"), str):
                pit_id = str(reply["pit_id"])
                body["pit"] = {"id": pit_id, "keep_alive": keep_alive}
            page = _page(reply, number)
            if not page.hits:
                return
            if page.cursor is not None:
                if page.cursor in seen:
                    raise PaginationError(
                        f"cursor stopped advancing at page {number}: the sort is not unique"
                    )
                seen.append(page.cursor)
                body["search_after"] = page.cursor
            yield page
            if len(page.hits) < size:
                return
        raise PaginationError(
            f"stopped after {max_pages} pages; raise max_pages or narrow the query"
        )
    finally:
        if pit_id is not None:
            with contextlib.suppress(Exception):
                client.send("DELETE", "/_pit", params={"id": pit_id})


def search_after_scan(
    client: Client,
    index: str,
    *,
    sort: Sequence[Any],
    size: int = 1000,
    query: Mapping[str, Any] | None = None,
    max_pages: int = 1000,
) -> Iterator[Page]:
    """`search_after` paging for clusters that predate point-in-time search.

    The final sort key must be unique across documents (a `_id`-like keyword, or
    an `_shard_doc` tiebreaker); a repeated cursor raises instead of looping.
    """

    if not sort:
        raise PaginationError("sort must contain a unique tiebreaker")
    body: dict[str, Any] = {"size": size, "sort": list(sort)}
    if query is not None:
        body["query"] = dict(query)

    seen: list[Any] = []
    for number in range(1, max_pages + 1):
        reply = client.post(f"/{index}/_search", body)
        if not isinstance(reply, Mapping):
            raise PaginationError("search reply was not an object")
        page = _page(reply, number)
        if not page.hits:
            return
        if page.cursor is None:
            raise PaginationError("hits carried no sort values; sort is required for search_after")
        if page.cursor in seen:
            raise PaginationError(f"cursor stopped advancing at page {number}")
        seen.append(page.cursor)
        yield page
        if len(page.hits) < size:
            return
        body["search_after"] = page.cursor
    raise PaginationError(f"stopped after {max_pages} pages")
