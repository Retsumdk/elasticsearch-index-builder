"""Index lifecycle: create, map, load, swap an alias, delete the old index.

The rebuild path here is the one production reindexes actually use: build into a
fresh versioned index while the live alias keeps serving, restore the real
settings after the bulk load, refresh, then flip the alias in a single atomic
request. At no point is the alias pointing at a half-loaded index.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .bulk import BulkIndexer, BulkOutcome, BulkProgress
from .client import Client
from .errors import ConfigError, EmptyLoadError, NotFoundError
from .mapping import infer_mappings

__all__ = ["DEFAULT_INDEX_SETTINGS", "BuildReport", "IndexManager"]

DEFAULT_INDEX_SETTINGS: dict[str, Any] = {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "refresh_interval": "1s",
}

_LOAD_SETTINGS: dict[str, Any] = {"refresh_interval": "-1", "number_of_replicas": 0}


@dataclass
class BuildReport:
    """The auditable outcome of one build."""

    alias: str
    index: str
    documents: int = 0
    loaded: int = 0
    failed: int = 0
    batches: int = 0
    previous: tuple[str, ...] = ()
    deleted: tuple[str, ...] = ()
    took_ms: int = 0
    dry_run: bool = False
    swapped: bool = False
    mappings_source: str = "inferred"
    warnings: list[str] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)

    @property
    def failure_count(self) -> int:
        return len(self.failures)

    @property
    def ok(self) -> bool:
        if self.dry_run:
            return self.documents > 0
        return self.failed == 0 and self.loaded == self.documents and self.swapped

    def as_dict(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "index": self.index,
            "documents": self.documents,
            "loaded": self.loaded,
            "failed": self.failed,
            "batches": self.batches,
            "previous": list(self.previous),
            "deleted": list(self.deleted),
            "took_ms": self.took_ms,
            "dry_run": self.dry_run,
            "swapped": self.swapped,
            "mappings_source": self.mappings_source,
            "ok": self.ok,
            "warnings": list(self.warnings),
            "failures": self.failures[:25],
            "failure_count": len(self.failures),
        }


class IndexManager:
    """Thin, typed wrapper over the index/admin endpoints the builder needs."""

    def __init__(self, client: Client, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._client = client
        self._clock = clock

    @property
    def client(self) -> Client:
        return self._client

    def exists(self, index: str) -> bool:
        reply = self._client.send("HEAD", f"/{index}")
        return reply.status == 200

    def create(
        self,
        index: str,
        *,
        mappings: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> bool:
        """Create an index. Returns False when it already exists (idempotent)."""

        body: dict[str, Any] = {}
        if mappings:
            body["mappings"] = _with_type(dict(mappings))
        if settings:
            body["settings"] = dict(settings)
        reply = self._client.send("PUT", f"/{index}", body=body)
        if reply.status == 400 and _type_of(reply.json()) == "resource_already_exists_exception":
            return False
        self._client.raise_for(reply)
        return True

    def delete(self, index: str, *, ignore_missing: bool = True) -> bool:
        reply = self._client.send("DELETE", f"/{index}")
        if reply.status == 404 and ignore_missing:
            return False
        self._client.raise_for(reply)
        return True

    def count(self, index: str) -> int:
        reply = self._client.get(f"/{index}/_count")
        value = reply.get("count") if isinstance(reply, dict) else None
        return value if isinstance(value, int) else 0

    def refresh(self, index: str) -> None:
        self._client.post(f"/{index}/_refresh")

    def load(
        self,
        index: str,
        documents: Iterable[Mapping[str, Any]],
        *,
        id_field: str | None = None,
        batch_size: int | None = None,
        batch_bytes: int | None = None,
        refresh: bool = False,
        on_failure: str = "raise",
        progress: Callable[[BulkProgress], None] | None = None,
    ) -> BulkOutcome:
        """Bulk-load documents into an existing index."""

        indexer = BulkIndexer(
            self._client,
            batch_size=batch_size or self._client.config.batch_size,
            batch_bytes=batch_bytes or self._client.config.batch_bytes,
        )
        return indexer.load(
            index,
            documents,
            id_field=id_field,
            refresh=refresh,
            on_failure=on_failure,
            progress=progress,
        )

    def mapping(self, index: str) -> dict[str, Any]:
        reply = self._client.get(f"/{index}/_mapping")
        if not isinstance(reply, dict) or not reply:
            return {}
        first = next(iter(reply.values()))
        if isinstance(first, dict):
            mappings = first.get("mappings")
            if isinstance(mappings, dict):
                return mappings
        return {}

    def put_mapping(self, index: str, mappings: Mapping[str, Any]) -> None:
        self._client.put(f"/{index}/_mapping", _with_type(dict(mappings)))

    def put_settings(self, index: str, settings: Mapping[str, Any]) -> None:
        self._client.put(f"/{index}/_settings", dict(settings))

    def indices_for_alias(self, alias: str) -> list[str]:
        reply = self._client.send("GET", f"/_alias/{alias}")
        if reply.status == 404:
            return []
        self._client.raise_for(reply)
        body = reply.json()
        return sorted(body) if isinstance(body, dict) else []

    def swap_alias(
        self,
        alias: str,
        new_index: str,
        *,
        remove: Iterable[str] = (),
        write_index: bool = True,
    ) -> list[str]:
        """Point ``alias`` at ``new_index`` and away from ``remove``, atomically.

        One ``_aliases`` request carries both the add and the removals, so the
        alias never resolves to nothing and never resolves to two indices.
        """

        actions: list[dict[str, Any]] = []
        for index in sorted(set(remove)):
            if index != new_index:
                actions.append({"remove": {"index": index, "alias": alias}})
        add: dict[str, Any] = {"index": new_index, "alias": alias}
        if write_index:
            add["is_write_index"] = True
        actions.append({"add": add})
        self._client.post("/_aliases", {"actions": actions})
        return [action["remove"]["index"] for action in actions if "remove" in action]

    def free_name(self, index: str) -> str:
        """Return `index`, or `index-2`, `index-3`... if that name is taken.

        Two rebuilds inside the same second would otherwise collide on the
        timestamped staging name and fail for no good reason.
        """

        if not self.exists(index):
            return index
        for suffix in range(2, 100):
            candidate = f"{index}-{suffix}"
            if not self.exists(candidate):
                return candidate
        raise ConfigError(f"no free index name near {index!r}", field="index")

    def staged_name(self, alias: str, *, now: datetime | None = None) -> str:
        """A collision-free, sortable name: ``<alias>-20261124T150405Z``."""

        if not alias or "/" in alias or alias.startswith(("_", "-")):
            raise ConfigError(f"invalid alias name: {alias!r}", field="alias")
        moment = (now or datetime.now(UTC)).astimezone(UTC)
        return f"{alias}-{moment.strftime('%Y%m%dT%H%M%SZ')}"

    def build(
        self,
        alias: str,
        documents: Iterable[Mapping[str, Any]],
        *,
        mappings: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
        id_field: str | None = None,
        batch_size: int | None = None,
        batch_bytes: int | None = None,
        delete_previous: bool = True,
        min_docs: int = 1,
        on_failure: str = "collect",
        dry_run: bool = False,
        now: datetime | None = None,
        progress: Callable[[BulkProgress], None] | None = None,
    ) -> BuildReport:
        """Load ``documents`` into a new index and repoint ``alias`` at it."""

        started = self._clock()
        index = self.free_name(self.staged_name(alias, now=now))
        report = BuildReport(alias=alias, index=index, dry_run=dry_run)
        final_settings = dict(DEFAULT_INDEX_SETTINGS)
        final_settings.update(dict(settings or {}))
        load_settings = {**final_settings, **_LOAD_SETTINGS}

        if dry_run:
            sample: list[dict[str, Any]] = []
            for document in documents:
                report.documents += 1
                if len(sample) < 500:
                    sample.append(dict(document))
            if mappings is None and sample:
                inferred = infer_mappings(sample)
                report.warnings.extend(inferred.warnings)
            report.warnings.append(f"dry run: would create {index} and swap alias {alias!r}")
            report.took_ms = int((self._clock() - started) * 1000)
            return report

        existing = self.indices_for_alias(alias)
        report.previous = tuple(existing)

        if mappings is None:
            documents, inferred_mappings, warnings = _infer_from_stream(documents)
            report.warnings.extend(warnings)
            mappings = inferred_mappings
        else:
            report.mappings_source = "explicit"
            documents = iter(documents)

        index = self.free_name(index)
        created = self.create(index, mappings=mappings, settings=load_settings)
        if not created:
            raise ConfigError(
                f"index {index} already exists — pass an explicit name or clear it first",
                field="index",
            )
        indexer = BulkIndexer(
            self._client,
            batch_size=batch_size or self._client.config.batch_size,
            batch_bytes=batch_bytes or self._client.config.batch_bytes,
        )
        outcome = indexer.load(
            index, documents, id_field=id_field, on_failure=on_failure, progress=progress
        )
        report.documents = outcome.indexed + outcome.failed
        report.loaded = outcome.indexed
        report.failed = outcome.failed
        report.batches = outcome.batches
        report.failures = [failure.as_dict() for failure in outcome.failures]

        if report.loaded < min_docs:
            self.delete(index)
            report.took_ms = int((self._clock() - started) * 1000)
            raise EmptyLoadError(
                f"refusing to point {alias!r} at a load of {report.loaded} documents "
                f"(min_docs={min_docs}); the staged index {index} was deleted"
            )

        self.put_settings(index, final_settings)
        self.refresh(index)
        counted = self.count(index)
        report.warnings.extend(_count_warnings(counted, report.loaded))

        removed = self.swap_alias(alias, index, remove=report.previous)
        report.swapped = True
        if delete_previous and removed:
            deleted: list[str] = []
            for name in removed:
                if self.delete(name, ignore_missing=True):
                    deleted.append(name)
            report.deleted = tuple(deleted)

        report.took_ms = int((self._clock() - started) * 1000)
        return report


def _infer_from_stream(
    documents: Iterable[Mapping[str, Any]], *, sample_limit: int = 1000
) -> tuple[Iterable[Mapping[str, Any]], dict[str, Any], list[str]]:
    """Peek at the head of a stream to infer a mapping, then replay it."""

    buffered: list[dict[str, Any]] = []
    iterator = iter(documents)
    exhausted = False
    while len(buffered) < sample_limit:
        try:
            buffered.append(dict(next(iterator)))
        except StopIteration:
            exhausted = True
            break
    if not buffered:
        raise EmptyLoadError(
            "no documents to build from; nothing was created and the alias was left alone"
        )
    inferred = infer_mappings(buffered)
    rest = iter(()) if exhausted else iterator
    return _chain(buffered, rest), inferred.mappings(), list(inferred.warnings)


def _chain(
    head: list[dict[str, Any]], tail: Iterable[Mapping[str, Any]]
) -> Iterable[Mapping[str, Any]]:
    yield from head
    for extra in tail:
        yield dict(extra)


def _count_warnings(counted: int, loaded: int) -> list[str]:
    if counted == loaded:
        return []
    return [
        f"cluster count ({counted}) differs from documents accepted ({loaded}); "
        "documents sharing an _id overwrite each other"
    ]


def _with_type(mappings: Mapping[str, Any]) -> dict[str, Any]:
    body = dict(mappings)
    if "properties" not in body and "dynamic" not in body:
        body = {"properties": body}
    return body


def _type_of(body: Any) -> str | None:
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict):
            value = error.get("type")
            return value if isinstance(value, str) else None
    return None


def resolve_alias_or_index(manager: IndexManager, name: str) -> list[str]:
    """Alias members if ``name`` is an alias, else ``[name]`` if it exists."""

    members = manager.indices_for_alias(name)
    if members:
        return members
    if manager.exists(name):
        return [name]
    raise NotFoundError(f"no index or alias named {name!r}")
