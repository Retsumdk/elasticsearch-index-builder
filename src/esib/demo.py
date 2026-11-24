"""The offline demo: a full build against an in-memory cluster.

`esib demo` runs this and CI diffs the transcript against ``demo-output.txt``, so
the documented output cannot drift away from the code.
"""

from __future__ import annotations

import io
import random
import sys
from datetime import UTC, datetime
from typing import Any, TextIO

from .bulk import BulkIndexer
from .client import Client
from .config import Config
from .indices import IndexManager
from .mapping import infer_mappings
from .search import scan
from .testing import FakeCluster, Fault

__all__ = ["run", "sample_products", "transcript"]

_FIXED_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def sample_products() -> list[dict[str, Any]]:
    """Seven clean rows plus one that drifted from the schema."""

    return [
        {
            "sku": "sku-1001",
            "name": "Aeron chair",
            "category": "seating",
            "price": 1299.0,
            "in_stock": True,
            "legacy_id": 1001,
            "released": "2024-03-01",
        },
        {
            "sku": "sku-1002",
            "name": "Standing desk",
            "category": "desks",
            "price": 749.5,
            "in_stock": True,
            "legacy_id": "1002",
            "released": "2024-04-18",
        },
        {
            "sku": "sku-1003",
            "name": "Monitor arm",
            "category": "accessories",
            "price": 189.0,
            "in_stock": False,
            "legacy_id": 1003,
            "released": "2023-11-02",
        },
        {
            "sku": "sku-1004",
            "name": "Task lamp",
            "category": "lighting",
            "price": 129.0,
            "in_stock": True,
            "legacy_id": 1004,
            "released": "2025-01-09",
        },
        {
            "sku": "sku-1005",
            "name": "Cable tray",
            "category": "accessories",
            "price": 39.0,
            "in_stock": True,
            "legacy_id": 1005,
            "released": "2023-08-21",
        },
        {
            "sku": "sku-1006",
            "name": "Bookcase",
            "category": "storage",
            "price": 460.0,
            "in_stock": False,
            "legacy_id": 1006,
            "released": "2022-06-30",
        },
        {
            "sku": "sku-1007",
            "name": "Footrest",
            "category": "accessories",
            "price": 89.0,
            "in_stock": True,
            "legacy_id": 1007,
            "released": "2024-09-12",
        },
        {
            "sku": "sku-1008",
            "name": "Sample unit (price as shipped)",
            "category": "seating",
            "price": "call for price",
            "in_stock": True,
            "legacy_id": 1008,
            "released": "2025-02-04",
        },
    ]


class _Transcript:
    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def line(self, text: str = "") -> None:
        self._stream.write(text + "\n")

    def step(self, number: int, title: str) -> None:
        self.line()
        self.line(f"{number}. {title}")


_DESCRIPTIONS = {
    "text": "text + keyword sub-field",
    "keyword": "keyword",
    "date": "date (ISO-8601)",
    "long": "long",
    "integer": "long",
    "double": "double",
    "boolean": "boolean",
}


def run(stream: TextIO | None = None) -> int:
    """Execute the demo, write the transcript, and return a process exit code."""

    out = _Transcript(stream if stream is not None else sys.stdout)
    rows = sample_products()
    clean = rows[:-1]

    cluster = FakeCluster(
        faults=[
            Fault(
                status=429,
                error_type="es_rejected_execution_exception",
                reason="rejected execution of bulk request",
                method="POST",
                path_contains="/_bulk",
                retry_after=0,
            )
        ]
    )
    client = Client(
        Config(base_url="http://fake-cluster:9200"),
        transport=cluster,
        sleep=lambda _seconds: None,
        rng=random.Random(7),
    )

    out.line("esib demo — a complete index build, no cluster and no network")

    inferred = infer_mappings(clean)
    out.step(1, f"infer a mapping from {len(clean)} sample documents")
    for name, spec in inferred.properties.items():
        declared = str(spec.get("type", "object"))
        out.line(f"   {name:<12}{_DESCRIPTIONS.get(declared, declared)}")
    out.line(f"   {inferred.field_count} fields from {inferred.documents_sampled} documents")
    for warning in inferred.warnings:
        out.line(f"   warning: {warning}")

    out.step(2, "build a versioned index and point the alias at it")
    manager = IndexManager(client)
    report = manager.build(
        "products",
        rows,
        mappings=inferred.mappings(),
        settings={"number_of_shards": 1, "number_of_replicas": 0},
        id_field="sku",
        batch_size=5,
        now=_FIXED_TIME,
    )
    out.line(f"   index {report.index}")
    out.line(
        f"   {report.loaded} of {report.documents} documents accepted "
        f"in {report.batches} bulk requests"
    )
    for failure in report.failures:
        out.line(f"   rejected {failure['id']}: {failure['type']} — {failure['reason']}")
    out.line(f"   {client.stats.retries} request retried after 429 (Retry-After honoured)")
    out.line(f"   alias products -> {report.index}")

    out.step(3, "read the alias back with a point-in-time scan")
    pages = list(scan(client, "products", size=4))
    for page in pages:
        ids = [str(hit.get("_id")) for hit in page.hits]
        out.line(f"   page {page.number}: {len(page.hits)} hits ({ids[0]} … {ids[-1]})")
    reachable = sum(len(page.hits) for page in pages)
    out.line(f"   {reachable} documents reachable through the alias")

    out.step(4, "re-run the same load to show it is idempotent")
    again = BulkIndexer(client, batch_size=4).load("products", clean, id_field="sku")
    out.line(f"   {again.updated} updated, {again.created} created, {again.failed} rejected")
    out.line(f"   count is still {manager.count('products')}")

    out.line()
    out.line(f"done: {manager.count('products')} documents searchable through 'products'")
    client.close()
    return 0


def transcript() -> str:
    """The demo output as a string, for tests and golden-file comparison."""

    buffer = io.StringIO()
    run(buffer)
    return buffer.getvalue()
