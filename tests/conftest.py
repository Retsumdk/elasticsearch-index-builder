"""Shared fixtures: a fake cluster plus the objects that talk to it."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from esib import Client, Config, FakeCluster, IndexManager


@pytest.fixture
def cluster() -> FakeCluster:
    return FakeCluster()


@pytest.fixture
def client(cluster: FakeCluster) -> Iterator[Client]:
    instance = Client(
        Config(base_url="http://cluster.test:9200", batch_size=10, batch_bytes=4096),
        transport=cluster,
        sleep=lambda _delay: None,
    )
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture
def manager(client: Client) -> IndexManager:
    return IndexManager(client)


@pytest.fixture
def catalog() -> list[dict[str, Any]]:
    """A small, realistic catalogue: strings, money, dates, arrays, nesting."""

    rows: list[dict[str, Any]] = []
    for index in range(10):
        rows.append(
            {
                "sku": f"SKU-{index:03d}",
                "name": f"Widget number {index}",
                "description": (
                    "A generous description of the widget, long enough that it should be "
                    "indexed as full text rather than as an exact keyword value."
                ),
                "price": round(9.99 + index, 2),
                "quantity": index * 7,
                "in_stock": index % 3 != 0,
                "created_at": f"2026-01-{index + 1:02d}T09:30:00Z",
                "tags": ["widget", f"batch-{index % 2}"],
                "supplier": {"name": f"Supplier {index % 3}", "country": "PT"},
            }
        )
    return rows
