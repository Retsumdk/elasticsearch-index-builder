#!/usr/bin/env python3
"""Rebuild a live index without a moment of downtime, and keep a rollback.

The pattern: load into a new versioned index, restore production settings,
refresh, then move the alias in one atomic request. Readers keep querying the
alias the whole time — it never resolves to nothing and never to two indices.

Run it offline (the default) to see every step, or against a real cluster:

    ESIB_BASE_URL=http://localhost:9200 python examples/zero_downtime_rebuild.py --live
"""

from __future__ import annotations

import argparse
import os
import sys

from esib import Client, Config, FakeCluster, IndexManager

ALIAS = "orders"
DOCUMENTS = [
    {"order_id": f"ord-{n:04d}", "total": n * 12.5, "status": "paid", "placed": "2026-02-01"}
    for n in range(1, 41)
]


def make_client(live: bool) -> Client:
    if live:
        base_url = os.environ.get("ESIB_BASE_URL", "http://localhost:9200")
        return Client(Config.load().with_changes(base_url=base_url))
    return Client(
        Config(base_url="http://in-memory:9200"),
        transport=FakeCluster(),
        sleep=lambda _seconds: None,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="use ESIB_BASE_URL instead of memory")
    parser.add_argument("--keep-previous", action="store_true", help="leave the old index in place")
    args = parser.parse_args(argv)

    client = make_client(args.live)
    try:
        manager = IndexManager(client)

        first = manager.build(ALIAS, DOCUMENTS, id_field="order_id", batch_size=10)
        print(f"first build:  {first.index} ({first.loaded} documents, {first.took_ms} ms)")
        print(f"alias {ALIAS} -> {manager.indices_for_alias(ALIAS)}")

        dry_run = manager.build(ALIAS, DOCUMENTS, id_field="order_id", dry_run=True)
        print(f"dry run:      would create {dry_run.index}, no request that writes")

        second = manager.build(
            ALIAS,
            DOCUMENTS[:30],
            id_field="order_id",
            batch_size=10,
            delete_previous=not args.keep_previous,
            min_docs=1,
        )
        print(f"second build: {second.index} ({second.loaded} documents)")
        print(f"  swapped atomically: {second.swapped}")
        print(f"  previous indices:   {list(second.previous)}")
        print(f"  removed afterwards: {list(second.deleted)}")
        print(f"alias {ALIAS} -> {manager.indices_for_alias(ALIAS)}")
        print(f"readers now see {manager.count(ALIAS)} of {len(DOCUMENTS)} documents")
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
