#!/usr/bin/env python3
"""Index a CSV file end to end: infer the mapping, load it, read it back.

Runs offline against an in-memory cluster by default, so you can see the whole
flow without installing Elasticsearch. Point it at a real cluster with:

    ESIB_BASE_URL=http://localhost:9200 python examples/index_csv_catalog.py --live

The CSV is written to a temporary directory so the script is self-contained.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

from esib import (
    Client,
    Config,
    FakeCluster,
    IndexManager,
    infer_mappings,
    iter_documents,
    scan,
)

CATALOG = """sku,name,category,price,in_stock,released
sku-1001,Aeron chair,seating,1299.00,true,2024-03-01
sku-1002,Standing desk,desks,749.50,true,2024-04-18
sku-1003,Monitor arm,accessories,189.00,false,2023-11-02
sku-1004,Footrest,accessories,89.00,true,2024-09-12
sku-1005,Task lamp,accessories,59.00,true,2025-01-06
"""

ALIAS = "catalog"


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
    parser.add_argument("--csv", help="index this file instead of the built-in sample")
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory() as directory:
        source = Path(args.csv) if args.csv else Path(directory) / "catalog.csv"
        if not args.csv:
            source.write_text(CATALOG, encoding="utf-8")

        documents = list(iter_documents(source))
        inferred = infer_mappings(documents)
        print(f"read {len(documents)} rows from {source.name}")
        print(f"inferred {inferred.field_count} fields from {inferred.documents_sampled} documents")
        for name in ("price", "released"):
            print(f"  {name:<10}{inferred.properties[name]}")

        client = make_client(args.live)
        try:
            manager = IndexManager(client)
            report = manager.build(ALIAS, documents, id_field="sku")
            print(
                f"built {report.index}: {report.loaded}/{report.documents} documents "
                f"in {report.batches} bulk {'request' if report.batches == 1 else 'requests'} "
                f"({report.took_ms} ms)"
            )
            if report.failures:
                for failure in report.failures:
                    print(f"  rejected {failure['id']}: {failure['type']} — {failure['reason']}")

            for page in scan(client, ALIAS, size=2):
                print(f"  page {page.number}: {', '.join(page.ids)}")
            print(f"alias {ALIAS} serves {manager.count(ALIAS)} documents")
        finally:
            client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
