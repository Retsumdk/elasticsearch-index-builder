"""The `esib` command line: inspect a mapping, load it, rebuild an alias."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any, TextIO

from . import __version__
from .bulk import BulkIndexer
from .client import Client
from .config import Config
from .demo import run as run_demo
from .errors import BulkError, ConfigError, ESError
from .indices import IndexManager
from .mapping import default_settings, infer_mappings
from .search import count, scan, search_after_scan
from .sources import iter_documents

__all__ = ["build_parser", "main"]

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_INTERRUPT = 130


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="esib",
        description="Build Elasticsearch indices from files: infer, load, swap, verify.",
    )
    parser.add_argument("--version", action="version", version=f"esib {__version__}")
    parser.add_argument("--config", metavar="FILE", help="JSON config file")
    parser.add_argument("--base-url", metavar="URL", help="cluster URL (overrides config)")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("-v", "--verbose", action="store_true", help="show request statistics")

    commands = parser.add_subparsers(dest="command", required=True)

    mapping = commands.add_parser("mapping", help="infer a mapping from sample documents")
    mapping.add_argument("source", help="file or directory of documents")
    mapping.add_argument("--limit", type=int, default=1000, help="documents to sample")
    mapping.add_argument("--dynamic", default="false", choices=["false", "true", "strict"])
    mapping.set_defaults(handler=_cmd_mapping)

    create = commands.add_parser("create", help="create an index from a file's documents")
    create.add_argument("index")
    create.add_argument("--source", required=True)
    create.add_argument("--id-field", default=None)
    create.add_argument("--shards", type=int, default=1)
    create.add_argument("--replicas", type=int, default=1)
    create.add_argument("--load", action="store_true", help="load the documents too")
    create.add_argument("--batch-size", type=int, default=500)
    create.set_defaults(handler=_cmd_create)

    load = commands.add_parser("load", help="bulk-load documents into an existing index")
    load.add_argument("index")
    load.add_argument("--source", required=True)
    load.add_argument("--id-field", default=None)
    load.add_argument("--batch-size", type=int, default=500)
    load.add_argument("--refresh", action="store_true")
    load.add_argument(
        "--tolerate-errors",
        action="store_true",
        help="report rejected documents instead of failing the run",
    )
    load.set_defaults(handler=_cmd_load)

    rebuild = commands.add_parser(
        "rebuild", help="build a new index and flip an alias to it (zero downtime)"
    )
    rebuild.add_argument("alias")
    rebuild.add_argument("--source", required=True)
    rebuild.add_argument("--id-field", default=None)
    rebuild.add_argument("--batch-size", type=int, default=500)
    rebuild.add_argument("--keep-previous", action="store_true")
    rebuild.add_argument("--dry-run", action="store_true")
    rebuild.set_defaults(handler=_cmd_rebuild)

    search = commands.add_parser("search", help="read an index back")
    search.add_argument("index")
    search.add_argument("--size", type=int, default=100)
    search.add_argument("--pages", type=int, default=1)
    search.add_argument("--sort", action="append", default=None, help="field:asc|desc")
    search.set_defaults(handler=_cmd_search)

    count_cmd = commands.add_parser("count", help="document count of an index or alias")
    count_cmd.add_argument("index")
    count_cmd.set_defaults(handler=_cmd_count)

    health = commands.add_parser("health", help="cluster health")
    health.add_argument("--wait-for-status", default=None)
    health.set_defaults(handler=_cmd_health)

    config = commands.add_parser("config", help="print the resolved settings (no secrets)")
    config.set_defaults(handler=_cmd_config)

    demo = commands.add_parser("demo", help="run the offline demo")
    demo.set_defaults(handler=_cmd_demo)

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stream: TextIO | None = None,
    client: Client | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    args.client = client
    out = stream if stream is not None else sys.stdout
    args.client = client
    try:
        return int(args.handler(args, out))
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPT
    except ConfigError as exc:
        _fail(out, args, exc, EXIT_USAGE)
        return EXIT_USAGE
    except ESError as exc:
        _fail(out, args, exc, EXIT_ERROR)
        return EXIT_ERROR
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR


def _fail(out: TextIO, args: argparse.Namespace, exc: ESError, code: int) -> None:
    if getattr(args, "json", False):
        out.write(json.dumps(exc.as_dict(), indent=2) + "\n")
        return
    print(f"error: {exc}", file=sys.stderr)
    hint = exc.hint()
    if hint:
        print(f"hint:  {hint}", file=sys.stderr)


def _client(args: argparse.Namespace) -> Client:
    changes: dict[str, Any] = {}
    if args.base_url:
        changes["base_url"] = args.base_url
    config = Config.load(args.config).with_changes(**changes)
    injected = getattr(args, "client", None)
    if isinstance(injected, Client):
        injected.config = config
        return injected
    return Client(config)


def _emit(out: TextIO, args: argparse.Namespace, payload: dict[str, Any], text: str) -> None:
    if args.json:
        out.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    else:
        out.write(text.rstrip("\n") + "\n")


def _stats_suffix(args: argparse.Namespace, client: Client) -> str:
    if not args.verbose:
        return ""
    stats = client.stats.as_dict()
    return (
        f"\n   {stats['requests']} requests, {stats['retries']} retries, "
        f"{stats['bytes_sent']} bytes sent, {stats['bytes_received']} bytes received"
    )


def _cmd_mapping(args: argparse.Namespace, out: TextIO) -> int:
    documents = list(iter_documents(args.source, limit=args.limit))
    inferred = infer_mappings(documents)
    payload = {
        "mappings": inferred.mappings(dynamic=args.dynamic),
        "settings": default_settings(),
        "field_count": inferred.field_count,
        "documents_sampled": inferred.documents_sampled,
        "warnings": list(inferred.warnings),
    }
    if args.json:
        out.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        return EXIT_OK
    for name, spec in inferred.properties.items():
        out.write(f"{name:<24}{spec.get('type', 'object')}\n")
    out.write(f"\n{inferred.field_count} fields from {inferred.documents_sampled} documents\n")
    for warning in inferred.warnings:
        out.write(f"warning: {warning}\n")
    return EXIT_OK


def _cmd_create(args: argparse.Namespace, out: TextIO) -> int:
    client = _client(args)
    try:
        documents = list(iter_documents(args.source))
        inferred = infer_mappings(documents)
        manager = IndexManager(client)
        created = manager.create(
            args.index,
            mappings=inferred.mappings(),
            settings=default_settings(shards=args.shards, replicas=args.replicas),
        )
        loaded = 0
        if args.load and created:
            indexed = BulkIndexer(client, batch_size=args.batch_size).load(
                args.index, documents, id_field=args.id_field
            )
            manager.refresh(args.index)
            loaded = indexed.indexed
        payload = {"index": args.index, "created": created, "loaded": loaded}
        text = (
            f"{'created' if created else 'already exists'}: {args.index}"
            f"{f', loaded {loaded} documents' if loaded else ''}"
            f"{_stats_suffix(args, client)}"
        )
        _emit(out, args, payload, text)
        return EXIT_OK
    finally:
        client.close()


def _cmd_load(args: argparse.Namespace, out: TextIO) -> int:
    client = _client(args)
    try:
        outcome = BulkIndexer(client, batch_size=args.batch_size).load(
            args.index,
            iter_documents(args.source),
            id_field=args.id_field,
            refresh=args.refresh,
            on_failure="collect",
        )
        text = (
            f"{outcome.indexed} indexed ({outcome.created} created, {outcome.updated} updated), "
            f"{outcome.failed} rejected, {outcome.batches} requests"
            f"{_stats_suffix(args, client)}"
        )
        _emit(out, args, outcome.summary(), text)
        if not outcome.ok and not args.tolerate_errors:
            raise BulkError(
                f"{outcome.failed} of {outcome.documents} documents were rejected",
                failures=outcome.failures,
            )
        return EXIT_OK if outcome.ok else EXIT_ERROR
    finally:
        client.close()


def _cmd_rebuild(args: argparse.Namespace, out: TextIO) -> int:
    client = _client(args)
    try:
        manager = IndexManager(client)
        report = manager.build(
            args.alias,
            iter_documents(args.source),
            id_field=args.id_field,
            batch_size=args.batch_size,
            delete_previous=not args.keep_previous,
            dry_run=args.dry_run,
        )
        text = (
            f"{report.index}: {report.loaded}/{report.documents} documents, "
            f"alias {report.alias} swapped={report.swapped}, "
            f"{len(report.deleted)} old index removed{_stats_suffix(args, client)}"
            if not report.dry_run
            else f"dry run: would build {report.index} from {report.documents} documents"
        )
        for warning in report.warnings:
            text += f"\n   warning: {warning}"
        _emit(out, args, report.as_dict(), text)
        return EXIT_OK if report.ok else EXIT_ERROR
    finally:
        client.close()


def _parse_sort(values: Sequence[str] | None) -> list[dict[str, Any]] | None:
    if not values:
        return None
    sort: list[dict[str, Any]] = []
    for value in values:
        field, _, direction = value.partition(":")
        order = direction.lower() or "asc"
        if order not in ("asc", "desc"):
            raise ConfigError(f"sort direction must be asc or desc: {value!r}", field="sort")
        sort.append({field: order})
    return sort


def _cmd_search(args: argparse.Namespace, out: TextIO) -> int:
    client = _client(args)
    try:
        sort = _parse_sort(args.sort)
        pages = (
            search_after_scan(client, args.index, sort=sort or [], size=args.size)
            if sort
            else scan(client, args.index, size=args.size)
        )
        emitted: list[dict[str, Any]] = []
        for page in pages:
            emitted.extend(dict(hit) for hit in page.hits)
            if page.number >= args.pages:
                break
        if args.json:
            out.write(json.dumps({"hits": emitted, "count": len(emitted)}, indent=2) + "\n")
            return EXIT_OK
        for hit in emitted:
            out.write(f"{hit.get('_id')}\t{json.dumps(hit.get('_source'), sort_keys=True)}\n")
        out.write(f"{len(emitted)} documents{_stats_suffix(args, client)}\n")
        return EXIT_OK
    finally:
        client.close()


def _cmd_count(args: argparse.Namespace, out: TextIO) -> int:
    client = _client(args)
    try:
        total = count(client, args.index)
        _emit(out, args, {"index": args.index, "count": total}, f"{args.index}: {total}")
        return EXIT_OK
    finally:
        client.close()


def _cmd_health(args: argparse.Namespace, out: TextIO) -> int:
    client = _client(args)
    try:
        health = client.health(wait_for_status=args.wait_for_status)
        name = health.get("cluster_name", "cluster")
        status = health.get("status", "unknown")
        _emit(out, args, health, f"{name}: {status}")
        return EXIT_OK
    finally:
        client.close()


def _cmd_config(args: argparse.Namespace, out: TextIO) -> int:
    config = Config.load(args.config)
    if args.base_url:
        config = config.with_changes(base_url=args.base_url)
    payload = config.redacted()
    if args.json:
        out.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    else:
        for key, value in payload.items():
            out.write(f"{key:<16}{value}\n")
    return EXIT_OK


def _cmd_demo(args: argparse.Namespace, out: TextIO) -> int:
    return run_demo(out)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
