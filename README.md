# elasticsearch-index-builder

[![CI](https://github.com/Retsumdk/elasticsearch-index-builder/actions/workflows/ci.yml/badge.svg)](https://github.com/Retsumdk/elasticsearch-index-builder/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Build an Elasticsearch index from ordinary files — infer the mapping, load the documents, swap the alias, verify the result — in one command, with no runtime dependencies.

```
esib rebuild products --source catalog.csv --id-field sku
```

```
products-20261124T153012Z: 4,811/4,812 documents, alias products swapped=True, 1 old index removed
   rejected sku-9184: mapper_parsing_exception — failed to parse field [price] of type [double]: value of type [str] is not accepted
```

## The problem

Getting documents into Elasticsearch correctly is a series of small decisions that are easy to get wrong, and the failures are quiet:

- **A bulk request returns HTTP 200 when half its documents were rejected.** The error is inside `items[]`, one entry per document, and the request-level status says nothing.
- **A rejected document fails for one of two very different reasons.** The cluster was busy (`429`, `es_rejected_execution_exception`) — retry it. Or the value does not fit the mapping (`mapper_parsing_exception`) — never retry it. Retrying both wastes an hour; retrying neither loses documents.
- **NDJSON framing is unforgiving.** A bulk body that does not end in a newline is a 400, and the error does not mention the missing newline.
- **`from + size` stops at 10,000.** Reading an index back means `search_after` or a point-in-time, and `search_after` silently repeats pages if the sort is not unique.
- **Reindexing in place means minutes of half-correct answers.** The zero-downtime pattern is: build a new versioned index, load it, restore production settings, then move the alias in one atomic request. Hand-rolled scripts usually skip the alias and stop the world instead.

Most teams solve this by adopting the vendor client plus a bespoke loader, or by pulling in a reindexing service. Both are more moving parts than the job needs.

## The solution

`elasticsearch-index-builder` is one small library and one CLI that does the whole sequence, and treats the failure modes above as first-class:

| Failure mode | What this library does |
| --- | --- |
| 200 with rejected items | Parses `items[]`, counts `created`/`updated`/`deleted`/`noop`, and surfaces every rejection with its `_id`, status and reason |
| Retryable vs permanent | Retries only `429`/`5xx`/`es_rejected_execution_exception`-class items, per document, with a bounded number of rounds — and reports the rest |
| Missing trailing newline | `encode_bulk` always frames correctly, and the shipped fake cluster rejects a body that does not (so the test suite proves it) |
| Pagination limits | `scan()` uses a point-in-time plus `search_after`, closes the PIT even when you stop early, and raises on a cursor that stops advancing |
| Downtime during reindex | `build()` stages a versioned index, loads it with `refresh_interval: -1` and `number_of_replicas: 0`, restores the real settings, then swaps the alias atomically |
| Mapping surprises | Infers a conservative mapping from the documents themselves, and warns instead of guessing when the samples disagree |

It has **zero runtime dependencies** — the HTTP layer is `urllib`, the config is JSON plus environment variables, the CLI is `argparse`. It installs anywhere Python 3.11 runs, including a build box that cannot reach a package index.

## How it works

```
 files (.ndjson .json .csv .tsv .gz)
        │
        ▼
  sources.iter_documents ──► mapping.infer_mappings ──► indices.IndexManager.build
        │                        │                          │
        │                        │                          ├─ create versioned index
        │                        │                          ├─ bulk.load (batched, item-level retry)
        │                        │                          ├─ put_settings → refresh → count
        │                        │                          └─ POST /_aliases (add new + remove old)
        ▼                        ▼                          ▼
  typed rows            properties + warnings        BuildReport
                                                        │
                                          search.scan ────┘  PIT + search_after
```

**One HTTP chokepoint.** `Client.send` is the only place a request leaves the process. It retries transport errors and retryable statuses, honours `Retry-After`, counts bytes and requests, and returns the final response verbatim. `Client.raise_for` maps that response onto a typed error (`AuthenticationError`, `RateLimited`, `PayloadTooLarge`, `ConflictError`, …) each carrying an actionable `hint()`.

**Errors are a taxonomy, not a string.** `esib.errors` maps Elasticsearch's `error.type` and HTTP status onto exceptions, so `except NotFoundError` works the way you expect and `BulkFailure.error_type` tells you whether the fix is a mapping change or a retry.

**Mapping inference is conservative.** A field that looks like an identifier becomes `keyword`; prose becomes `text` with a `keyword` sub-field capped by `ignore_above`; ISO-8601 strings become `date`; `long` and `double` widen to `double`. When the samples disagree — numbers and strings in one field, an object that is sometimes a scalar, a field that is only ever null, a key containing a dot — you get a warning naming the field, not a silent guess. `dynamic` defaults to `false` so a later load cannot reshape a mapping you reviewed.

**Bulk loading is a stream, not a list.** Documents are encoded and flushed in batches bounded by count *and* bytes, so a 2 GB file loads with megabytes of memory. Item-level failures are retried by *resending only the rejected documents*, with a bounded round count.

**The alias swap is the commit.** `POST /_aliases` carries both the `add` and the `remove`, in one request: readers never see an alias that resolves to nothing, or to two indices. `build()` restores `refresh_interval`/`number_of_replicas` before the swap, so the version that goes live is the version you configured, not the loader's temporarily-cheap settings.

**A fake cluster ships with it.** `esib.testing.FakeCluster` implements the endpoints the builder uses, with Elasticsearch's strictness where it matters (mapping coercion, bulk framing, PIT paging, alias atomicity). It backs the whole test suite and `esib demo`, and you can use it to test your own pipelines without a container.

## Getting started

```bash
git clone https://github.com/Retsumdk/elasticsearch-index-builder.git
cd elasticsearch-index-builder
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

esib demo          # a full build against the fake cluster, no network
pytest -q          # 242 tests
```

Point it at a real cluster:

```bash
pip install .                                    # no dependencies to resolve
export ESIB_BASE_URL="https://localhost:9200"
export ESIB_API_KEY="..."                        # or ESIB_USERNAME / ESIB_PASSWORD
esib health
```

Requires Python 3.11+. Elasticsearch 7.10+ (the point-in-time API); `search_after_scan` covers older clusters.

## Using the CLI

```bash
# what mapping would these files produce? (no cluster needed)
esib mapping catalog.csv --limit 500

# create an index from a file's own documents, and load them
esib create products --source catalog.ndjson --load --id-field sku

# append to an existing index, tolerating and reporting rejected documents
esib load products --source new.ndjson --id-field sku --tolerate-errors

# zero-downtime rebuild behind an alias, keeping the previous index for rollback
esib rebuild products --source catalog.csv --id-field sku --keep-previous

# read it back: PIT paging, one page at a time
esib search products --size 100 --pages 3

# counts, cluster health, and the resolved settings with secrets redacted
esib count products
esib health --wait-for-status yellow
esib config
```

Global flags: `--json` (machine-readable output on every command, including errors), `-v` (request/byte statistics), `--config FILE`, `--base-url URL`. Exit codes are stable: `0` success, `1` failure, `2` usage/validation, `130` interrupted.

`esib demo` output, byte-identical to the committed `demo-output.txt`:

```
esib demo — a complete index build, no cluster and no network

1. infer a mapping from 7 sample documents
   category    keyword
   in_stock    boolean
   legacy_id   text + keyword sub-field
   name        keyword
   price       double
   released    date (ISO-8601)
   sku         keyword
   7 fields from 7 documents
   warning: field 'legacy_id' holds both numbers and strings; mapped as text — sort or aggregate on the `.keyword` sub-field instead

2. build a versioned index and point the alias at it
   index products-20260101T120000Z
   7 of 8 documents accepted in 2 bulk requests
   rejected sku-1008: mapper_parsing_exception — failed to parse field [price] of type [double]: value of type [str] is not accepted
   1 request retried after 429 (Retry-After honoured)
   alias products -> products-20260101T120000Z

3. read the alias back with a point-in-time scan
   page 1: 4 hits (sku-1001 … sku-1004)
   page 2: 3 hits (sku-1005 … sku-1007)
   7 documents reachable through the alias

4. re-run the same load to show it is idempotent
   7 updated, 0 created, 0 rejected
   count is still 7

done: 7 documents searchable through 'products'
```

## Using the library

```python
from esib import Client, Config, IndexManager, infer_mappings, iter_documents, scan

client = Client(Config.load())                 # defaults < config.json < ESIB_* env
manager = IndexManager(client)

documents = list(iter_documents("catalog.csv")) # csv/tsv/json/ndjson, .gz, directories
inferred = infer_mappings(documents)           # conservative, warns instead of guessing
print(inferred.field_count, "fields", inferred.warnings)

report = manager.build(
    "products",                                # the alias clients query
    documents,
    id_field="sku",                            # makes reloads idempotent
    min_docs=100,                              # refuse to swap onto an empty load
)
print(report.index, report.loaded, report.swapped, report.ok)
for failure in report.failures:
    print(failure["id"], failure["type"], failure["reason"])

for page in scan(client, "products", size=1000):
    for hit in page.sources:
        ...
```

Every public name is exported from `esib`:

- `Client`, `Config`, `ClientStats` — connection, layering, counters
- `IndexManager`, `BuildReport` — create/load/swap/verify, and the audit record
- `BulkIndexer`, `BulkOutcome`, `encode_bulk` — batched loading with item-level retry
- `infer_mappings`, `InferredMapping`, `default_settings` — mapping from real documents
- `scan`, `search_after_scan`, `count`, `Page` — reading back without pagination traps
- `iter_documents`, `iter_paths`, `detect_format`, `coerce_value` — file sources
- `FakeCluster`, `Fault`, `StubTransport` — an in-memory cluster and a scripted transport
- `RetryPolicy`, `Retrier` — the retry policy on its own, with injectable clock and jitter
- the error taxonomy: `ESError`, `NotFoundError`, `RateLimited`, `BulkError`, …

### Configuration

`Config` layers defaults → JSON file → environment. Unknown keys are an error, not a silent no-op.

```json
{
  "base_url": "https://localhost:9200",
  "batch_size": 500,
  "batch_bytes": 5242880,
  "max_attempts": 4,
  "timeout": 30.0
}
```

| Field | Env | Default |
| --- | --- | --- |
| `base_url` | `ESIB_BASE_URL` | `http://localhost:9200` |
| `api_key` | `ESIB_API_KEY` | – (`ApiKey …`; a `Bearer …` prefix is respected) |
| `username` / `password` | `ESIB_USERNAME` / `ESIB_PASSWORD` | – (basic auth) |
| `timeout` | `ESIB_TIMEOUT` | `30.0` |
| `batch_size` | `ESIB_BATCH_SIZE` | `500` |
| `batch_bytes` | `ESIB_BATCH_BYTES` | `5242880` |
| `max_attempts` | `ESIB_MAX_ATTEMPTS` | `4` |
| `ca_bundle` | `ESIB_CA_BUNDLE` | – (system CA store) |

## Guarantees and limits

- **Bulk framing is always correct**, including the trailing newline; the bundled fake cluster rejects a body that omits it.
- **Retries are bounded**: `max_attempts` per request, `max_item_rounds` per batch, and only statuses/types known to be transient. `Retry-After` always wins over computed backoff.
- **The alias swap is one request**, so it cannot land half-applied.
- **An empty load is refused** (`min_docs`, default 1): the staged index is deleted and the alias is left alone.
- **A document without the configured `id_field` is a reported failure**, not an auto-generated id — otherwise a re-run would duplicate it.
- **Not included:** no `scroll` helper (the PIT path replaces it), no ingest-pipeline management, no ILM policy management, no SQL/DSL query builder, and no connection pooling (each request opens its own `urllib` connection). Mapping inference is heuristic: it reads the documents, not your intent — review the warnings, then commit the mapping to your repo.
- **Compatibility:** point-in-time search and `search_after` need Elasticsearch 7.10+. The loader posts to cluster-level `/_bulk` with `_index` in every action line (the bundled fake also accepts `/{index}/_bulk`), which is the form OpenSearch accepts too; the alias and PIT semantics are Elasticsearch's.

## Tests

```bash
pytest -q                        # 242 tests, 92% branch coverage
pytest --cov=esib --cov-report=term-missing
bash bin/verify.sh               # the full gate: lint, types, tests, demo diff, wheel
```

The suite includes a real-HTTP end-to-end run: `tests/test_live.py` starts a server in a thread, points `UrllibTransport` at it, and exercises bulk loading, alias swaps, PIT paging and a retried `429` over a real socket.

## Examples

- [`examples/index_csv_catalog.py`](examples/index_csv_catalog.py) — CSV → inferred mapping → versioned build → paged read-back. Runs offline against the fake cluster unless you pass `--live`.
- [`examples/zero_downtime_rebuild.py`](examples/zero_downtime_rebuild.py) — the reindex story end to end: first build, dry run, second build, rollback window.

```bash
python examples/index_csv_catalog.py
python examples/zero_downtime_rebuild.py --live   # against ESIB_BASE_URL
```

## Repository layout

```
src/esib/          the library (transport, client, mapping, bulk, indices, search, sources, cli)
tests/             242 tests: unit, fake-cluster behaviour, and a live HTTP end-to-end run
examples/          two runnable scripts, offline by default
bin/verify.sh      lint + types + tests + demo diff + wheel build, in one command
demo-output.txt    the committed demo transcript, byte-compared in CI
```

## License

MIT © 2026 Retsumdk
