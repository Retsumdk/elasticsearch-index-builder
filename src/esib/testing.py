"""An in-memory cluster that speaks the subset of the API this library uses.

It exists so a pipeline can be tested end to end — mapping inference, batching,
partial bulk failure, retry, alias swap, PIT paging — without a running
Elasticsearch. It is also the transport behind ``esib demo``, which is why the
demo output is byte-for-byte reproducible.

The fake is deliberately strict where Elasticsearch is strict: unknown fields on
a ``dynamic: strict`` mapping and type mismatches are rejected with
``mapper_parsing_exception``, so a broken mapping fails in tests, not in
production.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .errors import TransportError
from .transport import Response

__all__ = ["FakeCluster", "Fault"]


@dataclass(slots=True)
class Fault:
    """One scripted failure, consumed by the next matching request."""

    status: int
    reason: str = "scripted failure"
    error_type: str = "internal_server_error"
    method: str | None = None
    path_contains: str | None = None
    retry_after: float | None = None
    times: int = 1


@dataclass
class _Index:
    settings: dict[str, Any] = field(default_factory=dict)
    mappings: dict[str, Any] = field(default_factory=dict)
    documents: dict[str, dict[str, Any]] = field(default_factory=dict)
    pit: dict[str, int] = field(default_factory=dict)


_KIND_TO_TYPES = {
    "keyword": {"keyword"},
    "text": {"text"},
    "long": {"long", "integer", "short", "byte"},
    "double": {"double", "float", "half_float", "scaled_float"},
    "boolean": {"boolean"},
    "date": {"date", "date_nanos"},
    "object": {"object", "nested"},
}


def _python_kind(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "keyword"
    if isinstance(value, dict):
        return "object"
    return "keyword"


class FakeCluster:
    """A Transport implementation backed by an in-memory document store."""

    def __init__(self, *, faults: list[Fault] | None = None) -> None:
        self.indices: dict[str, _Index] = {}
        self.aliases: dict[str, set[str]] = {}
        self.faults: list[Fault] = list(faults or [])
        self.last_json_body: dict[str, Any] = {}
        self.bodies: list[dict[str, Any]] = []
        self.requests: list[tuple[str, str]] = []
        self._sequence = 0
        self._pit_sequence = 0
        self._generated = 0

    def add_fault(
        self,
        status: int,
        *,
        reason: str = "scripted failure",
        error_type: str = "internal_server_error",
        method: str | None = None,
        path_contains: str | None = None,
        retry_after: float | None = None,
        times: int = 1,
    ) -> Fault:
        fault = Fault(
            status=status,
            reason=reason,
            error_type=error_type,
            method=method,
            path_contains=path_contains,
            retry_after=retry_after,
            times=times,
        )
        self.faults.append(fault)
        return fault

    def document_count(self, index: str) -> int:
        name = self._resolve(index) or index
        entry = self.indices.get(name)
        return len(entry.documents) if entry else 0

    def _resolve(self, name: str) -> str | None:
        if name in self.indices:
            return name
        targets = self.aliases.get(name)
        if targets:
            return sorted(targets)[0]
        return None

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> Response:
        method = method.upper()
        parts = urlsplit(path)
        route = parts.path or "/"
        if params:
            query = {key: values[0] for key, values in parse_qs(parts.query).items()}
            query.update(dict(params.items()))
        else:
            query = {key: values[0] for key, values in parse_qs(parts.query).items()}
        self.requests.append((method, route))
        parsed = _parse_json(body)
        self.last_json_body = parsed if isinstance(parsed, dict) else {}
        self.bodies.append(dict(self.last_json_body))

        fault = self._take_fault(method, route)
        if fault is not None:
            headers = {}
            if fault.retry_after is not None:
                headers["Retry-After"] = str(fault.retry_after)
            return _error(
                fault.status,
                fault.error_type,
                fault.reason,
                headers=headers,
            )

        try:
            return self._route(method, route, query, body)
        except KeyError as exc:  # pragma: no cover - defensive
            raise TransportError(f"fake cluster cannot route {method} {route}: {exc}") from exc

    def _take_fault(self, method: str, route: str) -> Fault | None:
        for position, fault in enumerate(self.faults):
            if fault.times <= 0:
                continue
            if fault.method and fault.method.upper() != method:
                continue
            if fault.path_contains and fault.path_contains not in route:
                continue
            fault.times -= 1
            self.faults[position] = fault
            return fault
        return None

    def _route(
        self, method: str, route: str, query: dict[str, str], body: bytes | None
    ) -> Response:
        payload = _parse_json(body)
        segments = [segment for segment in route.split("/") if segment]

        if route == "/" and method == "GET":
            return _ok(
                {
                    "name": "fake-node",
                    "cluster_name": "fake-cluster",
                    "version": {"number": "8.13.0", "build_flavor": "default"},
                    "tagline": "You Know, for Search",
                }
            )
        if route == "/_cluster/health":
            return _ok(
                {"status": "green", "number_of_nodes": 1, "active_shards": len(self.indices)}
            )
        if route == "/_bulk":
            return self._bulk(body)
        if route == "/_aliases" and method == "POST":
            return self._alias_actions(payload)
        if route == "/_search" and method == "POST":
            return self._search(None, payload, pit_search=True)
        if route == "/_pit" and method == "DELETE":
            return _ok({"succeeded": True, "num_freed": 1})
        if route == "/_cat/indices":
            return _ok({"indices": sorted(self.indices)})
        if len(segments) == 2 and segments[0] == "_alias":
            return self._alias_lookup(segments[1])

        if not segments:
            return _error(400, "illegal_argument_exception", f"unsupported route {route}")
        name = segments[0]
        rest = segments[1:]

        if not rest:
            return self._index_route(method, name, payload)
        tail = "/".join(rest)
        if tail == "_bulk" and method == "POST":
            return self._bulk(body, default_index=name)
        if tail == "_count":
            resolved = self._resolve(name)
            if resolved is None:
                return _error(404, "index_not_found_exception", f"no such index [{name}]")
            return _ok({"count": len(self.indices[resolved].documents), "_shards": {"total": 1}})
        if tail == "_refresh":
            return _ok({"_shards": {"total": 1, "successful": 1, "failed": 0}})
        if tail == "_mapping":
            resolved = self._resolve(name)
            if resolved is None:
                return _error(404, "index_not_found_exception", f"no such index [{name}]")
            entry = self.indices[resolved]
            if method == "PUT":
                added = payload.get("properties") if isinstance(payload, dict) else None
                if isinstance(added, dict):
                    properties = entry.mappings.setdefault("properties", {})
                    if isinstance(properties, dict):
                        properties.update(added)
                return _ok({"acknowledged": True})
            return _ok({resolved: {"mappings": entry.mappings}})
        if tail == "_settings" and method == "PUT":
            resolved = self._resolve(name)
            if resolved is None:
                return _error(404, "index_not_found_exception", f"no such index [{name}]")
            self.indices[resolved].settings.update(_parse_settings(payload))
            return _ok({"acknowledged": True})
        if tail == "_pit":
            resolved = self._resolve(name)
            if resolved is None:
                return _error(404, "index_not_found_exception", f"no such index [{name}]")
            self._pit_sequence += 1
            pit_id = f"pit-{self._pit_sequence}"
            self.indices[resolved].pit[pit_id] = 0
            return _ok({"id": pit_id})
        if tail == "_search":
            return self._search(name, payload, pit_search=False)
        return _error(400, "illegal_argument_exception", f"unsupported route {route}")

    def _index_route(self, method: str, name: str, payload: Any) -> Response:
        if method == "HEAD":
            return Response(
                status=200 if (name in self.indices or name in self.aliases) else 404,
                headers={},
                body=b"",
            )
        if method == "PUT":
            if name in self.aliases:
                return _error(400, "invalid_index_name_exception", f"[{name}] is an alias")
            if name in self.indices:
                return _error(
                    400, "resource_already_exists_exception", f"index [{name}] already exists"
                )
            entry = _Index()
            if isinstance(payload, dict):
                entry.settings = _parse_settings(payload.get("settings"))
                mappings = payload.get("mappings")
                if isinstance(mappings, dict):
                    entry.mappings = dict(mappings)
            self.indices[name] = entry
            return _ok({"acknowledged": True, "shards_acknowledged": True, "index": name})
        if method == "DELETE":
            if name in self.aliases:
                return _error(400, "illegal_argument_exception", "cannot delete an alias")
            if name not in self.indices:
                return _error(404, "index_not_found_exception", f"no such index [{name}]")
            del self.indices[name]
            for targets in self.aliases.values():
                targets.discard(name)
            return _ok({"acknowledged": True})
        if method == "GET":
            resolved = self._resolve(name)
            if resolved is None:
                return _error(404, "index_not_found_exception", f"no such index [{name}]")
            entry = self.indices[resolved]
            return _ok(
                {
                    resolved: {
                        "aliases": {},
                        "mappings": entry.mappings,
                        "settings": {"index": entry.settings},
                    }
                }
            )
        return _error(405, "illegal_argument_exception", f"{method} not supported on an index")

    def _alias_lookup(self, alias: str) -> Response:
        targets = self.aliases.get(alias)
        if not targets:
            return _error(404, "aliases_not_found_exception", f"alias [{alias}] has no indices")
        return _ok({name: {"aliases": {alias: {}}} for name in sorted(targets)})

    def _alias_actions(self, payload: Any) -> Response:
        if not isinstance(payload, dict) or not isinstance(payload.get("actions"), list):
            return _error(400, "illegal_argument_exception", "actions must be an array")
        for action in payload["actions"]:
            if not isinstance(action, dict):
                continue
            for kind, detail in action.items():
                if not isinstance(detail, dict):
                    continue
                index = detail.get("index")
                alias = detail.get("alias")
                if not isinstance(index, str) or not isinstance(alias, str):
                    return _error(400, "illegal_argument_exception", "index and alias are required")
                if kind == "add":
                    if index not in self.indices:
                        return _error(404, "index_not_found_exception", f"no such index [{index}]")
                    self.aliases.setdefault(alias, set()).add(index)
                elif kind == "remove":
                    self.aliases.setdefault(alias, set()).discard(index)
                else:
                    return _error(
                        400, "illegal_argument_exception", f"unknown alias action {kind!r}"
                    )
        return _ok({"acknowledged": True})

    def _bulk(self, body: bytes | None, default_index: str | None = None) -> Response:
        if not body:
            return _error(400, "illegal_argument_exception", "request body is required")
        try:
            lines = [line for line in body.split(b"\n") if line]
        except AttributeError:  # pragma: no cover - defensive
            return _error(400, "illegal_argument_exception", "body must be bytes")
        if not lines:
            return _error(400, "illegal_argument_exception", "no requests added")
        if not body.endswith(b"\n"):
            return _error(
                400,
                "illegal_argument_exception",
                "The bulk request must be terminated by a newline [\\n]",
            )

        items: list[dict[str, Any]] = []
        errors = False
        position = 0
        while position < len(lines):
            try:
                action = json.loads(lines[position])
            except json.JSONDecodeError:
                return _error(400, "json_parse_exception", f"bad action line {position + 1}")
            if not isinstance(action, dict) or len(action) != 1:
                return _error(400, "illegal_argument_exception", "action line must hold one action")
            kind = next(iter(action))
            meta = action[kind]
            if kind not in ("index", "create", "delete", "update"):
                return _error(400, "illegal_argument_exception", f"unknown action [{kind}]")
            step = 1 if kind == "delete" else 2
            source: dict[str, Any] = {}
            if step == 2:
                if position + 1 >= len(lines):
                    return _error(
                        400, "illegal_argument_exception", f"[{kind}] is missing its source line"
                    )
                try:
                    source = json.loads(lines[position + 1])
                except json.JSONDecodeError:
                    return _error(400, "json_parse_exception", f"bad source line {position + 2}")
            metadata = dict(meta) if isinstance(meta, dict) else {}
            if default_index is not None and not metadata.get("_index"):
                metadata["_index"] = default_index
            result = self._bulk_item(kind, metadata, source)
            if "error" in next(iter(result.values())):
                errors = True
            items.append(result)
            position += step
        return _ok({"took": 1, "errors": errors, "items": items})

    def _bulk_item(self, kind: str, meta: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
        index_name = meta.get("_index")
        if not isinstance(index_name, str):
            return {kind: {"status": 400, "error": _error_body("illegal_argument", "no _index")}}
        resolved = self._resolve(index_name)
        if resolved is None:
            return {
                kind: {
                    "_index": index_name,
                    "status": 404,
                    "error": _error_body(
                        "index_not_found_exception", f"no such index [{index_name}]"
                    ),
                }
            }
        entry = self.indices[resolved]
        doc_id = meta.get("_id")
        if not isinstance(doc_id, str):
            self._generated += 1
            doc_id = f"auto-{self._generated}"
        if kind == "delete":
            if doc_id in entry.documents:
                del entry.documents[doc_id]
                return {
                    kind: {"_index": resolved, "_id": doc_id, "status": 200, "result": "deleted"}
                }
            return {kind: {"_index": resolved, "_id": doc_id, "status": 404, "result": "not_found"}}
        if kind == "create" and doc_id in entry.documents:
            return {
                kind: {
                    "_index": resolved,
                    "_id": doc_id,
                    "status": 409,
                    "error": _error_body(
                        "version_conflict_engine_exception", f"document already exists [{doc_id}]"
                    ),
                }
            }
        if kind == "update":
            if doc_id not in entry.documents:
                return {
                    kind: {
                        "_index": resolved,
                        "_id": doc_id,
                        "status": 404,
                        "error": _error_body("document_missing_exception", f"[{doc_id}]: missing"),
                    }
                }
            merged = {**entry.documents[doc_id], **source.get("doc", {})}
            problem = _mapping_problem(entry.mappings, merged)
            if problem is not None:
                error_type, reason = problem
                return {
                    kind: {
                        "_index": resolved,
                        "_id": doc_id,
                        "status": 400,
                        "error": _error_body(error_type, reason),
                    }
                }
            entry.documents[doc_id] = merged
            return {kind: {"_index": resolved, "_id": doc_id, "status": 200, "result": "updated"}}

        problem = _mapping_problem(entry.mappings, source)
        if problem is not None:
            error_type, reason = problem
            return {
                kind: {
                    "_index": resolved,
                    "_id": doc_id,
                    "status": 400,
                    "error": _error_body(error_type, reason),
                }
            }
        existed = doc_id in entry.documents
        entry.documents[doc_id] = dict(source)
        return {
            kind: {
                "_index": resolved,
                "_id": doc_id,
                "status": 200 if existed else 201,
                "result": "updated" if existed else "created",
            }
        }

    def _search(self, index: str | None, payload: Any, *, pit_search: bool) -> Response:
        body = payload if isinstance(payload, dict) else {}
        pit = body.get("pit")
        if pit_search:
            if not isinstance(pit, dict) or not isinstance(pit.get("id"), str):
                return _error(400, "illegal_argument_exception", "a PIT id is required")
            pit_id = pit["id"]
            target = next(
                (name for name, entry in self.indices.items() if pit_id in entry.pit), None
            )
            if target is None:
                return _error(404, "search_context_missing_exception", f"no PIT [{pit_id}]")
            entry = self.indices[target]
            target_name = target
            offset = entry.pit[pit_id]
        else:
            if index is None:
                return _error(400, "illegal_argument_exception", "an index is required")
            resolved = self._resolve(index)
            if resolved is None:
                return _error(404, "index_not_found_exception", f"no such index [{index}]")
            entry = self.indices[resolved]
            target_name = resolved
            offset = 0

        wanted = body.get("size", 10)
        size = wanted if isinstance(wanted, int) and wanted > 0 else 10
        ordered = sorted(entry.documents.items())
        keyed = [
            ([position, doc_id], doc_id, document)
            for position, (doc_id, document) in enumerate(ordered)
        ]
        cursor = body.get("search_after")
        if isinstance(cursor, list) and cursor:
            keyed = [row for row in keyed if _after(row[0], cursor)]
            window = keyed[:size]
        else:
            window = keyed[offset : offset + size]
        hits = [
            {
                "_index": target_name,
                "_id": doc_id,
                "_source": document,
                "_score": None,
                "sort": key,
            }
            for key, doc_id, document in window
        ]
        reply: dict[str, Any] = {
            "took": 1,
            "timed_out": False,
            "hits": {
                "total": {"value": len(ordered), "relation": "eq"},
                "max_score": None,
                "hits": hits,
            },
        }
        if pit_search and not (isinstance(cursor, list) and cursor):
            entry.pit[pit_id] = offset + len(window)
            reply["pit_id"] = pit_id
        return _ok(reply)


def _after(candidate: list[Any], cursor: list[Any]) -> bool:
    """True when `candidate` sorts strictly after `cursor` (search_after)."""

    for left, right in zip(candidate, cursor, strict=False):
        if left == right:
            continue
        try:
            return bool(left > right)
        except TypeError:  # pragma: no cover - mismatched cursor types
            return str(left) > str(right)
    return False


def _parse_json(body: bytes | None) -> Any:
    if not body:
        return None
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _parse_settings(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    index_block = raw.get("index")
    if isinstance(index_block, dict):
        return dict(index_block)
    return {key: value for key, value in raw.items() if not key.startswith("index.")}


def _error_body(error_type: str, reason: str) -> dict[str, Any]:
    return {"type": error_type, "reason": reason}


def _mapping_problem(mappings: dict[str, Any], document: dict[str, Any]) -> tuple[str, str] | None:
    """Reject what a real mapping would reject: unknown fields under `strict`."""

    properties = mappings.get("properties")
    if not isinstance(properties, dict) or not properties:
        return None
    dynamic = mappings.get("dynamic", "true")
    for field_name, value in document.items():
        spec = properties.get(field_name)
        if not isinstance(spec, dict):
            if dynamic == "strict":
                return (
                    "strict_dynamic_mapping_exception",
                    f"mapping set to strict, dynamic introduction of [{field_name}] is not allowed",
                )
            continue
        declared = spec.get("type")
        if not isinstance(declared, str) or value is None:
            continue
        problem = _coercion_problem(declared, value)
        if problem is not None:
            return (
                "mapper_parsing_exception",
                f"failed to parse field [{field_name}] of type [{declared}]: {problem}",
            )
    return None


_STRING_TYPES = frozenset({"keyword", "text", "wildcard", "ip", "constant_keyword"})
_INTEGER_TYPES = frozenset({"long", "integer", "short", "byte", "unsigned_long"})
_DECIMAL_TYPES = frozenset({"double", "float", "half_float", "scaled_float"})


def _coercion_problem(declared: str, value: Any) -> str | None:
    """What Elasticsearch itself would refuse, coercion included.

    A real cluster stringifies numbers into `text`/`keyword`, parses numeric
    strings into numeric fields and accepts a date string in a `date` field, so
    the fake accepts exactly those and nothing looser.
    """

    if declared in _STRING_TYPES:
        return (
            None
            if not isinstance(value, (dict, list))
            else f"[{type(value).__name__}] is not a string"
        )
    if declared == "boolean":
        if isinstance(value, bool):
            return None
        if isinstance(value, str) and value.lower() in ("true", "false"):
            return None
        return f"value of type [{type(value).__name__}] is not accepted"
    if declared in _INTEGER_TYPES:
        if isinstance(value, bool):
            return "value of type [bool] is not accepted"
        if isinstance(value, int):
            return None
        if isinstance(value, float) and value.is_integer():
            return None
        if isinstance(value, str):
            try:
                int(value.strip())
                return None
            except ValueError:
                return "value of type [str] is not accepted"
        return f"value of type [{type(value).__name__}] is not accepted"
    if declared in _DECIMAL_TYPES:
        if isinstance(value, bool):
            return "value of type [bool] is not accepted"
        if isinstance(value, (int, float)):
            return None
        if isinstance(value, str):
            try:
                float(value.strip())
                return None
            except ValueError:
                return "value of type [str] is not accepted"
        return f"value of type [{type(value).__name__}] is not accepted"
    if declared == "date":
        if isinstance(value, str):
            return None
        if isinstance(value, (int, float)):
            return None
        return f"value of type [{type(value).__name__}] is not accepted"
    if declared == "object":
        return (
            None
            if isinstance(value, dict)
            else f"value of type [{type(value).__name__}] is not accepted"
        )
    return None


def _ok(body: Any) -> Response:
    return Response(
        status=200, headers={"content-type": "application/json"}, body=json.dumps(body).encode()
    )


def _error(
    status: int, error_type: str, reason: str, *, headers: dict[str, str] | None = None
) -> Response:
    payload = {"error": {"type": error_type, "reason": reason}, "status": status}
    return Response(
        status=status,
        headers={"content-type": "application/json", **dict(headers or {})},
        body=json.dumps(payload).encode(),
    )
