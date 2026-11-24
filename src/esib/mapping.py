"""Infer an Elasticsearch mapping from real documents.

The inference is deliberately conservative: a field that looks like text becomes
`text` + a `keyword` sub-field, a field that looks like an identifier becomes
`keyword`, and anything the samples disagree about is reported in `warnings`
instead of being silently guessed.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import islice
from typing import Any

__all__ = ["MAX_DISTINCT_SAMPLES", "InferredMapping", "default_settings", "infer_mappings"]

MAX_DISTINCT_SAMPLES = 500
_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$")


def default_settings(
    *, shards: int = 1, replicas: int = 1, refresh_interval: str = "1s"
) -> dict[str, Any]:
    """Index settings that suit a freshly built index."""

    return {
        "number_of_shards": shards,
        "number_of_replicas": replicas,
        "refresh_interval": refresh_interval,
    }


def is_date_string(value: str) -> bool:
    """True for ISO-8601 dates and date-times (the formats ES parses by default)."""

    return bool(_DATE_ONLY.match(value) or _DATE_TIME.match(value))


@dataclass
class _Node:
    kinds: Counter[str] = field(default_factory=Counter)
    values: int = 0
    nulls: int = 0
    string_max_length: int = 0
    string_chars: int = 0
    string_samples: set[str] = field(default_factory=set)
    children: dict[str, _Node] = field(default_factory=dict)

    def child(self, key: str) -> _Node:
        node = self.children.get(key)
        if node is None:
            node = _Node()
            self.children[key] = node
        return node


@dataclass(frozen=True)
class InferredMapping:
    """The result of scanning sample documents."""

    properties: dict[str, Any]
    warnings: tuple[str, ...]
    field_count: int
    documents_sampled: int
    text_fields: tuple[str, ...] = ()
    keyword_fields: tuple[str, ...] = ()
    date_fields: tuple[str, ...] = ()
    numeric_fields: tuple[str, ...] = ()

    def mappings(self, *, dynamic: str = "false") -> dict[str, Any]:
        """The `mappings` block, ready for `PUT /<index>`.

        `dynamic: false` is the default because an inferred mapping should not
        silently grow new fields from a later load; pass ``dynamic="strict"`` in
        development to have unknown fields rejected loudly.
        """

        block: dict[str, Any] = {"dynamic": dynamic, "properties": self.properties}
        return block

    def to_dict(self) -> dict[str, Any]:
        return {
            "mappings": self.mappings(),
            "settings": default_settings(),
            "_meta": {
                "generator": "elasticsearch-index-builder",
                "inferred_from_documents": self.documents_sampled,
                "field_count": self.field_count,
                "warnings": list(self.warnings),
            },
        }


def infer_mappings(
    documents: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int = 1000,
    keyword_max_length: int = 64,
    keyword_ignore_above: int = 256,
    date_detection: bool = True,
) -> InferredMapping:
    """Scan up to `sample_limit` documents and infer a mapping for them."""

    root = _Node()
    warnings: list[str] = []
    seen = 0
    for document in islice(documents, sample_limit):
        _absorb(root, document, "", date_detection=date_detection, warnings=warnings)
        seen += 1

    if seen == 0:
        raise ValueError("no documents to infer a mapping from")

    properties = _resolve(
        root, "", keyword_max_length, keyword_ignore_above, date_detection, warnings
    )
    counts = _collect(properties, "")
    return InferredMapping(
        properties=properties,
        warnings=tuple(dict.fromkeys(warnings)),
        field_count=counts["field_count"],
        documents_sampled=seen,
        text_fields=tuple(counts["text"]),
        keyword_fields=tuple(counts["keyword"]),
        date_fields=tuple(counts["date"]),
        numeric_fields=tuple(counts["numeric"]),
    )


def _absorb(
    node: _Node,
    document: Mapping[str, Any],
    path: str,
    *,
    date_detection: bool,
    warnings: list[str],
) -> None:
    for key, value in document.items():
        name = str(key)
        if "." in name:
            warnings.append(
                f"field {_label(path, name)!r} contains a dot; "
                "Elasticsearch reads that as an object path"
            )
        child = node.child(name)
        _observe(child, value, _label(path, name), date_detection=date_detection, warnings=warnings)


def _observe(
    node: _Node, value: Any, path: str, *, date_detection: bool, warnings: list[str]
) -> None:
    if value is None:
        node.kinds["null"] += 1
        node.nulls += 1
        return
    if isinstance(value, bool):
        node.kinds["boolean"] += 1
        node.values += 1
        return
    if isinstance(value, int):
        node.kinds["long"] += 1
        node.values += 1
        return
    if isinstance(value, float):
        node.kinds["double"] += 1
        node.values += 1
        return
    if isinstance(value, str):
        if date_detection and is_date_string(value):
            node.kinds["date"] += 1
        else:
            node.kinds["string"] += 1
        node.values += 1
        node.string_max_length = max(node.string_max_length, len(value))
        node.string_chars += len(value)
        if len(node.string_samples) < MAX_DISTINCT_SAMPLES:
            node.string_samples.add(value)
        return
    if isinstance(value, Mapping):
        node.kinds["object"] += 1
        node.values += 1
        for key, nested in value.items():
            child = node.child(str(key))
            _observe(
                child,
                nested,
                _label(path, str(key)),
                date_detection=date_detection,
                warnings=warnings,
            )
        return
    if isinstance(value, Sequence):
        if not value:
            node.kinds["empty_array"] += 1
            return
        if any(isinstance(item, Mapping) for item in value):
            node.kinds["array_of_objects"] += 1
        for item in value:
            _observe(node, item, path, date_detection=date_detection, warnings=warnings)
        return
    node.kinds["string"] += 1
    node.values += 1
    warnings.append(f"field {path!r} held an unsupported value of type {type(value).__name__}")


def _resolve(
    node: _Node,
    path: str,
    keyword_max_length: int,
    keyword_ignore_above: int,
    date_detection: bool,
    warnings: list[str],
) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    for name in sorted(node.children):
        child = node.children[name]
        child_path = _label(path, name)
        resolved = _resolve_field(
            child,
            child_path,
            keyword_max_length,
            keyword_ignore_above,
            date_detection,
            warnings,
        )
        if resolved is not None:
            properties[name] = resolved
    return properties


def _resolve_field(
    node: _Node,
    path: str,
    keyword_max_length: int,
    keyword_ignore_above: int,
    date_detection: bool,
    warnings: list[str],
) -> dict[str, Any] | None:
    kinds = {kind for kind in node.kinds if kind not in ("null", "empty_array", "array_of_objects")}
    children: dict[str, Any] = _resolve(
        node, path, keyword_max_length, keyword_ignore_above, date_detection, warnings
    )

    if not kinds:
        if children:
            return {"type": "object", "properties": children}
        warnings.append(f"field {path!r} was only ever null; mapped as keyword")
        return {"type": "keyword"}

    if "object" in kinds:
        if node.kinds["array_of_objects"]:
            warnings.append(
                f"field {path!r} holds an array of objects mapped as `object`; "
                "use `nested` if you query the elements independently"
            )
        if len(kinds) > 1:
            warnings.append(
                f"field {path!r} is sometimes an object and sometimes a scalar; "
                "scalars are stored but not indexed"
            )
        return {"type": "object", "properties": children}

    if kinds == {"boolean"}:
        return {"type": "boolean"}
    if kinds == {"long"}:
        return {"type": "long"}
    if kinds == {"double"} or kinds == {"long", "double"}:
        return {"type": "double"}
    if kinds == {"date"}:
        return {"type": "date"}
    if kinds == {"date", "string"}:
        dated = node.kinds["date"]
        total = node.kinds["date"] + node.kinds["string"]
        if dated == total:
            return {"type": "date"}
        warnings.append(
            f"field {path!r} mixes dates and free text "
            f"({dated}/{total} values parsed as dates); mapped as text"
        )
        return _text_field(keyword_ignore_above)
    if kinds == {"string"}:
        kind = _string_kind(node, keyword_max_length)
        if kind == "keyword":
            return {"type": "keyword"}
        return _text_field(keyword_ignore_above)
    if (
        kinds == {"string", "long"}
        or kinds == {"string", "double"}
        or kinds
        == {
            "string",
            "long",
            "double",
        }
    ):
        warnings.append(
            f"field {path!r} holds both numbers and strings; mapped as text — "
            "sort or aggregate on the `.keyword` sub-field instead"
        )
        return _text_field(keyword_ignore_above)
    if kinds == {"boolean", "string"}:
        warnings.append(f"field {path!r} holds booleans and strings; mapped as keyword")
        return {"type": "keyword"}
    if "long" in kinds or "double" in kinds:
        warnings.append(f"field {path!r} mixes several kinds {sorted(kinds)}; mapped as text")
        return _text_field(keyword_ignore_above)
    warnings.append(f"field {path!r} mixes several kinds {sorted(kinds)}; mapped as keyword")
    return {"type": "keyword"}


def _text_field(ignore_above: int) -> dict[str, Any]:
    return {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": ignore_above}},
    }


def _string_kind(node: _Node, keyword_max_length: int) -> str:
    """`keyword` for identifiers and enums, `text` for prose.

    Three signals, in order: an over-long value is prose; whitespace plus a
    long average value is prose; near-unique long values are prose.
    """

    if node.string_max_length > keyword_max_length:
        return "text"
    values = max(1, node.kinds["string"])
    average = node.string_chars / values
    ratio = len(node.string_samples) / max(1, min(values, MAX_DISTINCT_SAMPLES))
    has_space = any(" " in sample for sample in node.string_samples)
    if has_space and average > 16:
        return "text"
    if ratio > 0.9 and average > 16:
        return "text"
    return "keyword"


def _collect(
    properties: Mapping[str, Any], path: str, out: dict[str, Any] | None = None
) -> dict[str, Any]:
    buckets: dict[str, Any] = (
        out
        if out is not None
        else {
            "field_count": 0,
            "text": [],
            "keyword": [],
            "date": [],
            "numeric": [],
        }
    )
    for name, spec in properties.items():
        full = _label(path, name)
        kind = spec.get("type")
        if kind == "object":
            nested = spec.get("properties")
            if isinstance(nested, Mapping):
                _collect(nested, full, buckets)
            buckets["field_count"] += 1
            continue
        buckets["field_count"] += 1
        if kind == "text":
            buckets["text"].append(full)
        elif kind == "keyword":
            buckets["keyword"].append(full)
        elif kind == "date":
            buckets["date"].append(full)
        elif kind in ("long", "double", "boolean"):
            buckets["numeric"].append(full)
    return buckets


def _label(path: str, name: str) -> str:
    return f"{path}.{name}" if path else name
