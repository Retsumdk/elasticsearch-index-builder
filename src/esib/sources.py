"""Document sources: streaming NDJSON/JSON/CSV readers with transparent gzip.

Every reader yields one ``dict`` per document, so a 4 GB export can be index
without holding it in memory.
"""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, TextIO

from .errors import SourceError

__all__ = [
    "SUPPORTED_SUFFIXES",
    "coerce_value",
    "detect_format",
    "iter_documents",
    "iter_paths",
]

SUPPORTED_SUFFIXES = (".ndjson", ".jsonl", ".json", ".csv", ".tsv", ".gz")

_TRUE = {"true", "yes", "y", "t"}
_FALSE = {"false", "no", "n", "f"}


def iter_paths(target: str | Path, *, recursive: bool = False) -> Iterator[Path]:
    """Yield every supported file at ``target`` (a file or a directory)."""

    path = Path(target)
    if path.is_file():
        yield path
        return
    if not path.is_dir():
        raise SourceError(f"no such file or directory: {path}", path=str(path))
    pattern = "**/*" if recursive else "*"
    for candidate in sorted(path.glob(pattern)):
        if candidate.is_file() and detect_format(candidate) is not None:
            yield candidate


def detect_format(path: str | Path) -> str | None:
    """Infer the document format from a file name, ignoring a trailing ``.gz``."""

    name = Path(path).name.lower()
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    for suffix, fmt in (
        (".ndjson", "ndjson"),
        (".jsonl", "ndjson"),
        (".json", "json"),
        (".tsv", "tsv"),
        (".csv", "csv"),
    ):
        if name.endswith(suffix):
            return fmt
    return None


def iter_documents(
    target: str | Path,
    *,
    format: str | None = None,
    limit: int | None = None,
    recursive: bool = False,
) -> Iterator[dict[str, Any]]:
    """Yield documents from a file or directory, in sorted file order.

    ``format`` overrides the file extension when a source has an unusual name.
    """

    if format is not None and format not in ("ndjson", "json", "csv", "tsv"):
        raise SourceError(f"unknown format {format!r}; expected ndjson, json, csv or tsv")

    emitted = 0
    for path in iter_paths(target, recursive=recursive):
        fmt = format or detect_format(path)
        if fmt is None:
            raise SourceError(f"unsupported file type: {path.name}", path=str(path))
        for document in _read_file(path, fmt):
            yield document
            emitted += 1
            if limit is not None and emitted >= limit:
                return


def _open_text(path: Path) -> TextIO:
    try:
        if path.name.lower().endswith(".gz"):
            return gzip.open(path, "rt", encoding="utf-8-sig", newline="")
        return path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise SourceError(f"cannot read {path}: {exc}", path=str(path)) from exc


def _read_file(path: Path, fmt: str) -> Iterator[dict[str, Any]]:
    if fmt == "ndjson":
        yield from _read_ndjson(path)
    elif fmt == "json":
        yield from _read_json(path)
    else:
        yield from _read_delimited(path, fmt)


def _read_ndjson(path: Path) -> Iterator[dict[str, Any]]:
    stream = _open_text(path)
    with stream:
        for number, line in enumerate(stream, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise SourceError(
                    f"{path.name}: line {number} is not valid JSON ({exc.msg})",
                    path=str(path),
                    line=number,
                ) from exc
            if not isinstance(parsed, Mapping):
                raise SourceError(
                    f"{path.name}: line {number} is a {type(parsed).__name__}, not an object",
                    path=str(path),
                    line=number,
                )
            yield dict(parsed)


def _read_json(path: Path) -> Iterator[dict[str, Any]]:
    stream = _open_text(path)
    with stream:
        try:
            parsed = json.load(stream)
        except json.JSONDecodeError as exc:
            raise SourceError(
                f"{path.name} is not valid JSON ({exc.msg} at line {exc.lineno})",
                path=str(path),
                line=exc.lineno,
            ) from exc
    if isinstance(parsed, Mapping):
        if "hits" in parsed and isinstance(parsed["hits"], Mapping):
            hits = parsed["hits"].get("hits")
            if isinstance(hits, list):
                for hit in hits:
                    if isinstance(hit, Mapping):
                        yield dict(hit.get("_source") or hit)
                return
        yield dict(parsed)
        return
    if isinstance(parsed, list):
        for number, item in enumerate(parsed, start=1):
            if not isinstance(item, Mapping):
                raise SourceError(
                    f"{path.name}: element {number} is a {type(item).__name__}, not an object",
                    path=str(path),
                )
            yield dict(item)
        return
    raise SourceError(
        f"{path.name} must contain an object or an array of objects",
        path=str(path),
    )


def _read_delimited(path: Path, fmt: str) -> Iterator[dict[str, Any]]:
    delimiter = "\t" if fmt == "tsv" else ","
    stream = _open_text(path)
    with stream:
        reader = csv.DictReader(stream, delimiter=delimiter)
        if reader.fieldnames is None:
            raise SourceError(f"{path.name} has no header row", path=str(path))
        for row in reader:
            yield {key: coerce_value(value) for key, value in row.items() if key is not None}


def coerce_value(value: str | None) -> Any:
    """Best-effort typing for delimited text: numbers, booleans, null, else the string."""

    if value is None:
        return None
    text = value.strip()
    if text == "":
        return None
    lowered = text.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    if lowered in ("null", "none", "nil"):
        return None
    if _looks_integer(text):
        return int(text)
    if _looks_float(text):
        return float(text)
    return text


def _looks_integer(text: str) -> bool:
    body = text[1:] if text[:1] in "+-" else text
    if not body.isdigit() or len(body) > 15:
        return False
    return not (len(body) > 1 and body[0] == "0")


def _looks_float(text: str) -> bool:
    body = text[1:] if text[:1] in "+-" else text
    if body.count(".") != 1:
        return False
    whole, _, fraction = body.partition(".")
    if not (whole.isdigit() or whole == "") or not fraction.isdigit():
        return False
    return not (len(whole) > 1 and whole[0] == "0")
