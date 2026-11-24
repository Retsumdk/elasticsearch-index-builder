"""Reading documents off disk: formats, coercion, and the error messages."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from esib import SourceError, coerce_value, detect_format, iter_documents, iter_paths


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_ndjson_streams_objects_and_skips_blank_lines(tmp_path: Path) -> None:
    source = _write(tmp_path / "docs.ndjson", '{"a": 1}\n\n{"a": 2}\n')
    assert list(iter_documents(source)) == [{"a": 1}, {"a": 2}]


def test_jsonl_is_treated_as_ndjson(tmp_path: Path) -> None:
    source = _write(tmp_path / "docs.jsonl", '{"a": 1}\n{"a": 2}\n')
    assert [row["a"] for row in iter_documents(source)] == [1, 2]


def test_bad_json_names_the_line(tmp_path: Path) -> None:
    source = _write(tmp_path / "docs.ndjson", '{"a": 1}\n{"a": }\n')
    with pytest.raises(SourceError) as caught:
        list(iter_documents(source))
    assert caught.value.line == 2
    assert "line 2" in caught.value.hint()


def test_a_scalar_line_is_rejected_not_silently_dropped(tmp_path: Path) -> None:
    source = _write(tmp_path / "docs.ndjson", '"just a string"\n')
    with pytest.raises(SourceError, match="not an object"):
        list(iter_documents(source))


def test_json_array_and_single_object(tmp_path: Path) -> None:
    array = _write(tmp_path / "array.json", json.dumps([{"a": 1}, {"a": 2}]))
    single = _write(tmp_path / "single.json", json.dumps({"a": 3}))
    assert [row["a"] for row in iter_documents(array)] == [1, 2]
    assert [row["a"] for row in iter_documents(single)] == [3]


def test_json_search_reply_is_unwrapped_to_its_sources(tmp_path: Path) -> None:
    reply = {
        "hits": {
            "total": {"value": 2},
            "hits": [{"_source": {"a": 1}}, {"_source": {"a": 2}}],
        }
    }
    source = _write(tmp_path / "reply.json", json.dumps(reply))
    assert [row["a"] for row in iter_documents(source)] == [1, 2]


def test_json_scalar_document_is_an_error(tmp_path: Path) -> None:
    source = _write(tmp_path / "bad.json", "42")
    with pytest.raises(SourceError, match="object or an array"):
        list(iter_documents(source))


def test_json_array_of_scalars_is_an_error(tmp_path: Path) -> None:
    source = _write(tmp_path / "bad.json", "[1, 2]")
    with pytest.raises(SourceError, match="element 1"):
        list(iter_documents(source))


def test_csv_values_are_typed(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "rows.csv",
        "sku,price,qty,in_stock,note\nA-1,9.99,3,true,\nB-2,10,4,FALSE,ok\n",
    )
    rows = list(iter_documents(source))
    assert rows[0] == {"sku": "A-1", "price": 9.99, "qty": 3, "in_stock": True, "note": None}
    assert rows[1]["in_stock"] is False
    assert rows[1]["price"] == 10


def test_tsv_is_delimited_by_tabs(tmp_path: Path) -> None:
    source = _write(tmp_path / "rows.tsv", "sku\tqty\nA-1\t3\n")
    assert list(iter_documents(source)) == [{"sku": "A-1", "qty": 3}]


def test_csv_without_a_header_is_an_error(tmp_path: Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    with pytest.raises(SourceError, match="header"):
        list(iter_documents(empty))


def test_gzip_sources_are_transparent(tmp_path: Path) -> None:
    path = tmp_path / "docs.ndjson.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write('{"a": 1}\n')
    assert list(iter_documents(path)) == [{"a": 1}]
    assert detect_format(path) == "ndjson"


def test_directory_sources_are_sorted_and_recursive_on_request(tmp_path: Path) -> None:
    _write(tmp_path / "b.ndjson", '{"file": "b"}\n')
    _write(tmp_path / "a.ndjson", '{"file": "a"}\n')
    (tmp_path / "nested").mkdir()
    _write(tmp_path / "nested" / "c.ndjson", '{"file": "c"}\n')
    assert [row["file"] for row in iter_documents(tmp_path)] == ["a", "b"]
    assert [row["file"] for row in iter_documents(tmp_path, recursive=True)] == ["a", "b", "c"]


def test_limit_stops_early(tmp_path: Path) -> None:
    _write(tmp_path / "docs.ndjson", '{"a": 1}\n{"a": 2}\n{"a": 3}\n')
    assert len(list(iter_documents(tmp_path, limit=2))) == 2


def test_unknown_suffix_is_rejected(tmp_path: Path) -> None:
    source = _write(tmp_path / "notes.yaml", "a: 1\n")
    with pytest.raises(SourceError, match="unsupported"):
        list(iter_documents(source))


def test_missing_path_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="no such file"):
        list(iter_documents(tmp_path / "nope.ndjson"))


def test_format_override_handles_an_unusual_name(tmp_path: Path) -> None:
    source = _write(tmp_path / "export", '{"a": 1}\n')
    assert list(iter_documents(source, format="ndjson")) == [{"a": 1}]
    with pytest.raises(SourceError, match="unknown format"):
        list(iter_documents(source, format="parquet"))


def test_iter_paths_accepts_a_single_file(tmp_path: Path) -> None:
    source = _write(tmp_path / "docs.ndjson", "")
    assert list(iter_paths(source)) == [source]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("42", 42),
        ("-7", -7),
        ("007", "007"),
        ("3.5", 3.5),
        ("-0.25", -0.25),
        ("1.2.3", "1.2.3"),
        ("TRUE", True),
        ("no", False),
        ("null", None),
        ("", None),
        ("  padded  ", "padded"),
        ("2026-01-02", "2026-01-02"),
        ("12345678901234567890", "12345678901234567890"),
    ],
)
def test_coerce_value(raw: str, expected: object) -> None:
    assert coerce_value(raw) == expected


def test_coerce_value_keeps_none() -> None:
    assert coerce_value(None) is None
