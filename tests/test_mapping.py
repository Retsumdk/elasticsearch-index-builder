"""Mapping inference: types, text/keyword heuristics, conflicts, warnings."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from esib import default_settings, infer_mappings
from esib.mapping import is_date_string


def spec(mappings: dict[str, Any], field: str) -> dict[str, Any]:
    properties: dict[str, Any] = mappings["properties"]
    found: dict[str, Any] = properties[field]
    return found


def test_scalars_get_their_native_types() -> None:
    inferred = infer_mappings([{"n": 1, "ratio": 0.5, "flag": True, "when": "2026-01-02"}])
    mappings = inferred.mappings()
    assert spec(mappings, "n") == {"type": "long"}
    assert spec(mappings, "ratio") == {"type": "double"}
    assert spec(mappings, "flag") == {"type": "boolean"}
    assert spec(mappings, "when") == {"type": "date"}


def test_int_and_float_widen_to_double() -> None:
    inferred = infer_mappings([{"amount": 1}, {"amount": 1.5}])
    assert spec(inferred.mappings(), "amount") == {"type": "double"}


def test_short_repeated_string_becomes_keyword() -> None:
    inferred = infer_mappings([{"country": "PT"}, {"country": "NL"}, {"country": "PT"}])
    assert spec(inferred.mappings(), "country") == {"type": "keyword"}


def test_long_varying_string_becomes_text_with_keyword_subfield() -> None:
    inferred = infer_mappings(
        [
            {"body": "the quick brown fox jumps over the lazy dog again and again"},
            {"body": "a completely different sentence of similar but not identical length now"},
        ]
    )
    field = spec(inferred.mappings(), "body")
    assert field["type"] == "text"
    assert field["fields"]["keyword"] == {"type": "keyword", "ignore_above": 256}


def test_date_detection_can_be_disabled() -> None:
    inferred = infer_mappings([{"when": "2026-01-02"}], date_detection=False)
    assert spec(inferred.mappings(), "when")["type"] == "keyword"


def test_mixed_date_and_text_collapses_to_text_and_warns() -> None:
    inferred = infer_mappings([{"when": "2026-01-02"}, {"when": "not a date at all"}])
    assert spec(inferred.mappings(), "when")["type"] in ("text", "keyword")
    assert any("when" in warning for warning in inferred.warnings)


def test_number_and_string_in_one_field_becomes_text() -> None:
    inferred = infer_mappings([{"code": 12}, {"code": "AB-12"}])
    field = spec(inferred.mappings(), "code")
    assert field["type"] == "text"
    assert field["fields"]["keyword"]["type"] == "keyword"
    assert any("code" in warning for warning in inferred.warnings)


def test_nested_objects_and_arrays() -> None:
    inferred = infer_mappings(
        [
            {"supplier": {"name": "Acme", "country": "PT"}, "tags": ["a", "b"]},
            {"supplier": {"name": "Globex", "country": "NL"}, "tags": ["c"]},
        ]
    )
    mappings = inferred.mappings()
    supplier = mappings["properties"]["supplier"]
    assert supplier["type"] == "object"
    assert supplier["properties"]["country"] == {"type": "keyword"}
    assert mappings["properties"]["tags"] == {"type": "keyword"}


def test_array_of_objects_is_flagged() -> None:
    inferred = infer_mappings([{"lines": [{"sku": "A", "qty": 1}, {"sku": "B", "qty": 2}]}])
    assert any("array of objects" in warning for warning in inferred.warnings)


def test_null_only_field_is_flagged_not_guessed() -> None:
    inferred = infer_mappings([{"note": None}, {"note": None}])
    assert spec(inferred.mappings(), "note") == {"type": "keyword"}
    assert any("only ever null" in warning for warning in inferred.warnings)


def test_dotted_keys_are_kept_literal_and_warned_about() -> None:
    inferred = infer_mappings([{"a.b": 1}])
    assert inferred.mappings()["properties"]["a.b"] == {"type": "long"}
    assert any("contains a dot" in warning for warning in inferred.warnings)


def test_dynamic_defaults_to_false_and_is_overridable() -> None:
    inferred = infer_mappings([{"a": 1}])
    assert inferred.mappings()["dynamic"] == "false"
    assert inferred.mappings(dynamic="strict")["dynamic"] == "strict"


def test_counts_and_sample_size_are_reported() -> None:
    inferred = infer_mappings([{"a": 1, "b": {"c": 2}}] * 3)
    assert inferred.documents_sampled == 3
    assert inferred.field_count == 3
    assert inferred.keyword_fields == ()
    assert "a" in inferred.numeric_fields


def test_sample_limit_stops_reading() -> None:
    def documents() -> Iterator[dict[str, int]]:
        for index in range(100):
            yield {"n": index}

    inferred = infer_mappings(documents(), sample_limit=5)
    assert inferred.documents_sampled == 5


def test_no_documents_is_an_error() -> None:
    with pytest.raises(ValueError):
        infer_mappings([])


def test_to_dict_carries_settings_and_meta() -> None:
    payload = infer_mappings([{"a": 1}]).to_dict()
    assert payload["settings"] == default_settings()
    assert payload["_meta"]["generator"] == "elasticsearch-index-builder"
    assert payload["_meta"]["field_count"] == 1


def test_date_string_helper() -> None:
    assert is_date_string("2026-01-02")
    assert is_date_string("2026-01-02T09:30:00Z")
    assert is_date_string("2026-01-02 09:30:00+01:00")
    assert not is_date_string("02/01/2026")
    assert not is_date_string("SKU-001")
