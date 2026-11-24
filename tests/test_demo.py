"""The demo transcript is a committed artifact: CI fails if it drifts."""

from __future__ import annotations

from pathlib import Path

from esib.demo import run, sample_products, transcript

GOLDEN = Path(__file__).resolve().parents[1] / "demo-output.txt"


def test_transcript_is_deterministic() -> None:
    assert transcript() == transcript()


def test_transcript_matches_the_committed_golden_file() -> None:
    assert transcript() == GOLDEN.read_text(encoding="utf-8")


def test_demo_returns_zero() -> None:
    assert run() == 0


def test_sample_rows_cover_the_drifted_document() -> None:
    rows = sample_products()
    assert len(rows) == 8
    assert isinstance(rows[-1]["price"], str)
    assert all(isinstance(row["sku"], str) for row in rows)
