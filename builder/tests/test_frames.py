"""Tests for builder/divkit_builder/frames.py — EDGAR frames quarter sweep."""

from __future__ import annotations

import json
import pathlib

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "frames_cy2022q1.json"


def test_parse_frame_entries(monkeypatch):
    data = json.loads(FIXTURE.read_text())
    from divkit_builder import frames

    monkeypatch.setattr(frames, "_get_json", lambda url: data)
    rows = frames.fetch_quarter("CommonStockDividendsPerShareDeclared", 2022, 1)
    assert len(rows) == 5
    assert rows[0].amount == data["data"][0]["val"]
    assert rows[0].period_end == data["data"][0]["end"]


def test_missing_data_key_returns_empty(monkeypatch):
    from divkit_builder import frames

    monkeypatch.setattr(frames, "_get_json", lambda url: {"taxonomy": "us-gaap"})
    rows = frames.fetch_quarter("CommonStockDividendsPerShareDeclared", 2022, 1)
    assert rows == []


def test_declared_preferred_over_cashpaid():
    from divkit_builder.frames import Row, _merge_prefer_declared

    decl = Row(
        cik=1,
        period_start="2022-01-01",
        period_end="2022-03-31",
        amount=0.5,
        concept="Declared",
        accn="a",
        form=None,
    )
    paid = Row(
        cik=1,
        period_start="2022-01-01",
        period_end="2022-03-31",
        amount=0.4,
        concept="CashPaid",
        accn="b",
        form=None,
    )
    merged = _merge_prefer_declared([paid, decl])
    assert len(merged) == 1 and merged[0].concept == "Declared"
