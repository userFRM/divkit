"""Tests for builder/divkit_builder/frames.py — EDGAR frames quarter sweep."""

from __future__ import annotations

import json
import pathlib

from divkit_builder.frames import Row, drop_cumulative_periods

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "frames_cy2022q1.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _row(cik: int, start: str, end: str, amount: float = 0.51) -> Row:
    return Row(
        cik=cik,
        period_start=start,
        period_end=end,
        amount=amount,
        concept="Declared",
        accn="test",
        form=None,
    )


# ---------------------------------------------------------------------------
# drop_cumulative_periods — containment filter
# ---------------------------------------------------------------------------

def test_drop_cumulative_ko_like():
    """KO 2025: 3 discrete quarters + 1 annual cumulative → only quarters survive."""
    q1 = _row(12345, "2025-01-01", "2025-03-28", 0.51)
    q2 = _row(12345, "2025-03-29", "2025-06-27", 0.51)
    q3 = _row(12345, "2025-06-28", "2025-09-26", 0.51)
    annual = _row(12345, "2025-01-01", "2025-12-31", 2.04)  # contains all 3 quarters

    result = drop_cumulative_periods([q1, q2, q3, annual])
    assert len(result) == 3, f"Expected 3 quarters, got {len(result)}"
    assert annual not in result, "Annual cumulative period should be dropped"
    assert q1 in result and q2 in result and q3 in result


def test_keep_pure_annual_payer():
    """A CIK with only one annual period and no sub-periods: that period is KEPT."""
    annual = _row(99999, "2025-01-01", "2025-12-31", 2.04)
    result = drop_cumulative_periods([annual])
    assert result == [annual], "Pure annual payer's single period must be kept"


def test_drop_six_month_cumulative():
    """6-month cumulative overlapping 2 quarters → 6mo dropped, quarters kept."""
    q1 = _row(11111, "2025-01-01", "2025-03-31", 0.51)
    q2 = _row(11111, "2025-04-01", "2025-06-30", 0.51)
    six_mo = _row(11111, "2025-01-01", "2025-06-30", 1.02)  # contains q1 and q2

    result = drop_cumulative_periods([q1, q2, six_mo])
    assert len(result) == 2, f"Expected 2 quarters, got {len(result)}"
    assert six_mo not in result, "6-month cumulative should be dropped"
    assert q1 in result and q2 in result


def test_keep_two_distinct_non_overlapping_quarters():
    """Two distinct non-overlapping quarters → both are kept."""
    q1 = _row(22222, "2025-01-01", "2025-03-31", 0.51)
    q2 = _row(22222, "2025-04-01", "2025-06-30", 0.51)
    result = drop_cumulative_periods([q1, q2])
    assert result == [q1, q2], "Both non-overlapping quarters must be kept"


def test_containment_is_per_cik():
    """A period that contains another row of a DIFFERENT cik is not dropped."""
    # cik=1: wide period that contains cik=2's period — must NOT be dropped (different cik)
    wide = _row(1, "2025-01-01", "2025-12-31", 2.04)
    narrow = _row(2, "2025-03-01", "2025-06-30", 0.51)

    result = drop_cumulative_periods([wide, narrow])
    assert wide in result and narrow in result, "Cross-cik containment must not trigger drop"


def test_input_order_preserved():
    """Survivors must appear in the same relative order as the input."""
    q1 = _row(33333, "2025-01-01", "2025-03-31", 0.51)
    q2 = _row(33333, "2025-04-01", "2025-06-30", 0.51)
    q3 = _row(33333, "2025-07-01", "2025-09-30", 0.51)
    annual = _row(33333, "2025-01-01", "2025-12-31", 2.04)

    result = drop_cumulative_periods([q3, annual, q1, q2])
    assert result == [q3, q1, q2], f"Input order of survivors not preserved: {result}"


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
