"""EDGAR XBRL frames-API quarter sweep for per-share dividend concepts."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from .sec import _get_json as _sec_get_json

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level alias so tests can monkeypatch frames._get_json independently
# ---------------------------------------------------------------------------
_get_json = _sec_get_json

_FRAMES_BASE = "https://data.sec.gov/api/xbrl/frames/us-gaap"
_CONCEPTS = (
    "CommonStockDividendsPerShareDeclared",
    "CommonStockDividendsPerShareCashPaid",
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class Row:
    cik: int
    period_start: str
    period_end: str
    amount: float
    concept: str   # "Declared" or "CashPaid"
    accn: str
    form: str | None


# ---------------------------------------------------------------------------
# Fetch a single quarter frame
# ---------------------------------------------------------------------------
def fetch_quarter(concept: str, year: int, q: int) -> list[Row]:
    """GET the XBRL frames JSON for *concept* / *year* / *q* and return parsed rows.

    Returns an empty list when the SEC returns HTTP 404 (frame not yet published).
    Network calls are routed through :func:`_get_json` so the shared ≤10 req/s
    rate limit applies.
    """
    url = f"{_FRAMES_BASE}/{concept}/USD-per-shares/CY{year}Q{q}.json"
    short = "Declared" if concept.endswith("Declared") else "CashPaid"
    try:
        data = _get_json(url)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return []
        raise

    if "data" not in data:
        logger.warning(
            "frames %s CY%dQ%d: 200 response missing 'data' key "
            "(possible SEC schema change)",
            concept,
            year,
            q,
        )
        return []

    rows: list[Row] = []
    for entry in data["data"]:
        rows.append(
            Row(
                cik=entry["cik"],
                period_start=entry["start"],
                period_end=entry["end"],
                amount=entry["val"],
                concept=short,
                accn=entry["accn"],
                form=None,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Containment filter — drop cumulative / YTD / annual periods
# ---------------------------------------------------------------------------
def drop_cumulative_periods(rows: list[Row]) -> list[Row]:
    """Remove rows whose period strictly contains another row's period for the same cik.

    EDGAR XBRL companyfacts includes both discrete-quarter entries and cumulative
    YTD / semi-annual / annual entries that span the same date range.  A cumulative
    row *strictly contains* at least one shorter row for the same cik (its
    ``period_start <= shorter.period_start`` AND ``shorter.period_end <=
    period_end`` with the intervals being distinct).  Discrete quarters contain
    nothing shorter within the same cik and are therefore kept.

    A pure annual payer whose only entry spans a full year also contains nothing
    shorter (no sub-periods exist for that cik) and is likewise kept — this is the
    correct behaviour for companies that pay a single annual dividend.

    Input order of surviving rows is preserved.
    """
    import datetime as _dt

    def _parse(s: str) -> _dt.date:
        return _dt.date(int(s[:4]), int(s[5:7]), int(s[8:10]))

    # Index periods per cik for O(n) containment check
    from collections import defaultdict
    cik_periods: dict[int, list[tuple[_dt.date, _dt.date]]] = defaultdict(list)
    for row in rows:
        cik_periods[row.cik].append((_parse(row.period_start), _parse(row.period_end)))

    def _is_cumulative(row: Row) -> bool:
        """Return True if *row* strictly contains at least one other period for the same cik."""
        a_start = _parse(row.period_start)
        a_end = _parse(row.period_end)
        for b_start, b_end in cik_periods[row.cik]:
            if (a_start, a_end) == (b_start, b_end):
                continue
            if a_start <= b_start and b_end <= a_end:
                return True
        return False

    return [row for row in rows if not _is_cumulative(row)]


# ---------------------------------------------------------------------------
# Deduplication — prefer Declared over CashPaid per (cik, period_end)
# ---------------------------------------------------------------------------
def _merge_prefer_declared(rows: list[Row]) -> list[Row]:
    """Deduplicate *rows* by ``(cik, period_end)``, keeping ``Declared`` over ``CashPaid``."""
    best: dict[tuple[int, str], Row] = {}
    for row in rows:
        key = (row.cik, row.period_end)
        existing = best.get(key)
        if existing is None:
            best[key] = row
        elif existing.concept != "Declared" and row.concept == "Declared":
            best[key] = row
    return list(best.values())


# ---------------------------------------------------------------------------
# Full sweep across years and quarters
# ---------------------------------------------------------------------------
def sweep(from_year: int, to_year: int) -> list[Row]:
    """Fetch both dividend concepts for every quarter in ``[from_year, to_year]``.

    Returns deduplicated rows with ``Declared`` taking precedence over ``CashPaid``
    for the same ``(cik, period_end)``.
    """
    all_rows: list[Row] = []
    for year in range(from_year, to_year + 1):
        for q in range(1, 5):
            for concept in _CONCEPTS:
                rows = fetch_quarter(concept, year, q)
                logger.info(
                    "frames %s CY%dQ%d -> %d rows", concept, year, q, len(rows)
                )
                all_rows.extend(rows)
    return _merge_prefer_declared(all_rows)
