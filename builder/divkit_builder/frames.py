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
    synthesized: bool = False


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

    .. deprecated::
        Use :func:`reconcile_periods` instead.  This function silently drops the
        container without recovering the residual discrete period (e.g. KO Q4).
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
# Period reconciliation — recover discrete residuals from cumulative rollups
# ---------------------------------------------------------------------------
def reconcile_periods(rows: list[Row]) -> list[Row]:
    """Reconcile cumulative XBRL rollup periods against the discrete leaves they contain.

    EDGAR XBRL filings include both discrete-quarter entries and cumulative YTD /
    semi-annual / annual entries.  A naive drop of containers loses the dividend
    for any discrete period that was never filed as a standalone quarter (e.g. a
    company that rolls Q4 into the annual filing).

    Algorithm (per ``cik``):

    1. Classify every row as a **leaf** (contains no shorter interval for the same
       cik) or a **container** (strictly contains ≥ 1 shorter interval).
    2. Keep all leaves.
    3. For each container ``C`` (value ``V_C``):

       - ``S`` = sum of *leaf* amounts whose interval is strictly inside ``C``.
         Nested containers are excluded from this sum — they will be dropped anyway.
       - ``residual = round(V_C - S, 6)``.
       - If ``residual > 0.0001`` and the leaves do not fully cover ``C``'s span:
         synthesize a single leaf Row with ``amount = residual``.  Its period is
         the largest uncovered contiguous tail of ``C`` (from the last contained
         leaf's ``period_end`` to ``C.period_end``; if empty, the leading gap from
         ``C.period_start`` to the first leaf's ``period_start``; if still empty,
         fall back to ``C``'s full span).
       - If ``residual <= 0.0001``: the discretes already account for the rollup
         (full-discrete reporter) — synthesize nothing.
       - If ``residual < 0``: data anomaly — synthesize nothing, log at DEBUG.
       - Drop ``C`` in all cases.

    4. Return leaves + synthesized rows in stable order (leaves in original input
       order; synthesized rows appended).
    """
    from collections import defaultdict

    def _strictly_contains(
        a_start: str, a_end: str, b_start: str, b_end: str
    ) -> bool:
        """Return True iff interval A strictly contains interval B (lexicographic ISO dates)."""
        return (
            a_start <= b_start
            and b_end <= a_end
            and (a_start, a_end) != (b_start, b_end)
        )

    # Group row indices by cik
    cik_indices: dict[int, list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        cik_indices[row.cik].append(idx)

    # Classify each row: leaf vs container
    is_container: list[bool] = [False] * len(rows)
    for cik, indices in cik_indices.items():
        for i in indices:
            a = rows[i]
            for j in indices:
                if i == j:
                    continue
                b = rows[j]
                if _strictly_contains(a.period_start, a.period_end, b.period_start, b.period_end):
                    is_container[i] = True
                    break

    leaves: list[Row] = [row for idx, row in enumerate(rows) if not is_container[idx]]

    # Build leaf lookup per cik for fast residual computation
    cik_leaves: dict[int, list[Row]] = defaultdict(list)
    for row in leaves:
        cik_leaves[row.cik].append(row)

    synthesized: list[Row] = []

    for idx, container in enumerate(rows):
        if not is_container[idx]:
            continue

        # Sum only leaf amounts strictly inside this container
        contained_leaves = [
            leaf for leaf in cik_leaves[container.cik]
            if _strictly_contains(
                container.period_start, container.period_end,
                leaf.period_start, leaf.period_end,
            )
        ]

        leaf_sum = sum(leaf.amount for leaf in contained_leaves)
        residual = round(container.amount - leaf_sum, 6)

        if residual < 0:
            logger.debug(
                "reconcile_periods: cik=%d container [%s, %s] residual=%.6f < 0 — "
                "leaf sum exceeds rollup (data anomaly); dropping container without synth",
                container.cik, container.period_start, container.period_end, residual,
            )
            continue  # drop container, no synth

        if residual <= 0.0001:
            # Full-discrete reporter: discretes account for the entire rollup
            continue  # drop container, no synth

        # Determine the uncovered span for the synthetic row.
        # Sort contained leaves by period_end to find coverage gaps.
        sorted_leaves = sorted(contained_leaves, key=lambda r: r.period_end)

        synth_start: str
        synth_end: str

        if sorted_leaves:
            # Primary: tail gap — from last leaf's period_end to container's period_end
            tail_start = sorted_leaves[-1].period_end
            tail_end = container.period_end
            if tail_start < tail_end:
                synth_start = tail_start
                synth_end = tail_end
            else:
                # Secondary: leading gap — from container's period_start to first leaf's period_start
                lead_start = container.period_start
                lead_end = sorted_leaves[0].period_start
                if lead_start < lead_end:
                    synth_start = lead_start
                    synth_end = lead_end
                else:
                    # Fallback: use container's full span
                    synth_start = container.period_start
                    synth_end = container.period_end
        else:
            # No contained leaves at all — use container's full span
            synth_start = container.period_start
            synth_end = container.period_end

        # Emit the synthesized residual row.
        #
        # If the residual's period is fully spanned by existing discrete leaves
        # (i.e. no uncovered gap exists), this represents a special/extra
        # dividend that the rollup captured but no standalone quarterly filing
        # recorded.  The row is retained in the event history so that the full
        # payment record is preserved, but it is correctly excluded from the
        # Indicated Annual Dividend (IAD): the IAD algorithm uses the median of
        # the last K regular payments, and the outlier-rejection property of the
        # median naturally suppresses one-off specials without any explicit
        # filtering step.
        synth = Row(
            cik=container.cik,
            period_start=synth_start,
            period_end=synth_end,
            amount=residual,
            concept=container.concept,
            accn=container.accn,
            form=container.form,
            synthesized=True,
        )
        synthesized.append(synth)

    return leaves + synthesized


# ---------------------------------------------------------------------------
# Deduplication — prefer Declared over CashPaid per (cik, period_end)
# ---------------------------------------------------------------------------
def _merge_prefer_declared(rows: list[Row]) -> list[Row]:
    """Deduplicate *rows* by ``(cik, period_end, synthesized)``, keeping ``Declared`` over
    ``CashPaid``.

    Synthesized rows are kept separate from non-synthesized rows at the same
    ``(cik, period_end)`` — they represent a recovered residual that must not be
    coalesced with a real declared leaf at the same key.

    Within identical ``(cik, period_end)`` and ``synthesized`` status, ``Declared``
    takes precedence over ``CashPaid``.  When concept and synthesized status both tie,
    the row with the lexicographically greatest ``accn`` wins (most-recent accession),
    producing a deterministic result independent of input order.
    """
    best: dict[tuple[int, str, bool], Row] = {}
    for row in rows:
        key = (row.cik, row.period_end, row.synthesized)
        existing = best.get(key)
        if existing is None:
            best[key] = row
            continue
        # Primary preference: Declared over CashPaid
        existing_is_declared = existing.concept == "Declared"
        row_is_declared = row.concept == "Declared"
        if existing_is_declared and not row_is_declared:
            continue  # keep existing
        if row_is_declared and not existing_is_declared:
            best[key] = row
            continue
        # Same concept: keep greatest accn (most-recent accession)
        if row.accn > existing.accn:
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
