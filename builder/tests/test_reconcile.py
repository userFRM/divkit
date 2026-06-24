"""TDD tests for reconcile_periods — XBRL cumulative-period reconciliation.

Algorithm: containers are dropped; a synthetic leaf is synthesized when the
contained leaves do not fully account for the container's value (i.e. residual
> 0.0001), recovering the "missing" discrete period (e.g. KO Q4).
"""

from __future__ import annotations

from divkit_builder.frames import Row, reconcile_periods


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row(
    cik: int,
    start: str,
    end: str,
    amount: float = 0.51,
    concept: str = "Declared",
    accn: str = "test",
) -> Row:
    return Row(
        cik=cik,
        period_start=start,
        period_end=end,
        amount=amount,
        concept=concept,
        accn=accn,
        form=None,
    )


# ---------------------------------------------------------------------------
# KO case: 3 discrete quarters + FY rollup — synthesize Q4 residual
# ---------------------------------------------------------------------------

def test_reconcile_ko_synthesizes_q4():
    """Q1–Q3 present (0.51 each), FY=2.04; reconcile must yield 4 rows summing to 2.04."""
    q1 = _row(12345, "2025-01-01", "2025-03-28", 0.51)
    q2 = _row(12345, "2025-03-29", "2025-06-27", 0.51)
    q3 = _row(12345, "2025-06-28", "2025-09-26", 0.51)
    fy = _row(12345, "2025-01-01", "2025-12-31", 2.04)

    result = reconcile_periods([q1, q2, q3, fy])

    # FY container must be dropped
    assert fy not in result, "FY container must be dropped"

    # All 3 original leaves must be present
    assert q1 in result
    assert q2 in result
    assert q3 in result

    # One synthetic row synthesized for the missing Q4 residual
    synthetic = [r for r in result if r not in (q1, q2, q3)]
    assert len(synthetic) == 1, f"Expected 1 synthetic row, got {len(synthetic)}: {synthetic}"

    synth = synthetic[0]
    # Amount must round to ≈ 0.51 (2.04 − 3×0.51 = 0.51)
    assert abs(synth.amount - 0.51) < 1e-4, f"Synthetic amount expected ≈0.51, got {synth.amount}"

    # Synthetic period must be within FY bounds
    assert synth.period_start >= "2025-01-01"
    assert synth.period_end <= "2025-12-31"

    # Synthetic period must NOT overlap any existing leaf (start at or after Q3 end)
    assert synth.period_start >= q3.period_end, (
        f"Synthetic start {synth.period_start} overlaps Q3 end {q3.period_end}"
    )

    # Total must sum to 2.04
    total = sum(r.amount for r in result)
    assert abs(total - 2.04) < 1e-4, f"Total expected 2.04, got {total}"

    # concept and cik propagated from container
    assert synth.cik == 12345
    assert synth.concept == "Declared"

    assert len(result) == 4


# ---------------------------------------------------------------------------
# Full-discrete reporter: 4 quarters + FY rollup with residual ≈ 0
# ---------------------------------------------------------------------------

def test_reconcile_full_discrete_no_synth():
    """4 quarters sum exactly to FY; FY dropped, no synthetic row produced."""
    q1 = _row(99, "2025-01-01", "2025-03-31", 0.51)
    q2 = _row(99, "2025-04-01", "2025-06-30", 0.51)
    q3 = _row(99, "2025-07-01", "2025-09-30", 0.51)
    q4 = _row(99, "2025-10-01", "2025-12-31", 0.51)
    fy = _row(99, "2025-01-01", "2025-12-31", 2.04)

    result = reconcile_periods([q1, q2, q3, q4, fy])

    # FY dropped
    assert fy not in result

    # All 4 quarters present
    for q in (q1, q2, q3, q4):
        assert q in result, f"Quarter {q} missing from result"

    # No synthetic row — exactly 4 rows, all of which are original quarters
    assert len(result) == 4, f"Expected 4 rows (no synth), got {len(result)}: {result}"
    for q in (q1, q2, q3, q4):
        assert q in result, f"Unexpected extra rows: {[r for r in result if r not in (q1, q2, q3, q4)]}"


# ---------------------------------------------------------------------------
# Pure annual payer: only one row, no sub-periods → kept as leaf
# ---------------------------------------------------------------------------

def test_reconcile_pure_annual_payer_kept():
    """Single annual row with no sub-periods: it is a leaf, must be returned unchanged."""
    annual = _row(77777, "2025-01-01", "2025-12-31", 2.04)

    result = reconcile_periods([annual])

    assert result == [annual], f"Pure annual payer must be kept; got {result}"


# ---------------------------------------------------------------------------
# Non-overlapping quarters: both kept, nothing synthesized
# ---------------------------------------------------------------------------

def test_reconcile_two_distinct_quarters_kept():
    """Two non-overlapping quarters with no container: both kept as-is."""
    q1 = _row(55555, "2025-01-01", "2025-03-31", 0.51)
    q2 = _row(55555, "2025-04-01", "2025-06-30", 0.51)

    result = reconcile_periods([q1, q2])

    assert q1 in result and q2 in result
    assert len(result) == 2


# ---------------------------------------------------------------------------
# residual < 0 anomaly: container dropped, no synth, no crash
# ---------------------------------------------------------------------------

def test_reconcile_negative_residual_no_crash():
    """Leaf sum EXCEEDS container value (data anomaly): container dropped, no synthetic, no exception."""
    q1 = _row(66666, "2025-01-01", "2025-03-31", 1.00)
    q2 = _row(66666, "2025-04-01", "2025-06-30", 1.00)
    # FY = 1.50, but leaves sum to 2.00 — negative residual
    fy = _row(66666, "2025-01-01", "2025-12-31", 1.50)

    result = reconcile_periods([q1, q2, fy])

    # FY must still be dropped (it's a container)
    assert fy not in result

    # Original leaves kept
    assert q1 in result and q2 in result

    # No synthetic row
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Multi-level nesting: annual ⊃ 6-month ⊃ 2 quarters
# Only leaves count toward sum; intermediate containers dropped
# ---------------------------------------------------------------------------

def test_reconcile_nested_containers():
    """annual ⊃ 6mo ⊃ q1+q2; 6mo and annual are both containers; only leaves survive + synth."""
    q1 = _row(33333, "2025-01-01", "2025-03-31", 0.51)
    q2 = _row(33333, "2025-04-01", "2025-06-30", 0.51)
    six_mo = _row(33333, "2025-01-01", "2025-06-30", 1.02)  # contains q1+q2
    annual = _row(33333, "2025-01-01", "2025-12-31", 2.04)  # contains everything

    result = reconcile_periods([q1, q2, six_mo, annual])

    # Both containers dropped
    assert six_mo not in result
    assert annual not in result

    # Original leaves present
    assert q1 in result and q2 in result

    # Synthetic row for Q3+Q4 residual (2.04 − 1.02 = 1.02); 6mo is NOT a leaf so excluded from sum
    synthetic = [r for r in result if r not in (q1, q2)]
    assert len(synthetic) == 1, f"Expected 1 synthetic, got {synthetic}"
    assert abs(synthetic[0].amount - 1.02) < 1e-4, f"Synthetic amount expected ≈1.02, got {synthetic[0].amount}"


# ---------------------------------------------------------------------------
# CIK isolation: containment check is per-cik only
# ---------------------------------------------------------------------------

def test_reconcile_containment_is_per_cik():
    """A wide period from cik=1 does not affect cik=2 rows."""
    wide = _row(1, "2025-01-01", "2025-12-31", 2.04)
    narrow = _row(2, "2025-03-01", "2025-06-30", 0.51)

    result = reconcile_periods([wide, narrow])

    # Both are leaves in their respective cik scope
    assert wide in result and narrow in result
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Stable order: leaves come before synthetics; original relative order preserved
# ---------------------------------------------------------------------------

def test_reconcile_result_order():
    """Leaves appear in original input order; synthetic rows appended after leaves."""
    q3 = _row(44444, "2025-07-01", "2025-09-30", 0.51)
    q1 = _row(44444, "2025-01-01", "2025-03-31", 0.51)
    q2 = _row(44444, "2025-04-01", "2025-06-30", 0.51)
    fy = _row(44444, "2025-01-01", "2025-12-31", 2.04)

    result = reconcile_periods([q3, q1, q2, fy])

    # FY dropped; 3 leaves in original order; 1 synthetic
    assert result[:3] == [q3, q1, q2], f"Leaves not in original order: {result[:3]}"
    assert len(result) == 4
    synth = result[3]
    assert abs(synth.amount - 0.51) < 1e-4
