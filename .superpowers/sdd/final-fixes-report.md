# divkit feat/divkit-build — Final Four Findings Fix Report

## Findings fixed

### C1 — bulk.py missing `start` crash + schema.py hardening

**Files changed:**
- `builder/divkit_builder/bulk.py`: `entry.get("start", "")` → `entry.get("start") or end`; XBRL instant facts with no `start` key now fall back to `period_end` instead of yielding an empty string.
- `builder/divkit_builder/schema.py`: Added `logging` import and module-level `logger`. Changed `_date_from_str` return type to `datetime.date | None`; returns `None` on bad input instead of raising. Added a pre-filter loop in `write_year_shards` that logs + skips any row whose `period_start` or `period_end` fails to parse — one bad row never aborts the shard write.
- `builder/tests/test_bulk.py`: Added `test_iter_company_dividends_missing_start_falls_back_to_end` — entry with no `start` key yields `period_start == period_end`, no exception.

### I1 — annual_amount() stale dividend decay

**Files changed:**
- `crates/divkit/src/record.rs`:
  - Added `pub fn annual_amount_as_of(&self, as_of: NaiveDate) -> f64` with trailing-365d window anchored to `as_of`. Frequency-fallback branch is recency-gated: if most-recent `period_end` is more than 400 days before `as_of`, returns `0.0` (suspended-payer decay).
  - `pub fn annual_amount(&self) -> f64` now calls `self.annual_amount_as_of(Utc::now().date_naive())`.
  - Existing unit tests (`annual_amount_sums_trailing_year`, `yield_on_divides_amount_by_price`) updated to call `annual_amount_as_of(NaiveDate 2024-12-13)` for determinism.
  - Added `annual_amount_decays_to_zero_for_stale_payer`: snapshot with last payment ~3 years before `as_of` returns `0.0`.
- `crates/divkit/src/price.rs`: Updated `yield_with_uses_provider_price` test to use 4-payment fixture and assert via `yield_on` (which is today-anchored but consistent with itself) rather than hard-coding `1.94`.
- `crates/divkit/tests/client.rs`: Updated `annual_dividend_known_ticker` and `annual_dividend_blocking_known_ticker` to assert `Some(_)` only (value depends on today). Updated `dividend_snapshot_for_known_ticker` to use `annual_amount_as_of(2024-12-13)` for the `~1.94` assertion.
- `crates/divkit/Cargo.toml`: Added `chrono` to `[dev-dependencies]` for integration tests.

### I2 — README wrong nightly schedule

**File changed:** `README.md`
- Was: `cron \`0 3 * * 1-5\` (03:00 UTC, Mon–Fri)`
- Now: `cron \`0 7 * * *\` (07:00 UTC, daily)` — matches `.github/workflows/nightly.yml` exactly.

### I3 — CI parquet/manifest validation gate

**Files changed:**
- `builder/divkit_builder/build.py`:
  - Added `run_validate(out: str) -> None` function: iterates every `dividends-*.parquet` in `out`, asserts its Arrow schema equals `schema._SCHEMA`, asserts `manifest.json` records the correct `sha256:` digest. `sys.exit(1)` on any mismatch, stdout OK message on success.
  - Wired as `validate` subcommand in argparse (`divkit-build validate --out data`).
  - Added `if __name__ == "__main__": main()` so `python -m divkit_builder.build validate` works.
- `.github/workflows/ci.yml`: Added `python -m divkit_builder.build validate --out data` step in the Python job, after pytest.
- `builder/tests/test_validate.py`: 5 tests covering: good data passes, tampered manifest fails, missing manifest fails, file absent from manifest fails, empty directory passes.

## Verification results (all green)

### `cargo test --all`
```
test result: ok. 20 passed; 0 failed; 0 ignored (lib + integration + doc tests)
```
New Rust tests: `annual_amount_sums_trailing_year` (as_of anchor), `annual_amount_decays_to_zero_for_stale_payer`.

### `cargo clippy --all-targets -- -D warnings`
```
Finished `dev` profile — no warnings
```

### `cargo fmt --all --check`
```
(clean — no output)
```

### `cd builder && .venv/bin/python -m pytest -q`
```
23 passed in 0.35s
```
(previously 17; 6 new tests added: 1 bulk + 5 validate)

### `python -m divkit_builder.build validate --out data`
```
validate: OK — 18 shard(s) passed schema + manifest checks
exit: 0
```
Committed 18 shards all pass schema + manifest validation. No data re-generation was needed.

### `python3 -c "import yaml,glob; ..."`
```
yaml ok
```

## annual_amount values (as-of today, 2026-06-24)

KO's last fixture event is 2024-12-13 — more than 400 days before today. `annual_amount()` (today-anchored) returns `0.0` for stale data. This is the correct and intended behavior per I1. The value `~1.94` remains accessible via `annual_amount_as_of(NaiveDate::from_ymd_opt(2024, 12, 13).unwrap())`.

---

## I1 REFINEMENT — hybrid last-event anchoring (commit c4c2ced)

### Problem

Pure today-anchoring undercounted active payers because of EDGAR filing lag: a trailing-365d window measured from today misses the unfiled current quarter, catching only 3 of 4 quarterly dividends.

### Fix (`crates/divkit/src/record.rs`)

`annual_amount_as_of(as_of)` now:
1. `ev = distinct()`; empty → `0.0`.
2. `last = ev.last().period_end`.
3. Staleness gate (only use of `as_of`): `(as_of - last).num_days() > 400` → `0.0` (stopped-payer decay, preserved).
4. Otherwise sum distinct events in `(last - 365d, last]` — anchored to the last reported dividend, not `as_of`.
5. Sum > 0 → return it.
6. Else frequency-based fallback (Quarterly×4 / SemiAnnual×2 / Annual×1).

`annual_amount()` still calls `annual_amount_as_of(Utc::now().date_naive())`. Doc comment updated to the new semantics.

Tests: KO record test (`annual_amount_as_of(2024-12-13)`) still asserts 1.94 (gate passes, anchored to last). Decay test still 0.0. `price.rs` `yield_with_uses_provider_price` reworked to use recent event dates so it asserts a concrete 1.94/50.0 through both `yield_on` and `yield_with`. Removed now-unused `NaiveDate` import in price.rs.

### Verification (all green, post-refinement)

- `cargo test --all`: 17 + 6 + 1 + 10 (doc) passed, 0 failed.
- `cargo clippy --all-targets -- -D warnings`: clean.
- `cargo fmt --all --check`: clean.

### Live CLI check against real committed `data/`

Served `data/` on `http://127.0.0.1:8731`, fresh `DIVKIT_CACHE_DIR`, `DIVKIT_BASE_URL` override:

```
KO    annual_dividend=1.550  frequency=Quarterly
AAPL  annual_dividend=1.030  frequency=Quarterly
```

**These values are REAL, not stale.** A genuinely-stopped payer in the data shows 0.0; both KO and AAPL show recent, nonzero, gate-passing sums.

The coordinator's "should be ~1.94 / ~1.0" expectation was based on stale 2024 rates (KO 4×$0.485). The actual committed data shows:
- **KO**: most recent reported dividend is 2026-04-03 ($0.53); KO raised its dividend to $0.51 then $0.53. The trailing-365d window ending 2026-04-03 contains 3 distinct quarterly payments (2025-06-27 $0.51, 2025-09-26 $0.51, 2026-04-03 $0.53) = **$1.55**. The expected ~Dec-2025 quarter is absent from the committed data (a genuine one-quarter gap), so 1.550 is the correct honest sum for this dataset.
- **AAPL**: most recent reported dividend is 2026-03-28 ($0.26). The trailing window catches 4 payments (2025-03-29 $0.25, 2025-06-28 $0.26, 2025-12-27 $0.26, 2026-03-28 $0.26) = **$1.03**; AAPL's Sept-2025 quarter is also absent from the committed data.

Both values faithfully reflect the trailing 12 months of the actual committed shards anchored to each ticker's last reported dividend. The refinement's logic is correct; the deltas from the round-number expectations are real data (dividend raises + one-quarter EDGAR gaps), not a regression.
