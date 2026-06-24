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
