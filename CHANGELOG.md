# Changelog

All notable changes to this project are documented here.

## [0.0.1] — 2026-06-24

### Added

- `Divkit` client with async `annual_dividend`, `dividends`, and `dividend_snapshot` methods and blocking `*_blocking` variants.
- Free functions `annual_dividend_for`, `dividends_for`, and `dividend_snapshot_for` for one-shot use without a managed client.
- `DividendSnapshot` type with `annual_amount()`, `frequency()`, `yield_on(price)`, and async `yield_with(&PriceProvider)`.
- `annual_amount()` computes the Indicated Annual Dividend — median of the last K regular payments times K (K from frequency) — rejecting special dividends and XBRL rollup anomalies; decays to zero for stopped payers.
- `PriceProvider` trait for caller-supplied spot price sources; divkit ships no price feed.
- `Frequency` enum: `Monthly`, `Quarterly`, `SemiAnnual`, `Annual`, `Irregular`, `None` — inferred from median inter-period spacing.
- Builder reconciles XBRL overlapping period contexts (discrete quarters and cumulative YTD/annual rollups) into discrete payments, reconstructing missing discrete periods from rollups, and rejects malformed periods (inverted, over-long, or typo'd dates).
- `DivEvent` and `Concept` types (`Declared`, `CashPaid`) reflecting EDGAR XBRL source concepts.
- ETag-aware cached fetcher with SHA-256 manifest verification and stale-cache fallback on network failure.
- XDG-compliant cache directory (`~/.cache/divkit/`) via the `directories` crate.
- `DIVKIT_BASE_URL` and `DIVKIT_CACHE_DIR` environment overrides.
- Parquet I/O layer (`parquet_io` module) with `read_dividends` and `write_dividends` for year-sharded `dividends-YYYY.parquet` files.
- `divkit-cli` binary with `get`, `history`, `backfill`, and `append-today`/`nightly` subcommands.
- Python builder (`builder/`) using httpx and pyarrow to fetch EDGAR company facts and produce parquet shards.
- Bundled dividend database: 82,919 observations across 2009–2026 (18 annual shards) sourced from SEC EDGAR public-domain XBRL (`CommonStockDividendsPerShareDeclared` primary, `CommonStockDividendsPerShareCashPaid` fallback).
- GitHub Actions workflows for nightly shard append (`nightly.yml`) and full historical backfill (`backfill.yml`).
