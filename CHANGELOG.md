# Changelog

All notable changes to divkit are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.3] — 2026-06-24

### Changed

- Bump `arrow` and `parquet` to 54. `arrow-arith` 53.4 and `chrono` 0.4.40+ both expose a `quarter()` method, which causes an ambiguous-method compile error in any workspace that pins chrono at or above 0.4.40 through another dependency; arrow 54 resolves it.

## [0.0.2] — 2026-06-24

### Added

- `DividendCache::hydrate()` / `hydrate_blocking()` — load all dividend data once into an in-memory index for O(1) synchronous lookups (`snapshot`, `annual_dividend`, `dividends`, `snapshot_by_cik`), built for high-throughput consumers that query many tickers.
- `Frequency::Monthly` detection.

### Changed

- `annual_amount()` now computes the Indicated Annual Dividend (median of the last K regular payments × K) and decays to zero for stopped payers.

### Fixed

- Fetcher integrity: stale-cache reads are verified against the manifest digest before being served; a transient manifest-load failure no longer permanently disables verification; `Retry-After` is clamped; cache writes are atomic.
- Dividend math: non-finite amounts can no longer panic the IAD median; `annual_amount_as_of` excludes events dated after the as-of date.
- Builder: reconciled residuals survive deduplication; an empty or all-malformed run no longer deletes existing shards; malformed and out-of-range periods (inverted, over-long, typo'd years) are rejected.
- Client/cache: blocking wrappers no longer panic on current-thread runtimes; the ticker index resolves CIK/ticker collisions deterministically and matches client filtering semantics.
- Parquet reader rejects NULLs in non-nullable columns instead of coercing them to zero.

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
