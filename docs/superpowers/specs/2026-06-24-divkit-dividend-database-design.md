# divkit — design

US equity dividend database for Rust. Per-share cash dividends and dividend yield for any ticker, sourced from SEC EDGAR public-domain XBRL. Companion to [`curvekit`](https://github.com/userFRM/curvekit) (risk-free rate), [`sectorkit`](https://github.com/userFRM/sectorkit) (SIC sector), [`indexkit`](https://github.com/userFRM/indexkit) (index constituents). Same shape: bundled parquet → runtime GitHub-raw fetch → local ETag cache → flat Rust SDK.

## Goal

Give a Rust caller, for any US-listed ticker:

- the **trailing-12-month per-share cash dividend** — the `annual_div` input that ThetaData's Greeks endpoints require for Black-Scholes (ThetaData itself ignores dividends and only accepts this value as a parameter)
- the **full per-period dividend history**, 10+ years deep
- a **dividend yield** given a spot price the caller supplies

Target quality: the most complete public-domain dividend database on GitHub — every SEC XBRL filer, full history, refreshed nightly, reproducible from a single bulk download.

## Why SEC EDGAR

Commercial dividend feeds (Tiingo, FMP, Nasdaq, Yahoo) are licensed and cannot be redistributed in a public repo — the same constraint that drove `sectorkit` to SIC over GICS. SEC EDGAR XBRL is public domain, authoritative, and already the source class for `sectorkit`/`indexkit`. Dividends are reported as standardized us-gaap XBRL concepts on every filer's periodic filings.

**Concepts used** (us-gaap, unit `USD/shares`, duration frames):

- `CommonStockDividendsPerShareDeclared` — primary
- `CommonStockDividendsPerShareCashPaid` — fallback when *Declared* is absent

**Known limitation — no ex-dates.** EDGAR XBRL gives dividend *amounts* dated by fiscal period (quarter/year), not the precise ex-dividend or pay date, and no forward dividend calendar. This is exactly sufficient for the `annual_div` Black-Scholes use case (sum the trailing four quarters) but divkit is **not** an ex-date calendar. The data model has no ex-date field; period-end dates are what EDGAR provides.

## Architecture

Two halves joined by `data/*.parquet`, a language-neutral interface. The published Rust crate never depends on Python; only CI/build tooling does.

```
┌─────────────────────────────┐        ┌──────────────────────────────┐
│  BUILDER (Python)           │        │  CONSUMER (Rust crate divkit)│
│  backfill + nightly         │ writes │  what user projects depend on│
│  edgartools + EDGAR frames  │──────▶ │  reads data/*.parquet        │
│  runs in GitHub Actions     │ data/  │  ETag cache, stale fallback  │
└─────────────────────────────┘        └──────────────────────────────┘
```

This intentionally diverges from `curvekit`, whose nightly is pure Rust: Treasury/SOFR are flat CSVs trivial to parse in Rust, whereas EDGAR XBRL is a large irregular taxonomy. [`edgartools`](https://github.com/dgunning/edgartools) (MIT) handles ticker/CIK lookup, company-facts retrieval, rate-limiting, and XBRL parsing far more robustly than hand-rolled Rust, and it only ever runs in CI.

### Builder (Python) — `builder/`

Produces `data/dividends-YYYY.parquet` sharded by dividend period-end year.

**Backfill — maximum-available history (configurable `--from-year`, default = earliest XBRL ~2009→present, ~15 years).** XBRL was phased in 2009–2011, so pre-2011 coverage is sparser but is included for completeness. Two passes:

1. **Frames sweep (primary).** For each quarter `CY{year}Q{n}` from the start year to present (~60 quarters at max depth), GET
   `https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/USD-per-shares/CY{Y}Q{Q}.json`
   for both concepts. Each call returns *every* filer that reported that concept for that period — one request covers ~1,000+ companies. ~40 quarters × 2 concepts ≈ 80 requests for a decade. Entry fields: `accn, cik, entityName, loc, start, end, val`. Accumulate per `(cik, end)`, preferring *Declared* over *CashPaid* on conflict.
2. **Bulk completeness pass.** Download `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` (~1.39 GB, refreshed daily). For each company JSON, extract the two dividend concepts to capture off-calendar fiscal periods and annual-only filers the quarterly frames miss. Union into the frames result, deduped by `(cik, end, concept-priority)`.

Ticker↔CIK from `https://www.sec.gov/files/company_tickers.json`. SEC `User-Agent` must be the bare `divkit <contact-email>` form (parenthetical/URL UAs get 403'd by the SEC WAF). Rate limit ≤ 10 req/s.

**Nightly delta.** Frames for the current and previous quarter (catches late filings) + companies appearing in the recent submissions feed. Append/update changed rows only; never re-pull the 1.39 GB zip nightly. Idempotent: re-running a day produces no diff.

**Output schema** (one row per dividend observation):

| column | type | note |
|---|---|---|
| `cik` | u32 | zero-stripped |
| `ticker` | string | primary ticker for the CIK; null if unmapped |
| `period_start` | date32 | XBRL `start` |
| `period_end` | date32 | XBRL `end` — the dividend's reference date |
| `amount` | f64 | per-share, `val` |
| `concept` | string | `Declared` \| `CashPaid` |
| `accn` | string | source accession, for provenance/audit |
| `form` | string | filing form (10-Q/10-K/8-K) where available |

A `manifest.json` (SHA-256 per parquet) is regenerated each build for the Rust fetcher's integrity check.

### Consumer (Rust crate `divkit`) — `crates/divkit/`

Pure Rust. Workspace mirrors curvekit: `crates/divkit` + `cli`. Reuses curvekit's `fetcher.rs` (ETag revalidation, single-flight, jsDelivr mirror fallback, SHA-256 manifest verification, stale-cache survival on network failure) and `parquet_io.rs` essentially verbatim. Env overrides `DIVKIT_BASE_URL`, `DIVKIT_CACHE_DIR`.

**Public API** — flat, curvekit-style ergonomics:

```rust
use divkit::Divkit;

#[tokio::main]
async fn main() -> divkit::Result<()> {
    let div = Divkit::new();                       // infallible

    // THE annual_div for Black-Scholes: trailing-12mo per-share cash dividend
    let annual = div.annual_dividend("AAPL").await?;          // Option<f64>

    // Full per-period history
    let history = div.dividends("AAPL").await?;               // Vec<DivEvent>

    // Snapshot: amount + yield helpers (the "both" yield model)
    let snap = div.dividend_snapshot("AAPL").await?;          // DividendSnapshot
    let amt   = snap.annual_amount;                           // always present
    let y     = snap.yield_on(spot_price);                    // pure fn — caller's spot
    let y2    = snap.yield_with(&provider).await?;            // optional precomputed via PriceProvider

    // Blocking wrappers for sync callers
    let annual = div.annual_dividend_blocking("AAPL")?;
    Ok(())
}

// Free functions for one-off scripts (no client setup)
let annual = divkit::annual_dividend_for("AAPL").await?;
```

**Types:**

- `Divkit` — stateful client; create once, reuse (connection pool + cache). Builder for base-url/cache-dir overrides.
- `DivEvent { period_start, period_end, amount, concept, accn, form }`.
- `DividendSnapshot { ticker, cik, annual_amount: f64, frequency: Frequency, history: Vec<DivEvent> }`.
  - `annual_amount` = sum of cash dividends with `period_end` in the trailing 365 days (falls back to 4× most-recent quarterly when only sparse periods exist; documented).
  - `yield_on(price: f64) -> f64` — `annual_amount / price`. Pure, price-free crate stays single-source-of-truth.
  - `yield_with(&impl PriceProvider) -> Result<f64>` — optional precomputed yield; `PriceProvider` is a one-method trait the caller wires to their own ThetaData/quote source. divkit ships no price feed (keeps the corporate-actions domain boundary clean).
- `Frequency` — inferred cadence (`Quarterly`/`SemiAnnual`/`Annual`/`Irregular`/`None`) from period spacing.
- `Error` / `Result` — one unified error enum.

**CLI** (`divkit-cli`): `get <TICKER>`, `history <TICKER>`, `backfill [--from-year N]`, `append-today`. `backfill`/`append-today` shell to the Python builder.

### CI / Actions (`.github/workflows/`)

- `nightly.yml` — daily after EDGAR's nightly XBRL refresh: run builder delta, commit `data/` if changed. Schedule chosen to fire *after* SEC publishes (EDGAR refreshes ~04:00–06:00 UTC).
- `backfill.yml` — `workflow_dispatch`, full max-depth (~2009→present) rebuild (allows the 1.39 GB completeness pass; longer timeout). `from_year` input overrides depth.
- `ci.yml` — `cargo test` + `clippy -D warnings` + `fmt --check`; lint the Python builder (`ruff`); validate parquet schema + manifest.
- `release.yml` — tag-driven crates.io publish.

## Repo layout

```
divkit/
├── Cargo.toml                 # workspace: crates/divkit, cli
├── crates/divkit/src/
│   ├── lib.rs  client.rs  snapshot.rs  frequency.rs
│   ├── fetcher.rs  parquet_io.rs        # from curvekit
│   ├── price.rs                          # PriceProvider trait
│   └── error.rs
├── cli/src/main.rs
├── builder/
│   ├── build.py  frames.py  bulk.py  schema.py
│   ├── pyproject.toml                    # edgartools, polars/pyarrow
│   └── README.md
├── data/dividends-YYYY.parquet  manifest.json
├── .github/workflows/{nightly,backfill,ci,release}.yml
├── README.md  CHANGELOG.md  LICENSE     # Apache-2.0 (crate); builder MIT-compatible
└── docs/superpowers/specs/2026-06-24-divkit-dividend-database-design.md
```

## Error handling

- Network failure with a warm cache → log warn, serve stale parquet (never hard-fail an existing workflow).
- Unknown ticker → `Ok(None)` from `annual_dividend`, empty `Vec` from `dividends` — not an error.
- Ticker with no dividend history (non-payer) → `annual_amount = 0.0`, `Frequency::None`.
- SHA-256 manifest mismatch → `Error::ChecksumMismatch`, corrupt bytes never cached.
- Builder: a single company's malformed XBRL is logged and skipped, never aborts the batch.

## Testing

- **Rust unit:** snapshot math (trailing-12mo sum, frequency inference, `yield_on`), date parsing, parquet round-trip, error mapping. Fixture parquet checked into `tests/`.
- **Rust integration:** `Divkit` against a local fixture server (mirror curvekit's fetcher tests); ETag 304 path; stale-fallback path; mirror fallback.
- **Builder:** frames-parse against a captured fixture JSON; dedup precedence (*Declared* over *CashPaid*); idempotent nightly (re-run → no diff); known-answer check on a few large-cap tickers (AAPL/MSFT/KO) against published dividend totals.
- **Schema guard in CI:** every `data/*.parquet` matches the declared schema; manifest digests verify.

## Out of scope (YAGNI)

- Ex-dividend / pay-date calendar (EDGAR lacks it).
- Forward/projected dividends.
- Special vs regular dividend classification (EDGAR doesn't reliably distinguish; all cash dividends are included).
- Non-US filers / ADR pass-through nuances beyond what EDGAR reports.
- A hosted JSON-RPC/REST service (curvekit has one; divkit ships SDK + CLI only unless later requested).
