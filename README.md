# divkit

US equity dividend service for Rust — Indicated Annual Dividend, payment
frequency, and yield calculation, served from bundled parquet shards sourced
from SEC EDGAR public-domain XBRL filings. No API keys. Offline after first
query.

## Install

```toml
# Cargo.toml
[dependencies]
divkit = { git = "https://github.com/userFRM/divkit" }
```

## Quick start — one-off scripts

```rust
use divkit::{annual_dividend_for, dividend_snapshot_for};

#[tokio::main]
async fn main() -> divkit::Result<()> {
    // Trailing 12-month annual dividend (primary use case: Black-Scholes input)
    if let Some(annual_div) = annual_dividend_for("AAPL").await? {
        println!("AAPL annual dividend: ${annual_div:.4}");
    }

    // Full snapshot — frequency, full history, and yield
    let snap = dividend_snapshot_for("KO").await?;
    let yield_pct = snap.yield_on(64.50) * 100.0;
    println!("KO dividend yield at $64.50: {yield_pct:.2}%");

    Ok(())
}
```

## Client pattern — connection pool reuse

```rust
use divkit::{Divkit, PriceProvider};

#[tokio::main]
async fn main() -> divkit::Result<()> {
    let client = Divkit::new();   // infallible, never fails

    // Trailing 12-month annual dividend
    if let Some(annual_div) = client.annual_dividend("AAPL").await? {
        println!("AAPL annual_div={annual_div:.4}");
    }

    // Full snapshot with frequency detection
    let snap = client.dividend_snapshot("KO").await?;
    println!("KO frequency: {:?}", snap.frequency());
    println!("KO annual:    ${:.4}", snap.annual_amount());

    // Yield given a known price
    let yield_pct = snap.yield_on(64.50) * 100.0;
    println!("KO yield at $64.50: {yield_pct:.2}%");

    // Yield wired to your own price feed (divkit ships no price source)
    // let yield_pct = snap.yield_with(&my_price_provider).await?;

    // Blocking from synchronous code — no async runtime needed
    if let Some(amt) = client.annual_dividend_blocking("MSFT")? {
        println!("MSFT annual dividend (sync): ${amt:.4}");
    }

    Ok(())
}
```

## Black-Scholes / option-Greeks integration

The primary use case for `annual_dividend` is supplying the continuous dividend
yield parameter to Black-Scholes option pricing. Many Greeks engines accept a
dividend yield as a caller-provided parameter rather than computing it
themselves. `divkit` closes that gap:

```rust
use divkit::annual_dividend_for;

async fn bs_dividend_yield(ticker: &str, spot: f64) -> divkit::Result<f64> {
    let annual_div = annual_dividend_for(ticker).await?.unwrap_or(0.0);
    // continuous dividend yield: q = annual_div / spot
    Ok(annual_div / spot)
}
```

Pass the result as the annual-dividend (or dividend-yield) argument to your
option-pricing or Greeks routine.

> [!TIP]
> `annual_amount()` returns the **Indicated Annual Dividend (IAD)** — the median of the last K regular payments times K, where K is the detected payment frequency (monthly 12, quarterly 4, semi-annual 2, annual 1). Using the median rejects special dividends and XBRL period-rollup anomalies, the same way institutional dividend feeds compute IAD. It decays to `0.0` once the most recent dividend is older than ~400 days, so a company that stopped paying reads as a non-payer rather than stale data.

## Data source and limitations

Data comes from SEC EDGAR public-domain XBRL filings. The primary concept is
`CommonStockDividendsPerShareDeclared`; `CommonStockDividendsPerShareCashPaid`
is used as a fallback when the primary concept is absent.

XBRL reports dividends in overlapping period contexts (discrete quarters plus
cumulative year-to-date and annual rollups). divkit reconciles these into
discrete payments — reconstructing a missing discrete period from a rollup
where needed (e.g. an issuer that files Q1–Q3 quarterly and rolls Q4 into the
annual figure) — so amounts are neither double-counted nor dropped.

The committed database holds **111,370 reconciled dividend observations across
2009–2026, every US SEC XBRL dividend filer** (frames sweep plus the
companyfacts bulk completeness pass).

> [!CAUTION]
> **divkit gives dividend _amounts_ and the two fiscal-period dates (`period_start`, `period_end`) — NOT ex-dividend dates, record dates, or pay dates.** SEC EDGAR does not publish those in structured form. divkit is the source for annual dividend, payment frequency, and dividend yield. It is **not** an ex-date calendar or a forward dividend schedule. If you need ex-dates, use a dedicated (licensed) corporate-actions feed.

> [!NOTE]
> Coverage is **US SEC XBRL filers, 2009 onward** — structured XBRL dividend reporting did not exist before ~2009, so there is no earlier history. The most recent one or two quarters may lag until issuers file. This is comprehensive for US dividend-payers in the XBRL era, not a claim of universal history. Frequency detection and IAD are most accurate for regular quarterly and monthly payers; a small number of issuers with irregular or internally inconsistent XBRL period reporting may have an approximate annual figure.

> [!IMPORTANT]
> The data is refreshed automatically by GitHub Actions (`nightly.yml` daily; `backfill.yml` for a full rebuild). The published crate reads pre-built parquet from the repo — it never calls SEC at runtime and needs no API key.

## API surface

### Free functions (one-off scripts)

| Function | Returns |
|---|---|
| `annual_dividend_for(ticker)` | `Result<Option<f64>>` — trailing 12-month sum |
| `dividends_for(ticker)` | `Result<Vec<DivEvent>>` — full history |
| `dividend_snapshot_for(ticker)` | `Result<DividendSnapshot>` |

### Client methods — async

| Method | Returns |
|---|---|
| `annual_dividend(ticker)` | `Result<Option<f64>>` |
| `dividends(ticker)` | `Result<Vec<DivEvent>>` |
| `dividend_snapshot(ticker)` | `Result<DividendSnapshot>` |

### Client methods — blocking (sync)

| Method | Returns |
|---|---|
| `annual_dividend_blocking(ticker)` | `Result<Option<f64>>` |
| `dividends_blocking(ticker)` | `Result<Vec<DivEvent>>` |
| `dividend_snapshot_blocking(ticker)` | `Result<DividendSnapshot>` |

### `DividendSnapshot`

| Method | Description |
|---|---|
| `annual_amount()` | Indicated Annual Dividend — median of last K payments × K |
| `frequency()` | Detected payment frequency (`Monthly`, `Quarterly`, `SemiAnnual`, `Annual`, `Irregular`, `None`) |
| `yield_on(price: f64)` | `annual_amount() / price` |
| `yield_with(&PriceProvider)` | Async yield using a caller-supplied price source |

### `PriceProvider` trait

```rust
pub trait PriceProvider {
    fn spot<'a>(&'a self, ticker: &'a str)
        -> Pin<Box<dyn Future<Output = Result<f64>> + Send + 'a>>;
}
```

Implement this trait against your own quote source and pass it to
`snap.yield_with(...)`. divkit ships no price feed.

## CLI

```bash
# Print trailing-year annual dividend and frequency
divkit-cli get AAPL

# Print full dividend event history
divkit-cli history KO

# Rebuild all parquet shards via the Python builder (run from repo root)
divkit-cli backfill

# Rebuild from a specific year
divkit-cli backfill --from-year 2020

# Use the bulk EDGAR download instead of per-company queries
divkit-cli backfill --with-bulk

# Append today's EDGAR filings to the current-year shard (nightly update)
divkit-cli append-today
# or equivalently:
divkit-cli nightly
```

The `backfill` and `append-today`/`nightly` subcommands delegate to the Python
builder. Install it first: `cd builder && pip install -e .`

## Data pipeline

The parquet shards in `data/` are built by `builder/` — a pure Python package
using `httpx` and `pyarrow` that fetches EDGAR company facts via the
SEC EDGAR public API. The Rust crate itself is pure Rust with no Python
dependency at runtime.

Set the `DIVKIT_CONTACT_EMAIL` environment variable when running the builder to
include a contact address in the SEC User-Agent header (required by EDGAR
policy). CI uses the `CONTACT_EMAIL` repository secret.

Two GitHub Actions workflows keep the data current:

- **nightly.yml** — cron `0 7 * * *` (07:00 UTC, daily): appends the latest
  EDGAR filings to the current-year shard via `divkit-cli nightly`.
- **backfill.yml** — `workflow_dispatch`: full historical fetch across all years.

## Environment overrides

| Variable | Effect |
|---|---|
| `DIVKIT_BASE_URL` | Replace the GitHub raw data origin |
| `DIVKIT_CACHE_DIR` | Override the on-disk cache directory |

## Cache

On first use, `Divkit` downloads each year shard from
`raw.githubusercontent.com/userFRM/divkit/main/data/` and writes it to
`~/.cache/divkit/` (XDG-compliant via the `directories` crate). Each shard's
SHA-256 digest is verified against `manifest.json` before being written to
cache. On network failure the stale cached file is returned so existing
workflows survive transient outages.

## Crates

| Crate | Description |
|---|---|
| `divkit` | Library — fetcher, cache, types, yield math |
| `divkit-cli` | Binary — get, history, backfill, append-today |

## License

Apache-2.0 — see [`LICENSE`](LICENSE).

Copyright 2026 userFRM
