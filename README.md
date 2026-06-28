# divkit

US equity dividends and dividend yield for Rust, from SEC EDGAR public-domain XBRL. Served from bundled parquet with on-demand fetch and a local cache. No API keys. Offline after the first query.

## Install

```toml
[dependencies]
divkit = "0.0.3"
```

To track unreleased changes, depend on the repository directly:

```toml
divkit = { git = "https://github.com/userFRM/divkit" }
```

## Quick start

```rust,no_run
use divkit::{annual_dividend_for, dividend_snapshot_for};

#[tokio::main]
async fn main() -> divkit::Result<()> {
    // Trailing 12-month annual dividend
    if let Some(annual_div) = annual_dividend_for("AAPL").await? {
        println!("AAPL annual dividend: ${annual_div:.4}");
    }

    // Full snapshot: frequency, history, and yield
    let snap = dividend_snapshot_for("KO").await?;
    let yield_pct = snap.yield_on(64.50) * 100.0;
    println!("KO dividend yield at $64.50: {yield_pct:.2}%");
    Ok(())
}
```

## Client pattern

Create a `Divkit` client once and reuse it across lookups. It owns the local cache and serves repeated queries without re-fetching.

```rust,no_run
use divkit::Divkit;

#[tokio::main]
async fn main() -> divkit::Result<()> {
    let client = Divkit::new(); // infallible

    if let Some(annual_div) = client.annual_dividend("AAPL").await? {
        println!("AAPL annual_div={annual_div:.4}");
    }

    let snap = client.dividend_snapshot("KO").await?;
    println!("KO frequency: {:?}", snap.frequency());
    println!("KO annual:    ${:.4}", snap.annual_amount());
    println!("KO yield at $64.50: {:.2}%", snap.yield_on(64.50) * 100.0);

    // Blocking variant for synchronous code, no async runtime needed
    if let Some(amt) = client.annual_dividend_blocking("MSFT")? {
        println!("MSFT annual dividend (sync): ${amt:.4}");
    }
    Ok(())
}
```

`annual_amount()` returns the Indicated Annual Dividend: the median of the last K regular payments times K, where K is the detected payment frequency. Using the median rejects special dividends and period-rollup anomalies, and the figure decays to `0.0` once the most recent dividend is older than about 400 days, so a company that stopped paying reads as a non-payer rather than stale data.

## Black-Scholes / option-Greeks integration

The annual dividend is the continuous dividend yield input to Black-Scholes option pricing. Most Greeks engines take the dividend yield as a caller-provided parameter, so divkit supplies it directly.

```rust,no_run
use divkit::annual_dividend_for;

async fn bs_dividend_yield(ticker: &str, spot: f64) -> divkit::Result<f64> {
    let annual_div = annual_dividend_for(ticker).await?.unwrap_or(0.0);
    Ok(annual_div / spot) // continuous dividend yield q = annual_div / spot
}
```

## CLI

```bash
divkit-cli get AAPL       # trailing-year annual dividend and frequency
divkit-cli history KO     # full dividend event history
divkit-cli backfill       # rebuild all parquet shards
divkit-cli nightly        # append today's filings to the current-year shard
```

## Data

Data comes from SEC EDGAR public-domain XBRL filings. The committed database holds 111,370 reconciled dividend observations across 2009 to 2026, covering every US SEC XBRL dividend filer. Structured XBRL dividend reporting did not exist before about 2009, so there is no earlier history, and the most recent one or two quarters may lag until issuers file.

divkit provides dividend amounts and the two fiscal-period dates (`period_start`, `period_end`). It does not provide ex-dividend, record, or pay dates, which SEC EDGAR does not publish in structured form. For those, use a dedicated corporate-actions feed.

The data is refreshed automatically by a nightly job. The published crate reads pre-built parquet from the repository, so it never calls SEC at runtime and needs no API key.

## Cache

On first use the client downloads each year shard and writes it to a local cache directory (`~/.cache/divkit/`, XDG-compliant). Each shard is verified against a published digest before it is cached. Subsequent queries are served from the local cache, so they run offline. On a network failure the cached copy is returned so existing workflows survive transient outages.

## API

Full API reference is on [docs.rs](https://docs.rs/divkit).

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
