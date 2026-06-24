//! `divkit-cli` — command-line interface for the divkit dividend database.
//!
//! # Subcommands
//!
//! - `get <TICKER>` — print trailing-year annual dividend and payment frequency.
//! - `history <TICKER>` — print every `DivEvent` in the ticker's history, ascending.
//! - `backfill [--from-year N] [--to-year M] [--with-bulk]` — delegate to the Python builder.
//! - `append-today` / `nightly` — run the nightly builder append.
//!
//! # Environment
//!
//! - `DIVKIT_BASE_URL` — override the data origin (default: GitHub raw).
//! - `DIVKIT_CACHE_DIR` — override the on-disk cache directory.
//!
//! # Builder prerequisite
//!
//! `backfill` and `append-today`/`nightly` require the `divkit-build` Python
//! package to be installed in the active environment:
//!
//! ```text
//! cd builder && pip install -e . && cd ..
//! ```

use clap::{Parser, Subcommand};
use divkit::{dividend_snapshot_for, Frequency};

// ---------------------------------------------------------------------------
// CLI shape
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "divkit-cli",
    about = "US equity dividend data — backed by SEC EDGAR XBRL"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print trailing-year annual dividend and frequency for a ticker.
    Get {
        /// Ticker symbol (case-insensitive).
        ticker: String,
    },
    /// Print the full dividend event history for a ticker, ascending by period end.
    History {
        /// Ticker symbol (case-insensitive).
        ticker: String,
    },
    /// Rebuild dividend parquet shards via the Python builder.
    ///
    /// Requires `divkit-build` installed: `cd builder && pip install -e .`
    Backfill {
        /// First year to include (default: builder default).
        #[arg(long, value_name = "YEAR")]
        from_year: Option<u32>,
        /// Last year to include (default: builder default).
        #[arg(long, value_name = "YEAR")]
        to_year: Option<u32>,
        /// Use bulk EDGAR download instead of per-company queries.
        #[arg(long)]
        with_bulk: bool,
    },
    /// Append today's EDGAR filings to the existing shards (nightly update).
    ///
    /// Alias: `nightly`. Requires `divkit-build` installed.
    #[command(alias = "nightly")]
    AppendToday,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Cmd::Get { ticker } => cmd_get(&ticker).await,
        Cmd::History { ticker } => cmd_history(&ticker).await,
        Cmd::Backfill { from_year, to_year, with_bulk } => {
            cmd_backfill(from_year, to_year, with_bulk)
        }
        Cmd::AppendToday => cmd_append_today(),
    }
}

// ---------------------------------------------------------------------------
// `get`
// ---------------------------------------------------------------------------

async fn cmd_get(ticker: &str) -> anyhow::Result<()> {
    match dividend_snapshot_for(ticker).await {
        Ok(snap) => {
            let annual = snap.annual_amount();
            let freq = snap.frequency();
            if annual == 0.0 && matches!(freq, Frequency::None) {
                println!("{ticker}  no dividend history");
            } else {
                println!(
                    "{ticker}  annual_dividend={annual:.3}  frequency={freq}",
                    ticker = snap.ticker,
                    freq = freq_label(&freq),
                );
            }
        }
        Err(divkit::Error::NotFound(_)) => {
            println!("{ticker}  no dividend history", ticker = ticker.to_uppercase());
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `history`
// ---------------------------------------------------------------------------

async fn cmd_history(ticker: &str) -> anyhow::Result<()> {
    match dividend_snapshot_for(ticker).await {
        Ok(snap) => {
            if snap.history.is_empty() {
                println!("{ticker}  no dividend history", ticker = snap.ticker);
                return Ok(());
            }
            // Header
            println!("{:<12}  {:>8}  {:<10}  form", "period_end", "amount", "concept");
            for ev in &snap.history {
                let concept = format!("{:?}", ev.concept);
                let form = ev.form.as_deref().unwrap_or("-");
                println!(
                    "{:<12}  {:>8.4}  {:<10}  {}",
                    ev.period_end, ev.amount, concept, form
                );
            }
        }
        Err(divkit::Error::NotFound(_)) => {
            println!("{ticker}  no dividend history", ticker = ticker.to_uppercase());
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `backfill`
// ---------------------------------------------------------------------------

fn cmd_backfill(from_year: Option<u32>, to_year: Option<u32>, with_bulk: bool) -> anyhow::Result<()> {
    let mut args = vec![
        "-m".to_string(),
        "divkit_builder.build".to_string(),
        "backfill".to_string(),
        "--out".to_string(),
        "data".to_string(),
    ];
    if let Some(y) = from_year {
        args.push("--from-year".to_string());
        args.push(y.to_string());
    }
    if let Some(y) = to_year {
        args.push("--to-year".to_string());
        args.push(y.to_string());
    }
    if with_bulk {
        args.push("--with-bulk".to_string());
    }

    let status = std::process::Command::new("python3")
        .args(&args)
        .status()
        .map_err(|e| anyhow::anyhow!("failed to launch python3: {e}\n\nEnsure divkit-build is installed: cd builder && pip install -e ."))?;

    if !status.success() {
        anyhow::bail!("builder exited with {status}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `append-today` / `nightly`
// ---------------------------------------------------------------------------

fn cmd_append_today() -> anyhow::Result<()> {
    let status = std::process::Command::new("python3")
        .args(["-m", "divkit_builder.build", "nightly", "--out", "data"])
        .status()
        .map_err(|e| anyhow::anyhow!("failed to launch python3: {e}\n\nEnsure divkit-build is installed: cd builder && pip install -e ."))?;

    if !status.success() {
        anyhow::bail!("builder exited with {status}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn freq_label(f: &Frequency) -> &'static str {
    match f {
        Frequency::Quarterly => "Quarterly",
        Frequency::SemiAnnual => "SemiAnnual",
        Frequency::Annual => "Annual",
        Frequency::Irregular => "Irregular",
        Frequency::None => "None",
    }
}
