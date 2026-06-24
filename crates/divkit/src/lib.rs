//! `divkit` — US equity dividends and dividend yield for Rust, from SEC EDGAR.
//!
//! Provides trailing-year annual dividend, dividend frequency, and yield
//! calculations sourced from EDGAR public-domain XBRL filings.
//!
//! # Quick start — free functions
//!
//! The simplest path: one call, no client to manage.
//!
//! ```no_run
//! use divkit::{annual_dividend_for, dividend_snapshot_for};
//!
//! #[tokio::main]
//! async fn main() -> divkit::Result<()> {
//!     // Trailing 12-month dividend
//!     if let Some(amt) = annual_dividend_for("KO").await? {
//!         println!("KO annual dividend: ${amt:.4}");
//!     }
//!
//!     // Full snapshot — frequency, history, and yield
//!     let snap = dividend_snapshot_for("KO").await?;
//!     let yield_pct = snap.yield_on(64.50) * 100.0;
//!     println!("KO dividend yield at $64.50: {yield_pct:.2}%");
//!     Ok(())
//! }
//! ```
//!
//! # Client pattern (connection-pool reuse)
//!
//! Create [`Divkit`] once and reuse it across calls to share the internal
//! reqwest connection pool.
//!
//! ```no_run
//! use divkit::Divkit;
//!
//! #[tokio::main]
//! async fn main() -> divkit::Result<()> {
//!     let client = Divkit::new();
//!
//!     // Annual dividend (trailing 12 months)
//!     if let Some(amt) = client.annual_dividend("KO").await? {
//!         println!("KO: ${amt:.4}");
//!     }
//!
//!     // Snapshot with frequency detection and yield helper
//!     let snap = client.dividend_snapshot("MSFT").await?;
//!     println!("MSFT frequency: {:?}", snap.frequency());
//!     println!("MSFT yield at $420: {:.2}%", snap.yield_on(420.0) * 100.0);
//!     Ok(())
//! }
//! ```
#![forbid(unsafe_code)]

mod error;
pub use error::{Error, Result};

mod record;
pub use record::{Concept, DivEvent, DividendSnapshot, Frequency};

mod price;
pub use price::PriceProvider;

pub mod parquet_io;
pub use parquet_io::{read_dividends, write_dividends, DivRow};

mod fetcher;

mod client;
pub use client::{annual_dividend_for, dividend_snapshot_for, dividends_for, Divkit};

mod cache;
pub use cache::DividendCache;
