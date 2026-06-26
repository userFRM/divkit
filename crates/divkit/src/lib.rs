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
//! For connection-pool reuse across many lookups, create a [`Divkit`] client
//! once and call its methods instead of the free functions.
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
