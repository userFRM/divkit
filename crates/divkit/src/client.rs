//! Stateful `Divkit` client — async dividend endpoints with blocking wrappers.
//!
//! Fetches year-partitioned parquet shards from GitHub raw (or a configurable
//! origin) with ETag-aware caching, SHA-256 manifest verification, and CDN
//! mirror fallback. Falls back to stale cache on transient network failures.
//!
//! # Quick start — free functions
//!
//! ```no_run
//! use divkit::{annual_dividend_for, dividend_snapshot_for};
//!
//! #[tokio::main]
//! async fn main() -> divkit::Result<()> {
//!     // Annual dividend — trailing 365-day sum.
//!     if let Some(amt) = annual_dividend_for("KO").await? {
//!         println!("KO annual dividend: ${amt:.4}");
//!     }
//!
//!     // Full snapshot with frequency + yield helpers.
//!     let snap = dividend_snapshot_for("KO").await?;
//!     let yield_pct = snap.yield_on(64.50) * 100.0;
//!     println!("KO dividend yield at $64.50: {yield_pct:.2}%");
//!     Ok(())
//! }
//! ```
//!
//! # Client pattern (reuse across calls)
//!
//! ```no_run
//! use divkit::Divkit;
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> divkit::Result<()> {
//!     let client = Divkit::new();
//!
//!     let annual = client.annual_dividend("KO").await?;
//!     println!("KO: {:?}", annual);
//!
//!     let snap = client.dividend_snapshot("MSFT").await?;
//!     println!("MSFT yield at $420: {:.2}%", snap.yield_on(420.0) * 100.0);
//!     Ok(())
//! }
//! ```

use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::fetcher::{default_cache_dir, resolved_base_url, CachedFetcher};
use crate::parquet_io::{read_dividends, DivRow};
use crate::record::{DivEvent, DividendSnapshot};

// ---------------------------------------------------------------------------
// Divkit client
// ---------------------------------------------------------------------------

/// Stateful divkit client.
///
/// Wraps an ETag-aware cached fetcher and exposes flat async dividend
/// endpoint methods. Create once and reuse; the internal reqwest client is
/// kept alive for connection pooling.
///
/// # Infallible construction
///
/// ```no_run
/// use divkit::Divkit;
/// let client = Divkit::new(); // never fails
/// ```
///
/// # Builder pattern
///
/// ```no_run
/// use divkit::Divkit;
/// use std::path::PathBuf;
///
/// let client = Divkit::new()
///     .with_base_url("https://my-mirror.example.com/divkit")
///     .with_cache_dir(PathBuf::from("/tmp/divkit-test"));
/// ```
pub struct Divkit {
    fetcher: CachedFetcher,
}

impl Divkit {
    /// Create a client with the default GitHub raw backend and XDG cache.
    ///
    /// Reads `DIVKIT_BASE_URL` and `DIVKIT_CACHE_DIR` from the environment
    /// if set, otherwise uses the GitHub raw origin and `~/.cache/divkit/`.
    ///
    /// **This function never fails.** Errors are deferred to the first fetch.
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .user_agent("divkit/0.1 (+https://github.com/userFRM/divkit)")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            fetcher: CachedFetcher::new(http, resolved_base_url(), default_cache_dir()),
        }
    }

    /// Override the origin URL.
    ///
    /// Default: `https://raw.githubusercontent.com/userFRM/divkit/main/data`.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.fetcher.set_base_url(url.into());
        self
    }

    /// Override the on-disk cache directory.
    ///
    /// Default: `~/.cache/divkit/` (XDG via the `directories` crate).
    pub fn with_cache_dir(mut self, dir: PathBuf) -> Self {
        self.fetcher.set_cache_dir(dir);
        self
    }

    /// Override the CDN mirror URL used when the primary fetch fails.
    ///
    /// - `Some(url)` — use a custom mirror.
    /// - `None` — disable mirror fallback entirely (useful in tests).
    pub fn with_mirror_url(mut self, url: Option<String>) -> Self {
        self.fetcher.set_mirror_url(url);
        self
    }

    // ── Async endpoints ───────────────────────────────────────────────────────

    /// Fetch all dividend events for `ticker` across all available year shards.
    ///
    /// Ticker matching is case-insensitive.
    ///
    /// # Errors
    ///
    /// - Network failure with no cached shards.
    /// - Corrupt parquet data.
    pub async fn dividends(&self, ticker: &str) -> Result<Vec<DivEvent>> {
        let rows = self.load_all_rows().await?;
        Ok(filter_ticker(&rows, ticker)
            .into_iter()
            .map(row_to_event)
            .collect())
    }

    /// Build a `DividendSnapshot` for `ticker` from all available year shards.
    ///
    /// Returns `NotFound` if the ticker is absent from every shard.
    ///
    /// # Errors
    ///
    /// - Network failure with no cached shards.
    /// - `ticker` not found in any shard (returns [`Error::NotFound`]).
    pub async fn dividend_snapshot(&self, ticker: &str) -> Result<DividendSnapshot> {
        let rows = self.load_all_rows().await?;
        let matching = filter_ticker(&rows, ticker);
        if matching.is_empty() {
            return Err(Error::NotFound(format!("no dividend data for {ticker}")));
        }
        // All matching rows share the same CIK; take it from the first.
        let cik = matching[0].cik;
        let events: Vec<DivEvent> = matching.into_iter().map(row_to_event).collect();
        Ok(DividendSnapshot::from_events(
            ticker.to_uppercase(),
            cik,
            events,
        ))
    }

    /// Trailing 12-month annual dividend for `ticker`.
    ///
    /// Returns `Ok(None)` when the ticker has no dividend history in any shard.
    ///
    /// # Errors
    ///
    /// - Network failure with no cached shards.
    /// - Corrupt parquet data.
    pub async fn annual_dividend(&self, ticker: &str) -> Result<Option<f64>> {
        let rows = self.load_all_rows().await?;
        let matching = filter_ticker(&rows, ticker);
        if matching.is_empty() {
            return Ok(None);
        }
        let cik = matching[0].cik;
        let events: Vec<DivEvent> = matching.into_iter().map(row_to_event).collect();
        let snap = DividendSnapshot::from_events(ticker.to_uppercase(), cik, events);
        Ok(Some(snap.annual_amount()))
    }

    // ── Blocking wrappers ─────────────────────────────────────────────────────

    /// Blocking variant of [`dividends`][Self::dividends].
    ///
    /// Works from both sync and async contexts — see [`annual_dividend_blocking`][Self::annual_dividend_blocking].
    pub fn dividends_blocking(&self, ticker: &str) -> Result<Vec<DivEvent>> {
        block(self.dividends(ticker))
    }

    /// Blocking variant of [`dividend_snapshot`][Self::dividend_snapshot].
    pub fn dividend_snapshot_blocking(&self, ticker: &str) -> Result<DividendSnapshot> {
        block(self.dividend_snapshot(ticker))
    }

    /// Blocking variant of [`annual_dividend`][Self::annual_dividend].
    ///
    /// Works from both sync and async contexts:
    /// - Inside a tokio multi-thread runtime: uses `block_in_place` + `Handle::block_on`.
    /// - Outside any runtime: spins up a minimal current-thread runtime.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use divkit::Divkit;
    ///
    /// // From synchronous code — no async needed
    /// let client = Divkit::new();
    /// if let Some(amt) = client.annual_dividend_blocking("KO")? {
    ///     println!("KO annual dividend: ${amt:.4}");
    /// }
    /// # Ok::<(), divkit::Error>(())
    /// ```
    pub fn annual_dividend_blocking(&self, ticker: &str) -> Result<Option<f64>> {
        block(self.annual_dividend(ticker))
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Fetch `manifest.json`, collect `dividends-YYYY.parquet` keys, fetch
    /// each shard, and flat-concatenate all rows.
    pub(crate) async fn load_all_rows(&self) -> Result<Vec<DivRow>> {
        let shard_keys = self.discover_shards().await?;
        let mut all_rows = Vec::new();
        for key in shard_keys {
            // key is like "dividends-2024" (no extension); fetcher appends .parquet
            let bytes = self.fetcher.fetch(&key).await?;
            let rows = read_dividends(&bytes)?;
            all_rows.extend(rows);
        }
        Ok(all_rows)
    }

    /// Fetch `manifest.json` and return sorted shard keys (without `.parquet`).
    ///
    /// The manifest is a JSON object whose keys are filenames like
    /// `"dividends-2024.parquet"`. We strip the `.parquet` suffix to get the
    /// fetch key passed to `CachedFetcher::fetch`.
    async fn discover_shards(&self) -> Result<Vec<String>> {
        let manifest_url = format!("{}/manifest.json", self.fetcher.base_url);
        let resp = self
            .fetcher
            .http
            .get(&manifest_url)
            .send()
            .await
            .map_err(Error::Http)?;

        if !resp.status().is_success() {
            return Err(Error::Other(format!(
                "manifest.json: HTTP {} {}",
                resp.status().as_u16(),
                resp.status().canonical_reason().unwrap_or("")
            )));
        }

        let manifest: serde_json::Value = resp.json().await.map_err(Error::Http)?;

        let obj = manifest
            .as_object()
            .ok_or_else(|| Error::Other("manifest.json is not a JSON object".into()))?;

        let mut keys: Vec<String> = obj
            .keys()
            .filter(|k| is_dividend_shard(k))
            .map(|k| k.trim_end_matches(".parquet").to_string())
            .collect();
        keys.sort();
        Ok(keys)
    }
}

impl Default for Divkit {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Module-level free functions
// ---------------------------------------------------------------------------

/// Annual dividend for `ticker` using a temporary one-shot client.
///
/// Equivalent to `Divkit::new().annual_dividend(ticker).await`.
/// Use when you need a single call and do not want to manage a client instance.
///
/// # Example
///
/// ```no_run
/// use divkit::annual_dividend_for;
///
/// # #[tokio::main] async fn main() -> divkit::Result<()> {
/// if let Some(amt) = annual_dividend_for("KO").await? {
///     println!("KO annual dividend: ${amt:.4}");
/// }
/// # Ok(()) }
/// ```
pub async fn annual_dividend_for(ticker: &str) -> Result<Option<f64>> {
    Divkit::new().annual_dividend(ticker).await
}

/// All dividend events for `ticker` using a temporary one-shot client.
///
/// # Example
///
/// ```no_run
/// use divkit::dividends_for;
///
/// # #[tokio::main] async fn main() -> divkit::Result<()> {
/// for ev in dividends_for("KO").await? {
///     println!("{}: ${}", ev.period_end, ev.amount);
/// }
/// # Ok(()) }
/// ```
pub async fn dividends_for(ticker: &str) -> Result<Vec<DivEvent>> {
    Divkit::new().dividends(ticker).await
}

/// `DividendSnapshot` for `ticker` using a temporary one-shot client.
///
/// # Example
///
/// ```no_run
/// use divkit::dividend_snapshot_for;
///
/// # #[tokio::main] async fn main() -> divkit::Result<()> {
/// let snap = dividend_snapshot_for("KO").await?;
/// println!("KO yield at $64.50: {:.2}%", snap.yield_on(64.50) * 100.0);
/// # Ok(()) }
/// ```
pub async fn dividend_snapshot_for(ticker: &str) -> Result<DividendSnapshot> {
    Divkit::new().dividend_snapshot(ticker).await
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Return `true` for filenames matching `dividends-YYYY.parquet`.
fn is_dividend_shard(name: &str) -> bool {
    // Expected shape: dividends-NNNN.parquet  (4+ digit year is fine too)
    let Some(rest) = name.strip_prefix("dividends-") else {
        return false;
    };
    let Some(year_str) = rest.strip_suffix(".parquet") else {
        return false;
    };
    !year_str.is_empty() && year_str.bytes().all(|b| b.is_ascii_digit())
}

/// Case-insensitive filter: keep rows whose `ticker` matches `target`.
fn filter_ticker<'a>(rows: &'a [DivRow], target: &str) -> Vec<&'a DivRow> {
    let upper = target.to_uppercase();
    rows.iter()
        .filter(|r| r.ticker.as_deref().map(|t| t.to_uppercase()) == Some(upper.clone()))
        .collect()
}

/// Map a `DivRow` reference to a `DivEvent`.
pub(crate) fn row_to_event(row: &DivRow) -> DivEvent {
    DivEvent {
        period_start: row.period_start,
        period_end: row.period_end,
        amount: row.amount,
        concept: row.concept,
        accn: row.accn.clone(),
        form: row.form.clone(),
    }
}

// ---------------------------------------------------------------------------
// Blocking helper
// ---------------------------------------------------------------------------

/// Drive a future to completion from any context (sync or async).
///
/// - Inside a tokio multi-thread runtime: `block_in_place` + `Handle::block_on`.
/// - Outside any runtime: spins up a minimal current-thread runtime.
pub(crate) fn block<F: std::future::Future<Output = Result<T>>, T>(fut: F) -> Result<T> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(Error::Io)?;
            rt.block_on(fut)
        }
    }
}
