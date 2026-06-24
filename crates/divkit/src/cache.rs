//! In-memory hydrated dividend cache — load once, O(1) synchronous lookups.
//!
//! High-throughput consumers (analytics engines querying hundreds of tickers)
//! should hydrate once at startup and then use synchronous O(1) accessors
//! rather than issuing per-ticker network calls.
//!
//! # Example
//!
//! ```no_run
//! use divkit::DividendCache;
//!
//! #[tokio::main]
//! async fn main() -> divkit::Result<()> {
//!     // Load all dividend data once.
//!     let cache = DividendCache::hydrate().await?;
//!
//!     // O(1) synchronous lookups — no further network I/O.
//!     if let Some(annual) = cache.annual_dividend("KO") {
//!         println!("KO annual dividend: ${annual:.4}");
//!     }
//!
//!     println!("Total tickers indexed: {}", cache.len());
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::client::{block, row_to_event};
use crate::error::Result;
use crate::record::{DivEvent, DividendSnapshot};
use crate::Divkit;

// ---------------------------------------------------------------------------
// Inner index
// ---------------------------------------------------------------------------

struct CacheInner {
    by_ticker: HashMap<String, DividendSnapshot>,
    by_cik: HashMap<u32, DividendSnapshot>,
}

// ---------------------------------------------------------------------------
// DividendCache
// ---------------------------------------------------------------------------

/// In-memory hydrated dividend cache.
///
/// Loaded once from all available year shards, then provides O(1) synchronous
/// lookups by ticker symbol or CIK. The underlying data is reference-counted,
/// so cloning is cheap — share a single hydrated cache across threads.
///
/// # Thread safety
///
/// `DividendCache` is `Send + Sync`. Clone it freely for use across tasks
/// or threads.
#[derive(Clone)]
pub struct DividendCache {
    inner: Arc<CacheInner>,
}

impl DividendCache {
    // ── Construction ──────────────────────────────────────────────────────────

    /// Load all dividend data from the default backend and build the in-memory
    /// index.
    ///
    /// Equivalent to `DividendCache::hydrate_with(&Divkit::new()).await`.
    pub async fn hydrate() -> Result<Self> {
        Self::hydrate_with(&Divkit::new()).await
    }

    /// Load all dividend data from `client` and build the in-memory index.
    ///
    /// Groups rows by CIK, derives a representative ticker (first non-`None`
    /// ticker among each CIK's rows, uppercased), constructs a
    /// [`DividendSnapshot`] per CIK, and indexes entries by both CIK and
    /// ticker symbol.
    pub async fn hydrate_with(client: &Divkit) -> Result<Self> {
        let rows = client.load_all_rows().await?;

        // Group rows by CIK.
        let mut by_cik_rows: HashMap<u32, Vec<usize>> = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            by_cik_rows.entry(row.cik).or_default().push(i);
        }

        let mut by_ticker: HashMap<String, DividendSnapshot> = HashMap::new();
        let mut by_cik: HashMap<u32, DividendSnapshot> = HashMap::new();

        for (cik, indices) in by_cik_rows {
            // Pick the first non-None ticker among this CIK's rows.
            let ticker_opt: Option<String> = indices
                .iter()
                .find_map(|&i| rows[i].ticker.as_deref().map(|t| t.to_uppercase()));

            let events: Vec<DivEvent> = indices.iter().map(|&i| row_to_event(&rows[i])).collect();

            let ticker_str = ticker_opt.clone().unwrap_or_default();
            let snap = DividendSnapshot::from_events(ticker_str, cik, events);

            if let Some(t) = ticker_opt {
                by_ticker.insert(t, snap.clone());
            }
            by_cik.insert(cik, snap);
        }

        Ok(Self {
            inner: Arc::new(CacheInner { by_ticker, by_cik }),
        })
    }

    /// Synchronous variant of [`hydrate`][Self::hydrate].
    ///
    /// Drives `hydrate()` to completion from any context (sync or async),
    /// using the same runtime strategy as the blocking client methods.
    pub fn hydrate_blocking() -> Result<Self> {
        block(Self::hydrate())
    }

    // ── Lookups ───────────────────────────────────────────────────────────────

    /// Return the `DividendSnapshot` for `ticker` (case-insensitive), or
    /// `None` if `ticker` is not in the cache.
    ///
    /// O(1).
    pub fn snapshot(&self, ticker: &str) -> Option<&DividendSnapshot> {
        self.inner.by_ticker.get(&ticker.to_uppercase())
    }

    /// Return the trailing-year annual dividend for `ticker`, or `None` if
    /// the ticker is absent from the cache.
    ///
    /// O(1).
    pub fn annual_dividend(&self, ticker: &str) -> Option<f64> {
        self.snapshot(ticker).map(|s| s.annual_amount())
    }

    /// Return all dividend events for `ticker` in ascending `period_end`
    /// order.
    ///
    /// Returns an empty slice for unknown tickers. O(1).
    pub fn dividends(&self, ticker: &str) -> &[DivEvent] {
        self.snapshot(ticker)
            .map(|s| s.history.as_slice())
            .unwrap_or(&[])
    }

    /// Return the `DividendSnapshot` for `cik`, or `None` if absent.
    ///
    /// O(1).
    pub fn snapshot_by_cik(&self, cik: u32) -> Option<&DividendSnapshot> {
        self.inner.by_cik.get(&cik)
    }

    /// Iterate over all indexed ticker symbols (uppercased).
    pub fn tickers(&self) -> impl Iterator<Item = &str> {
        self.inner.by_ticker.keys().map(String::as_str)
    }

    /// Number of CIK entries in the cache (one per issuer).
    pub fn len(&self) -> usize {
        self.inner.by_cik.len()
    }

    /// `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.by_cik.is_empty()
    }

    // ── Reload ────────────────────────────────────────────────────────────────

    /// Re-hydrate from the default backend and return a fresh cache.
    ///
    /// The existing cache is not mutated. Callers that share a cache via
    /// `Arc` or `Clone` should swap their reference to the returned value.
    ///
    /// ```no_run
    /// # use divkit::DividendCache;
    /// # async fn example() -> divkit::Result<()> {
    /// let mut cache = DividendCache::hydrate().await?;
    /// // ... later, refresh:
    /// cache = cache.reload().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reload(&self) -> Result<Self> {
        Self::hydrate().await
    }
}
