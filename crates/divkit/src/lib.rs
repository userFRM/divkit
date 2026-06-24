//! `divkit` — US equity dividends and dividend yield for Rust, from SEC EDGAR.
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
pub(crate) use fetcher::{default_cache_dir, resolved_base_url, CachedFetcher};
