//! `divkit` — US equity dividends and dividend yield for Rust, from SEC EDGAR.
#![forbid(unsafe_code)]

mod error;
pub use error::{Error, Result};

mod record;
pub use record::{Concept, DivEvent, DividendSnapshot, Frequency};
