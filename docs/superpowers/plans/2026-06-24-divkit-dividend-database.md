# divkit Dividend Database Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build divkit — a public-domain US-equity dividend database (per-share cash dividends + dividend yield for any ticker) sourced from SEC EDGAR XBRL, shipped as a pure-Rust SDK over bundled parquet, with a Python builder that backfills ~15 years and refreshes nightly.

**Architecture:** Two halves joined by `data/*.parquet`. A Python builder (`builder/`, edgartools + EDGAR frames API + companyfacts.zip) produces year-sharded parquet committed to the repo. A pure-Rust crate (`crates/divkit`) reads that parquet over an ETag-cached GitHub-raw fetcher (adapted from curvekit) and exposes a flat async client + blocking wrappers + CLI. Python never enters the published crate's dependency tree.

**Tech Stack:** Rust (tokio, reqwest, arrow/parquet, chrono, thiserror, serde) · Python 3.11+ (edgartools, polars or pyarrow, httpx) · GitHub Actions.

## Global Constraints

- Rust edition `2021`; crate license `Apache-2.0`; workspace members `crates/divkit`, `cli`.
- Mirror curvekit conventions exactly: flat client methods, infallible `Divkit::new()`, blocking wrappers, env overrides `DIVKIT_BASE_URL` / `DIVKIT_CACHE_DIR`, XDG cache dir default `~/.cache/divkit/`.
- SEC `User-Agent` MUST be the bare form `divkit <contact-email>` — no parenthetical, no URL (SEC WAF returns 403 otherwise). The email is read from env `DIVKIT_CONTACT_EMAIL`, default generic placeholder `divkit-builder@example.com` (SEC accepts generic UAs — returns 200). NEVER hardcode a personal email in source committed to the public repo. CI injects the real address from GitHub secret `CONTACT_EMAIL`. Rate limit ≤ 10 req/s to `data.sec.gov` / `www.sec.gov`.
- XBRL concepts: `CommonStockDividendsPerShareDeclared` (primary), `CommonStockDividendsPerShareCashPaid` (fallback). Unit `USD-per-shares`. Duration frame keys `CY{YYYY}Q{n}` (NOT the `I` instant suffix).
- Parquet schema (one row per dividend observation): `cik:u32, ticker:string?, period_start:date32, period_end:date32, amount:f64, concept:string, accn:string, form:string?`. Files named `data/dividends-YYYY.parquet` sharded by `period_end` year. `manifest.json` is a FLAT object mapping filename → `"sha256:<hexdigest>"` (e.g. `{"dividends-2024.parquet": "sha256:abc..."}`) — the format the Rust fetcher deserializes as `HashMap<String,String>`; a nested form silently disables verification.
- Backfill default depth = earliest available XBRL (~2009) → present; `--from-year` overrides.
- No ex-date / pay-date fields (EDGAR doesn't provide them) — out of scope by design.
- Naming: no `Manager`/`Helper`/`Util`; use `Snapshot`/`Event`/`Provider`/`Client` idiom.
- Frequent commits; TDD (failing test first); DRY; YAGNI.

---

## File Structure

```
divkit/
├── Cargo.toml                          # workspace root
├── crates/divkit/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                      # crate docs, re-exports, free fns
│       ├── error.rs                    # Error / Result
│       ├── fetcher.rs                  # CachedFetcher (adapted from curvekit)
│       ├── parquet_io.rs               # read dividend rows from parquet
│       ├── record.rs                   # DivEvent, DividendSnapshot, Frequency
│       ├── price.rs                    # PriceProvider trait
│       └── client.rs                   # Divkit async client + blocking wrappers
├── cli/
│   ├── Cargo.toml
│   └── src/main.rs                     # get / history / backfill / append-today
├── builder/
│   ├── pyproject.toml
│   ├── divkit_builder/
│   │   ├── __init__.py
│   │   ├── sec.py                      # UA, rate-limited session, ticker→CIK
│   │   ├── frames.py                   # frames-API quarter sweep
│   │   ├── bulk.py                     # companyfacts.zip completeness pass
│   │   ├── schema.py                   # row dataclass + parquet write/merge
│   │   └── build.py                    # CLI: backfill / nightly
│   └── tests/
│       ├── fixtures/frames_cy2022q1.json
│       └── test_frames.py test_schema.py
├── data/                               # generated parquet + manifest.json
├── .github/workflows/{ci,nightly,backfill,release}.yml
├── README.md  CHANGELOG.md  LICENSE
└── docs/superpowers/{specs,plans}/...
```

---

### Task 1: Workspace scaffold

**Files:**
- Create: `Cargo.toml`, `crates/divkit/Cargo.toml`, `crates/divkit/src/lib.rs`, `cli/Cargo.toml`, `cli/src/main.rs`, `LICENSE`, `.gitignore`

**Interfaces:**
- Produces: a compiling empty workspace; crate name `divkit`, binary `divkit-cli`.

- [ ] **Step 1: Root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/divkit", "cli"]

[workspace.package]
version = "0.1.0"
edition = "2021"
authors = ["userFRM"]
license = "Apache-2.0"
repository = "https://github.com/userFRM/divkit"
documentation = "https://docs.rs/divkit"

[workspace.dependencies]
anyhow = "1.0"
thiserror = "2.0"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
chrono = { version = "0.4", features = ["serde"] }
reqwest = { version = "0.12", features = ["json", "gzip", "stream"] }
bytes = "1"
futures = "0.3"
sha2 = "0.10"
arrow = "53"
parquet = "53"
```

- [ ] **Step 2: `crates/divkit/Cargo.toml`**

```toml
[package]
name = "divkit"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
documentation.workspace = true
description = "US equity dividends and dividend yield for Rust, from SEC EDGAR public-domain XBRL. Companion to curvekit."

[dependencies]
thiserror.workspace = true
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }
chrono = { workspace = true }
reqwest = { workspace = true }
bytes = { workspace = true }
futures = { workspace = true }
sha2 = { workspace = true }
arrow = { workspace = true }
parquet = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 3: `crates/divkit/src/lib.rs` stub**

```rust
//! `divkit` — US equity dividends and dividend yield for Rust, from SEC EDGAR.
#![forbid(unsafe_code)]
```

- [ ] **Step 4: `cli/Cargo.toml`**

```toml
[package]
name = "divkit-cli"
version.workspace = true
edition.workspace = true
license.workspace = true

[[bin]]
name = "divkit-cli"
path = "src/main.rs"

[dependencies]
divkit = { path = "../crates/divkit" }
tokio = { workspace = true }
anyhow = { workspace = true }
```

- [ ] **Step 5: `cli/src/main.rs` stub**

```rust
fn main() {
    println!("divkit-cli");
}
```

- [ ] **Step 6: `.gitignore` + `LICENSE`**

`.gitignore`:
```
/target
**/*.rs.bk
__pycache__/
*.pyc
.venv/
/builder/.cache/
```
`LICENSE`: standard Apache-2.0 text, copyright `2026 userFRM`.

- [ ] **Step 7: Verify build**

Run: `cargo build`
Expected: workspace compiles, `divkit-cli` binary produced.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates cli LICENSE .gitignore
git commit -m "chore: scaffold divkit workspace"
```

---

### Task 2: Error type

**Files:**
- Create: `crates/divkit/src/error.rs`
- Modify: `crates/divkit/src/lib.rs`

**Interfaces:**
- Produces: `divkit::Error` (enum), `divkit::Result<T>`. Variants: `Http(#[from] reqwest::Error)`, `Io(#[from] std::io::Error)`, `Parquet(String)`, `Arrow(#[from] arrow::error::ArrowError)`, `ParquetNative(#[from] parquet::errors::ParquetError)`, `ChecksumMismatch{file,expected,actual}`, `NotFound(String)`, `Build(String)`, `Other(String)`.

- [ ] **Step 1: Write the failing test** — `crates/divkit/src/error.rs` bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checksum_mismatch_displays_both_digests() {
        let e = Error::ChecksumMismatch {
            file: "dividends-2020.parquet".into(),
            expected: "aaa".into(),
            actual: "bbb".into(),
        };
        let s = e.to_string();
        assert!(s.contains("aaa") && s.contains("bbb") && s.contains("dividends-2020.parquet"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p divkit error::`
Expected: FAIL — `Error` not defined.

- [ ] **Step 3: Implement `error.rs`** (modeled on curvekit's unified enum, dividend-specific variants):

```rust
//! Unified error type for divkit. All public methods return `divkit::Result<T>`.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parquet I/O error: {0}")]
    Parquet(String),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    ParquetNative(#[from] parquet::errors::ParquetError),
    #[error("checksum mismatch for {file}: expected sha256:{expected} got sha256:{actual}")]
    ChecksumMismatch { file: String, expected: String, actual: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("build error: {0}")]
    Build(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
```

- [ ] **Step 4: Wire into lib.rs**

Add to `lib.rs`:
```rust
mod error;
pub use error::{Error, Result};
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p divkit error::`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/divkit/src/error.rs crates/divkit/src/lib.rs
git commit -m "feat: divkit unified error type"
```

---

### Task 3: Dividend record types + snapshot math

**Files:**
- Create: `crates/divkit/src/record.rs`
- Modify: `crates/divkit/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `DivEvent { period_start: NaiveDate, period_end: NaiveDate, amount: f64, concept: Concept, accn: String, form: Option<String> }`
  - `enum Concept { Declared, CashPaid }`
  - `enum Frequency { Quarterly, SemiAnnual, Annual, Irregular, None }`
  - `DividendSnapshot { ticker: String, cik: u32, history: Vec<DivEvent> }` with methods:
    - `annual_amount(&self) -> f64` — trailing-365-day sum of `amount`, deduped by `period_end`; if no event in trailing 365d but history exists, `4 ×` most-recent amount when `frequency()==Quarterly`, `2×` for SemiAnnual, `1×` for Annual, else most-recent amount.
    - `frequency(&self) -> Frequency` — from median spacing of distinct `period_end`s (≤45d→Quarterly via count, see code).
    - `yield_on(&self, price: f64) -> f64` — `annual_amount()/price` (0.0 if price≤0).
  - `DividendSnapshot::from_events(ticker, cik, events) -> Self` — sorts history ascending by `period_end`.

- [ ] **Step 1: Write the failing tests** — `record.rs` bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ev(end: &str, amt: f64) -> DivEvent {
        let d = NaiveDate::parse_from_str(end, "%Y-%m-%d").unwrap();
        DivEvent { period_start: d, period_end: d, amount: amt,
            concept: Concept::Declared, accn: "x".into(), form: None }
    }

    #[test]
    fn annual_amount_sums_trailing_year() {
        // 4 quarterly dividends within the trailing 365d window from the last one
        let snap = DividendSnapshot::from_events("KO".into(), 21344,
            vec![ev("2024-03-15",0.485), ev("2024-06-14",0.485),
                 ev("2024-09-13",0.485), ev("2024-12-13",0.485)]);
        assert!((snap.annual_amount() - 1.94).abs() < 1e-9);
    }

    #[test]
    fn frequency_quarterly_detected() {
        let snap = DividendSnapshot::from_events("KO".into(), 21344,
            vec![ev("2024-03-15",0.485), ev("2024-06-14",0.485),
                 ev("2024-09-13",0.485), ev("2024-12-13",0.485)]);
        assert_eq!(snap.frequency(), Frequency::Quarterly);
    }

    #[test]
    fn non_payer_is_zero_and_none() {
        let snap = DividendSnapshot::from_events("XYZ".into(), 1, vec![]);
        assert_eq!(snap.annual_amount(), 0.0);
        assert_eq!(snap.frequency(), Frequency::None);
        assert_eq!(snap.yield_on(100.0), 0.0);
    }

    #[test]
    fn yield_on_divides_amount_by_price() {
        let snap = DividendSnapshot::from_events("KO".into(), 21344,
            vec![ev("2024-03-15",0.485), ev("2024-06-14",0.485),
                 ev("2024-09-13",0.485), ev("2024-12-13",0.485)]);
        let y = snap.yield_on(50.0);
        assert!((y - (1.94/50.0)).abs() < 1e-9);
        assert_eq!(snap.yield_on(0.0), 0.0);
    }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p divkit record::`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement `record.rs`**

```rust
//! Dividend event and snapshot types with trailing-year and yield math.
use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Concept { Declared, CashPaid }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Frequency { Quarterly, SemiAnnual, Annual, Irregular, None }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DivEvent {
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub amount: f64,
    pub concept: Concept,
    pub accn: String,
    pub form: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DividendSnapshot {
    pub ticker: String,
    pub cik: u32,
    pub history: Vec<DivEvent>, // ascending by period_end
}

impl DividendSnapshot {
    pub fn from_events(ticker: String, cik: u32, mut events: Vec<DivEvent>) -> Self {
        events.sort_by_key(|e| e.period_end);
        Self { ticker, cik, history: events }
    }

    /// Distinct period_end events (dedup keeps first = Declared-preferred upstream).
    fn distinct(&self) -> Vec<&DivEvent> {
        let mut seen = std::collections::HashSet::new();
        self.history.iter().filter(|e| seen.insert(e.period_end)).collect()
    }

    pub fn frequency(&self) -> Frequency {
        let ev = self.distinct();
        if ev.is_empty() { return Frequency::None; }
        if ev.len() == 1 { return Frequency::Irregular; }
        // median spacing in days between consecutive distinct period_ends
        let mut gaps: Vec<i64> = ev.windows(2)
            .map(|w| (w[1].period_end - w[0].period_end).num_days()).collect();
        gaps.sort_unstable();
        let med = gaps[gaps.len() / 2];
        match med {
            d if d <= 45 => Frequency::Quarterly,   // monthly payers also bucket here as "frequent"
            d if d <= 135 => Frequency::Quarterly,
            d if d <= 225 => Frequency::SemiAnnual,
            d if d <= 450 => Frequency::Annual,
            _ => Frequency::Irregular,
        }
    }

    pub fn annual_amount(&self) -> f64 {
        let ev = self.distinct();
        if ev.is_empty() { return 0.0; }
        let last = ev.last().unwrap().period_end;
        let cutoff = last - Duration::days(365);
        let trailing: f64 = ev.iter()
            .filter(|e| e.period_end > cutoff && e.period_end <= last)
            .map(|e| e.amount).sum();
        if trailing > 0.0 { return trailing; }
        // Fallback: annualize the most-recent amount by inferred frequency.
        let recent = ev.last().unwrap().amount;
        match self.frequency() {
            Frequency::Quarterly => recent * 4.0,
            Frequency::SemiAnnual => recent * 2.0,
            Frequency::Annual => recent,
            _ => recent,
        }
    }

    pub fn yield_on(&self, price: f64) -> f64 {
        if price <= 0.0 { return 0.0; }
        self.annual_amount() / price
    }
}
```

- [ ] **Step 4: Wire into lib.rs**

```rust
mod record;
pub use record::{Concept, DivEvent, DividendSnapshot, Frequency};
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p divkit record::`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/divkit/src/record.rs crates/divkit/src/lib.rs
git commit -m "feat: dividend record types + trailing-year/yield math"
```

---

### Task 4: PriceProvider trait + yield_with

**Files:**
- Create: `crates/divkit/src/price.rs`
- Modify: `crates/divkit/src/record.rs` (add async `yield_with`), `crates/divkit/src/lib.rs`

**Interfaces:**
- Produces: `trait PriceProvider { async fn spot(&self, ticker: &str) -> Result<f64>; }` (via `async-trait`-free native async or boxed). Use a simple object-safe form:
  ```rust
  pub trait PriceProvider {
      fn spot<'a>(&'a self, ticker: &'a str)
          -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<f64>> + Send + 'a>>;
  }
  ```
- `DividendSnapshot::yield_with(&self, p: &dyn PriceProvider) -> Result<f64>`.

- [ ] **Step 1: Write the failing test** — `price.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DivEvent, DividendSnapshot, Concept};
    use chrono::NaiveDate;

    struct Fixed(f64);
    impl PriceProvider for Fixed {
        fn spot<'a>(&'a self, _t: &'a str)
            -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<f64>> + Send + 'a>> {
            let v = self.0;
            Box::pin(async move { Ok(v) })
        }
    }

    #[tokio::test]
    async fn yield_with_uses_provider_price() {
        let d = NaiveDate::parse_from_str("2024-12-13", "%Y-%m-%d").unwrap();
        let snap = DividendSnapshot::from_events("KO".into(), 21344, vec![
            DivEvent { period_start: d, period_end: d, amount: 1.94,
                concept: Concept::Declared, accn: "x".into(), form: None }]);
        // single event → annual_amount fallback = recent (Irregular) = 1.94
        let y = snap.yield_with(&Fixed(50.0)).await.unwrap();
        assert!((y - 1.94/50.0).abs() < 1e-9);
    }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p divkit price::`
Expected: FAIL — `PriceProvider` not defined.

- [ ] **Step 3: Implement `price.rs`**

```rust
//! `PriceProvider` — caller-supplied spot price source for precomputed yield.
//! divkit ships no price feed; wire this to your own market-data/quote source.
use crate::Result;

pub trait PriceProvider {
    fn spot<'a>(&'a self, ticker: &'a str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<f64>> + Send + 'a>>;
}
```

- [ ] **Step 4: Add `yield_with` to `record.rs`**

```rust
impl DividendSnapshot {
    pub async fn yield_with(&self, p: &dyn crate::price::PriceProvider) -> crate::Result<f64> {
        let price = p.spot(&self.ticker).await?;
        Ok(self.yield_on(price))
    }
}
```

- [ ] **Step 5: Wire into lib.rs**

```rust
mod price;
pub use price::PriceProvider;
```

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p divkit price::`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/divkit/src/price.rs crates/divkit/src/record.rs crates/divkit/src/lib.rs
git commit -m "feat: PriceProvider trait + yield_with"
```

---

### Task 5: Parquet reader for dividend rows

**Files:**
- Create: `crates/divkit/src/parquet_io.rs`
- Modify: `crates/divkit/src/lib.rs`
- Test fixture: `crates/divkit/tests/fixtures/dividends-2024.parquet` (generate in Step 1)

**Interfaces:**
- Consumes: `DivEvent`, `Concept`.
- Produces:
  - `struct DivRow { cik: u32, ticker: Option<String>, period_start: NaiveDate, period_end: NaiveDate, amount: f64, concept: Concept, accn: String, form: Option<String> }`
  - `pub fn read_dividends(bytes: &[u8]) -> Result<Vec<DivRow>>` — parses one parquet file (in memory) into rows.
  - `pub fn write_dividends(path: &Path, rows: &[DivRow]) -> Result<()>` — used by tests + any Rust-side tooling; column order per Global Constraints schema.

- [ ] **Step 1: Generate the fixture** — write a tiny Rust test helper or use Python builder later; for now create via a `#[test] #[ignore] fn make_fixture()` that calls `write_dividends` with 4 KO rows, run it once with `--ignored`, commit the output. (Concrete: implement `write_dividends` first in Step 3, then Step 1b generates the fixture.)

- [ ] **Step 2: Write the failing test** — `parquet_io.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trips_dividend_rows() {
        let dir = std::env::temp_dir().join("divkit_pq_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dividends-2024.parquet");
        let d = chrono::NaiveDate::parse_from_str("2024-03-15","%Y-%m-%d").unwrap();
        let rows = vec![DivRow {
            cik: 21344, ticker: Some("KO".into()), period_start: d, period_end: d,
            amount: 0.485, concept: crate::Concept::Declared, accn: "a".into(), form: Some("10-Q".into()) }];
        write_dividends(&path, &rows).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let back = read_dividends(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].cik, 21344);
        assert_eq!(back[0].ticker.as_deref(), Some("KO"));
        assert!((back[0].amount - 0.485).abs() < 1e-9);
        assert_eq!(back[0].concept, crate::Concept::Declared);
    }
}
```

- [ ] **Step 3: Run to verify fail**

Run: `cargo test -p divkit parquet_io::`
Expected: FAIL — `DivRow` / functions undefined.

- [ ] **Step 4: Implement `parquet_io.rs`** using arrow `RecordBatch` + parquet reader/writer. Build columns: `UInt32Array` (cik), `StringArray` (ticker, nullable), `Date32Array` (period_start, period_end — days since epoch via `NaiveDate - epoch`), `Float64Array` (amount), `StringArray` (concept as `"Declared"`/`"CashPaid"`), `StringArray` (accn), `StringArray` (form, nullable). Reader maps back; `Date32` → `NaiveDate` via `epoch + Duration::days(v)`. Map arrow/parquet errors through `?` (the `#[from]` variants). Reference curvekit `crates/curvekit/src/sources/parquet_io.rs` for the exact arrow 53 builder/reader idioms.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p divkit parquet_io::`
Expected: PASS.

- [ ] **Step 6: Generate + commit fixture**

Add `#[ignore] fn make_fixture()` writing 4 KO rows to `crates/divkit/tests/fixtures/dividends-2024.parquet`; run `cargo test -p divkit make_fixture -- --ignored`; commit the parquet.

- [ ] **Step 7: Commit**

```bash
git add crates/divkit/src/parquet_io.rs crates/divkit/src/lib.rs crates/divkit/tests/fixtures/
git commit -m "feat: parquet read/write for dividend rows"
```

---

### Task 6: CachedFetcher (adapted from curvekit)

**Files:**
- Create: `crates/divkit/src/fetcher.rs`
- Modify: `crates/divkit/src/lib.rs`

**Interfaces:**
- Consumes: `Error`/`Result`.
- Produces: `CachedFetcher` with `pub fn new(http: reqwest::Client, base_url: String, cache_dir: PathBuf) -> Self` and `pub async fn fetch(&self, key: &str) -> Result<Bytes>`; helpers `default_cache_dir() -> PathBuf` (`DIVKIT_CACHE_DIR` env → else XDG `~/.cache/divkit`), `resolved_base_url() -> String` (`DIVKIT_BASE_URL` env → else `https://raw.githubusercontent.com/userFRM/divkit/main/data`).

- [ ] **Step 1: Copy curvekit fetcher** — copy `curvekit/crates/curvekit/src/fetcher.rs` into `crates/divkit/src/fetcher.rs`. Replace `CURVEKIT_CACHE_DIR`→`DIVKIT_CACHE_DIR`, `CURVEKIT_BASE_URL`→`DIVKIT_BASE_URL`, cache dir name `curvekit`→`divkit`, and the default GitHub raw URL to the divkit repo `data` path. Keep ETag/single-flight/mirror/SHA-256/stale-fallback logic intact. Adjust any `crate::error::Error` variant names to divkit's (`Parquet`, `ChecksumMismatch`, etc.).

- [ ] **Step 2: Write a failing test** — local fixture server returns bytes + ETag; second fetch sends `If-None-Match` and gets 304 → returns cached bytes. (Mirror curvekit's fetcher test; adapt names.) If curvekit has no such test, write one with a `tokio` `TcpListener` stub returning a fixed body then `304`.

```rust
#[cfg(test)]
mod tests {
    // 1) first fetch writes cache, 2) second fetch with matching ETag → 304 → cached bytes returned
    // (full server stub adapted from curvekit fetcher tests)
}
```

- [ ] **Step 3: Run to verify fail then pass**

Run: `cargo test -p divkit fetcher::`
Expected: compiles; test passes after wiring.

- [ ] **Step 4: Wire into lib.rs**

```rust
mod fetcher;
pub(crate) use fetcher::{default_cache_dir, resolved_base_url, CachedFetcher};
```

- [ ] **Step 5: Commit**

```bash
git add crates/divkit/src/fetcher.rs crates/divkit/src/lib.rs
git commit -m "feat: ETag-cached GitHub-raw fetcher (adapted from curvekit)"
```

---

### Task 7: Divkit client + free functions

**Files:**
- Create: `crates/divkit/src/client.rs`
- Modify: `crates/divkit/src/lib.rs`

**Interfaces:**
- Consumes: `CachedFetcher`, `read_dividends`, `DivRow`, `DivEvent`, `DividendSnapshot`, `Concept`.
- Produces:
  - `struct Divkit` with `pub fn new() -> Self` (infallible), builder `with_base_url`, `with_cache_dir`.
  - `pub async fn dividend_snapshot(&self, ticker: &str) -> Result<DividendSnapshot>` — fetch the year shards, filter rows by ticker (case-insensitive), build snapshot.
  - `pub async fn annual_dividend(&self, ticker: &str) -> Result<Option<f64>>` — `None` if no history, else `Some(snapshot.annual_amount())`.
  - `pub async fn dividends(&self, ticker: &str) -> Result<Vec<DivEvent>>`.
  - Blocking wrappers `*_blocking` via an internal `tokio::runtime::Runtime` (mirror curvekit).
  - Free fns: `annual_dividend_for(ticker)`, `dividends_for(ticker)`, `dividend_snapshot_for(ticker)`.
- Data access: the client needs to know which year shards exist. Ship a fetched `index.json` listing available years (builder writes it), or fetch `manifest.json` keys. Use `manifest.json` (already required by fetcher for checksums): parse it for the list of `dividends-YYYY.parquet` keys, fetch each, concatenate rows, filter by ticker.

- [ ] **Step 1: Write the failing integration test** — `crates/divkit/tests/client.rs`: serve `crates/divkit/tests/fixtures/` (manifest + dividends-2024.parquet) over a local HTTP server, point `DIVKIT_BASE_URL` at it, assert `annual_dividend("KO")` ≈ trailing sum and unknown ticker → `Ok(None)`.

```rust
#[tokio::test]
async fn annual_dividend_for_known_ticker() {
    // start static server over tests/fixtures, set base url via Divkit::new().with_base_url(...)
    // assert annual_dividend("KO") == Some(~1.94) and annual_dividend("NOPE") == None
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p divkit --test client`
Expected: FAIL — methods undefined.

- [ ] **Step 3: Implement `client.rs`** — construct reqwest client, `CachedFetcher`; `load_all_rows()` fetches `manifest.json`, iterates `dividends-*.parquet` keys, `read_dividends` each, flat-concat; `filter_ticker` case-insensitive on `DivRow.ticker`; map `DivRow → DivEvent`; build `DividendSnapshot::from_events`. Add blocking wrappers and module-level free fns constructing a temporary `Divkit::new()`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p divkit --test client`
Expected: PASS.

- [ ] **Step 5: Wire into lib.rs + crate docs** — `pub use client::Divkit;` + free fns; write the `//!` crate doc with quick-start (free fns) and client-pattern examples mirroring curvekit's lib.rs.

- [ ] **Step 6: Commit**

```bash
git add crates/divkit/src/client.rs crates/divkit/src/lib.rs crates/divkit/tests/
git commit -m "feat: Divkit client, blocking wrappers, free functions"
```

---

### Task 8: Python builder — SEC session + ticker→CIK

**Files:**
- Create: `builder/pyproject.toml`, `builder/divkit_builder/__init__.py`, `builder/divkit_builder/sec.py`, `builder/tests/test_sec.py`

**Interfaces:**
- Produces:
  - `sec.session() -> httpx.Client` with `User-Agent: divkit <email>` where `email = os.environ.get("DIVKIT_CONTACT_EMAIL", "divkit-builder@example.com")`, HTTP/2, and a ≤10 req/s limiter.
  - `sec.user_agent() -> str` — returns the resolved `divkit <email>` UA string (single source of truth, reused by frames/bulk).
  - `sec.ticker_cik_map() -> dict[str, int]` from `https://www.sec.gov/files/company_tickers.json` → `{TICKER: cik_int}` (upper-cased ticker, int CIK).
  - `sec.cik_ticker_map() -> dict[int, str]` inverse (first ticker wins).

- [ ] **Step 1: `pyproject.toml`**

```toml
[project]
name = "divkit-builder"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["edgartools>=2.6", "httpx[http2]>=0.27", "polars>=1.0", "pyarrow>=16"]

[project.scripts]
divkit-build = "divkit_builder.build:main"

[tool.ruff]
line-length = 100
```

- [ ] **Step 2: Write the failing test** — `builder/tests/test_sec.py`:

```python
from divkit_builder import sec

def test_ticker_cik_map_parses_fixture(monkeypatch):
    sample = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
    monkeypatch.setattr(sec, "_get_json", lambda url: sample)
    m = sec.ticker_cik_map()
    assert m["AAPL"] == 320193
```

- [ ] **Step 3: Run to verify fail**

Run: `cd builder && python -m pytest tests/test_sec.py -q`
Expected: FAIL — module/functions missing.

- [ ] **Step 4: Implement `sec.py`** — `CONTACT_EMAIL = os.environ.get("DIVKIT_CONTACT_EMAIL", "divkit-builder@example.com")`; `def user_agent() -> str: return f"divkit {CONTACT_EMAIL}"` (bare form, no parens/URL); module-level rate limiter (simple timestamp gate ≥0.1s between calls); `_get_json(url)` via shared client using `user_agent()`; `ticker_cik_map`/`cik_ticker_map` parse the dict-of-dicts JSON. `__init__.py` re-exports `sec`. Add a unit test asserting `user_agent()` is the bare `divkit <email>` form (no `(` or `http`).

- [ ] **Step 5: Run to verify pass** → `pytest tests/test_sec.py -q` PASS.

- [ ] **Step 6: Commit**

```bash
git add builder/pyproject.toml builder/divkit_builder/__init__.py builder/divkit_builder/sec.py builder/tests/test_sec.py
git commit -m "feat(builder): SEC session + ticker-CIK map"
```

---

### Task 9: Python builder — frames sweep

**Files:**
- Create: `builder/divkit_builder/frames.py`, `builder/tests/fixtures/frames_cy2022q1.json`, `builder/tests/test_frames.py`
- The fixture: save the real `CommonStockDividendsPerShareDeclared/USD-per-shares/CY2022Q1.json` (≈164 KB, already verified to return 200) — trim to ~5 entries for the test.

**Interfaces:**
- Produces:
  - `frames.fetch_quarter(concept: str, year: int, q: int) -> list[FrameEntry]` — GET the frames URL, return parsed entries (`cik, entityName, start, end, val, accn`).
  - `frames.sweep(from_year: int, to_year: int, quarters_done=None) -> Iterator[Row]` — for both concepts × every quarter, yield normalized rows; prefer `Declared` over `CashPaid` per `(cik, end)`.
  - `Row` dataclass matching the parquet schema (minus `ticker`, joined later): `cik, period_start, period_end, amount, concept, accn, form`.

- [ ] **Step 1: Save the fixture**

Run (proper UA):
```bash
curl -s -H "User-Agent: divkit divkit-builder@example.com" \
  "https://data.sec.gov/api/xbrl/frames/us-gaap/CommonStockDividendsPerShareDeclared/USD-per-shares/CY2022Q1.json" \
  | python -c "import json,sys; d=json.load(sys.stdin); d['data']=d['data'][:5]; print(json.dumps(d))" \
  > builder/tests/fixtures/frames_cy2022q1.json
```

- [ ] **Step 2: Write the failing test** — `test_frames.py`:

```python
import json
from divkit_builder import frames

def test_parse_frame_entries(monkeypatch):
    data = json.load(open("tests/fixtures/frames_cy2022q1.json"))
    monkeypatch.setattr(frames, "_get_json", lambda url: data)
    rows = frames.fetch_quarter("CommonStockDividendsPerShareDeclared", 2022, 1)
    assert len(rows) == 5
    assert rows[0].amount == data["data"][0]["val"]
    assert rows[0].period_end == data["data"][0]["end"]

def test_declared_preferred_over_cashpaid():
    # same (cik,end) from both concepts → Declared kept
    from divkit_builder.frames import _merge_prefer_declared, Row
    decl = Row(cik=1, period_start="2022-01-01", period_end="2022-03-31",
               amount=0.5, concept="Declared", accn="a", form=None)
    paid = Row(cik=1, period_start="2022-01-01", period_end="2022-03-31",
               amount=0.4, concept="CashPaid", accn="b", form=None)
    merged = _merge_prefer_declared([paid, decl])
    assert len(merged) == 1 and merged[0].concept == "Declared"
```

- [ ] **Step 3: Run to verify fail** → `pytest tests/test_frames.py -q` FAIL.

- [ ] **Step 4: Implement `frames.py`** — URL `https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/USD-per-shares/CY{year}Q{q}.json`; `fetch_quarter` maps each `data` entry → `Row(concept="Declared"|"CashPaid")`; on HTTP 404 (no such frame) return `[]`. `_merge_prefer_declared` dedups by `(cik, period_end)` keeping `Declared`. `sweep` loops concepts × quarters, accumulates, yields merged rows; logs per-quarter counts.

- [ ] **Step 5: Run to verify pass** → PASS.

- [ ] **Step 6: Commit**

```bash
git add builder/divkit_builder/frames.py builder/tests/fixtures/frames_cy2022q1.json builder/tests/test_frames.py
git commit -m "feat(builder): EDGAR frames quarter sweep with Declared precedence"
```

---

### Task 10: Python builder — bulk completeness pass

**Files:**
- Create: `builder/divkit_builder/bulk.py`, `builder/tests/test_bulk.py`

**Interfaces:**
- Produces:
  - `bulk.iter_company_dividends(zip_path: str, from_year: int) -> Iterator[Row]` — stream `companyfacts.zip`, for each `CIK#######.json` read `facts.us-gaap.{concept}.units.USD/shares[]`, yield `Row`s with `period_end` year ≥ from_year. Uses `zipfile` + streaming JSON per entry (do NOT load 1.39 GB into RAM at once).
  - `bulk.download(dest: str) -> str` — stream-download `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` with the SEC UA; resumes/skips if present and fresh.

- [ ] **Step 1: Write the failing test** — build a tiny in-memory zip with one fake `CIK0000021344.json` containing a `CommonStockDividendsPerShareDeclared` units array; assert `iter_company_dividends` yields the expected row.

```python
import io, json, zipfile
from divkit_builder import bulk

def test_iter_company_dividends_from_zip(tmp_path):
    facts = {"cik": 21344, "facts": {"us-gaap": {
        "CommonStockDividendsPerShareDeclared": {"units": {"USD/shares": [
            {"start":"2024-01-01","end":"2024-03-31","val":0.485,"accn":"a","form":"10-Q"}]}}}}}
    zp = tmp_path/"companyfacts.zip"
    with zipfile.ZipFile(zp,"w") as z:
        z.writestr("CIK0000021344.json", json.dumps(facts))
    rows = list(bulk.iter_company_dividends(str(zp), from_year=2009))
    assert len(rows) == 1 and rows[0].cik == 21344 and rows[0].amount == 0.485
```

- [ ] **Step 2: Run to verify fail** → FAIL.

- [ ] **Step 3: Implement `bulk.py`** — `zipfile.ZipFile`, iterate `namelist()`, for each `CIK*.json` `json.loads(z.read(name))`, drill into both concepts' `units["USD/shares"]`, filter by `from_year`, yield `Row`. `download` streams with `httpx` `stream=True` writing chunks. Reuse `frames.Row` + `_merge_prefer_declared`.

- [ ] **Step 4: Run to verify pass** → PASS.

- [ ] **Step 5: Commit**

```bash
git add builder/divkit_builder/bulk.py builder/tests/test_bulk.py
git commit -m "feat(builder): companyfacts.zip completeness pass"
```

---

### Task 11: Python builder — schema, parquet write, manifest

**Files:**
- Create: `builder/divkit_builder/schema.py`, `builder/tests/test_schema.py`

**Interfaces:**
- Produces:
  - `schema.write_year_shards(rows: Iterable[Row], cik_ticker: dict[int,str], out_dir: str) -> list[str]` — join ticker by CIK, group by `period_end` year, write `dividends-YYYY.parquet` with the exact column order/types from Global Constraints (polars or pyarrow). Returns written paths.
  - `schema.write_manifest(out_dir: str) -> None` — SHA-256 each `dividends-*.parquet`, write `manifest.json` in the EXACT FLAT format the Rust fetcher reads: a flat object mapping filename → `"sha256:<hexdigest>"` string, e.g. `{ "dividends-2024.parquet": "sha256:abcd...", "dividends-2025.parquet": "sha256:ef01..." }`. NOT a nested `{"sha256": "..."}` object — the fetcher deserializes the manifest as `HashMap<String,String>` and strips the `sha256:` prefix (`crates/divkit/src/fetcher.rs::manifest_digest_for`). A nested form fails to parse and silently disables verification. The hex digest is lowercase SHA-256 of the parquet file bytes.

- [ ] **Step 1: Write the failing test** — write 4 rows across 2 years → assert two parquet files exist with the right schema and a `manifest.json` with matching sha256.

- [ ] **Step 2: Run to verify fail** → FAIL.

- [ ] **Step 3: Implement `schema.py`** — explicit arrow schema (`cik:uint32, ticker:string, period_start:date32, period_end:date32, amount:float64, concept:string, accn:string, form:string`); bind columns explicitly (no silent-null getattr); dedup `(cik, period_end)` Declared-preferred before write; sort by `cik, period_end`.

- [ ] **Step 4: Run to verify pass** → PASS.

- [ ] **Step 5: Commit**

```bash
git add builder/divkit_builder/schema.py builder/tests/test_schema.py
git commit -m "feat(builder): year-sharded parquet + sha256 manifest"
```

---

### Task 12: Python builder — `build.py` CLI (backfill / nightly)

**Files:**
- Create: `builder/divkit_builder/build.py`, `builder/README.md`

**Interfaces:**
- Produces: `main()` argparse CLI:
  - `divkit-build backfill --from-year 2009 --out ../data [--with-bulk]` — frames sweep (+ optional bulk pass) → write shards + manifest.
  - `divkit-build nightly --out ../data` — frames for current + previous quarter, merge into existing shards (read existing parquet, union, dedup, rewrite touched years + manifest). Idempotent.

- [ ] **Step 1: Write a smoke test** — `backfill --from-year 2024 --to-year 2024` against monkeypatched frames returning the fixture → asserts `data/dividends-2024.parquet` + `manifest.json` written. (No network in test.)

- [ ] **Step 2: Run to verify fail** → FAIL.

- [ ] **Step 3: Implement `build.py`** — wire `sec` + `frames.sweep` + (optional) `bulk` + `schema`. `nightly` reads existing shards via pyarrow, unions new quarter rows, dedups, rewrites only changed years, regenerates manifest. Default `--from-year` resolves to earliest available (2009).

- [ ] **Step 4: Run to verify pass** → PASS. `builder/README.md` documents `pip install -e builder && divkit-build backfill`.

- [ ] **Step 5: Commit**

```bash
git add builder/divkit_builder/build.py builder/README.md builder/tests/
git commit -m "feat(builder): backfill + nightly CLI"
```

---

### Task 13: Generate the real database (backfill run)

**Files:**
- Create: `data/dividends-*.parquet`, `data/manifest.json` (generated artifacts, committed)

- [ ] **Step 1: Install builder** — `python -m pip install -e builder`.
- [ ] **Step 2: Run full backfill** — `divkit-build backfill --from-year 2009 --out data --with-bulk` (frames sweep first; bulk pass for completeness). Expect ~60 quarters × 2 concepts of frames + one 1.39 GB zip stream. Watch SEC rate limit.
- [ ] **Step 3: Sanity-check** — `python -c "import polars as pl; print(pl.read_parquet('data/dividends-2024.parquet').filter(pl.col('ticker')=='KO'))"` shows KO ~0.485/qtr; spot-check AAPL, MSFT.
- [ ] **Step 4: Commit data**

```bash
git add data/
git commit -m "chore(data): initial EDGAR dividend backfill 2009-present"
```

(If the parquet set is large, confirm with the user before committing vs. Git LFS — see Execution Note.)

---

### Task 14: Rust CLI

**Files:**
- Modify: `cli/src/main.rs`, `cli/Cargo.toml`

**Interfaces:**
- Consumes: `divkit::Divkit`, free fns.
- Produces: subcommands `get <TICKER>` (prints annual dividend + yield-less summary), `history <TICKER>` (table of events), `backfill [--from-year N]` and `append-today` (shell out to `python -m divkit_builder.build`).

- [ ] **Step 1: Add `clap`** to `cli/Cargo.toml` (`clap = { version = "4", features = ["derive"] }`).
- [ ] **Step 2: Implement `main.rs`** — `get` calls `annual_dividend_for`, prints `TICKER  annual_div=…  freq=…`; `history` prints each `DivEvent`; `backfill`/`append-today` `std::process::Command` to the Python builder with `--out data`.
- [ ] **Step 3: Smoke test** — `cargo run -p divkit-cli -- get KO` against committed `data/` (set `DIVKIT_BASE_URL=file://…` or local) prints a value.
- [ ] **Step 4: Commit**

```bash
git add cli/
git commit -m "feat: divkit-cli get/history/backfill/append-today"
```

---

### Task 15: GitHub Actions

**Files:**
- Create: `.github/workflows/ci.yml`, `nightly.yml`, `backfill.yml`, `release.yml`

- [ ] **Step 1: `ci.yml`** — on push/PR: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --all`; Python job: `pip install -e builder && ruff check builder && pytest builder/tests`.
- [ ] **Step 2: `nightly.yml`** — `cron: "0 7 * * *"` (after EDGAR's nightly XBRL refresh ~04:00–06:00 UTC) + `workflow_dispatch`; `permissions: contents: write`; setup Python, `pip install -e builder`, `python -m divkit_builder.build nightly --out data`, commit `data/` if changed (curvekit nightly commit idiom). The builder step sets `env: DIVKIT_CONTACT_EMAIL: ${{ secrets.CONTACT_EMAIL }}` so the SEC User-Agent uses the real contact address; if the secret is unset the builder falls back to the generic placeholder (SEC still returns 200).
- [ ] **Step 3: `backfill.yml`** — `workflow_dispatch` with `from_year` input (default `2009`); `timeout-minutes: 120`; runs `backfill --from-year ${{ inputs.from_year }} --with-bulk`, commits `data/`. Same `env: DIVKIT_CONTACT_EMAIL: ${{ secrets.CONTACT_EMAIL }}` on the builder step.
- [ ] **Step 4: `release.yml`** — on tag `v*`: `cargo publish -p divkit` (crate only; cli optional).
- [ ] **Step 5: Commit**

```bash
git add .github/workflows/
git commit -m "ci: ci/nightly/backfill/release workflows"
```

---

### Task 16: README + CHANGELOG + polish

**Files:**
- Create: `README.md`, `CHANGELOG.md`

- [ ] **Step 1: `README.md`** — mirror curvekit's structure: one-line pitch, install (`divkit = { git = ... }`), quick-start free-fn example, client-pattern example (`annual_dividend`, `dividend_snapshot`, `yield_on`, `yield_with`), the `annual_div`→Black-Scholes framing, data-source/limitation note (EDGAR amounts, no ex-dates), CLI section, env overrides table.
- [ ] **Step 2: `CHANGELOG.md`** — `## [0.1.0]` initial release notes (Conventional, no marketing vocab).
- [ ] **Step 3: Final verification** — `cargo test --all`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, `pytest builder/tests`, `cargo doc -p divkit --no-deps`.
- [ ] **Step 4: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: README + CHANGELOG for 0.1.0"
```

---

## Execution Note — data footprint

Before Task 13 commits parquet, check total `data/` size. If it materially bloats the repo (frames-only should be modest; the bulk pass may add more), confirm with the user whether to (a) commit parquet directly like curvekit/sectorkit, or (b) use Git LFS. Default to direct commit to match the existing kits unless size forces otherwise.

## Self-Review

- **Spec coverage:** source=EDGAR (T9/T10) ✓; 10yr+ backfill max-depth (T12 `--from-year` default 2009, T13 run) ✓; frames + bulk passes (T9/T10) ✓; polyglot split (builder T8–T12 Python, crate T1–T7 Rust) ✓; yield "both" model — amount always (T3 `annual_amount`), `yield_on` (T3), `yield_with`/PriceProvider (T4) ✓; parquet schema (T5/T11, Global Constraints) ✓; fetcher reuse + ETag/stale/manifest (T6) ✓; client + blocking + free fns (T7) ✓; CLI (T14) ✓; nightly/backfill/ci/release (T15) ✓; README + limitation note (T16) ✓; no ex-date field ✓ (out of scope, enforced by schema).
- **Placeholder scan:** code shown for all novel logic; copied infra (fetcher/parquet idioms) references concrete curvekit source paths + exact signatures. No TBD/TODO.
- **Type consistency:** `DivEvent`/`DividendSnapshot`/`Concept`/`Frequency`/`DivRow`/`PriceProvider`/`Divkit` names consistent across T3–T7; builder `Row`/`FrameEntry` consistent across T9–T12; schema column order identical in Global Constraints, T5, T11.
