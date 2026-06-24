//! Golden interop test: the parquet file in `fixtures/python_golden/` is
//! produced by the Python builder (`divkit_builder.schema.write_year_shards`).
//! This proves the real Python-writer -> Rust-reader path, not just each
//! side's own round-trip.

use chrono::NaiveDate;
use divkit::{read_dividends, Concept};

#[test]
fn reads_python_written_golden_parquet() {
    let bytes = include_bytes!("fixtures/python_golden/dividends-2024.parquet");
    let rows = read_dividends(bytes).expect("read_dividends should parse Python-written parquet");

    assert_eq!(rows.len(), 3, "expected 3 rows in golden fixture");

    // Rows are sorted by (cik, period_end) by the Python writer, so cik=320193
    // (AAPL) rows come first, then cik=999999.
    let aapl_q1 = &rows[0];
    assert_eq!(aapl_q1.cik, 320193);
    assert_eq!(aapl_q1.ticker.as_deref(), Some("AAPL"));
    assert!(
        (aapl_q1.amount - 0.24).abs() < 1e-9,
        "amount = {}",
        aapl_q1.amount
    );
    assert_eq!(aapl_q1.concept, Concept::Declared);
    assert_eq!(aapl_q1.form.as_deref(), Some("10-Q"));
    assert_eq!(
        aapl_q1.period_start,
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
    );
    assert_eq!(
        aapl_q1.period_end,
        NaiveDate::from_ymd_opt(2024, 3, 31).unwrap()
    );

    let aapl_q2 = &rows[1];
    assert_eq!(aapl_q2.cik, 320193);
    assert_eq!(aapl_q2.ticker.as_deref(), Some("AAPL"));
    assert!(
        (aapl_q2.amount - 0.25).abs() < 1e-9,
        "amount = {}",
        aapl_q2.amount
    );
    assert_eq!(aapl_q2.concept, Concept::Declared);
    assert_eq!(
        aapl_q2.period_end,
        NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()
    );

    // Unmapped CIK: ticker must be null (None), concept CashPaid, form None.
    let unmapped = &rows[2];
    assert_eq!(unmapped.cik, 999999);
    assert_eq!(
        unmapped.ticker, None,
        "unmapped CIK should have null ticker"
    );
    assert_eq!(unmapped.concept, Concept::CashPaid);
    assert_eq!(unmapped.form, None);
    assert_eq!(
        unmapped.period_end,
        NaiveDate::from_ymd_opt(2024, 9, 30).unwrap()
    );
}
