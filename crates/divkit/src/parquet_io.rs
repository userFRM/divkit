//! Parquet reader/writer for dividend rows.
//!
//! # File layout
//!
//! ```text
//! dividends-{year}.parquet   (UInt32 cik, Utf8 ticker?, Date32 period_start,
//!                              Date32 period_end, Float64 amount,
//!                              Utf8 concept, Utf8 accn, Utf8 form?)
//! ```
//!
//! Dates are stored as Arrow `Date32` (days since Unix epoch, 1970-01-01).
//! `concept` is stored as `"Declared"` or `"CashPaid"`.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Date32Array, Float64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::error::{Error, Result};
use crate::record::Concept;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A flat row suitable for columnar parquet storage, combining CIK + ticker
/// with the per-event fields from `DivEvent`.
#[derive(Debug, Clone, PartialEq)]
pub struct DivRow {
    pub cik: u32,
    pub ticker: Option<String>,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub amount: f64,
    pub concept: Concept,
    pub accn: String,
    pub form: Option<String>,
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

fn dividend_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("cik", DataType::UInt32, false),
        Field::new("ticker", DataType::Utf8, true),
        Field::new("period_start", DataType::Date32, false),
        Field::new("period_end", DataType::Date32, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("concept", DataType::Utf8, false),
        Field::new("accn", DataType::Utf8, false),
        Field::new("form", DataType::Utf8, true),
    ]))
}

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------

fn to_date32(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (d - epoch).num_days() as i32
}

fn from_date32(days: i32) -> Option<NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    epoch.checked_add_signed(chrono::Duration::days(days as i64))
}

// ---------------------------------------------------------------------------
// Concept helpers
// ---------------------------------------------------------------------------

fn concept_to_str(c: Concept) -> &'static str {
    match c {
        Concept::Declared => "Declared",
        Concept::CashPaid => "CashPaid",
    }
}

fn str_to_concept(s: &str) -> Result<Concept> {
    match s {
        "Declared" => Ok(Concept::Declared),
        "CashPaid" => Ok(Concept::CashPaid),
        other => Err(Error::Parquet(format!("unknown concept value: {other:?}"))),
    }
}

// ---------------------------------------------------------------------------
// Writer properties
// ---------------------------------------------------------------------------

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid zstd level"),
        ))
        .set_max_row_group_row_count(Some(10_000))
        .build()
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Write dividend rows to a parquet file at `path` (creates or overwrites).
///
/// Column order: `cik`, `ticker`, `period_start`, `period_end`, `amount`,
/// `concept`, `accn`, `form`.
pub fn write_dividends(path: &Path, rows: &[DivRow]) -> Result<()> {
    let schema = dividend_schema();

    let cik: UInt32Array = rows.iter().map(|r| Some(r.cik)).collect();
    let ticker: StringArray = rows.iter().map(|r| r.ticker.as_deref()).collect();
    let period_start: Date32Array = rows
        .iter()
        .map(|r| Some(to_date32(r.period_start)))
        .collect();
    let period_end: Date32Array = rows.iter().map(|r| Some(to_date32(r.period_end))).collect();
    let amount: Float64Array = rows.iter().map(|r| Some(r.amount)).collect();
    let concept: StringArray = rows
        .iter()
        .map(|r| Some(concept_to_str(r.concept)))
        .collect();
    let accn: StringArray = rows.iter().map(|r| Some(r.accn.as_str())).collect();
    let form: StringArray = rows.iter().map(|r| r.form.as_deref()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(cik),
            Arc::new(ticker),
            Arc::new(period_start),
            Arc::new(period_end),
            Arc::new(amount),
            Arc::new(concept),
            Arc::new(accn),
            Arc::new(form),
        ],
    )?;

    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(writer_props()))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Resolve a column by name and downcast it to the expected array type.
///
/// Returns `Error::Parquet` (naming the offending column) if the column is
/// absent or has an unexpected Arrow type, rather than panicking.
fn column_as<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a A> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Parquet(format!("missing column: {name}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Parquet(format!("{name} column type mismatch")))
}

/// Guard a non-nullable field: return `Err` if the value at row `i` is null
/// rather than silently coercing to a zero/empty default.
#[inline]
fn require_non_null(col: &dyn Array, field: &str, i: usize) -> Result<()> {
    if col.is_null(i) {
        Err(Error::Parquet(format!("null {field} at row {i}")))
    } else {
        Ok(())
    }
}

/// Parse a parquet file (supplied as in-memory bytes) into `DivRow` records.
pub fn read_dividends(bytes: &[u8]) -> Result<Vec<DivRow>> {
    let owned: bytes::Bytes = bytes::Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(owned)?;
    let reader = builder.build()?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;

        // Look columns up by name, not by position: the Python builder writes
        // these files too, and column ordering must not be load-bearing on read.
        let cik_col = column_as::<UInt32Array>(&batch, "cik")?;
        let ticker_col = column_as::<StringArray>(&batch, "ticker")?;
        let period_start_col = column_as::<Date32Array>(&batch, "period_start")?;
        let period_end_col = column_as::<Date32Array>(&batch, "period_end")?;
        let amount_col = column_as::<Float64Array>(&batch, "amount")?;
        let concept_col = column_as::<StringArray>(&batch, "concept")?;
        let accn_col = column_as::<StringArray>(&batch, "accn")?;
        let form_col = column_as::<StringArray>(&batch, "form")?;

        for i in 0..batch.num_rows() {
            // Non-nullable fields: reject silently-coerced nulls.
            require_non_null(cik_col, "cik", i)?;
            require_non_null(period_start_col, "period_start", i)?;
            require_non_null(period_end_col, "period_end", i)?;
            require_non_null(amount_col, "amount", i)?;
            require_non_null(accn_col, "accn", i)?;
            require_non_null(concept_col, "concept", i)?;

            let period_start = from_date32(period_start_col.value(i))
                .ok_or_else(|| Error::Parquet(format!("invalid period_start at row {i}")))?;
            let period_end = from_date32(period_end_col.value(i))
                .ok_or_else(|| Error::Parquet(format!("invalid period_end at row {i}")))?;
            let concept = str_to_concept(concept_col.value(i))?;

            rows.push(DivRow {
                cik: cik_col.value(i),
                ticker: if ticker_col.is_null(i) {
                    None
                } else {
                    Some(ticker_col.value(i).to_owned())
                },
                period_start,
                period_end,
                amount: amount_col.value(i),
                concept,
                accn: accn_col.value(i).to_owned(),
                form: if form_col.is_null(i) {
                    None
                } else {
                    Some(form_col.value(i).to_owned())
                },
            });
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_dividend_rows() {
        let dir = std::env::temp_dir().join("divkit_pq_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dividends-2024.parquet");
        let d = chrono::NaiveDate::parse_from_str("2024-03-15", "%Y-%m-%d").unwrap();
        let d2 = chrono::NaiveDate::parse_from_str("2023-09-01", "%Y-%m-%d").unwrap();
        let rows = vec![
            DivRow {
                cik: 21344,
                ticker: Some("KO".into()),
                period_start: d,
                period_end: d,
                amount: 0.485,
                concept: crate::Concept::Declared,
                accn: "a".into(),
                form: Some("10-Q".into()),
            },
            // Null ticker + null form + CashPaid concept: exercises the
            // nullable-string read path and the CashPaid string mapping.
            DivRow {
                cik: 320193,
                ticker: None,
                period_start: d2,
                period_end: d2,
                amount: 0.24,
                concept: crate::Concept::CashPaid,
                accn: "b".into(),
                form: None,
            },
        ];
        write_dividends(&path, &rows).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let back = read_dividends(&bytes).unwrap();
        assert_eq!(back.len(), 2);

        assert_eq!(back[0].cik, 21344);
        assert_eq!(back[0].ticker.as_deref(), Some("KO"));
        assert!((back[0].amount - 0.485).abs() < 1e-9);
        assert_eq!(back[0].concept, crate::Concept::Declared);
        assert_eq!(back[0].form.as_deref(), Some("10-Q"));

        assert_eq!(back[1].cik, 320193);
        // Nulls must come back as None, never Some("").
        assert_eq!(back[1].ticker, None);
        assert_eq!(back[1].form, None);
        assert!((back[1].amount - 0.24).abs() < 1e-9);
        assert_eq!(back[1].concept, crate::Concept::CashPaid);
    }

    /// Build a parquet where `amount` is nullable and contains a NULL at row 0.
    /// `read_dividends` must return `Err`, not `Ok` with `amount = 0.0`.
    #[test]
    fn rejects_null_in_non_nullable_amount() {
        use arrow::array::{Date32Array, Float64Array, StringArray, UInt32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        // Build a schema where `amount` is marked nullable (mimicking a buggy
        // Python/pandas writer that defaults all columns to nullable).
        let schema = Arc::new(Schema::new(vec![
            Field::new("cik", DataType::UInt32, false),
            Field::new("ticker", DataType::Utf8, true),
            Field::new("period_start", DataType::Date32, false),
            Field::new("period_end", DataType::Date32, false),
            Field::new("amount", DataType::Float64, true), // nullable — the bad case
            Field::new("concept", DataType::Utf8, false),
            Field::new("accn", DataType::Utf8, false),
            Field::new("form", DataType::Utf8, true),
        ]));

        let epoch_days = 19_800i32; // some valid date

        // amount column: one NULL value
        let amount: Float64Array = vec![None].into_iter().collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![42u32])),
                Arc::new(StringArray::from(vec![Some("AAPL")])),
                Arc::new(Date32Array::from(vec![epoch_days])),
                Arc::new(Date32Array::from(vec![epoch_days])),
                Arc::new(amount),
                Arc::new(StringArray::from(vec![Some("Declared")])),
                Arc::new(StringArray::from(vec![Some("0001234567-24-000001")])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
            ],
        )
        .expect("batch construction");

        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).expect("writer creation");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        let result = read_dividends(&buf);
        assert!(
            result.is_err(),
            "expected Err for null amount, got Ok({:?})",
            result.ok()
        );
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("null amount"),
            "error message should mention 'null amount', got: {msg:?}"
        );
    }

    #[test]
    fn rejects_null_in_non_nullable_cik() {
        use arrow::array::{Date32Array, Float64Array, StringArray, UInt32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("cik", DataType::UInt32, true), // nullable — the bad case
            Field::new("ticker", DataType::Utf8, true),
            Field::new("period_start", DataType::Date32, false),
            Field::new("period_end", DataType::Date32, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("concept", DataType::Utf8, false),
            Field::new("accn", DataType::Utf8, false),
            Field::new("form", DataType::Utf8, true),
        ]));

        let epoch_days = 19_800i32;
        let cik: UInt32Array = vec![None].into_iter().collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cik),
                Arc::new(StringArray::from(vec![Some("AAPL")])),
                Arc::new(Date32Array::from(vec![epoch_days])),
                Arc::new(Date32Array::from(vec![epoch_days])),
                Arc::new(Float64Array::from(vec![0.25f64])),
                Arc::new(StringArray::from(vec![Some("Declared")])),
                Arc::new(StringArray::from(vec![Some("0001234567-24-000001")])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
            ],
        )
        .expect("batch construction");

        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).expect("writer creation");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        let result = read_dividends(&buf);
        assert!(
            result.is_err(),
            "expected Err for null cik, got Ok({:?})",
            result.ok()
        );
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("null cik"),
            "error message should mention 'null cik', got: {msg:?}"
        );
    }

    #[test]
    #[ignore]
    fn make_fixture() {
        let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        std::fs::create_dir_all(&fixture_dir).unwrap();
        let path = fixture_dir.join("dividends-2024.parquet");

        let rows = vec![
            DivRow {
                cik: 21344,
                ticker: Some("KO".into()),
                period_start: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                period_end: chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
                amount: 0.485,
                concept: crate::Concept::Declared,
                accn: "ko".into(),
                form: Some("10-Q".into()),
            },
            DivRow {
                cik: 21344,
                ticker: Some("KO".into()),
                period_start: chrono::NaiveDate::from_ymd_opt(2024, 4, 1).unwrap(),
                period_end: chrono::NaiveDate::from_ymd_opt(2024, 6, 14).unwrap(),
                amount: 0.485,
                concept: crate::Concept::Declared,
                accn: "ko".into(),
                form: Some("10-Q".into()),
            },
            DivRow {
                cik: 21344,
                ticker: Some("KO".into()),
                period_start: chrono::NaiveDate::from_ymd_opt(2024, 7, 1).unwrap(),
                period_end: chrono::NaiveDate::from_ymd_opt(2024, 9, 13).unwrap(),
                amount: 0.485,
                concept: crate::Concept::Declared,
                accn: "ko".into(),
                form: Some("10-Q".into()),
            },
            DivRow {
                cik: 21344,
                ticker: Some("KO".into()),
                period_start: chrono::NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
                period_end: chrono::NaiveDate::from_ymd_opt(2024, 12, 13).unwrap(),
                amount: 0.485,
                concept: crate::Concept::Declared,
                accn: "ko".into(),
                form: Some("10-Q".into()),
            },
        ];

        write_dividends(&path, &rows).unwrap();
        println!("wrote fixture → {}", path.display());
    }
}
