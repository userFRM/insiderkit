//! Parquet reader/writer for insider-transaction rows.
//!
//! # File layout
//!
//! One row per transaction. Columns, in order:
//!
//! ```text
//! filing_date Int32(YYYYMMDD), accession Utf8, doc_type Utf8,
//! issuer_cik UInt32, issuer_name Utf8, ticker Utf8, owner_cik UInt32,
//! owner_name Utf8, role Utf8, officer_title Utf8, security_title Utf8,
//! txn_date Int32(YYYYMMDD), txn_code Utf8, shares Float64, price Float64,
//! acquired_disposed Utf8, shares_owned_after Float64, direct_indirect Utf8,
//! is_derivative Boolean
//! ```
//!
//! Dates are plain `i32` `YYYYMMDD` integers, not Arrow `Date32`, so a consumer
//! never needs a calendar library to compare or bucket them.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::error::{Error, Result};
use crate::record::{Role, Txn};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// The bundled-parquet schema, bound field by field. Every column non-null;
/// the writer fills empty strings rather than nulls so the read path can reject
/// any unexpected null as corruption.
fn txn_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("filing_date", DataType::Int32, false),
        Field::new("accession", DataType::Utf8, false),
        Field::new("doc_type", DataType::Utf8, false),
        Field::new("issuer_cik", DataType::UInt32, false),
        Field::new("issuer_name", DataType::Utf8, false),
        Field::new("ticker", DataType::Utf8, false),
        Field::new("owner_cik", DataType::UInt32, false),
        Field::new("owner_name", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("officer_title", DataType::Utf8, false),
        Field::new("security_title", DataType::Utf8, false),
        Field::new("txn_date", DataType::Int32, false),
        Field::new("txn_code", DataType::Utf8, false),
        Field::new("shares", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("acquired_disposed", DataType::Utf8, false),
        Field::new("shares_owned_after", DataType::Float64, false),
        Field::new("direct_indirect", DataType::Utf8, false),
        Field::new("is_derivative", DataType::Boolean, false),
    ]))
}

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid zstd level"),
        ))
        .set_max_row_group_row_count(Some(50_000))
        .build()
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

/// Write `rows` to a parquet file at `path` (creates or overwrites).
pub fn write_transactions(path: &Path, rows: &[Txn]) -> Result<()> {
    let schema = txn_schema();
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props()))?;
    // Chunk into row-group-sized batches so a multi-year file stays streamable.
    for chunk in rows.chunks(50_000) {
        writer.write(&batch_of(&schema, chunk)?)?;
    }
    writer.close()?;
    Ok(())
}

fn batch_of(schema: &Arc<Schema>, rows: &[Txn]) -> Result<RecordBatch> {
    let filing_date: Int32Array = rows.iter().map(|r| Some(r.filing_date)).collect();
    let accession: StringArray = rows.iter().map(|r| Some(r.accession.as_str())).collect();
    let doc_type: StringArray = rows.iter().map(|r| Some(r.doc_type.as_str())).collect();
    let issuer_cik: UInt32Array = rows.iter().map(|r| Some(r.issuer_cik)).collect();
    let issuer_name: StringArray = rows.iter().map(|r| Some(r.issuer_name.as_str())).collect();
    let ticker: StringArray = rows.iter().map(|r| Some(r.ticker.as_str())).collect();
    let owner_cik: UInt32Array = rows.iter().map(|r| Some(r.owner_cik)).collect();
    let owner_name: StringArray = rows.iter().map(|r| Some(r.owner_name.as_str())).collect();
    let role: StringArray = rows.iter().map(|r| Some(r.role.as_str())).collect();
    let officer_title: StringArray = rows
        .iter()
        .map(|r| Some(r.officer_title.as_str()))
        .collect();
    let security_title: StringArray = rows
        .iter()
        .map(|r| Some(r.security_title.as_str()))
        .collect();
    let txn_date: Int32Array = rows.iter().map(|r| Some(r.txn_date)).collect();
    let txn_code: StringArray = rows.iter().map(|r| Some(r.txn_code.as_str())).collect();
    let shares: Float64Array = rows.iter().map(|r| Some(r.shares)).collect();
    let price: Float64Array = rows.iter().map(|r| Some(r.price)).collect();
    let acquired_disposed: StringArray = rows
        .iter()
        .map(|r| Some(r.acquired_disposed.as_str()))
        .collect();
    let shares_owned_after: Float64Array =
        rows.iter().map(|r| Some(r.shares_owned_after)).collect();
    let direct_indirect: StringArray = rows
        .iter()
        .map(|r| Some(r.direct_indirect.as_str()))
        .collect();
    let is_derivative: BooleanArray = rows.iter().map(|r| Some(r.is_derivative)).collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(filing_date),
            Arc::new(accession),
            Arc::new(doc_type),
            Arc::new(issuer_cik),
            Arc::new(issuer_name),
            Arc::new(ticker),
            Arc::new(owner_cik),
            Arc::new(owner_name),
            Arc::new(role),
            Arc::new(officer_title),
            Arc::new(security_title),
            Arc::new(txn_date),
            Arc::new(txn_code),
            Arc::new(shares),
            Arc::new(price),
            Arc::new(acquired_disposed),
            Arc::new(shares_owned_after),
            Arc::new(direct_indirect),
            Arc::new(is_derivative),
        ],
    )
    .map_err(Error::Arrow)
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

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

#[inline]
fn require_non_null(col: &dyn Array, field: &str, i: usize) -> Result<()> {
    if col.is_null(i) {
        Err(Error::Parquet(format!("null {field} at row {i}")))
    } else {
        Ok(())
    }
}

/// Parse a parquet file (in-memory bytes) into [`Txn`] records.
pub fn read_transactions(bytes: &[u8]) -> Result<Vec<Txn>> {
    let owned: bytes::Bytes = bytes::Bytes::copy_from_slice(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(owned)?.build()?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;
        let filing_date = column_as::<Int32Array>(&batch, "filing_date")?;
        let accession = column_as::<StringArray>(&batch, "accession")?;
        let doc_type = column_as::<StringArray>(&batch, "doc_type")?;
        let issuer_cik = column_as::<UInt32Array>(&batch, "issuer_cik")?;
        let issuer_name = column_as::<StringArray>(&batch, "issuer_name")?;
        let ticker = column_as::<StringArray>(&batch, "ticker")?;
        let owner_cik = column_as::<UInt32Array>(&batch, "owner_cik")?;
        let owner_name = column_as::<StringArray>(&batch, "owner_name")?;
        let role = column_as::<StringArray>(&batch, "role")?;
        let officer_title = column_as::<StringArray>(&batch, "officer_title")?;
        let security_title = column_as::<StringArray>(&batch, "security_title")?;
        let txn_date = column_as::<Int32Array>(&batch, "txn_date")?;
        let txn_code = column_as::<StringArray>(&batch, "txn_code")?;
        let shares = column_as::<Float64Array>(&batch, "shares")?;
        let price = column_as::<Float64Array>(&batch, "price")?;
        let acquired_disposed = column_as::<StringArray>(&batch, "acquired_disposed")?;
        let shares_owned_after = column_as::<Float64Array>(&batch, "shares_owned_after")?;
        let direct_indirect = column_as::<StringArray>(&batch, "direct_indirect")?;
        let is_derivative = column_as::<BooleanArray>(&batch, "is_derivative")?;

        for i in 0..batch.num_rows() {
            require_non_null(filing_date, "filing_date", i)?;
            require_non_null(accession, "accession", i)?;
            require_non_null(issuer_cik, "issuer_cik", i)?;
            require_non_null(owner_cik, "owner_cik", i)?;
            require_non_null(txn_date, "txn_date", i)?;
            require_non_null(role, "role", i)?;

            let role_val = Role::parse(role.value(i)).ok_or_else(|| {
                Error::Parquet(format!("unknown role {:?} at row {i}", role.value(i)))
            })?;

            rows.push(Txn {
                filing_date: filing_date.value(i),
                accession: accession.value(i).to_owned(),
                doc_type: doc_type.value(i).to_owned(),
                issuer_cik: issuer_cik.value(i),
                issuer_name: issuer_name.value(i).to_owned(),
                ticker: ticker.value(i).to_owned(),
                owner_cik: owner_cik.value(i),
                owner_name: owner_name.value(i).to_owned(),
                role: role_val,
                officer_title: officer_title.value(i).to_owned(),
                security_title: security_title.value(i).to_owned(),
                txn_date: txn_date.value(i),
                txn_code: txn_code.value(i).to_owned(),
                shares: shares.value(i),
                price: price.value(i),
                acquired_disposed: acquired_disposed.value(i).to_owned(),
                shares_owned_after: shares_owned_after.value(i),
                direct_indirect: direct_indirect.value(i).to_owned(),
                is_derivative: is_derivative.value(i),
            });
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Txn {
        Txn {
            filing_date: 20240205,
            accession: "0000320193-24-000017".into(),
            doc_type: "4".into(),
            issuer_cik: 320193,
            issuer_name: "Apple Inc.".into(),
            ticker: "AAPL".into(),
            owner_cik: 1214128,
            owner_name: "COOK TIMOTHY D".into(),
            role: Role::Officer,
            officer_title: "Chief Executive Officer".into(),
            security_title: "Common Stock".into(),
            txn_date: 20240201,
            txn_code: "S".into(),
            shares: 196410.0,
            price: 188.01,
            acquired_disposed: "D".into(),
            shares_owned_after: 3280930.0,
            direct_indirect: "D".into(),
            is_derivative: false,
        }
    }

    #[test]
    fn round_trips_rows() {
        let dir = std::env::temp_dir().join("insiderkit_pq_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("insider-2024.parquet");
        let rows = vec![sample()];
        write_transactions(&path, &rows).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let back = read_transactions(&bytes).unwrap();
        assert_eq!(back, rows);
    }

    #[test]
    fn rejects_null_in_non_nullable_filing_date() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("filing_date", DataType::Int32, true), // nullable — the bad case
            Field::new("accession", DataType::Utf8, false),
            Field::new("doc_type", DataType::Utf8, false),
            Field::new("issuer_cik", DataType::UInt32, false),
            Field::new("issuer_name", DataType::Utf8, false),
            Field::new("ticker", DataType::Utf8, false),
            Field::new("owner_cik", DataType::UInt32, false),
            Field::new("owner_name", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, false),
            Field::new("officer_title", DataType::Utf8, false),
            Field::new("security_title", DataType::Utf8, false),
            Field::new("txn_date", DataType::Int32, false),
            Field::new("txn_code", DataType::Utf8, false),
            Field::new("shares", DataType::Float64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("acquired_disposed", DataType::Utf8, false),
            Field::new("shares_owned_after", DataType::Float64, false),
            Field::new("direct_indirect", DataType::Utf8, false),
            Field::new("is_derivative", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["4"])),
                Arc::new(UInt32Array::from(vec![1u32])),
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(StringArray::from(vec!["X"])),
                Arc::new(UInt32Array::from(vec![2u32])),
                Arc::new(StringArray::from(vec!["o"])),
                Arc::new(StringArray::from(vec!["officer"])),
                Arc::new(StringArray::from(vec![""])),
                Arc::new(StringArray::from(vec!["Common Stock"])),
                Arc::new(Int32Array::from(vec![20240201])),
                Arc::new(StringArray::from(vec!["S"])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec!["D"])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec!["D"])),
                Arc::new(BooleanArray::from(vec![false])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let err = read_transactions(&buf).unwrap_err().to_string();
        assert!(err.contains("null filing_date"), "got: {err}");
    }
}
