//! Same-day incremental ingest from the EDGAR daily index + Form 3/4/5
//! ownership XML.
//!
//! The quarterly Form 345 data set only refreshes every few weeks, so it is
//! stale day to day. Form 4s are filed within two business days of a trade, so
//! the daily index gives same-day coverage. This module:
//!
//! 1. fetches `form.{YYYYMMDD}.idx` and keeps the 3 / 4 / 4-A / 5 / 5-A rows,
//! 2. for each, fetches the filing's primary ownership XML,
//! 3. parses the `ownershipDocument` schema into the same [`Txn`] shape the
//!    quarterly TSV path produces.

use std::time::Duration;

use anyhow::{Context, Result};
use insiderkit::{Role, Txn};
use serde::Deserialize;

use crate::ingest::parse_date;

/// SEC asks for <= 10 requests/second. We stay well under with a per-request
/// pause and serial fetches.
const REQUEST_PAUSE: Duration = Duration::from_millis(150);

/// One filing referenced by a daily-index row.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    pub form_type: String,
    pub cik: u32,
    pub accession: String,
}

/// Parse a `form.{YYYYMMDD}.idx` body, keeping only ownership forms.
///
/// The file has banner lines, then fixed-ish columns. The form type is the
/// first whitespace token and the path is the last token; CIK is recovered
/// from the path so column alignment never has to be load-bearing.
pub fn parse_daily_index(body: &str) -> Vec<IndexEntry> {
    let mut out = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with(|c: char| c.is_ascii_digit()) {
            continue; // banner / header / divider lines start with letters or symbols
        }
        let form = trimmed.split_whitespace().next().unwrap_or("");
        if !is_ownership_form(form) {
            continue;
        }
        let Some(path) = trimmed.split_whitespace().last() else {
            continue;
        };
        // path: edgar/data/{cik}/{accession}.txt
        let Some((cik, accession)) = parse_filing_path(path) else {
            continue;
        };
        out.push(IndexEntry {
            form_type: form.to_string(),
            cik,
            accession,
        });
    }
    out
}

/// Keep one entry per accession (first wins). EDGAR lists a multi-owner filing
/// once per reporting owner, all pointing at the same ownership XML.
fn dedup_by_accession(entries: Vec<IndexEntry>) -> Vec<IndexEntry> {
    let mut seen = std::collections::HashSet::new();
    entries
        .into_iter()
        .filter(|e| seen.insert(e.accession.clone()))
        .collect()
}

fn is_ownership_form(form: &str) -> bool {
    matches!(form, "3" | "4" | "4/A" | "5" | "5/A")
}

/// `edgar/data/1663719/0001709164-26-000096.txt` -> (1663719, accession).
fn parse_filing_path(path: &str) -> Option<(u32, String)> {
    let rest = path.strip_prefix("edgar/data/")?;
    let (cik, file) = rest.split_once('/')?;
    let accession = file.strip_suffix(".txt")?;
    // accession looks like 0001709164-26-000096
    if accession.len() != 20 || accession.matches('-').count() != 2 {
        return None;
    }
    Some((cik.parse().ok()?, accession.to_string()))
}

// ---------------------------------------------------------------------------
// ownershipDocument XML -> Txn
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename = "ownershipDocument")]
struct OwnershipDocument {
    #[serde(rename = "documentType", default)]
    document_type: String,
    issuer: Issuer,
    #[serde(rename = "reportingOwner", default)]
    reporting_owner: Vec<ReportingOwner>,
    #[serde(rename = "nonDerivativeTable", default)]
    non_derivative_table: Option<Table>,
    #[serde(rename = "derivativeTable", default)]
    derivative_table: Option<Table>,
}

#[derive(Debug, Deserialize)]
struct Issuer {
    #[serde(rename = "issuerCik", default)]
    cik: String,
    #[serde(rename = "issuerName", default)]
    name: String,
    #[serde(rename = "issuerTradingSymbol", default)]
    symbol: String,
}

#[derive(Debug, Deserialize)]
struct ReportingOwner {
    #[serde(rename = "reportingOwnerId", default)]
    id: Option<OwnerId>,
    #[serde(rename = "reportingOwnerRelationship", default)]
    relationship: Option<Relationship>,
}

#[derive(Debug, Deserialize)]
struct OwnerId {
    #[serde(rename = "rptOwnerCik", default)]
    cik: String,
    #[serde(rename = "rptOwnerName", default)]
    name: String,
}

#[derive(Debug, Default, Deserialize)]
struct Relationship {
    #[serde(rename = "isDirector", default)]
    is_director: String,
    #[serde(rename = "isOfficer", default)]
    is_officer: String,
    #[serde(rename = "isTenPercentOwner", default)]
    is_ten_percent: String,
    #[serde(rename = "officerTitle", default)]
    officer_title: String,
}

/// A table's children, in document order. Transactions and holdings interleave
/// freely in real filings, so they are collected as one heterogeneous sequence
/// (`$value`) and the holdings are dropped — a per-element-name `Vec` would trip
/// quick-xml's "duplicate field" on non-consecutive repeats.
#[derive(Debug, Deserialize)]
struct Table {
    #[serde(rename = "$value", default)]
    rows: Vec<TableRow>,
}

#[derive(Debug, Deserialize)]
enum TableRow {
    #[serde(rename = "nonDerivativeTransaction")]
    NonDerivTxn(Transaction),
    #[serde(rename = "derivativeTransaction")]
    DerivTxn(Transaction),
    // Holdings are current positions, not trades; parsed then discarded.
    #[serde(rename = "nonDerivativeHolding")]
    NonDerivHolding(serde::de::IgnoredAny),
    #[serde(rename = "derivativeHolding")]
    DerivHolding(serde::de::IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(rename = "securityTitle", default)]
    security_title: Option<Value>,
    #[serde(rename = "transactionDate", default)]
    transaction_date: Option<Value>,
    #[serde(rename = "transactionCoding", default)]
    coding: Option<Coding>,
    #[serde(rename = "transactionAmounts", default)]
    amounts: Option<Amounts>,
    #[serde(rename = "postTransactionAmounts", default)]
    post: Option<Post>,
    #[serde(rename = "ownershipNature", default)]
    ownership: Option<Ownership>,
}

#[derive(Debug, Deserialize)]
struct Coding {
    #[serde(rename = "transactionCode", default)]
    code: String,
}

#[derive(Debug, Deserialize)]
struct Amounts {
    #[serde(rename = "transactionShares", default)]
    shares: Option<Value>,
    #[serde(rename = "transactionPricePerShare", default)]
    price: Option<Value>,
    #[serde(rename = "transactionAcquiredDisposedCode", default)]
    acquired_disposed: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct Post {
    #[serde(rename = "sharesOwnedFollowingTransaction", default)]
    shares_owned: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct Ownership {
    #[serde(rename = "directOrIndirectOwnership", default)]
    direct_indirect: Option<Value>,
}

/// A `<field><value>X</value>…</field>` wrapper. The `value` is optional
/// because SEC writes a bare `<footnoteId/>` with no value when a figure is
/// withheld (e.g. price on a gift).
#[derive(Debug, Deserialize)]
struct Value {
    #[serde(rename = "value", default)]
    value: Option<String>,
}

impl Value {
    fn text(opt: &Option<Value>) -> String {
        opt.as_ref()
            .and_then(|v| v.value.clone())
            .unwrap_or_default()
    }
}

/// Parse an ownership-document XML body into rows. `accession` and
/// `filing_date` (the index date) are supplied by the caller since the XML's
/// own dates are the period of report, not the filing date.
pub fn parse_ownership_xml(xml: &str, accession: &str, filing_date: i32) -> Result<Vec<Txn>> {
    let doc: OwnershipDocument = quick_xml::de::from_str(xml).context("parse ownershipDocument")?;

    let issuer_cik = doc.issuer.cik.trim().parse().unwrap_or(0);
    let issuer_name = doc.issuer.name.clone();
    let ticker = doc.issuer.symbol.trim().to_string();
    let doc_type = doc.document_type.trim().to_string();

    // First reporting owner wins (matches the quarterly TSV path).
    let (owner_cik, owner_name, role, officer_title) = match doc.reporting_owner.first() {
        Some(o) => {
            let cik =
                o.id.as_ref()
                    .map(|i| i.cik.trim().parse().unwrap_or(0))
                    .unwrap_or(0);
            let name = o.id.as_ref().map(|i| i.name.clone()).unwrap_or_default();
            let rel = o.relationship.clone().unwrap_or_default();
            (cik, name, role_from_flags(&rel), rel.officer_title.clone())
        }
        None => (0, String::new(), Role::Other, String::new()),
    };

    let mut rows = Vec::new();
    let mut push = |t: &Transaction, is_derivative: bool| {
        rows.push(Txn {
            filing_date,
            accession: accession.to_string(),
            doc_type: doc_type.clone(),
            issuer_cik,
            issuer_name: issuer_name.clone(),
            ticker: ticker.clone(),
            owner_cik,
            owner_name: owner_name.clone(),
            role,
            officer_title: officer_title.clone(),
            security_title: Value::text(&t.security_title),
            txn_date: parse_date(&Value::text(&t.transaction_date)),
            txn_code: t
                .coding
                .as_ref()
                .map(|c| c.code.trim().to_string())
                .unwrap_or_default(),
            shares: parse_num(t.amounts.as_ref().and_then(|a| a.shares.as_ref())),
            price: parse_num(t.amounts.as_ref().and_then(|a| a.price.as_ref())),
            acquired_disposed: t
                .amounts
                .as_ref()
                .map(|a| Value::text(&a.acquired_disposed))
                .unwrap_or_default(),
            shares_owned_after: parse_num(t.post.as_ref().and_then(|p| p.shares_owned.as_ref())),
            direct_indirect: t
                .ownership
                .as_ref()
                .map(|o| Value::text(&o.direct_indirect))
                .unwrap_or_default(),
            is_derivative,
        });
    };

    // is_derivative follows the actual element type, not which table it sat in,
    // so a stray derivative row in the non-derivative table is still flagged.
    for table in [&doc.non_derivative_table, &doc.derivative_table]
        .into_iter()
        .flatten()
    {
        for row in &table.rows {
            match row {
                TableRow::NonDerivTxn(t) => push(t, false),
                TableRow::DerivTxn(t) => push(t, true),
                TableRow::NonDerivHolding(_) | TableRow::DerivHolding(_) => {}
            }
        }
    }
    Ok(rows)
}

// Relationship needs Clone for the `.clone().unwrap_or_default()` above.
impl Clone for Relationship {
    fn clone(&self) -> Self {
        Relationship {
            is_director: self.is_director.clone(),
            is_officer: self.is_officer.clone(),
            is_ten_percent: self.is_ten_percent.clone(),
            officer_title: self.officer_title.clone(),
        }
    }
}

fn role_from_flags(r: &Relationship) -> Role {
    if is_true(&r.is_director) {
        Role::Director
    } else if is_true(&r.is_officer) {
        Role::Officer
    } else if is_true(&r.is_ten_percent) {
        Role::TenPct
    } else {
        Role::Other
    }
}

/// SEC writes relationship flags as `1`/`0` or `true`/`false`.
fn is_true(s: &str) -> bool {
    matches!(s.trim(), "1" | "true" | "TRUE" | "True")
}

fn parse_num(v: Option<&Value>) -> f64 {
    v.and_then(|v| v.value.as_deref())
        .unwrap_or("")
        .trim()
        .parse()
        .unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// async fetch orchestration
// ---------------------------------------------------------------------------

/// Fetch and parse every ownership filing in the daily index for `date`
/// (YYYYMMDD). Returns the parsed rows; filings that fail to fetch or parse are
/// counted and skipped, never fatal.
pub async fn ingest_day(client: &reqwest::Client, date: i32) -> Result<Vec<Txn>> {
    let (y, m, _d) = split_ymd(date);
    let q = (m - 1) / 3 + 1;
    let url =
        format!("https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/form.{date:08}.idx");
    let body = match get_text(client, &url).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            eprintln!("{date}: no daily index (weekend/holiday), skipping");
            return Ok(Vec::new());
        }
        Err(e) => {
            eprintln!("{date}: daily index fetch failed ({e}), skipping");
            return Ok(Vec::new());
        }
    };

    // A multi-owner filing lists the same accession once per reporting owner;
    // dedup so each filing's XML is fetched and parsed exactly once.
    let entries = dedup_by_accession(parse_daily_index(&body));
    let mut rows = Vec::new();
    let (mut ok, mut failed) = (0u32, 0u32);
    for e in &entries {
        match fetch_filing_rows(client, e, date).await {
            Ok(mut r) => {
                ok += 1;
                rows.append(&mut r);
            }
            Err(err) => {
                failed += 1;
                tracing_skip(&e.accession, &err);
            }
        }
        tokio::time::sleep(REQUEST_PAUSE).await;
    }
    eprintln!(
        "{date}: {} ownership filings, {ok} parsed, {failed} skipped, {} rows",
        entries.len(),
        rows.len()
    );
    Ok(rows)
}

fn tracing_skip(accession: &str, err: &anyhow::Error) {
    eprintln!("  skip {accession}: {err}");
}

/// Locate and fetch one filing's ownership XML, then parse it.
async fn fetch_filing_rows(
    client: &reqwest::Client,
    entry: &IndexEntry,
    filing_date: i32,
) -> Result<Vec<Txn>> {
    let nodash: String = entry.accession.chars().filter(|c| *c != '-').collect();
    let index_url = format!(
        "https://www.sec.gov/Archives/edgar/data/{}/{}/index.json",
        entry.cik, nodash
    );
    let index = get_text(client, &index_url)
        .await?
        .context("filing index.json 404")?;
    let xml_name = pick_ownership_xml(&index).context("no ownership xml in filing")?;
    let xml_url = format!(
        "https://www.sec.gov/Archives/edgar/data/{}/{}/{}",
        entry.cik, nodash, xml_name
    );
    let xml = get_text(client, &xml_url)
        .await?
        .context("ownership xml 404")?;
    parse_ownership_xml(&xml, &entry.accession, filing_date)
}

/// Pick the ownership-document file from a filing `index.json`: the `.xml`
/// document, preferring a name that looks like an ownership doc.
fn pick_ownership_xml(index_json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(index_json).ok()?;
    let items = v.get("directory")?.get("item")?.as_array()?;
    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| i.get("name")?.as_str())
        .filter(|n| n.ends_with(".xml"))
        .collect();
    // Prefer a form{3,4,5}.xml / ownership-flavoured name; else the only .xml.
    names
        .iter()
        .find(|n| {
            let l = n.to_ascii_lowercase();
            l.starts_with("form") || l.contains("ownership") || l.contains("doc")
        })
        .or_else(|| names.first())
        .map(|s| s.to_string())
}

/// GET a URL as text. `Ok(None)` for 404 (not yet published / weekend index);
/// retries once on 429/5xx.
async fn get_text(client: &reqwest::Client, url: &str) -> Result<Option<String>> {
    for attempt in 0..2 {
        let resp = client.get(url).send().await.context("send")?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status.is_success() {
            return Ok(Some(resp.text().await.context("body")?));
        }
        if (status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
            && attempt == 0
        {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            continue;
        }
        anyhow::bail!("HTTP {status} for {url}");
    }
    anyhow::bail!("exhausted retries for {url}")
}

// ---------------------------------------------------------------------------
// date helpers (YYYYMMDD ints)
// ---------------------------------------------------------------------------

fn split_ymd(d: i32) -> (i32, i32, i32) {
    (d / 10000, (d / 100) % 100, d % 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_index_path() {
        assert_eq!(
            parse_filing_path("edgar/data/1663719/0001709164-26-000096.txt"),
            Some((1663719, "0001709164-26-000096".to_string()))
        );
        assert_eq!(parse_filing_path("edgar/data/5/bad.txt"), None);
    }

    #[test]
    fn keeps_only_ownership_forms() {
        let body = "\
Form Type   Company   CIK   Date Filed   File Name
----------
4                Acme Inc   123   20260602   edgar/data/123/0001000000-26-000001.txt
10-K             Beta Co    456   20260602   edgar/data/456/0001000000-26-000002.txt
4/A              Gamma LLC  789   20260602   edgar/data/789/0001000000-26-000003.txt
3                Delta Inc  111   20260602   edgar/data/111/0001000000-26-000004.txt
";
        let e = parse_daily_index(body);
        assert_eq!(e.len(), 3);
        assert_eq!(e[0].form_type, "4");
        assert_eq!(e[1].form_type, "4/A");
        assert_eq!(e[2].form_type, "3");
    }

    #[test]
    fn dedup_keeps_one_per_accession() {
        let e = |accn: &str, cik: u32| IndexEntry {
            form_type: "4".into(),
            cik,
            accession: accn.into(),
        };
        // Same accession listed for three different reporting owners.
        let got = dedup_by_accession(vec![e("A", 1), e("A", 2), e("B", 3), e("A", 4)]);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].accession, "A");
        assert_eq!(got[0].cik, 1); // first wins
        assert_eq!(got[1].accession, "B");
    }

    #[test]
    fn parses_nonderiv_ownership_xml() {
        let xml = r#"<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0001214128</rptOwnerCik><rptOwnerName>COOK TIM</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>false</isDirector><isOfficer>true</isOfficer><officerTitle>CEO</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-06-01</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1922</value></transactionShares>
        <transactionPricePerShare><value>10.02</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>1057231</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"#;
        let rows = parse_ownership_xml(xml, "0001234567-26-000001", 20260602).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.ticker, "AAPL");
        assert_eq!(r.issuer_cik, 320193);
        assert_eq!(r.owner_cik, 1214128);
        assert_eq!(r.role, Role::Officer);
        assert_eq!(r.officer_title, "CEO");
        assert_eq!(r.txn_date, 20260601);
        assert_eq!(r.txn_code, "S");
        assert!((r.shares - 1922.0).abs() < 1e-9);
        assert!((r.price - 10.02).abs() < 1e-9);
        assert_eq!(r.acquired_disposed, "D");
        assert!((r.shares_owned_after - 1057231.0).abs() < 1e-9);
        assert!(!r.is_derivative);
        assert_eq!(r.filing_date, 20260602);
    }

    /// A table that interleaves holdings (current positions) with a transaction
    /// must parse: holdings are ignored, the transaction is emitted. Real
    /// filings (e.g. Aveanna AVAH) order them holding/transaction/holding, which
    /// is the case that broke per-element `Vec` deserialization.
    #[test]
    fn holdings_interleaved_with_transactions_are_ignored() {
        let holding = r#"<nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>15523810</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeHolding>"#;
        let xml = format!(
            r#"<ownershipDocument>
  <documentType>4</documentType>
  <issuer><issuerCik>1</issuerCik><issuerName>X</issuerName><issuerTradingSymbol>X</issuerTradingSymbol></issuer>
  <reportingOwner><reportingOwnerId><rptOwnerCik>2</rptOwnerCik><rptOwnerName>Y</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship></reportingOwner>
  <nonDerivativeTable>
    {holding}
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-06-24</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>919389</value></transactionShares>
        <transactionPricePerShare><value>8.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
    {holding}
  </nonDerivativeTable>
</ownershipDocument>"#
        );
        let xml = xml.as_str();
        let rows = parse_ownership_xml(xml, "a", 20260626).unwrap();
        assert_eq!(rows.len(), 1, "only the transaction, not the holding");
        assert_eq!(rows[0].txn_code, "S");
        assert_eq!(rows[0].role, Role::TenPct);
        assert!((rows[0].shares - 919389.0).abs() < 1e-9);
    }

    #[test]
    fn derivative_with_withheld_price_is_zero() {
        let xml = r#"<ownershipDocument>
  <documentType>4</documentType>
  <issuer><issuerCik>1</issuerCik><issuerName>X</issuerName><issuerTradingSymbol>X</issuerTradingSymbol></issuer>
  <reportingOwner><reportingOwnerId><rptOwnerCik>2</rptOwnerCik><rptOwnerName>Y</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isOther>1</isOther></reportingOwnerRelationship></reportingOwner>
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Class B</value></securityTitle>
      <transactionDate><value>2026-05-29</value></transactionDate>
      <transactionCoding><transactionCode>G</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1391</value></transactionShares>
        <transactionPricePerShare><footnoteId id="F1"/></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>"#;
        let rows = parse_ownership_xml(xml, "a", 20260602).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_derivative);
        assert_eq!(rows[0].price, 0.0);
        assert_eq!(rows[0].role, Role::Other);
        assert_eq!(rows[0].txn_code, "G");
    }
}
