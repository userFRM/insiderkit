//! Ingest SEC Insider Transactions Data Set quarterly ZIPs into [`Txn`] rows.
//!
//! Each quarterly ZIP holds tab-separated TSVs (first row = header). We join
//! SUBMISSION + REPORTINGOWNER + (NONDERIV_TRANS | DERIV_TRANS) on
//! ACCESSION_NUMBER and emit one [`Txn`] per transaction line.

use std::collections::HashMap;
use std::io::Read;

use anyhow::{Context, Result};
use insiderkit::{Role, Txn};

/// SEC submission metadata, keyed by accession number.
struct Submission {
    filing_date: i32,
    doc_type: String,
    issuer_cik: u32,
    issuer_name: String,
    ticker: String,
}

/// SEC reporting-owner metadata, keyed by accession number (first owner wins).
struct Owner {
    cik: u32,
    name: String,
    role: Role,
    title: String,
}

/// Parse a quarterly `*_form345.zip` (raw bytes) into transaction rows.
pub fn parse_quarter_zip(zip_bytes: &[u8]) -> Result<Vec<Txn>> {
    let cursor = std::io::Cursor::new(zip_bytes);
    let mut zip = zip::ZipArchive::new(cursor).context("open quarterly zip")?;

    let submission = parse_submissions(read_tsv(&mut zip, "SUBMISSION.tsv")?);
    let owners = parse_owners(read_tsv(&mut zip, "REPORTINGOWNER.tsv")?);

    let nonderiv = read_tsv(&mut zip, "NONDERIV_TRANS.tsv")?;
    let deriv = read_tsv(&mut zip, "DERIV_TRANS.tsv")?;

    let mut rows = Vec::new();
    emit_transactions(&nonderiv, false, &submission, &owners, &mut rows);
    emit_transactions(&deriv, true, &submission, &owners, &mut rows);
    Ok(rows)
}

/// A parsed TSV: the header column names and the data rows.
struct Tsv {
    cols: HashMap<String, usize>,
    rows: Vec<Vec<String>>,
}

impl Tsv {
    fn get<'a>(&self, row: &'a [String], col: &str) -> &'a str {
        match self.cols.get(col) {
            Some(&i) => row.get(i).map(String::as_str).unwrap_or(""),
            None => "",
        }
    }
}

fn read_tsv<R: Read + std::io::Seek>(zip: &mut zip::ZipArchive<R>, name: &str) -> Result<Tsv> {
    let mut file = zip.by_name(name).with_context(|| format!("read {name}"))?;
    let mut text = String::new();
    file.read_to_string(&mut text)
        .with_context(|| format!("decode {name}"))?;

    let mut lines = text.lines();
    let header = lines.next().unwrap_or("");
    let cols: HashMap<String, usize> = header
        .split('\t')
        .enumerate()
        .map(|(i, c)| (c.trim().to_string(), i))
        .collect();
    let rows = lines
        .map(|l| l.split('\t').map(|s| s.trim().to_string()).collect())
        .collect();
    Ok(Tsv { cols, rows })
}

fn parse_submissions(tsv: Tsv) -> HashMap<String, Submission> {
    let mut map = HashMap::new();
    for row in &tsv.rows {
        let accn = tsv.get(row, "ACCESSION_NUMBER").to_string();
        if accn.is_empty() {
            continue;
        }
        map.insert(
            accn,
            Submission {
                filing_date: parse_date(tsv.get(row, "FILING_DATE")),
                doc_type: tsv.get(row, "DOCUMENT_TYPE").to_string(),
                issuer_cik: parse_cik(tsv.get(row, "ISSUERCIK")),
                issuer_name: tsv.get(row, "ISSUERNAME").to_string(),
                ticker: tsv.get(row, "ISSUERTRADINGSYMBOL").to_string(),
            },
        );
    }
    map
}

fn parse_owners(tsv: Tsv) -> HashMap<String, Owner> {
    let mut map: HashMap<String, Owner> = HashMap::new();
    for row in &tsv.rows {
        let accn = tsv.get(row, "ACCESSION_NUMBER").to_string();
        if accn.is_empty() {
            continue;
        }
        // First owner per accession wins; multi-owner filings are rare and the
        // transaction line carries no owner key to disambiguate against.
        map.entry(accn).or_insert_with(|| Owner {
            cik: parse_cik(tsv.get(row, "RPTOWNERCIK")),
            name: tsv.get(row, "RPTOWNERNAME").to_string(),
            role: Role::from_relationship(tsv.get(row, "RPTOWNER_RELATIONSHIP")),
            title: tsv.get(row, "RPTOWNER_TITLE").to_string(),
        });
    }
    map
}

fn emit_transactions(
    tsv: &Tsv,
    is_derivative: bool,
    submission: &HashMap<String, Submission>,
    owners: &HashMap<String, Owner>,
    out: &mut Vec<Txn>,
) {
    for row in &tsv.rows {
        let accn = tsv.get(row, "ACCESSION_NUMBER");
        if accn.is_empty() {
            continue;
        }
        let Some(sub) = submission.get(accn) else {
            continue;
        };
        let (owner_cik, owner_name, role, title) = match owners.get(accn) {
            Some(o) => (o.cik, o.name.clone(), o.role, o.title.clone()),
            None => (0, String::new(), Role::Other, String::new()),
        };

        out.push(Txn {
            filing_date: sub.filing_date,
            accession: accn.to_string(),
            doc_type: sub.doc_type.clone(),
            issuer_cik: sub.issuer_cik,
            issuer_name: sub.issuer_name.clone(),
            ticker: sub.ticker.clone(),
            owner_cik,
            owner_name,
            role,
            officer_title: title,
            security_title: tsv.get(row, "SECURITY_TITLE").to_string(),
            txn_date: parse_date(tsv.get(row, "TRANS_DATE")),
            txn_code: tsv.get(row, "TRANS_CODE").to_string(),
            shares: parse_f64(tsv.get(row, "TRANS_SHARES")),
            price: parse_f64(tsv.get(row, "TRANS_PRICEPERSHARE")),
            acquired_disposed: tsv.get(row, "TRANS_ACQUIRED_DISP_CD").to_string(),
            shares_owned_after: parse_f64(tsv.get(row, "SHRS_OWND_FOLWNG_TRANS")),
            direct_indirect: tsv.get(row, "DIRECT_INDIRECT_OWNERSHIP").to_string(),
            is_derivative,
        });
    }
}

// ---------------------------------------------------------------------------
// Field parsers
// ---------------------------------------------------------------------------

fn parse_cik(s: &str) -> u32 {
    s.trim().parse().unwrap_or(0)
}

fn parse_f64(s: &str) -> f64 {
    s.trim().parse().unwrap_or(0.0)
}

/// Parse a SEC date to `i32` `YYYYMMDD`. Accepts `DD-MON-YYYY` ("01-APR-2024")
/// and ISO `YYYY-MM-DD`. Returns 0 on an unrecognised or empty value.
pub fn parse_date(s: &str) -> i32 {
    let s = s.trim();
    if s.is_empty() {
        return 0;
    }
    // ISO: YYYY-MM-DD
    if let Some((y, rest)) = s.split_once('-') {
        if y.len() == 4 {
            if let Some((m, d)) = rest.split_once('-') {
                if let (Ok(y), Ok(m), Ok(d)) =
                    (y.parse::<i32>(), m.parse::<i32>(), d.parse::<i32>())
                {
                    return y * 10000 + m * 100 + d;
                }
            }
        }
    }
    // DD-MON-YYYY
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() == 3 {
        if let (Ok(d), Some(m), Ok(y)) = (
            parts[0].parse::<i32>(),
            month_num(parts[1]),
            parts[2].parse::<i32>(),
        ) {
            return y * 10000 + m * 100 + d;
        }
    }
    0
}

fn month_num(m: &str) -> Option<i32> {
    Some(match m.to_ascii_uppercase().as_str() {
        "JAN" => 1,
        "FEB" => 2,
        "MAR" => 3,
        "APR" => 4,
        "MAY" => 5,
        "JUN" => 6,
        "JUL" => 7,
        "AUG" => 8,
        "SEP" => 9,
        "OCT" => 10,
        "NOV" => 11,
        "DEC" => 12,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_date_forms() {
        assert_eq!(parse_date("01-APR-2024"), 20240401);
        assert_eq!(parse_date("2024-04-01"), 20240401);
        assert_eq!(parse_date("31-DEC-2014"), 20141231);
        assert_eq!(parse_date(""), 0);
        assert_eq!(parse_date("garbage"), 0);
    }

    #[test]
    fn parses_numbers_leniently() {
        assert_eq!(parse_cik("0000320193"), 320193);
        assert_eq!(parse_cik(""), 0);
        assert!((parse_f64("188.01") - 188.01).abs() < 1e-9);
        assert_eq!(parse_f64(""), 0.0);
    }
}
