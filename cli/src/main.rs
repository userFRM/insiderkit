//! `insiderkit-cli` — build, refresh, and query the bundled insider-transaction
//! parquet data.
//!
//! # Commands
//!
//! ```text
//! insiderkit-cli backfill [--from 2014] [--to 2024]
//! insiderkit-cli nightly-append
//! insiderkit-cli reconcile
//! insiderkit-cli manifest
//! insiderkit-cli query --ticker AAPL
//! insiderkit-cli query --owner "COOK TIMOTHY"
//! ```
//!
//! `backfill` downloads the SEC Insider Transactions Data Set quarterly ZIPs
//! and writes one parquet per year under `data/year=YYYY/insider-YYYY.parquet`.
//! It is the authoritative historical path.
//!
//! `nightly-append` gives same-day coverage: the quarterly ZIP only refreshes
//! every few weeks, so it walks the EDGAR daily index from the last date
//! already present through today, parses each new Form 3/4/5 ownership XML, and
//! merges the rows into the current-year parquet, deduplicated by accession.
//!
//! `reconcile` refetches the current quarter's ZIP and supersedes the
//! daily-index rows by accession, absorbing later amendments and corrections.
//! Run it weekly.

mod daily;
mod ingest;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use insiderkit::{read_transactions, write_transactions, Txn};
use sha2::{Digest, Sha256};

/// Default first backfill year. The SEC data sets start at 2006q1; the seed
/// ships >= 10 years and the workflow can override `--from` down to 2006.
const DEFAULT_FROM_YEAR: i32 = 2014;

/// Bare `<name> <email>` User-Agent for SEC fetches (parenthetical/URL UAs 403).
fn user_agent() -> String {
    std::env::var("INSIDERKIT_SEC_USER_AGENT")
        .unwrap_or_else(|_| "insiderkit contact@example.com".to_string())
}

#[derive(Parser)]
#[command(
    name = "insiderkit-cli",
    about = "SEC Forms 3/4/5 insider transactions"
)]
struct Cli {
    /// Data directory (default: `<cwd>/data`).
    #[arg(long, env = "INSIDERKIT_DATA_DIR", global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download and rebuild per-year parquet from SEC quarterly data sets.
    Backfill {
        /// First year to include (default 2014; data sets reach back to 2006).
        #[arg(long)]
        from: Option<i32>,
        /// Last year to include (default: current year).
        #[arg(long)]
        to: Option<i32>,
    },
    /// Pull same-day filings from the EDGAR daily index and merge them into
    /// the current-year parquet, deduplicated by accession.
    NightlyAppend,
    /// Refetch the current quarter's data set and supersede daily-index rows by
    /// accession (absorbs amendments). Run weekly.
    Reconcile,
    /// Generate `data/manifest.json` with a SHA-256 per parquet file.
    Manifest,
    /// Read bundled parquet and print matching transactions.
    Query {
        /// Issuer ticker (case-insensitive).
        #[arg(long)]
        ticker: Option<String>,
        /// Reporting owner name substring or CIK.
        #[arg(long)]
        owner: Option<String>,
        /// Maximum rows to print.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(|| PathBuf::from("data"));

    match cli.cmd {
        Command::Backfill { from, to } => {
            let from = from.unwrap_or(DEFAULT_FROM_YEAR);
            let to = to.unwrap_or_else(current_year);
            backfill(&data_dir, from, to).await
        }
        Command::NightlyAppend => nightly_append(&data_dir).await,
        Command::Reconcile => reconcile(&data_dir).await,
        Command::Manifest => write_manifest(&data_dir),
        Command::Query {
            ticker,
            owner,
            limit,
        } => query(&data_dir, ticker, owner, limit),
    }
}

// ---------------------------------------------------------------------------
// backfill
// ---------------------------------------------------------------------------

async fn backfill(data_dir: &Path, from: i32, to: i32) -> Result<()> {
    let client = http_client()?;
    for year in from..=to {
        let mut rows = Vec::new();
        for q in 1..=4 {
            if year == to && q > current_quarter() && year == current_year() {
                break; // future quarter of the current year does not exist yet
            }
            match fetch_quarter(&client, year, q).await {
                Ok(Some(bytes)) => {
                    let parsed = ingest::parse_quarter_zip(&bytes)
                        .with_context(|| format!("parse {year}q{q}"))?;
                    eprintln!("{year}q{q}: {} rows", parsed.len());
                    rows.extend(parsed);
                }
                Ok(None) => eprintln!("{year}q{q}: not published, skipping"),
                Err(e) => eprintln!("{year}q{q}: fetch failed ({e}), skipping"),
            }
        }
        write_year(data_dir, year, &rows)?;
    }
    write_manifest(data_dir)
}

// ---------------------------------------------------------------------------
// nightly-append: daily-index incremental, merged into the current year
// ---------------------------------------------------------------------------

async fn nightly_append(data_dir: &Path) -> Result<()> {
    let today = today_ymd();
    let year = today / 10000;
    let client = http_client()?;

    let existing = load_year(data_dir, year)?;
    // Resume from the day after the latest filing already present; if the year
    // file is empty, start at Jan 1 (the daily index skips weekends/holidays).
    let last = existing.iter().map(|r| r.filing_date).max().unwrap_or(0);
    let start = if last >= year * 10000 + 101 {
        next_day(last)
    } else {
        year * 10000 + 101
    };
    eprintln!(
        "nightly-append: {start} through {today} (year {year}, {} existing rows)",
        existing.len()
    );

    let mut fresh = Vec::new();
    let mut day = start;
    while day <= today {
        fresh.extend(daily::ingest_day(&client, day).await?);
        day = next_day(day);
    }

    if fresh.is_empty() {
        eprintln!("no new filings; leaving data unchanged");
        return Ok(());
    }
    let added = fresh.len();
    let merged = merge_by_accession(existing, fresh);
    eprintln!(
        "merged: {added} fresh rows in, {} total after dedup",
        merged.len()
    );
    write_year(data_dir, year, &merged)?;
    write_manifest(data_dir)
}

// ---------------------------------------------------------------------------
// reconcile: quarterly ZIP supersedes daily-index rows by accession
// ---------------------------------------------------------------------------

async fn reconcile(data_dir: &Path) -> Result<()> {
    let year = current_year();
    let client = http_client()?;
    let mut authoritative = Vec::new();
    for q in 1..=current_quarter() {
        match fetch_quarter(&client, year, q).await {
            Ok(Some(bytes)) => {
                let parsed = ingest::parse_quarter_zip(&bytes)
                    .with_context(|| format!("parse {year}q{q}"))?;
                eprintln!("{year}q{q}: {} rows", parsed.len());
                authoritative.extend(parsed);
            }
            Ok(None) => eprintln!("{year}q{q}: not published, skipping"),
            Err(e) => eprintln!("{year}q{q}: fetch failed ({e}), skipping"),
        }
    }
    if authoritative.is_empty() {
        eprintln!("no quarterly rows fetched for {year}; leaving data unchanged");
        return Ok(());
    }
    // Quarterly data is authoritative: it supersedes any daily-index rows for
    // the same accession, but keep daily-index rows for accessions the ZIP does
    // not yet carry (the ZIP lags the daily feed).
    let existing = load_year(data_dir, year)?;
    let merged = merge_by_accession(existing, authoritative);
    eprintln!(
        "reconciled: {} rows after superseding by accession",
        merged.len()
    );
    write_year(data_dir, year, &merged)?;
    write_manifest(data_dir)
}

/// Merge `incoming` into `existing`, deduplicated at the filing (accession)
/// level: every existing row whose accession appears in `incoming` is dropped,
/// then all `incoming` rows are appended. Idempotent — re-running with the same
/// `incoming` is a no-op on row count.
fn merge_by_accession(existing: Vec<Txn>, incoming: Vec<Txn>) -> Vec<Txn> {
    use std::collections::HashSet;
    let incoming_accns: HashSet<&str> = incoming.iter().map(|r| r.accession.as_str()).collect();
    let mut out: Vec<Txn> = existing
        .into_iter()
        .filter(|r| !incoming_accns.contains(r.accession.as_str()))
        .collect();
    out.extend(incoming);
    out
}

/// Read the current per-year parquet, or an empty vec if it does not exist yet.
fn load_year(data_dir: &Path, year: i32) -> Result<Vec<Txn>> {
    let path = data_dir
        .join(format!("year={year}"))
        .join(format!("insider-{year}.parquet"));
    if !path.exists() {
        return Ok(Vec::new());
    }
    read_transactions(&std::fs::read(&path)?).with_context(|| format!("read {}", path.display()))
}

// ---------------------------------------------------------------------------
// SEC fetch
// ---------------------------------------------------------------------------

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(user_agent())
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .context("build http client")
}

/// Fetch one quarterly ZIP. Returns `Ok(None)` for a 404 (quarter not yet
/// published); any other non-success is an error.
async fn fetch_quarter(client: &reqwest::Client, year: i32, q: u32) -> Result<Option<Vec<u8>>> {
    let url = format!(
        "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{year}q{q}_form345.zip"
    );
    let resp = client.get(&url).send().await.context("send request")?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        bail!("HTTP {} for {url}", resp.status());
    }
    Ok(Some(resp.bytes().await.context("read body")?.to_vec()))
}

// ---------------------------------------------------------------------------
// write per-year parquet
// ---------------------------------------------------------------------------

fn write_year(data_dir: &Path, year: i32, rows: &[Txn]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let dir = data_dir.join(format!("year={year}"));
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(format!("insider-{year}.parquet"));
    write_transactions(&path, rows).with_context(|| format!("write {}", path.display()))?;
    eprintln!("wrote {} ({} rows)", path.display(), rows.len());
    Ok(())
}

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

/// Write `data/manifest.json` mapping `insider-YYYY.parquet` -> `sha256:<hex>`.
/// Keys are bare filenames so the client (which fetches flat keys) resolves
/// them regardless of the on-disk `year=YYYY/` partitioning.
fn write_manifest(data_dir: &Path) -> Result<()> {
    let mut entries: BTreeMap<String, String> = BTreeMap::new();
    for path in find_parquet(data_dir)? {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("parquet filename")?
            .to_string();
        let bytes = std::fs::read(&path)?;
        let mut h = Sha256::new();
        h.update(&bytes);
        let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
        entries.insert(name, format!("sha256:{hex}"));
    }
    let json = serde_json::to_string_pretty(&entries)?;
    let path = data_dir.join("manifest.json");
    std::fs::create_dir_all(data_dir)?;
    std::fs::write(&path, json)?;
    eprintln!("wrote {} ({} files)", path.display(), entries.len());
    Ok(())
}

fn find_parquet(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !data_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            for sub in std::fs::read_dir(&path)? {
                let p = sub?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(p);
                }
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

// ---------------------------------------------------------------------------
// query (reads local parquet)
// ---------------------------------------------------------------------------

fn query(
    data_dir: &Path,
    ticker: Option<String>,
    owner: Option<String>,
    limit: usize,
) -> Result<()> {
    let mut rows = Vec::new();
    for path in find_parquet(data_dir)? {
        rows.extend(read_transactions(&std::fs::read(&path)?)?);
    }

    if let Some(t) = &ticker {
        let upper = t.to_uppercase();
        rows.retain(|r| r.ticker.eq_ignore_ascii_case(&upper));
    }
    if let Some(o) = &owner {
        if let Ok(cik) = o.parse::<u32>() {
            rows.retain(|r| r.owner_cik == cik);
        } else {
            let needle = o.to_lowercase();
            rows.retain(|r| r.owner_name.to_lowercase().contains(&needle));
        }
    }
    rows.sort_by_key(|r| std::cmp::Reverse(r.txn_date));

    println!(
        "{:<10} {:<6} {:<22} {:<6} {:<6} {:>12} {:>10}",
        "txn_date", "tick", "owner", "role", "code", "shares", "price"
    );
    for r in rows.iter().take(limit) {
        println!(
            "{:<10} {:<6} {:<22} {:<6} {:<6} {:>12.0} {:>10.2}",
            r.txn_date,
            r.ticker,
            truncate(&r.owner_name, 22),
            r.role.as_str(),
            r.txn_code,
            r.shares,
            r.price,
        );
    }
    Ok(())
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n - 1).collect::<String>() + "…"
    }
}

// ---------------------------------------------------------------------------
// calendar helpers (system clock; YYYYMMDD math)
// ---------------------------------------------------------------------------

fn current_year() -> i32 {
    today_ymd() / 10000
}

fn current_quarter() -> u32 {
    (((today_ymd() / 100) % 100 - 1) / 3 + 1) as u32
}

/// Today as a `YYYYMMDD` integer from the system clock.
fn today_ymd() -> i32 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    days_to_ymd(secs / 86_400)
}

/// The calendar day after a `YYYYMMDD` integer.
fn next_day(yyyymmdd: i32) -> i32 {
    days_to_ymd(ymd_to_days(yyyymmdd) + 1)
}

/// `YYYYMMDD` -> days since 1970-01-01 (Hinnant's days-from-civil).
fn ymd_to_days(d: i32) -> i64 {
    let y = (d / 10000) as i64;
    let m = ((d / 100) % 100) as i64;
    let day = (d % 100) as i64;
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Days since 1970-01-01 -> `YYYYMMDD` (Hinnant's civil-from-days).
fn days_to_ymd(days: i64) -> i32 {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y * 10000 + m * 100 + d) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ymd_days_round_trip() {
        assert_eq!(ymd_to_days(19700101), 0);
        assert_eq!(days_to_ymd(0), 19700101);
        assert_eq!(next_day(20240228), 20240229); // leap year
        assert_eq!(next_day(20240229), 20240301);
        assert_eq!(next_day(20251231), 20260101);
        assert_eq!(days_to_ymd(ymd_to_days(20260602)), 20260602);
    }

    /// Live proof: seed a temp data dir whose 2026 file ends a few business
    /// days ago, run the real `nightly_append`, and assert it appended recent
    /// days, deduped, and the rows read back. Re-running is idempotent.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn live_nightly_append_appends_and_dedups() {
        let dir = tempfile::TempDir::new().unwrap();
        let year = today_ymd() / 10000;
        let yd = dir.path().join(format!("year={year}"));
        std::fs::create_dir_all(&yd).unwrap();
        // Seed one row dated a few days ago so resume covers at least one
        // business day (the daily index skips weekends/holidays).
        let seed_date = days_to_ymd(ymd_to_days(today_ymd()) - 3);
        let seed = Txn {
            filing_date: seed_date,
            accession: "0000000000-00-000000".into(),
            doc_type: "4".into(),
            issuer_cik: 1,
            issuer_name: "SEED".into(),
            ticker: "SEED".into(),
            owner_cik: 1,
            owner_name: "SEED".into(),
            role: insiderkit::Role::Other,
            officer_title: String::new(),
            security_title: "Common".into(),
            txn_date: seed_date,
            txn_code: "P".into(),
            shares: 1.0,
            price: 1.0,
            acquired_disposed: "A".into(),
            shares_owned_after: 1.0,
            direct_indirect: "D".into(),
            is_derivative: false,
        };
        write_transactions(&yd.join(format!("insider-{year}.parquet")), &[seed]).unwrap();

        nightly_append(dir.path()).await.unwrap();

        let after = load_year(dir.path(), year).unwrap();
        eprintln!("after first run: {} rows", after.len());
        assert!(after.len() > 1, "should have appended recent filings");
        assert!(
            after.iter().any(|r| r.filing_date > seed_date),
            "newer filings present"
        );
        // Client reads the produced parquet back.
        let bytes = std::fs::read(yd.join(format!("insider-{year}.parquet"))).unwrap();
        assert_eq!(read_transactions(&bytes).unwrap().len(), after.len());

        // Idempotent: a second run from the new max date adds nothing for days
        // already covered (row count does not shrink, no duplication of seed).
        let count1 = after.len();
        nightly_append(dir.path()).await.unwrap();
        let after2 = load_year(dir.path(), year).unwrap();
        eprintln!("after second run: {} rows", after2.len());
        assert!(after2.len() >= count1, "no rows lost on re-run");
        let seed_rows = after2
            .iter()
            .filter(|r| r.accession == "0000000000-00-000000")
            .count();
        assert_eq!(seed_rows, 1, "seed not duplicated");
    }

    #[test]
    fn merge_dedups_by_accession() {
        let mk = |accn: &str, code: &str| Txn {
            filing_date: 20260601,
            accession: accn.into(),
            doc_type: "4".into(),
            issuer_cik: 1,
            issuer_name: "X".into(),
            ticker: "X".into(),
            owner_cik: 2,
            owner_name: "Y".into(),
            role: insiderkit::Role::Officer,
            officer_title: String::new(),
            security_title: "Common".into(),
            txn_date: 20260601,
            txn_code: code.into(),
            shares: 1.0,
            price: 1.0,
            acquired_disposed: "D".into(),
            shares_owned_after: 1.0,
            direct_indirect: "D".into(),
            is_derivative: false,
        };
        let existing = vec![mk("acc-1", "S"), mk("acc-2", "P")];
        // Re-ingesting acc-1 (now corrected) plus a new acc-3.
        let incoming = vec![mk("acc-1", "A"), mk("acc-3", "S")];
        let merged = merge_by_accession(existing, incoming);
        assert_eq!(merged.len(), 3); // acc-1 replaced, acc-2 kept, acc-3 added
        let acc1: Vec<_> = merged.iter().filter(|r| r.accession == "acc-1").collect();
        assert_eq!(acc1.len(), 1);
        assert_eq!(acc1[0].txn_code, "A"); // incoming won

        // Idempotent: merging the same incoming again does not grow.
        let again = merge_by_accession(merged.clone(), vec![mk("acc-1", "A"), mk("acc-3", "S")]);
        assert_eq!(again.len(), 3);
    }
}
