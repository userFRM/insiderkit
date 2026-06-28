//! `insiderkit-cli` — build, refresh, and query the bundled insider-transaction
//! parquet data.
//!
//! # Commands
//!
//! ```text
//! insiderkit-cli backfill [--from 2014] [--to 2024]
//! insiderkit-cli nightly-append
//! insiderkit-cli manifest
//! insiderkit-cli query --ticker AAPL
//! insiderkit-cli query --owner "COOK TIMOTHY"
//! ```
//!
//! `backfill` and `nightly-append` download the SEC Insider Transactions Data
//! Set quarterly ZIPs, parse them, and write one parquet per year under
//! `data/year=YYYY/insider-YYYY.parquet`. `nightly-append` refreshes only the
//! current quarter's year file (SEC keeps the in-progress quarter's ZIP fresh).

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
    /// Refresh the current quarter's year file (nightly update).
    NightlyAppend,
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
// nightly-append: re-fetch the current quarter and rewrite the current year
// ---------------------------------------------------------------------------

async fn nightly_append(data_dir: &Path) -> Result<()> {
    let year = current_year();
    let client = http_client()?;
    let mut rows = Vec::new();
    for q in 1..=current_quarter() {
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
    if rows.is_empty() {
        eprintln!("no rows fetched for {year}; leaving data unchanged");
        return Ok(());
    }
    write_year(data_dir, year, &rows)?;
    write_manifest(data_dir)
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
    ymd_now().0
}

fn current_quarter() -> u32 {
    (ymd_now().1 - 1) / 3 + 1
}

/// (year, month) from the system clock, via days-since-epoch with Hinnant's
/// civil-from-days algorithm. Avoids a chrono dependency in the CLI.
fn ymd_now() -> (i32, u32) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let z = secs / 86_400 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32)
}
