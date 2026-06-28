//! Stateful `Insiderkit` client — async insider-transaction endpoints with
//! blocking wrappers.
//!
//! Fetches year-partitioned parquet shards from GitHub raw (or a configurable
//! origin) with ETag-aware caching, SHA-256 manifest verification, and CDN
//! mirror fallback. Falls back to stale cache on transient network failures.
//!
//! # Quick start — free functions
//!
//! ```no_run
//! use insiderkit::transactions_for;
//!
//! #[tokio::main]
//! async fn main() -> insiderkit::Result<()> {
//!     for t in transactions_for("AAPL").await?.iter().take(5) {
//!         println!("{} {} {} {} shares @ {}", t.txn_date, t.owner_name, t.txn_code, t.shares, t.price);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Client pattern (reuse across calls)
//!
//! ```no_run
//! use insiderkit::Insiderkit;
//!
//! #[tokio::main]
//! async fn main() -> insiderkit::Result<()> {
//!     let client = Insiderkit::new();
//!     let buys = client.buys("NVDA").await?;
//!     println!("{} insider purchases", buys.len());
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::fetcher::{default_cache_dir, resolved_base_url, CachedFetcher};
use crate::parquet_io::read_transactions;
use crate::record::Txn;

/// Stateful insiderkit client.
///
/// Wraps an ETag-aware cached fetcher and exposes flat async query methods.
/// Create once and reuse; the internal reqwest client is kept alive for
/// connection pooling.
///
/// ```no_run
/// use insiderkit::Insiderkit;
/// use std::path::PathBuf;
///
/// let client = Insiderkit::new()
///     .with_base_url("https://my-mirror.example.com/insiderkit")
///     .with_cache_dir(PathBuf::from("/tmp/insiderkit-test"));
/// ```
#[derive(Clone)]
pub struct Insiderkit {
    fetcher: CachedFetcher,
}

impl Insiderkit {
    /// Create a client with the default GitHub raw backend and XDG cache.
    ///
    /// Reads `INSIDERKIT_BASE_URL` and `INSIDERKIT_CACHE_DIR` from the
    /// environment if set. **This function never fails.** Errors are deferred
    /// to the first fetch.
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .user_agent("insiderkit/0.1 (+https://github.com/userFRM/insiderkit)")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            fetcher: CachedFetcher::new(http, resolved_base_url(), default_cache_dir()),
        }
    }

    /// Override the origin URL.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.fetcher.set_base_url(url.into());
        self
    }

    /// Override the on-disk cache directory.
    pub fn with_cache_dir(mut self, dir: PathBuf) -> Self {
        self.fetcher.set_cache_dir(dir);
        self
    }

    /// Override the CDN mirror URL. `None` disables mirror fallback.
    pub fn with_mirror_url(mut self, url: Option<String>) -> Self {
        self.fetcher.set_mirror_url(url);
        self
    }

    // ── Async query endpoints ───────────────────────────────────────────────

    /// All transactions for an issuer `ticker` (case-insensitive), most recent
    /// transaction date first.
    pub async fn transactions_for(&self, ticker: &str) -> Result<Vec<Txn>> {
        let rows = self.load_all_rows().await?;
        let upper = ticker.to_uppercase();
        Ok(sort_desc(
            rows.into_iter()
                .filter(|r| r.ticker.eq_ignore_ascii_case(&upper))
                .collect(),
        ))
    }

    /// All transactions by a reporting owner, matched by exact owner CIK if
    /// `name_or_cik` parses as an integer, otherwise by case-insensitive
    /// substring of the owner name. Most recent first.
    pub async fn by_owner(&self, name_or_cik: &str) -> Result<Vec<Txn>> {
        let rows = self.load_all_rows().await?;
        let matched: Vec<Txn> = if let Ok(cik) = name_or_cik.parse::<u32>() {
            rows.into_iter().filter(|r| r.owner_cik == cik).collect()
        } else {
            let needle = name_or_cik.to_lowercase();
            rows.into_iter()
                .filter(|r| r.owner_name.to_lowercase().contains(&needle))
                .collect()
        };
        Ok(sort_desc(matched))
    }

    /// The `n` most recent transactions across all issuers, by filing date.
    pub async fn latest(&self, n: usize) -> Result<Vec<Txn>> {
        let mut rows = self.load_all_rows().await?;
        rows.sort_by_key(|r| std::cmp::Reverse(r.filing_date));
        rows.truncate(n);
        Ok(rows)
    }

    /// Open-market purchases (transaction code `P`) for an issuer, most recent first.
    pub async fn buys(&self, ticker: &str) -> Result<Vec<Txn>> {
        Ok(self
            .transactions_for(ticker)
            .await?
            .into_iter()
            .filter(Txn::is_buy)
            .collect())
    }

    /// Open-market sales (transaction code `S`) for an issuer, most recent first.
    pub async fn sells(&self, ticker: &str) -> Result<Vec<Txn>> {
        Ok(self
            .transactions_for(ticker)
            .await?
            .into_iter()
            .filter(Txn::is_sell)
            .collect())
    }

    /// Cluster buys: issuers where two or more distinct owners each made a
    /// purchase (code `P`) within a rolling `window_days` span. Returns one
    /// [`ClusterBuy`] per qualifying issuer, most recent cluster first.
    pub async fn cluster_buys(&self, window_days: i64) -> Result<Vec<ClusterBuy>> {
        let rows = self.load_all_rows().await?;
        Ok(cluster_buys(&rows, window_days))
    }

    // ── Blocking wrappers ───────────────────────────────────────────────────

    /// Blocking variant of [`transactions_for`](Self::transactions_for).
    pub fn transactions_for_blocking(&self, ticker: &str) -> Result<Vec<Txn>> {
        let c = self.clone();
        let t = ticker.to_owned();
        block(async move { c.transactions_for(&t).await })
    }

    /// Blocking variant of [`by_owner`](Self::by_owner).
    pub fn by_owner_blocking(&self, name_or_cik: &str) -> Result<Vec<Txn>> {
        let c = self.clone();
        let q = name_or_cik.to_owned();
        block(async move { c.by_owner(&q).await })
    }

    /// Blocking variant of [`latest`](Self::latest).
    pub fn latest_blocking(&self, n: usize) -> Result<Vec<Txn>> {
        let c = self.clone();
        block(async move { c.latest(n).await })
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    /// Fetch every `insider-YYYY.parquet` shard listed in the manifest and
    /// flat-concatenate the rows.
    pub(crate) async fn load_all_rows(&self) -> Result<Vec<Txn>> {
        let keys = self.discover_shards().await?;
        let mut all = Vec::new();
        for key in keys {
            let bytes = self.fetcher.fetch(&key).await?;
            all.extend(read_transactions(&bytes)?);
        }
        Ok(all)
    }

    /// Fetch `manifest.json` and return sorted shard keys (without `.parquet`).
    async fn discover_shards(&self) -> Result<Vec<String>> {
        let url = format!("{}/manifest.json", self.fetcher.base_url);
        let resp = self
            .fetcher
            .http
            .get(&url)
            .send()
            .await
            .map_err(Error::Http)?;
        if !resp.status().is_success() {
            return Err(Error::Other(format!(
                "manifest.json: HTTP {} {}",
                resp.status().as_u16(),
                resp.status().canonical_reason().unwrap_or("")
            )));
        }
        let manifest: serde_json::Value = resp.json().await.map_err(Error::Http)?;
        let obj = manifest
            .as_object()
            .ok_or_else(|| Error::Other("manifest.json is not a JSON object".into()))?;
        let mut keys: Vec<String> = obj
            .keys()
            .filter(|k| is_insider_shard(k))
            .map(|k| k.trim_end_matches(".parquet").to_string())
            .collect();
        keys.sort();
        Ok(keys)
    }
}

impl Default for Insiderkit {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Cluster-buy detection
// ---------------------------------------------------------------------------

/// A run of purchases by multiple distinct owners of one issuer inside a window.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterBuy {
    pub ticker: String,
    pub issuer_cik: u32,
    pub issuer_name: String,
    /// Distinct reporting owners who bought inside the window.
    pub owners: Vec<String>,
    /// Earliest purchase transaction date in the cluster (YYYYMMDD).
    pub first_date: i32,
    /// Latest purchase transaction date in the cluster (YYYYMMDD).
    pub last_date: i32,
}

/// Find, per issuer, the densest window of distinct-owner purchases. An issuer
/// qualifies when at least two distinct owners bought within `window_days` of
/// each other (measured on `txn_date`).
fn cluster_buys(rows: &[Txn], window_days: i64) -> Vec<ClusterBuy> {
    // Group purchase rows by issuer CIK.
    let mut by_issuer: HashMap<u32, Vec<&Txn>> = HashMap::new();
    for r in rows.iter().filter(|r| r.is_buy()) {
        by_issuer.entry(r.issuer_cik).or_default().push(r);
    }

    let mut out = Vec::new();
    for (cik, mut buys) in by_issuer {
        buys.sort_by_key(|r| r.txn_date);
        // Slide a window over sorted purchase dates; track the widest distinct
        // owner set seen in any window. ponytail: O(n^2) per issuer; issuer
        // purchase counts are small (tens), upgrade to a deque sweep if a
        // pathological issuer ever shows up.
        let mut best: Option<ClusterBuy> = None;
        for (i, anchor) in buys.iter().enumerate() {
            let mut owners: Vec<String> = Vec::new();
            let mut last_date = anchor.txn_date;
            for w in &buys[i..] {
                if days_between(anchor.txn_date, w.txn_date) > window_days {
                    break;
                }
                if !owners.iter().any(|o| o == &w.owner_name) {
                    owners.push(w.owner_name.clone());
                }
                last_date = w.txn_date;
            }
            if owners.len() >= 2 {
                let candidate = ClusterBuy {
                    ticker: anchor.ticker.clone(),
                    issuer_cik: cik,
                    issuer_name: anchor.issuer_name.clone(),
                    first_date: anchor.txn_date,
                    last_date,
                    owners,
                };
                let take = best
                    .as_ref()
                    .map(|b| candidate.owners.len() > b.owners.len())
                    .unwrap_or(true);
                if take {
                    best = Some(candidate);
                }
            }
        }
        if let Some(b) = best {
            out.push(b);
        }
    }
    out.sort_by_key(|c| std::cmp::Reverse(c.last_date));
    out
}

/// Calendar-day gap between two `YYYYMMDD` integers. Cheap and exact for the
/// proleptic Gregorian calendar; no calendar library needed.
fn days_between(a: i32, b: i32) -> i64 {
    (ymd_to_days(b) - ymd_to_days(a)).abs()
}

/// Days since 1970-01-01 for a `YYYYMMDD` integer (Howard Hinnant's algorithm).
fn ymd_to_days(yyyymmdd: i32) -> i64 {
    let y = (yyyymmdd / 10000) as i64;
    let m = ((yyyymmdd / 100) % 100) as i64;
    let d = (yyyymmdd % 100) as i64;
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn sort_desc(mut rows: Vec<Txn>) -> Vec<Txn> {
    rows.sort_by_key(|r| std::cmp::Reverse(r.txn_date));
    rows
}

/// Return `true` for filenames matching `insider-YYYY.parquet`.
fn is_insider_shard(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("insider-") else {
        return false;
    };
    let Some(year) = rest.strip_suffix(".parquet") else {
        return false;
    };
    !year.is_empty() && year.bytes().all(|b| b.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// All transactions for `ticker` using a temporary one-shot client.
pub async fn transactions_for(ticker: &str) -> Result<Vec<Txn>> {
    Insiderkit::new().transactions_for(ticker).await
}

/// All transactions by a reporting owner (name or CIK), one-shot client.
pub async fn by_owner(name_or_cik: &str) -> Result<Vec<Txn>> {
    Insiderkit::new().by_owner(name_or_cik).await
}

/// The `n` most recent transactions across all issuers, one-shot client.
pub async fn latest(n: usize) -> Result<Vec<Txn>> {
    Insiderkit::new().latest(n).await
}

// ---------------------------------------------------------------------------
// Blocking helper
// ---------------------------------------------------------------------------

/// Drive a future to completion from any context (sync or async).
///
/// - Inside a tokio **multi-thread** runtime: `block_in_place` + `block_on`.
/// - Inside a **current-thread** runtime or no runtime: the future is driven on
///   a dedicated OS thread with its own runtime so the caller is not re-entered.
pub(crate) fn block<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        _ => std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(Error::Io)
                .and_then(|rt| rt.block_on(fut))
        })
        .join()
        .expect("blocking thread panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Role;

    fn tx(ticker: &str, owner: &str, code: &str, date: i32) -> Txn {
        Txn {
            filing_date: date,
            accession: "a".into(),
            doc_type: "4".into(),
            issuer_cik: ticker.bytes().map(|b| b as u32).sum(),
            issuer_name: ticker.into(),
            ticker: ticker.into(),
            owner_cik: owner.bytes().map(|b| b as u32).sum(),
            owner_name: owner.into(),
            role: Role::Officer,
            officer_title: String::new(),
            security_title: "Common Stock".into(),
            txn_date: date,
            txn_code: code.into(),
            shares: 100.0,
            price: 10.0,
            acquired_disposed: if code == "P" { "A" } else { "D" }.into(),
            shares_owned_after: 100.0,
            direct_indirect: "D".into(),
            is_derivative: false,
        }
    }

    #[test]
    fn ymd_to_days_matches_known_epoch() {
        assert_eq!(ymd_to_days(19700101), 0);
        assert_eq!(ymd_to_days(20000101), 10957);
        assert_eq!(days_between(20240101, 20240131), 30);
        assert_eq!(days_between(20240301, 20240201), 29); // 2024 leap year
    }

    #[test]
    fn cluster_needs_two_distinct_owners() {
        // Same owner twice in window → not a cluster.
        let rows = vec![
            tx("ACME", "Alice", "P", 20240101),
            tx("ACME", "Alice", "P", 20240103),
        ];
        assert!(cluster_buys(&rows, 10).is_empty());

        // Two distinct owners within window → one cluster.
        let rows = vec![
            tx("ACME", "Alice", "P", 20240101),
            tx("ACME", "Bob", "P", 20240105),
            tx("ACME", "Carol", "P", 20240120), // outside 10-day window of anchor
        ];
        let clusters = cluster_buys(&rows, 10);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].owners.len(), 2);
        assert_eq!(clusters[0].ticker, "ACME");
    }

    #[test]
    fn cluster_ignores_sells() {
        let rows = vec![
            tx("ACME", "Alice", "S", 20240101),
            tx("ACME", "Bob", "S", 20240102),
        ];
        assert!(cluster_buys(&rows, 10).is_empty());
    }

    #[test]
    fn is_shard_matches_year_files_only() {
        assert!(is_insider_shard("insider-2024.parquet"));
        assert!(!is_insider_shard("manifest.json"));
        assert!(!is_insider_shard("insider-.parquet"));
        assert!(!is_insider_shard("dividends-2024.parquet"));
    }
}
