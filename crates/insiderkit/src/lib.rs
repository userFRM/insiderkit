//! `insiderkit` — SEC Forms 3/4/5 insider transactions for Rust.
//!
//! Fetches year-partitioned parquet files on demand from GitHub raw, caches
//! them locally with ETag revalidation, and falls back to stale cache on
//! network errors. No API keys. Offline after the first successful fetch of
//! each year file.
//!
//! Data comes from the SEC's public-domain Insider Transactions Data Sets.
//! Each row is one reported transaction; dates are `i32` `YYYYMMDD`.
//!
//! # Quick start — free functions
//!
//! ```no_run
//! use insiderkit::transactions_for;
//!
//! #[tokio::main]
//! async fn main() -> insiderkit::Result<()> {
//!     for t in transactions_for("AAPL").await?.iter().take(5) {
//!         println!("{} {} {} {} @ {}", t.txn_date, t.owner_name, t.txn_code, t.shares, t.price);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! For connection-pool reuse across many lookups, create an [`Insiderkit`]
//! client once and call its methods instead of the free functions.
//!
//! # Environment overrides
//!
//! | Variable | Effect |
//! |---|---|
//! | `INSIDERKIT_BASE_URL` | Replace the GitHub raw origin URL |
//! | `INSIDERKIT_CACHE_DIR` | Override `~/.cache/insiderkit/` |
//! | `INSIDERKIT_MIRROR_URL` | Override the jsDelivr CDN mirror |
#![forbid(unsafe_code)]

mod error;
pub use error::{Error, Result};

mod record;
pub use record::{Role, Txn};

pub mod parquet_io;
pub use parquet_io::{read_transactions, write_transactions};

mod fetcher;

mod client;
pub use client::{by_owner, latest, transactions_for, ClusterBuy, Insiderkit};
