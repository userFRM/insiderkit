# insiderkit

SEC Forms 3/4/5 insider transactions for Rust. Served from bundled parquet with on-demand fetch and a local cache. No API keys. Offline after the first query.

## Install

```toml
[dependencies]
insiderkit = "0.1.0"
```

To track unreleased changes, depend on the repository directly:

```toml
insiderkit = { git = "https://github.com/userFRM/insiderkit" }
```

## Quick start

```rust,no_run
#[tokio::main]
async fn main() -> insiderkit::Result<()> {
    // Recent transactions for an issuer, most recent first.
    for t in insiderkit::transactions_for("AAPL").await?.iter().take(5) {
        println!("{} {} {} {} @ {}", t.txn_date, t.owner_name, t.txn_code, t.shares, t.price);
    }

    // Everything a single insider has filed.
    let _by_owner = insiderkit::by_owner("COOK TIMOTHY").await?;

    // The 10 most recent filings across all issuers.
    let _latest = insiderkit::latest(10).await?;
    Ok(())
}
```

## Client pattern

```rust,no_run
use insiderkit::Insiderkit;

#[tokio::main]
async fn main() -> insiderkit::Result<()> {
    let client = Insiderkit::new();

    let buys = client.buys("NVDA").await?;
    let sells = client.sells("NVDA").await?;
    println!("{} purchases, {} sales", buys.len(), sells.len());

    // Issuers where two or more distinct insiders bought within 30 days.
    for c in client.cluster_buys(30).await? {
        println!("{}: {} insiders bought", c.ticker, c.owners.len());
    }
    Ok(())
}
```

Blocking siblings (`transactions_for_blocking`, `by_owner_blocking`, `latest_blocking`) call the async methods from synchronous code and are safe inside any tokio runtime.

## Transaction codes

`txn_code` carries the SEC Form 3/4/5 transaction code. The most common are `P` (open-market or private purchase) and `S` (open-market or private sale); `Txn::is_buy` and `Txn::is_sell` test for these. Other codes include `A` (grant or award), `M` (option exercise), `G` (gift), and `F` (shares withheld for tax). `acquired_disposed` is `A` or `D`.

## CLI

```bash
insiderkit-cli backfill --from 2014 --to 2025
insiderkit-cli nightly-append
insiderkit-cli manifest
insiderkit-cli query --ticker AAPL
insiderkit-cli query --owner "COOK TIMOTHY"
```

## Data

Sourced from the SEC's Insider Transactions Data Sets, which are public domain. One parquet file per year under `data/year=YYYY/insider-YYYY.parquet`, zstd-compressed, one row per reported transaction. A nightly job refreshes the current year; `data/manifest.json` carries a SHA-256 digest per file. Dates are stored as `i32` `YYYYMMDD`.

## Cache

Fetched parquet is cached on disk (XDG cache dir, e.g. `~/.cache/insiderkit/`) with ETag revalidation, so repeat queries are offline. On a network failure the client serves the last good cached copy. Override the origin with `INSIDERKIT_BASE_URL` and the cache location with `INSIDERKIT_CACHE_DIR`.

## API

Full API reference is on [docs.rs](https://docs.rs/insiderkit).

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
