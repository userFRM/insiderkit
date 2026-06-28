# insiderkit

SEC Forms 3/4/5 insider transactions for Rust.

```toml
[dependencies]
insiderkit = "0.1.0"
```

```rust,no_run
#[tokio::main]
async fn main() -> insiderkit::Result<()> {
    for t in insiderkit::transactions_for("AAPL").await?.iter().take(5) {
        println!("{} {} {} {} @ {}", t.txn_date, t.owner_name, t.txn_code, t.shares, t.price);
    }
    Ok(())
}
```

Full documentation: <https://github.com/userFRM/insiderkit>

Licensed under MIT OR Apache-2.0.
