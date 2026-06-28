//! End-to-end: serve a manifest + a real parquet shard, then confirm the
//! client fetches, reads, and filters it.

use insiderkit::{write_transactions, Insiderkit, Role, Txn};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn tx(ticker: &str, owner: &str, code: &str, date: i32) -> Txn {
    Txn {
        filing_date: date,
        accession: "0000000000-24-000001".into(),
        doc_type: "4".into(),
        issuer_cik: 320193,
        issuer_name: "Apple Inc.".into(),
        ticker: ticker.into(),
        owner_cik: 1214128,
        owner_name: owner.into(),
        role: Role::Officer,
        officer_title: "CEO".into(),
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

#[tokio::test(flavor = "multi_thread")]
async fn client_reads_served_parquet() {
    let dir = tempfile::TempDir::new().unwrap();
    let shard_path = dir.path().join("insider-2024.parquet");
    let rows = vec![
        tx("AAPL", "COOK TIMOTHY D", "S", 20240201),
        tx("AAPL", "WILLIAMS JEFFREY E", "P", 20240105),
        tx("MSFT", "NADELLA SATYA", "S", 20240115),
    ];
    write_transactions(&shard_path, &rows).unwrap();
    let parquet = std::fs::read(&shard_path).unwrap();
    let digest = sha256_hex(&parquet);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!(r#"{{"insider-2024.parquet":"sha256:{digest}"}}"#)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/insider-2024.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .mount(&server)
        .await;

    let cache = tempfile::TempDir::new().unwrap();
    let client = Insiderkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache.path().to_path_buf())
        .with_mirror_url(None);

    let aapl = client.transactions_for("aapl").await.unwrap();
    assert_eq!(aapl.len(), 2, "two AAPL rows");
    assert_eq!(aapl[0].txn_date, 20240201, "sorted most-recent first");

    let buys = client.buys("AAPL").await.unwrap();
    assert_eq!(buys.len(), 1);
    assert_eq!(buys[0].txn_code, "P");

    let by_owner = client.by_owner("COOK").await.unwrap();
    assert_eq!(by_owner.len(), 1);

    let latest = client.latest(2).await.unwrap();
    assert_eq!(latest.len(), 2);
}
