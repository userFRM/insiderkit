<!-- Canonical CHANGELOG header for every *kit. The body keeps each kit's real
release history; only this top block is standardized. -->
# Changelog

All notable changes to insiderkit are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

Initial release.

- Async `Insiderkit` client plus blocking siblings and one-shot free functions.
- Query surface: `transactions_for`, `by_owner`, `latest`, `buys`, `sells`, `cluster_buys`.
- Bundled per-year parquet (`data/year=YYYY/insider-YYYY.parquet`) served from GitHub raw with on-demand fetch, ETag revalidation, SHA-256 manifest verification, and a CDN mirror plus stale-cache fallback.
- `insiderkit-cli` with `backfill`, `nightly-append`, `manifest`, and `query`.
