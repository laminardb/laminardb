# Vendored Apache Iceberg Rust 0.11.0 on Arrow 59

Status: adopted 2026-11-09. Remove this fork and `vendor/iceberg-catalog-rest` and
`vendor/iceberg-storage-opendal` once an upstream `iceberg` release uses the Arrow major that the
workspace's `deltalake` requires, then restore the `iceberg*` workspace dependencies to crates.io.

## Why

`iceberg` 0.11.0 requires Arrow and Parquet `^58.4`; `deltalake` 1.1 requires `^59` and DataFusion
`^55`. The workspace can hold only one Arrow major across the Iceberg and Delta sinks. A `[patch]`
or git dependency does not reach consumers of the published `laminar-connectors`, so the fork is
renamed and published.

## Provenance

| Crate | Source |
|---|---|
| `vendor/iceberg` | crates.io `iceberg` 0.11.0, sha256 `25dd5d1aa2fa92d2c9bc426f72bc16717a27eb73255805beb8845afcca31e58d` |
| `vendor/iceberg-catalog-rest` | crates.io `iceberg-catalog-rest` 0.11.0, sha256 `6adef6989310849467f1d8fdccf4bdb78c736db7c06d302a3b1663a32236373b` |
| `vendor/iceberg-storage-opendal` | crates.io `iceberg-storage-opendal` 0.11.0, sha256 `08691d92b8e3ce0d827fa3d6e858b6d5fd948fb337bc57c244eb7d6bed1cddbc` |

All three are upstream git `20eb0feddac8f40bb9e9108a0744b7f356d69480`, Apache-2.0.

## Local changes

Manifests only; no source file differs from the crates.io packages.

- Package names gain `laminardb-`; library names are unchanged so `iceberg::` paths still resolve.
- Versions follow the workspace.
- `arrow-*` and `parquet` requirements move from `58.4` to `59.3`. Upstream made the same move on
  `main` with manifest edits alone (apache/iceberg-rust#2958).
- The `serde_arrow` dev-dependency feature moves from `arrow-58` to `arrow-59`.
- `Cargo.lock`, `Cargo.toml.orig` (a reserved name in published packages), `.cargo_vcs_info.json`,
  `DEPENDENCIES.rust.tsv` and `public-api.txt` are not kept.

RustSec advisories name upstream packages, so `cargo audit` does not match this fork. Check new
`iceberg` advisories by hand while it exists.
