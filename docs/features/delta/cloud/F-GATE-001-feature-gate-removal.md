# F-GATE-001: Feature Gate Removal

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-GATE-001 |
| **Status** | Done |
| **Priority** | P0 |
| **Phase** | 7a |
| **Effort** | M (2-3 days) |
| **Dependencies** | None |
| **Blocks** | F-CFG-001, F-S3-001, F-CRASH-001, F-DISAGG-001 |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-db`, `laminar-core`, `laminar-server` |

## Summary

Remove `durable` and `delta` feature gates from `laminar-db`. The `delta` feature remains on `laminar-core` to gate heavy distributed deps (tonic, openraft, chitchat) — library users don't pull them in. The server binary unconditionally enables `laminar-core/delta`, so delta mode is always available in the server without needing `--features delta`.

## Goals

- Remove `durable = []` and `delta = ["durable"]` from `laminar-db/Cargo.toml`
- Make delta dependencies (tonic, prost, openraft, chitchat) unconditional in `laminar-core/Cargo.toml`
- Remove all `#[cfg(feature = "delta")]` annotations from source files (~23 occurrences across 7 files)
- Remove `validate_features()` checks for `durable`/`delta` in `Profile`
- Ensure `cargo build` and `cargo test` pass without any `--features` flags

## Non-Goals

- Removing platform-specific feature gates (aws, gcs, azure, kafka, postgres-cdc, io-uring, etc.)
- Changing the runtime behavior of any profile
- Refactoring the `Profile` enum (deferred to F-CFG-001)

## Technical Design

### Files to Modify

| File | Change |
|------|--------|
| `crates/laminar-db/Cargo.toml` | Remove `durable` and `delta` features; keep cloud/connector gates |
| `crates/laminar-core/Cargo.toml` | Make tonic, prost, openraft, chitchat, tokio-util, xxhash-rust unconditional deps |
| `crates/laminar-server/Cargo.toml` | Remove `delta` from features list and default; make `laminar-core` dep unconditional |
| `crates/laminar-core/src/lib.rs` | Remove `#[cfg(feature = "delta")]` from `pub mod delta` |
| `crates/laminar-core/src/aggregation/mod.rs` | Remove 2x `#[cfg(feature = "delta")]` from `gossip_aggregates` and `grpc_fanout` |
| `crates/laminar-core/src/lookup/mod.rs` | Remove `#[cfg(feature = "delta")]` from `partitioned` |
| `crates/laminar-core/build.rs` | Remove `#[cfg(feature = "delta")]` around protoc build |
| `crates/laminar-server/src/main.rs` | Remove `#[cfg(feature = "delta")]` from `mod delta` |
| `crates/laminar-server/src/server.rs` | Remove 3x `#[cfg(feature = "delta")]` from `ServerHandle::Delta` and dispatch |
| `crates/laminar-db/src/profile.rs` | Remove feature-gate checks in `validate_features()` for durable/delta |
| `crates/laminar-core/benches/delta_checkpoint_bench.rs` | Remove 7x `#[cfg(feature = "delta")]` |

### Error Handling

If protoc is not available, `build.rs` should emit a clear error message rather than silently skipping proto compilation.

## Test Plan

- `cargo build` passes without `--features delta`
- `cargo test` passes without `--features delta`
- `cargo clippy -- -D warnings` passes
- All existing tests still pass with default features
- `Profile::Delta.validate_features()` returns `Ok(())`

## Completion Checklist

- [ ] Feature gates removed from all 3 Cargo.toml files
- [ ] All `#[cfg(feature = "delta")]` removed from source files
- [ ] `Profile::validate_features()` updated
- [ ] `cargo build` passes (no features)
- [ ] `cargo test` passes (no features)
- [ ] `cargo clippy -- -D warnings` passes
- [ ] Feature INDEX.md updated
