# F-PROFILE-001: Deployment Profiles

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-PROFILE-001 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STATE-001 (StateStore trait), F-DCKP-004 (ObjectStoreCheckpointer) |
| **Blocks** | F-SERVER-001 (TOML Configuration), F-SERVER-002 (Engine Construction) |
| **Owner** | TBD |
| **Created** | 2026-02-17 |
| **Crate** | `laminar-db` |
| **Module** | `laminar-db/src/profile.rs` |

## Summary

Deployment profiles define a spectrum of configurations from zero-dependency embedded use to full distributed constellation mode. Each profile selects the appropriate state backend, checkpoint strategy, index backend, and network layer. Profiles replace ad-hoc feature flag combinations with named, tested configurations that users select via a single `profile` setting.

The four profiles form a strict superset chain: each higher profile includes all capabilities of lower ones plus additional infrastructure.

## Goals

- Four named deployment profiles: `bare-metal`, `embedded`, `durable`, `constellation`
- Single configuration point: `LaminarDB::with_profile(Profile::Durable)` or `profile = "durable"` in TOML
- Each profile selects state backend, checkpoint strategy, index backend, and networking
- Profiles are compile-time feature-gated where possible to minimize binary size
- Clear documentation of what each profile provides and requires
- Validation that profile requirements are met at startup (e.g., `durable` requires object store config)

## Non-Goals

- Custom profile composition (users pick one of four, not mix-and-match)
- Runtime profile switching (profile is set at construction time)
- Profile-specific SQL syntax differences
- Automatic profile detection based on environment

## Technical Design

### Profile Spectrum

| Profile | State Backend | Checkpoints | Indexes | Networking | Use Case |
|---------|--------------|-------------|---------|------------|----------|
| `bare-metal` | `FxHashMap` | None | None | None | Benchmarks, unit tests, zero-dependency embedding |
| `embedded` | `FxHashMap` | Local WAL only | redb (optional) | None | Single-process applications, SQLite-like usage |
| `durable` | `FxHashMap` | rkyv + object_store (S3/GCS/local) | redb | None | Production single-node with crash recovery |
| `constellation` | `FxHashMap` | rkyv + object_store | redb | gRPC + gossip | Multi-node distributed deployment |

### Profile Enum

```rust
/// Deployment profile controlling infrastructure selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Zero-dependency, no persistence. For benchmarks and tests.
    BareMetal,
    /// Local WAL for crash recovery. Optional redb indexes.
    Embedded,
    /// Full persistence via object_store. redb indexes enabled.
    Durable,
    /// Multi-node with gossip discovery and Raft coordination.
    Constellation,
}
```

### Feature Gate Mapping

| Profile | Required Cargo features |
|---------|------------------------|
| `bare-metal` | (none — always available) |
| `embedded` | (none — WAL is always compiled) |
| `durable` | `durable` (enables object_store, rkyv checkpoint) |
| `constellation` | `constellation` (enables durable + chitchat, openraft, tonic) |

The `constellation` feature implies `durable`. The `durable` feature implies redb support.

### Builder API

```rust
let db = LaminarDB::builder()
    .profile(Profile::Durable)
    .object_store_url("s3://my-bucket/checkpoints")
    .build()
    .await?;
```

The builder validates that required dependencies for the selected profile are configured. For example, `Profile::Durable` without an `object_store_url` returns an error at build time, not at runtime.

### TOML Configuration

```toml
[engine]
profile = "durable"

[checkpoint]
object_store_url = "s3://my-bucket/checkpoints"
interval = "30s"

[indexes]
enabled = true
db_path = "laminardb_indexes.redb"
```

### Profile Capabilities Matrix

| Capability | bare-metal | embedded | durable | constellation |
|-----------|:---------:|:--------:|:-------:|:------------:|
| SQL queries | Y | Y | Y | Y |
| Streaming pipelines | Y | Y | Y | Y |
| State persistence (WAL) | - | Y | Y | Y |
| Object store checkpoints | - | - | Y | Y |
| Crash recovery | - | partial | full | full |
| Secondary indexes (redb) | - | optional | Y | Y |
| Multi-node scaling | - | - | - | Y |
| Gossip discovery | - | - | - | Y |
| Raft coordination | - | - | - | Y |
| Cross-node lookups | - | - | - | Y |

### Startup Validation

Each profile validates its requirements at construction time:

| Profile | Validation |
|---------|-----------|
| `bare-metal` | None |
| `embedded` | WAL directory writable |
| `durable` | object_store URL parseable, credentials available |
| `constellation` | durable validation + bind address available, seed nodes reachable |

Validation failures produce clear error messages indicating which profile requirement was not met.

## Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ProfileRequirementError` | Missing config for profile (e.g., no object_store URL for durable) | User fixes config |
| `FeatureNotCompiled` | Profile requires uncompiled feature (e.g., constellation without `constellation` feature) | Compile error or runtime error with clear message |
| `InvalidProfileName` | Unknown profile string in TOML | Error lists valid profiles |

## Test Plan

### Unit Tests

- [ ] `Profile::BareMetal` constructs without any configuration
- [ ] `Profile::Embedded` constructs with default WAL directory
- [ ] `Profile::Durable` fails without object_store URL
- [ ] `Profile::Durable` succeeds with valid local filesystem object_store
- [ ] `Profile::Constellation` fails without `constellation` feature
- [ ] Profile capabilities matrix matches documented expectations

### Integration Tests

- [ ] `BareMetal` → `CREATE SOURCE → INSERT → SELECT` works (no persistence)
- [ ] `Embedded` → crash and restart recovers from WAL
- [ ] `Durable` → crash and restart recovers from object store checkpoint
- [ ] TOML config parsing selects correct profile

## Completion Checklist

- [ ] `Profile` enum defined in `laminar-db/src/profile.rs`
- [ ] `LaminarDB::builder().profile()` API implemented
- [ ] Feature gate mapping (`durable`, `constellation`) implemented
- [ ] TOML config parsing for profile selection
- [ ] Startup validation for each profile
- [ ] Unit and integration tests passing
- [ ] Feature INDEX.md updated

---

## References

- [Storage Architecture Research](../../../research/storage-architecture.md)
- [F-STATE-001: Revised StateStore Trait](../state/F-STATE-001-state-store-trait.md)
- [F-DCKP-004: ObjectStoreCheckpointer](../checkpoint/F-DCKP-004-object-store-checkpointer.md)
- [F-IDX-001: redb Secondary Indexes](../indexes/F-IDX-001-redb-secondary-indexes.md)
- [F-SERVER-001: TOML Configuration](../server/F-SERVER-001-toml-config.md)
