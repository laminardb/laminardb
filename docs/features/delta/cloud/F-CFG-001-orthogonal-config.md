# F-CFG-001: Orthogonal Config Model

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CFG-001 |
| **Status** | Done |
| **Priority** | P0 |
| **Phase** | 7a |
| **Effort** | M (2-3 days) |
| **Dependencies** | F-GATE-001 |
| **Blocks** | F-S3-001, F-S3-003 |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-db`, `laminar-server` |

## Summary

Replace the `Profile` enum hierarchy (`BareMetal < Embedded < Durable < Delta`) with orthogonal configuration. The operating mode (embedded vs distributed) and checkpoint backend (none, local, S3) become independent choices auto-detected from config, eliminating the need for explicit `.profile()` calls.

## Goals

- Auto-detect effective profile from config: no checkpoint URL = in-memory; `file://` = local; `s3://` = durable; discovery section = delta
- Add `[checkpoint.tiering]` config section for S3 storage class tiering
- Maintain backward compatibility with existing builder API (`.profile()` still works)
- Maintain backward compatibility with existing TOML configs

## Non-Goals

- Removing the `Profile` enum entirely (keep for backward compat and debug logging)
- Implementing tiering logic (deferred to F-S3-003)

## Technical Design

### Config Changes

```toml
[checkpoint.tiering]
hot_class = "EXPRESS_ONE_ZONE"   # Active checkpoints
warm_class = "STANDARD"          # Older checkpoints
cold_class = "GLACIER_IR"        # Archive (optional)
hot_retention = "24h"            # Time before hot -> warm
warm_retention = "7d"            # Time before warm -> cold
```

### Files to Modify

| File | Change |
|------|--------|
| `crates/laminar-db/src/profile.rs` | Add `Profile::from_config()` auto-detection method |
| `crates/laminar-db/src/config.rs` | Add `TieringConfig` struct, add to `LaminarConfig` |
| `crates/laminar-db/src/builder.rs` | Auto-detect profile in `build()` when not explicitly set |
| `crates/laminar-server/src/config.rs` | Add `[checkpoint.tiering]` section with serde defaults |

### Auto-Detection Logic

```rust
impl Profile {
    pub fn from_config(config: &LaminarConfig, has_discovery: bool) -> Self {
        if has_discovery {
            Profile::Delta
        } else if let Some(url) = &config.object_store_url {
            if url.starts_with("file://") {
                Profile::Embedded
            } else {
                Profile::Durable
            }
        } else if config.storage_dir.is_some() {
            Profile::Embedded
        } else {
            Profile::BareMetal
        }
    }
}
```

## Test Plan

- Existing TOML configs parse and produce the same effective profile
- Builder without `.profile()` auto-detects correctly for each scenario
- `[checkpoint.tiering]` section parses with defaults when absent
- Explicit `.profile()` overrides auto-detection

## Completion Checklist

- [x] `Profile::from_config()` implemented
- [x] `TieringConfig` struct added
- [x] Server config updated with tiering section
- [x] Builder auto-detection wired
- [x] Backward compatibility tests pass (18 profile, 6 builder, 74 server)
- [x] Feature INDEX.md updated
