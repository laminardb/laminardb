# F-S3-003: Storage Class Tiering

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-S3-003 |
| **Status** | Done |
| **Priority** | P2 |
| **Phase** | 7b |
| **Effort** | M (2-3 days) |
| **Dependencies** | F-CFG-001, F-S3-001 |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-storage` |
| **Module** | `crates/laminar-storage/src/tiering.rs` |

## Summary

Implement S3 storage class tiering for checkpoint data. Active checkpoints use S3 Express One Zone (single-digit ms latency, -85% GET cost). Older checkpoints move to S3 Standard. Archive checkpoints move to Glacier Instant Retrieval. Uses S3 Lifecycle rules via object tagging.

## Goals

- `StorageClass` enum: `Standard`, `ExpressOneZone`, `IntelligentTiering`, `GlacierInstantRetrieval`
- Tag objects with `laminardb-tier: hot|warm|cold` for lifecycle rule targeting
- Apply configured storage class on PUT via `object_store` `Attribute::StorageClass`
- Zstd compression for warm/cold tier objects (better ratio than LZ4)

## Non-Goals

- Creating S3 Lifecycle rules (user manages via AWS console/terraform)
- Cross-AZ replication for Express One Zone (documented recommendation only)

## Technical Design

### Tiering Strategy

| Tier | Storage Class | Use Case | Compression |
|------|--------------|----------|-------------|
| Hot | EXPRESS_ONE_ZONE | Active checkpoints, recent window results | LZ4 |
| Warm | STANDARD | Historical checkpoints (1-7 days) | Zstd level 3 |
| Cold | GLACIER_IR | Archive, compliance, audit | Zstd level 19 |

### Cost Impact

S3 Express One Zone (2025 pricing): PUTs -55%, GETs -85% vs Standard. Makes checkpoint I/O dramatically cheaper for active state.

### API

```rust
pub enum StorageClass { Standard, ExpressOneZone, IntelligentTiering, GlacierInstantRetrieval }
pub enum StorageTier { Hot, Warm, Cold }

pub struct TieringPolicy { /* hot/warm/cold class */ }
impl TieringPolicy {
    pub fn new(hot_class: &str, warm_class: &str, cold_class: &str) -> Self;
    pub fn standard() -> Self;
    pub fn storage_class(&self, tier: StorageTier) -> StorageClass;
    pub fn put_options(&self, tier: StorageTier) -> PutOptions;
    pub fn put_options_create(&self, tier: StorageTier) -> PutOptions;
}

pub fn compress_for_tier(data: &[u8], tier: StorageTier) -> Vec<u8>;
pub fn decompress_for_tier(compressed: &[u8], tier: StorageTier) -> Result<Vec<u8>, DecompressionError>;
```

### Files Modified

| File | Change |
|------|--------|
| `Cargo.toml` | Add `zstd = "0.13"` to workspace deps |
| `crates/laminar-storage/Cargo.toml` | Add `zstd` dependency |
| `crates/laminar-storage/src/tiering.rs` | New: `StorageClass`, `StorageTier`, `TieringPolicy`, compression |
| `crates/laminar-storage/src/lib.rs` | Export new module and types |

## Test Plan

- Storage class enum round-trips config strings
- Objects tagged with correct tier via `TagSet`
- `PutOptions` includes `Attribute::StorageClass`
- LZ4 roundtrip for hot tier
- Zstd roundtrip for warm and cold tiers
- Cold compresses better than warm (level 19 vs 3)
- Corrupt data returns decompression error

## Completion Checklist

- [x] `StorageClass` enum with `as_s3_str()`, `from_config()`, `Display`
- [x] `StorageTier` enum with `tag_value()`, `Display`
- [x] `TieringPolicy` with `new()`, `standard()`, `storage_class()`, `put_options()`, `put_options_create()`
- [x] `PutOptions` sets `Attribute::StorageClass` and `TagSet` with `laminardb-tier`
- [x] `compress_for_tier()`: LZ4 for hot, Zstd level 3 for warm, Zstd level 19 for cold
- [x] `decompress_for_tier()`: auto-selects decompression by tier
- [x] `DecompressionError` type for corrupt data
- [x] Config wired: `TieringConfig` in laminar-db, `TieringSection` in laminar-server (from F-CFG-001)
- [x] 16 unit tests: class strings, config parsing, policy, tags, storage class attr, create mode, compression roundtrips, cold vs warm ratio, corrupt data
- [x] Feature INDEX.md updated
