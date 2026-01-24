# F003: State Store Interface

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F003 |
| **Status** | ✅ Done |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F001 |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-22 |

## Summary

Define the core `StateStore` trait that all state backend implementations must satisfy. This abstraction allows operators to work with any state backend while enabling different implementations for different use cases.

## Technical Design

### API/Interface

```rust
/// Core trait for key-value state storage (dyn-compatible)
pub trait StateStore: Send {
    /// Get a value by key (target: < 500ns)
    fn get(&self, key: &[u8]) -> Option<Bytes>;

    /// Store a key-value pair
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError>;

    /// Delete a key
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError>;

    /// Scan all keys with a given prefix
    fn prefix_scan<'a>(&'a self, prefix: &'a [u8]) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Range scan between two keys
    fn range_scan<'a>(&'a self, range: Range<&'a [u8]>) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Check if a key exists
    fn contains(&self, key: &[u8]) -> bool;

    /// Create a snapshot for checkpointing
    fn snapshot(&self) -> StateSnapshot;

    /// Restore from a snapshot
    fn restore(&mut self, snapshot: StateSnapshot);

    /// Get approximate size in bytes
    fn size_bytes(&self) -> usize;

    /// Get number of entries
    fn len(&self) -> usize;

    /// Clear all entries
    fn clear(&mut self);
}

/// Extension trait for typed access (not dyn-compatible)
pub trait StateStoreExt: StateStore {
    fn get_typed<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, StateError>;
    fn put_typed<T: Serialize>(&mut self, key: &[u8], value: &T) -> Result<(), StateError>;
    fn update<F>(&mut self, key: &[u8], f: F) -> Result<(), StateError>;
}
```

### Implementation

- `InMemoryStore`: FxHashMap-based implementation with O(1) lookups
- `StateSnapshot`: Serializable checkpoint format with timestamp and version

### Performance Results

| Metric | Target | Achieved |
|--------|--------|----------|
| Get (100 entries) | < 500ns | ~17ns |
| Get (1K entries) | < 500ns | ~15ns |
| Get (10K entries) | < 500ns | ~15ns |
| Get (100K entries) | < 500ns | ~17ns |

**29x better than target** - FxHashMap provides consistent sub-20ns lookups.

## Completion Checklist

- [x] Trait defined (`StateStore`)
- [x] Extension trait for typed access (`StateStoreExt`)
- [x] Documentation complete
- [x] Example implementation (`InMemoryStore`)
- [x] Snapshot/restore for checkpointing
- [x] Comprehensive tests (14 tests)
- [x] Benchmarks validating < 500ns target
- [x] Clippy clean
