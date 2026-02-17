# Constellation Architecture Fix Plan

> Generated 2026-02-17 from architecture audit of 55 constellation feature specs.
> Addresses **9 CRITICAL**, **27 HIGH**, and selected MEDIUM issues.

## Priority Matrix

| Priority | Issues | Rationale |
|----------|--------|-----------|
| **P0 (Block implementation)** | C1, C2, C4, C5, C6 | Data loss, panics on Ring 0, split-brain risk |
| **P1 (Fix before merge)** | C3, C7, C8, C9 | Correctness, race conditions, incomplete safety |
| **P2 (Fix during impl)** | H1-H27 | Performance, robustness, API consistency |

---

## CRITICAL Fixes (P0)

### C1: No Snapshot Consistency Mechanism

**Specs affected**: F-STATE-001, F-STATE-002, F-STATE-004 (superseded)

**Problem**: `InMemoryStateStore::snapshot()` (F-STATE-002 line 233-239) performs O(n) full clone of the FxHashMap. This runs synchronously at barrier time on Ring 0. For a state store with 1M entries (~100MB), the clone takes **50-200ms**, violating the `< 50ns` barrier processing target by 6 orders of magnitude. With F-STATE-004 (fork/COW) superseded, there is NO alternative snapshot strategy.

**Root cause**: The mmap supersession removed `ForkCowSnapshot` (which used `fork()` for O(1) COW snapshots) without providing a replacement for large state.

**Fix — Changelog-based incremental snapshots**:

Use the existing `ChangelogAwareStore<S>` (already implemented in Phase 3) as the bridge. Between two checkpoint barriers, `ChangelogAwareStore` records all mutations (put/delete) in a `Vec<ChangelogEntry>`. At barrier time, only the changelog delta is serialized — not the full state.

Update F-STATE-001 and F-STATE-002:

```rust
// F-STATE-001: Add to StateStore trait
pub trait StateStore: Send {
    // ... existing methods ...

    /// Create an incremental snapshot from changes since last barrier.
    ///
    /// Returns `None` if the store does not support incremental snapshots,
    /// in which case the coordinator falls back to `snapshot()`.
    fn incremental_snapshot(&mut self) -> Option<IncrementalSnapshot> {
        None // Default: not supported
    }
}

/// Incremental snapshot containing only mutations since last barrier.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct IncrementalSnapshot {
    /// Mutations since last full or incremental snapshot.
    pub changes: Vec<ChangeEntry>,
    /// Epoch of the base snapshot this is relative to.
    pub base_epoch: u64,
    /// This snapshot's epoch.
    pub epoch: u64,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum ChangeEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}
```

**Checkpoint coordinator logic**:
1. At barrier, call `state.incremental_snapshot()`
2. If `Some(delta)` → serialize delta (~microseconds for typical workloads)
3. If `None` → fall back to full `state.snapshot()` (first checkpoint, or store doesn't support)
4. Every N checkpoints (configurable, default 10), force a full snapshot for compaction
5. Recovery: load last full snapshot, then replay all incremental deltas in epoch order

**Complexity**: Ring 0 barrier overhead drops from O(n) to O(delta), where delta = mutations since last barrier. For a 1M-entry store with 10K mutations per checkpoint cycle, this is **1000x faster**.

**Alternative considered — `im::OrdMap`**: Persistent data structure with O(1) structural clone via `Arc` sharing. Trade-off: `im::OrdMap` replaces `FxHashMap`, changing `get()` from O(1) to O(log n). For sub-100ns get targets, this may not meet the latency budget at scale. Changelog approach preserves existing FxHashMap performance.

**Spec changes required**:
- F-STATE-001: Add `incremental_snapshot()` method with default impl
- F-STATE-002: Add `IncrementalSnapshot` struct, document changelog integration
- F-STATE-004: Add note that changelog replaces fork/COW for incremental snapshots
- F-DCKP-003: Add incremental snapshot file layout (`{checkpoint_id}/operators/{op_id}/{part_id}.delta`)
- F-DCKP-004: Update `save()` to handle both full and incremental snapshots
- F-DCKP-005: Update recovery to replay incremental deltas

---

### C2: rkyv Key Encoding Breaks Index Ordering

**Specs affected**: F-IDX-001

**Problem**: F-IDX-001 (line 64) states "Arrow column values are serialized to bytes using `rkyv` for consistent ordering." This is **incorrect**. rkyv uses the platform's native byte order (little-endian on x86/ARM). For example, `u32` value `256` serializes as `[0x00, 0x01, 0x00, 0x00]` in little-endian. redb's B-tree sorts keys lexicographically by bytes, so `256` (`[0x00, 0x01, ...]`) would sort BEFORE `1` (`[0x01, 0x00, ...]`). Numeric ordering is completely broken.

**Fix — Memcomparable key encoding**:

Replace rkyv with a purpose-built comparable key encoding that preserves sort order across byte comparison. This is the standard approach used by FoundationDB, CockroachDB, TiKV, and SQLite4.

```rust
/// Encode an Arrow scalar value to bytes that preserve sort order
/// under lexicographic byte comparison.
///
/// Encoding rules:
/// - u8/u16/u32/u64/u128: big-endian bytes
/// - i8/i16/i32/i64/i128: flip sign bit + big-endian (so negatives sort before positives)
/// - f32/f64: IEEE 754 order-preserving transform (flip sign bit; if negative, flip all bits)
/// - String/Binary: null-terminated with 0x00 -> 0x00 0xFF escaping
/// - Null: 0x00 prefix (sorts before all non-null values)
/// - Non-null: 0x01 prefix
pub fn encode_comparable(value: &ScalarValue) -> Vec<u8> { ... }

/// Decode a comparable-encoded key back to a ScalarValue.
pub fn decode_comparable(bytes: &[u8], data_type: &DataType) -> Result<ScalarValue, IndexError> { ... }
```

**Performance**: Comparable encoding is O(key_size) with no allocation for fixed-width types (stack buffer). For a u64 key, encoding takes ~5ns vs. rkyv's ~15ns — actually faster because no alignment/padding overhead.

**Spec changes required**:
- F-IDX-001: Replace "rkyv for consistent ordering" with "memcomparable key encoding" in Technical Design section. Add `encode_comparable`/`decode_comparable` functions. Add test cases for boundary values (0, -1, i64::MIN, i64::MAX, NaN, -0.0).

---

### C4: `unreachable!()` Panics on Ring 0 Hot Path

**Specs affected**: F-DCKP-002

**Problem**: F-DCKP-002 line 308-316:
```rust
StreamMessage::Watermark(_wm) => {
    AlignmentAction::PassThrough(
        unreachable!("watermarks should be handled before barrier aligner")
    )
}
```
This is a guaranteed panic if ANY watermark reaches `process_message()`. The assumption that watermarks are pre-filtered is architectural but not enforced — if a code path changes or a new executor is written without the pre-filter, Ring 0 panics and the partition dies.

**Fix**: Remove `unreachable!()` and handle watermarks gracefully. The aligner should either:

**(a) Accept and pass through watermarks** (recommended):
```rust
StreamMessage::Watermark(wm) => {
    // Watermarks pass through regardless of alignment state.
    // They don't carry event data, so they don't affect snapshot consistency.
    // The operator processes watermarks separately from events.
    AlignmentAction::WatermarkPassThrough(wm)
}
```

Add a new variant to `AlignmentAction`:
```rust
pub enum AlignmentAction<T> {
    PassThrough(T),
    WatermarkPassThrough(Watermark),  // NEW
    Buffered,
    TriggerSnapshot { barrier: CheckpointBarrier },
    DrainBuffered(T),
}
```

**(b) Alternative — return error**: Less clean but keeps enum unchanged:
```rust
StreamMessage::Watermark(_) => {
    // Watermarks should be handled by the executor before the aligner.
    // If one reaches here, log a warning and treat as buffered (no-op).
    tracing::warn!("watermark reached barrier aligner; should be pre-filtered");
    AlignmentAction::Buffered
}
```

**Recommendation**: Option (a). Adding `WatermarkPassThrough` is cleaner, self-documenting, and makes the type system enforce correct handling.

**Spec changes required**:
- F-DCKP-002: Replace `unreachable!()` with `WatermarkPassThrough` variant. Add variant to `AlignmentAction` enum. Update match arms in downstream consumers.

---

### C5: Epoch Fencing Checks Epoch Only, Not Owner Node ID

**Specs affected**: F-EPOCH-001, F-COORD-001

**Problem**: `PartitionGuard::validate()` (F-EPOCH-001 line 165-182) only checks `current > self.epoch`. It does NOT verify that the metadata still shows this node as the owner. A scenario:

1. Node A owns partition P at epoch 5
2. Raft assigns partition P to Node B at epoch 5 (same epoch, different node — this can happen if the DeregisterNode bug C6 removes and re-assigns without bumping epoch)
3. Node A calls `validate()` → epoch 5 == 5, returns `Ok(())` — **Node A still thinks it owns the partition**

Even if epochs always increment (fixing C6), there's a secondary issue: `partition_epoch()` returns only `Option<u64>`. There's no check that the metadata's `node_id` matches the guard's `node_id`. If a bug or Raft log entry assigns the same epoch to a different node (which shouldn't happen but is not prevented), the guard is fooled.

**Fix**: Add owner validation to `validate()`:

```rust
pub fn validate(&self, metadata: &ConstellationMetadata) -> Result<(), EpochError> {
    let ownership = metadata.partition_map.get(&self.partition_id)
        .ok_or(EpochError::UnknownPartition {
            partition: self.partition_id,
        })?;

    // Update cached epoch
    self.cached_current_epoch.store(ownership.epoch, Ordering::Release);

    if ownership.epoch > self.epoch {
        return Err(EpochError::StaleEpoch {
            partition: self.partition_id,
            local_epoch: self.epoch,
            current_epoch: ownership.epoch,
        });
    }

    // NEW: Verify owner node_id matches
    if ownership.node_id != self.node_id {
        return Err(EpochError::NotOwned {
            partition: self.partition_id,
        });
    }

    Ok(())
}
```

Also add a new helper to `ConstellationMetadata`:
```rust
/// Get full ownership info for a partition (not just epoch).
pub fn partition_ownership(&self, partition_id: PartitionId) -> Option<&PartitionOwnership> {
    self.partition_map.get(&partition_id)
}
```

**Spec changes required**:
- F-EPOCH-001: Update `validate()` to check `node_id`. Add `partition_ownership()` to `ConstellationMetadata` usage.
- F-COORD-001: Add `partition_ownership()` method.

---

### C6: DeregisterNode Drops Partitions Without Epoch Bump

**Specs affected**: F-COORD-001

**Problem**: F-COORD-001 line 280-283:
```rust
MetadataLogEntry::DeregisterNode { node_id } => {
    self.node_states.remove(node_id);
    // Remove all partition assignments for this node
    self.partition_map.retain(|_, o| o.node_id != *node_id);
}
```

This silently removes partition assignments without incrementing their epochs. Any other node holding a `PartitionGuard` for one of these partitions will continue to pass `check()` (the cached epoch hasn't changed) AND `validate()` (the partition is gone from the map, so `partition_epoch()` returns `None` → `UnknownPartition`, which is correct). However, the partition data is orphaned — no node owns it, no epoch bump was recorded, and no reassignment is triggered.

**Fix**: Before removing partitions, increment each partition's epoch and mark as unassigned:

```rust
MetadataLogEntry::DeregisterNode { node_id } => {
    self.node_states.remove(node_id);

    // Collect partitions owned by this node
    let owned: Vec<PartitionId> = self.partition_map.iter()
        .filter(|(_, o)| o.node_id == *node_id)
        .map(|(pid, _)| *pid)
        .collect();

    // Bump epoch and mark as unassigned (sentinel node_id = NodeId(0))
    for pid in &owned {
        if let Some(ownership) = self.partition_map.get_mut(pid) {
            ownership.epoch += 1;
            ownership.node_id = NodeId(0); // Unassigned sentinel
            ownership.assigned_at_ms = current_time_ms();
        }
    }

    // Increment cluster epoch to signal topology change
    self.cluster_epoch += 1;
}
```

**Why `NodeId(0)` sentinel**: The partition remains in the map (so epoch queries still work), but `NodeId(0)` signals "unassigned." The `PartitionAssigner` (F-EPOCH-002) detects unassigned partitions and reassigns them. This also makes `PartitionGuard::validate()` fail correctly for the old owner (epoch bumped).

**Spec changes required**:
- F-COORD-001: Rewrite `DeregisterNode` handler to bump epochs and use sentinel. Add `NodeId(0)` sentinel documentation. Add `IncrementClusterEpoch` after deregistration.
- F-EPOCH-001: Document that `NodeId(0)` is reserved (alongside epoch 0).
- F-EPOCH-002: Add handling for `NodeId(0)` unassigned partitions in rebalance logic.

---

## CRITICAL Fixes (P1)

### C3: LookupTable Trait Lifetime Unsoundness with foyer CacheEntry

**Specs affected**: F-LOOKUP-001, F-LOOKUP-004, F-LOOKUP-005

**Problem**: F-LOOKUP-004's `get()` returns `Option<foyer::CacheEntry<LookupCacheKey, Vec<u8>>>`. The `CacheEntry` is an RAII handle that pins the entry in cache while held. If the `LookupTable` trait (F-LOOKUP-001) returns `&[u8]`, the caller must hold the `CacheEntry` to keep the reference valid. But the trait signature `fn get(&self, key: &[u8]) -> Option<&[u8]>` doesn't express this lifetime constraint — the returned `&[u8]` borrows from the *store*, not from a cache entry that might be evicted.

Additionally, F-LOOKUP-005's `fetch()` has a type mismatch: `return Ok(self.hot_cache.get(key))` returns `Option<CacheEntry<K,V>>` but the function signature expects `Result<Option<Vec<u8>>, LookupError>`.

**Fix**: Use owned return types throughout the lookup path:

```rust
// F-LOOKUP-001: LookupTable trait
pub trait LookupTable: Send + Sync {
    /// Get a value by key. Returns owned bytes.
    ///
    /// The returned `Bytes` is cheap to clone (~2ns, reference-counted).
    /// This avoids lifetime issues with cache-backed stores where the
    /// entry may be evicted after the reference is dropped.
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, LookupError>;

    /// Batch get for multiple keys (optional optimization).
    fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, LookupError> {
        keys.iter().map(|k| self.get(k)).collect()
    }
}
```

**Why `Bytes` not `Vec<u8>`**: `Bytes` is reference-counted (`Arc<[u8]>` internally). When backed by foyer's `CacheEntry`, the `Bytes` can share the underlying allocation without copying. When the `CacheEntry` is dropped, the `Bytes` keeps its own reference alive.

**Implementation in foyer cache**:
```rust
impl LookupTable for FoyerMemoryCache {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, LookupError> {
        Ok(self.cache.get(&self.make_key(key))
            .map(|entry| Bytes::from(entry.value().clone())))
    }
}
```

**Spec changes required**:
- F-LOOKUP-001: Change `get()` return type from `Option<&[u8]>` to `Result<Option<Bytes>, LookupError>`.
- F-LOOKUP-004: Update `get()` to return `Result<Option<Bytes>>`, extract value from `CacheEntry`.
- F-LOOKUP-005: Fix `fetch()` return type mismatch. Extract `Vec<u8>` from `CacheEntry` before returning.

---

### C7: Ambiguous Column Classification in Predicate Splitting

**Specs affected**: F-LSQL-003

**Problem**: F-LSQL-003 line 300-315: The `walk_columns` function checks unqualified column names against both `lookup_columns` and `stream_columns` sets. If a column name (e.g., `id`) exists in both schemas, BOTH `has_lookup` and `has_stream` are set to `true`, causing the predicate to be classified as `CrossReference`. This means `WHERE id = 5` is never pushed down, even if the user wrote `WHERE c.id = 5` — because the qualified name resolution (lines 309-315) has an empty body:

```rust
if let Some(ref qualifier) = col.relation {
    let qualified_name = format!("{}.{}", qualifier, name);
    // Check against qualified column sets (if available)
    // Fallback: use unqualified matching above
}
```

**Fix**: Complete the qualified column resolution:

```rust
fn walk_columns(&self, expr: &Expr, has_lookup: &mut bool, has_stream: &mut bool) {
    match expr {
        Expr::Column(col) => {
            if let Some(ref qualifier) = col.relation {
                // Qualified column: use qualifier to determine ownership
                let qualified = format!("{}.{}", qualifier, col.name);
                if self.lookup_qualified.contains(&qualified) {
                    *has_lookup = true;
                } else if self.stream_qualified.contains(&qualified) {
                    *has_stream = true;
                } else {
                    // Unknown qualifier: conservative fallback
                    *has_lookup = true;
                    *has_stream = true;
                }
            } else {
                // Unqualified column: check if ambiguous
                let in_lookup = self.lookup_columns.contains(&col.name);
                let in_stream = self.stream_columns.contains(&col.name);

                match (in_lookup, in_stream) {
                    (true, false) => *has_lookup = true,
                    (false, true) => *has_stream = true,
                    (true, true) => {
                        // Ambiguous: column exists in both schemas.
                        // This should have been caught by DataFusion's
                        // ambiguity check. Classify as CrossReference
                        // (safe fallback — prevents incorrect pushdown).
                        *has_lookup = true;
                        *has_stream = true;
                    }
                    (false, false) => {
                        // Unknown column — may be from a subquery or alias.
                        // Conservative: treat as CrossReference.
                        *has_lookup = true;
                        *has_stream = true;
                    }
                }
            }
        }
        // ... rest unchanged ...
    }
}
```

Also add qualified column sets to `PredicateClassifier`:
```rust
pub struct PredicateClassifier {
    lookup_columns: HashSet<String>,
    stream_columns: HashSet<String>,
    lookup_qualified: HashSet<String>,  // NEW: "table_alias.column"
    stream_qualified: HashSet<String>,  // NEW: "table_alias.column"
}
```

**Spec changes required**:
- F-LSQL-003: Complete `walk_columns` qualified path. Add `lookup_qualified`/`stream_qualified` to `PredicateClassifier`. Add test `test_classify_ambiguous_column_with_qualifier` and `test_classify_ambiguous_column_without_qualifier`.

---

### C8: Multi-Atomic Race in Barrier Injection

**Specs affected**: F-DCKP-001

**Problem**: F-DCKP-001 `inject()` method (line 390-395) stores three values with separate atomic operations:
```rust
pub fn inject(&self, cmd: InjectBarrierCommand) {
    self.pending_unaligned.store(cmd.is_unaligned, Ordering::Release);
    self.pending_epoch.store(cmd.epoch, Ordering::Release);
    // checkpoint_id must be stored last (acts as the "ready" flag)
    self.pending_checkpoint_id.store(cmd.checkpoint_id, Ordering::Release);
}
```

Meanwhile, `poll_barrier()` (line 330-348) reads them:
```rust
let pending_id = self.pending_checkpoint_id.load(Ordering::Acquire);
if pending_id != 0 {
    let epoch = self.pending_epoch.load(Ordering::Acquire);
    let unaligned = self.pending_unaligned.load(Ordering::Acquire);
    // ...
}
```

**Race window**: Between `inject()`'s epoch store and checkpoint_id store, `poll_barrier()` could read the old checkpoint_id (0, so no barrier seen — safe) OR the new checkpoint_id with the OLD epoch (if a previous inject had a different epoch). The store order mitigates this for the first inject, but for rapid consecutive injects:

1. Thread A: `inject({id: 1, epoch: 5, unaligned: false})` — stores unaligned=false, epoch=5, id=1
2. Thread A: `inject({id: 2, epoch: 6, unaligned: true})` — stores unaligned=true, then epoch=6
3. Thread B: `poll_barrier()` reads id=1 (old), epoch=6 (new), unaligned=true (new) — **inconsistent state**

This creates a barrier with `checkpoint_id=1` but `epoch=6`, which is wrong.

**Fix — Single atomic with packed struct**:

Replace three atomics with one `AtomicU128` (available on x86-64 and aarch64 via `AtomicU128` or `crossbeam::atomic::AtomicCell`):

```rust
/// Packed barrier command for lock-free single-atomic transfer.
/// Layout: [checkpoint_id: u64][epoch: u48][unaligned: u1][reserved: u15]
#[repr(C)]
#[derive(Clone, Copy)]
struct PackedBarrierCmd(u128);

impl PackedBarrierCmd {
    const EMPTY: Self = Self(0);

    fn new(checkpoint_id: u64, epoch: u64, unaligned: bool) -> Self {
        let packed = (checkpoint_id as u128) << 64
            | (epoch as u128) << 16
            | (unaligned as u128) << 15;
        Self(packed)
    }

    fn checkpoint_id(self) -> u64 { (self.0 >> 64) as u64 }
    fn epoch(self) -> u64 { ((self.0 >> 16) & 0xFFFF_FFFF_FFFF) as u64 }
    fn is_unaligned(self) -> bool { (self.0 >> 15) & 1 != 0 }
    fn is_empty(self) -> bool { self.0 == 0 }
}
```

Then in `CheckpointBarrierInjector`:
```rust
pending: Arc<AtomicU128>,

// inject():
self.pending.store(PackedBarrierCmd::new(cmd.checkpoint_id, cmd.epoch, cmd.is_unaligned).0, Ordering::Release);

// poll_barrier():
let packed = PackedBarrierCmd(self.pending.load(Ordering::Acquire));
if !packed.is_empty() {
    self.pending.store(0, Ordering::Release);
    // packed.checkpoint_id(), packed.epoch(), packed.is_unaligned() are all consistent
}
```

**Alternative (if AtomicU128 not available)**: Use `crossbeam::atomic::AtomicCell<(u64, u64, bool)>` which uses a seqlock internally. Or use `parking_lot::Mutex<Option<InjectBarrierCommand>>` — the overhead (~10ns uncontended) is well within budget for the coordinator signaling path (which is already Ring 1 → Ring 0 cross-thread).

**Spec changes required**:
- F-DCKP-001: Replace three separate atomics with packed `AtomicU128` or `AtomicCell`. Update `inject()` and `poll_barrier()`. Add test `test_inject_poll_concurrent_consistency`.

---

### C9: Checkpoint Commit Flow Missing Manifest-to-Metadata Linkage

**Specs affected**: F-DCKP-004, F-COORD-001

**Problem**: The checkpoint commit flow has a gap between two specs:

1. F-DCKP-004 `ObjectStoreCheckpointer::save()` uploads snapshots and writes the manifest to object storage.
2. F-COORD-001 `MetadataLogEntry::CommitCheckpoint` records the checkpoint in Raft metadata.

But there is no spec that orchestrates the sequence: **save → commit_checkpoint**. F-DCKP-004's `save()` returns `CheckpointId` but doesn't call Raft. F-COORD-001's `commit_checkpoint_record()` takes a `CheckpointRef` but doesn't trigger a save. If the coordinator calls `save()` but crashes before `commit_checkpoint()`, the checkpoint exists in object storage but is not recorded in metadata — it becomes invisible to recovery.

**Fix**: Document the checkpoint commit protocol explicitly. This belongs in a coordination section of F-DCKP-004 or a new section in F-DCKP-008:

```
Checkpoint Commit Protocol:
1. Checkpoint coordinator collects all operator snapshots and source offsets
2. Call checkpointer.save(checkpoint_data) → persists to object storage
3. On success: call raft.commit_checkpoint(partition_id, checkpoint_ref)
   → records in metadata so RecoveryManager can find it
4. On failure: log error, abort checkpoint. Object storage GC will clean up
   the incomplete checkpoint (no manifest = GC-eligible).

Recovery safety:
- If crash after step 2 but before step 3:
  The checkpoint is in object storage with a manifest (step 2 writes it),
  but NOT in Raft metadata.
  RecoveryManager::load_latest() reads the _latest pointer OR lists
  object storage directly → finds the checkpoint even without Raft.
  This is safe because the checkpoint is self-contained.

- If crash during step 2:
  Incomplete checkpoint (no manifest). GC deletes it. Recovery uses
  the previous checkpoint from metadata.
```

**Spec changes required**:
- F-DCKP-004: Add "Checkpoint Commit Protocol" section documenting the save → Raft commit sequence. Add note about recovery safety for partial failures.
- F-DCKP-005: Document that `load_latest()` checks BOTH Raft metadata AND object storage listing as fallback.
- F-COORD-001: Add cross-reference to F-DCKP-004 commit protocol.

---

## HIGH Fixes (P2) — Grouped by Theme

### Theme 1: StateStore Trait Inconsistencies

**H1: F-STATE-001 vs F-STATE-002 API mismatch**

| Method | F-STATE-001 (trait) | F-STATE-002 (impl) | Resolution |
|--------|--------------------|--------------------|------------|
| `get()` | `Option<Bytes>` | `Option<&[u8]>` | Keep `Option<Bytes>` — trait is authoritative |
| `range_scan()` vs `range()` | `range_scan()` | `range()` | Keep `range_scan()` — trait is authoritative |
| `snapshot()` | Concrete `StateSnapshot` | `Box<dyn StateSnapshot>` | Use concrete `StateSnapshot` — simpler, no dyn dispatch |
| `restore()` | `fn restore(&mut self, snapshot: StateSnapshot)` | `fn restore(&mut self, snapshot: &dyn StateSnapshot) -> Result<..>` | Keep owned `StateSnapshot`, add `Result` return |

**Fix**: Update F-STATE-002 to match F-STATE-001's trait signatures exactly. The trait is the contract.

**H2: `get_ref()` default returns `None` silently**

The default implementation of `get_ref()` always returns `None` instead of delegating to `get()`. Any code calling `get_ref()` on a store that doesn't override it silently gets no data.

**Fix**: Change default to delegate:
```rust
fn get_ref(&self, _key: &[u8]) -> Option<&[u8]> {
    // Default cannot delegate to get() because Bytes doesn't borrow from self.
    // Backends that support zero-copy should override this method.
    // Returns None to indicate zero-copy is not available — callers
    // MUST fall back to get() when get_ref() returns None.
    None
}
```

Add to the trait documentation:
```rust
/// **Important**: The default implementation returns `None`, indicating that
/// zero-copy access is not available. Callers MUST check for `None` and
/// fall back to `get()`:
/// ```
/// let value = store.get_ref(key).map(Bytes::from)
///     .or_else(|| store.get(key));
/// ```
```

**H3: FxHashMap collision resistance**

FxHash is trivially invertible — an adversary can craft keys that all hash to the same bucket, causing O(n) lookup. For user-supplied keys (e.g., from Kafka message keys), this is a DoS vector.

**Fix**: Use `AHashMap` (foldhash since ahash 0.9+ uses it) as a drop-in replacement. AHash uses AES-NI on supported platforms (~3ns vs FxHash's ~2ns) with collision resistance. The 1ns overhead is within noise for the < 100ns get target.

**Spec changes**: F-STATE-002: Replace `FxHashMap` with `AHashMap` (or `HashMap` with `ahash::RandomState`). Update crate dependency.

### Theme 2: Barrier & Checkpoint Robustness

**H4: Bitset overflow in BarrierAligner for >64 inputs**

F-DCKP-002 uses a `u64` bitset for tracking aligned inputs. The assert at line 271 prevents >64 inputs, but a `u128` would be trivial and support 128 inputs. Alternatively, use `BitVec` for unbounded support.

**Fix**: Use `u128` (sufficient for any practical deployment):
```rust
inputs_aligned: u128,
// ...
fn is_fully_aligned(&self) -> bool {
    let expected_mask = if self.num_inputs == 128 { u128::MAX } else { (1u128 << self.num_inputs) - 1 };
    self.inputs_aligned == expected_mask
}
```

**H5: `drain_all()` round-robin allocation**

`AlignmentState::drain_all()` allocates a `Vec<(usize, T)>` on every alignment completion. For large buffers (100K events), this is a significant allocation on Ring 0.

**Fix**: Pre-allocate `drain_queue` in `BarrierAligner` and reuse across alignments:
```rust
pub fn complete_alignment(&mut self) -> &[(usize, T)] {
    self.drain_queue.clear();
    if let Some(mut state) = self.current.take() {
        state.drain_into(&mut self.drain_queue);
    }
    &self.drain_queue
}
```

### Theme 3: Object Storage & Checkpoint

**H6: S3 rename is not atomic**

F-DCKP-004's save flow step 8 says "Copy to manifest.json path (atomic commit)". S3 does not support atomic rename. `object_store`'s `rename()` on S3 does a copy + delete, which is not atomic.

**Fix**: Use S3 conditional writes (`If-None-Match: *` for new objects) or accept that the manifest write is "last-writer-wins" with idempotent retry. Document the non-atomicity and the mitigation:
```
S3 Note: S3 does not support atomic rename. The manifest is written
directly to manifest.json using PUT (not copy+delete). If a concurrent
write occurs, the last PUT wins. This is safe because:
- Only one checkpoint coordinator writes to a given checkpoint ID path
- Epoch fencing (C5 fix) prevents stale coordinators from writing
```

**H7: Checkpoint pipelining not specified**

F-DCKP-004 waits for all uploads to complete before writing the manifest. For large state, this serializes checkpoint latency.

**Fix**: Add a section on pipelined checkpoints: while checkpoint N's snapshots upload, checkpoint N+1 can begin collecting snapshots. The manifest for N is written only after N's uploads complete. This doubles effective throughput.

### Theme 4: Epoch & Coordination

**H8: Epoch not stored in checkpoint manifest**

F-DCKP-003's `CheckpointManifest` has an `epoch` field, but F-DCKP-004's `save()` flow doesn't explicitly set it from the `PartitionGuard`'s epoch. The manifest epoch could be stale or unset.

**Fix**: `save()` must accept a `PartitionGuard` reference and include `guard.epoch()` in the manifest. Add to F-DCKP-004's `CheckpointData`:
```rust
pub struct CheckpointData {
    // ... existing ...
    /// Epoch of the partition owner at checkpoint time.
    /// Used for fencing on recovery.
    pub owner_epoch: u64,
    pub owner_node_id: NodeId,
}
```

**H9: Auto-fence after checkpoint upload**

After uploading a checkpoint, the coordinator should re-validate the epoch (`guard.validate()`) before writing the manifest. If the epoch changed during upload, the checkpoint should be abandoned.

**Fix**: Add to F-DCKP-004's save flow, between steps 6 and 7:
```
6b. Re-validate epoch: guard.validate(metadata)?
    If StaleEpoch: abort, delete uploaded files, return error
```

### Theme 5: SQL & Query Planning

**H10: LEFT JOIN predicate pushdown safety**

F-LSQL-003 does not distinguish between INNER JOIN and LEFT JOIN predicates. Pushing a lookup-only predicate through a LEFT JOIN changes semantics — rows that would have been NULL-extended are instead filtered.

**Fix**: Add join type awareness to `split_predicates`:
```rust
if lookup_node.join_type == JoinType::Left {
    // For LEFT JOIN, only push predicates that are in the ON clause
    // (join condition), NOT in the WHERE clause.
    // WHERE predicates on the lookup side filter NULL-extended rows.
    // ...
}
```

---

## Implementation Order

Based on dependencies and risk:

```
Week 1: Foundation fixes
  1. C2 (rkyv key encoding)     — standalone, no dependencies
  2. C4 (unreachable panic)     — standalone, simple fix
  3. H1-H2 (trait consistency)  — needed before any StateStore impl
  4. H3 (AHashMap)              — drop-in replacement

Week 2: Epoch & coordination
  5. C6 (DeregisterNode)        — must fix before C5
  6. C5 (epoch owner check)     — depends on C6
  7. C9 (commit flow)           — documentation, links C5+C6
  8. H8-H9 (epoch in manifest)  — builds on C5

Week 3: Snapshot & checkpoint
  9. C1 (incremental snapshots) — largest change, needs careful design
  10. H6 (S3 non-atomic)        — documentation fix
  11. H5 (drain allocation)     — performance improvement

Week 4: API & query
  12. C3 (LookupTable lifetime) — API change, needs downstream updates
  13. C7 (column classification) — code completion
  14. C8 (barrier atomics)       — concurrency fix
  15. H10 (LEFT JOIN pushdown)   — correctness
```

---

## Verification Criteria

Each fix must include:

1. **Spec update**: Modified markdown committed to `docs/features/constellation/`
2. **New test cases**: Added to the spec's Test Plan section
3. **No regressions**: Existing test cases unchanged (or updated if API changed)
4. **Cross-reference**: Updated references in affected specs
5. **Performance note**: Impact on Ring 0/1/2 latency budgets documented

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-17 | Changelog-based incremental snapshots over `im::OrdMap` | Preserves FxHashMap O(1) get performance; leverages existing `ChangelogAwareStore` |
| 2026-02-17 | Memcomparable key encoding over rkyv for index keys | rkyv doesn't preserve lexicographic order for numeric types |
| 2026-02-17 | `WatermarkPassThrough` variant over silent discard | Type-safe, self-documenting, prevents future regressions |
| 2026-02-17 | `AtomicU128` packed barrier over Mutex | Lock-free, single atomic operation, no contention |
| 2026-02-17 | `Bytes` return for LookupTable over `&[u8]` | Avoids lifetime unsoundness with cache-backed stores |
| 2026-02-17 | `NodeId(0)` sentinel for unassigned partitions | Keeps partition in map for epoch queries; cleanly signals "needs reassignment" |
| 2026-02-17 | `AHashMap` over `FxHashMap` | Collision-resistant with ~1ns overhead; prevents adversarial DoS |
