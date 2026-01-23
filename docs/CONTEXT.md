# Session Context

> This file tracks session continuity. Update at the end of each session.

## Last Session

**Date**: 2026-01-23
**Duration**: ~1 hour

### What Was Accomplished
- ✅ Implemented F006 - Basic SQL Parser with streaming extensions
- ✅ Created comprehensive parser module structure:
  - `statements.rs` - Streaming statement types (CreateSource, CreateSink, etc.)
  - `parser_simple.rs` - Simple parser implementation for streaming SQL
  - `window_rewriter.rs` - Window function rewriting infrastructure
- ✅ Implemented support for:
  - CREATE SOURCE with watermark definitions
  - CREATE SINK with connector options
  - CREATE CONTINUOUS QUERY with EMIT clauses
  - Window functions (TUMBLE, HOP, SESSION) structures
  - EMIT clauses (AFTER WATERMARK, ON WINDOW CLOSE, PERIODICALLY)
- ✅ All 45 tests passing in laminar-sql
- ✅ Updated sqlparser 0.60 compatibility

### Where We Left Off
Successfully implemented F006 - Basic SQL Parser. The parser module now supports streaming SQL extensions including CREATE SOURCE/SINK, window functions, watermark definitions, and EMIT clauses. The implementation uses a simplified approach compatible with sqlparser 0.60 and provides the foundation for streaming SQL support.

### Immediate Next Steps
1. **F007 - Write-Ahead Log** (P1) - Durability layer for state persistence
2. **F008 - Basic Checkpointing** (P1) - Recovery mechanism for state stores
3. **F009 - Event Time Processing** (P1) - Time-based semantics

### Open Issues
- None currently - F001, F002, F003, F004, F005, F006 are complete

### Code Pointers
- **StateStoreExt with rkyv**: `crates/laminar-core/src/state/mod.rs:229-280`
- **StateSnapshot rkyv**: `crates/laminar-core/src/state/mod.rs:305-395`
- **WindowId rkyv**: `crates/laminar-core/src/operator/window.rs:45-85`
- **Accumulator derives**: `crates/laminar-core/src/operator/window.rs:268-545`
- **Streaming statements**: `crates/laminar-sql/src/parser/statements.rs`
- **Parser implementation**: `crates/laminar-sql/src/parser/parser_simple.rs`
- **Window rewriter**: `crates/laminar-sql/src/parser/window_rewriter.rs`

---

## Session Notes

**bincode → rkyv Migration:**
- bincode discontinued in December 2025 (maintainer harassment, intentionally broken builds)
- rkyv chosen for zero-copy deserialization (~1.2ns access vs microseconds)
- Uses `aligned` feature for Ring 0 hot path (AlignedVec for proper memory alignment)
- Breaking change: Types need `#[derive(Archive, Serialize, Deserialize)]` from rkyv

**rkyv Usage Patterns:**
```rust
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use rkyv::util::AlignedVec;

// Derive rkyv traits
#[derive(Archive, Serialize, Deserialize)]
struct MyType { ... }

// Serialize to aligned bytes
let bytes: AlignedVec = rkyv::to_bytes::<Error>(&value)?;

// Zero-copy access (hot path)
let archived = rkyv::access::<Archived<MyType>, Error>(&bytes)?;

// Full deserialization when needed
let owned: MyType = rkyv::deserialize::<MyType, Error>(archived)?;
```

**Trait Bounds for StateStoreExt:**
- `get_typed<T>` requires: `T: Archive`, `T::Archived: CheckBytes + Deserialize<T>`
- `put_typed<T>` requires: `T: Serialize<HighSerializer<...>>`

---

## Quick Reference

### Current Focus
- **Phase**: 1 - Core Engine
- **Completed**: F001 (Reactor), F002 (Memory-Mapped State Store), F003 (State Store Interface), F004 (Tumbling Windows), F005 (DataFusion Integration), F006 (Basic SQL Parser)
- **Next**: F007 (Write-Ahead Log), F008 (Basic Checkpointing)

### Key Files
```
crates/laminar-sql/src/datafusion/
├── mod.rs              # Module exports and integration functions
├── source.rs           # StreamSource trait
├── bridge.rs           # StreamBridge channel bridge
├── exec.rs             # StreamingScanExec (ExecutionPlan)
├── table_provider.rs   # StreamingTableProvider
└── channel_source.rs   # ChannelStreamSource implementation
```

### Useful Commands
```bash
# Build and test laminar-sql
cargo build -p laminar-sql
cargo test -p laminar-sql --lib

# Run clippy
cargo clippy -p laminar-sql -- -D warnings

# Build all
cargo build --release
cargo test --all
```

### Recent Decisions
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-22 | Migrate bincode → rkyv | Zero-copy deserialization (~1.2ns access), bincode discontinued |
| 2026-01-22 | Use aligned buffers (rkyv) | Ring 0 state store uses AlignedVec for optimal CPU access |
| 2026-01-22 | `take_sender()` pattern | Ensures channel closure for proper stream termination |
| 2026-01-22 | Unbounded stream boundedness | Correctly marks streaming sources as unbounded |
| 2026-01-22 | Aggregation rejection | Aggregations on unbounded streams fail (require windows/F006) |
| 2026-01-22 | Channel-based bridge | tokio mpsc provides efficient push-to-pull conversion |
| 2026-01-22 | Cache output schema in operator | Reduces emit time by 57% (1.8μs → 773ns) |
| 2026-01-22 | Defer SQL syntax to F005/F006 | Keep F004 focused on core windowing logic |
| 2026-01-22 | Separate Assigner from Operator | Enables reuse for sliding/hopping windows |

---

## History

### Previous Sessions

<details>
<summary>Session - 2026-01-22 (bincode → rkyv migration)</summary>

**Accomplished**:
- ✅ Migrated serialization from bincode to rkyv
- ✅ Updated StateSnapshot, StateStoreExt, WindowId, and all accumulators
- ✅ All 89 tests passing

**Notes**:
- bincode was discontinued in December 2025
- rkyv provides zero-copy deserialization (~1.2ns access vs microseconds)
- Uses aligned buffers for Ring 0 hot path operations
- Breaking change: Types must now derive `rkyv::Archive, Serialize, Deserialize`

</details>

<details>
<summary>Session - 2026-01-22 (F005 Implementation)</summary>

**Accomplished**:
- ✅ Implemented F005 - DataFusion Integration
- ✅ StreamSource trait, StreamBridge, StreamingScanExec, StreamingTableProvider
- ✅ 35 tests passing

**Notes**:
- Push-to-pull bridge using tokio mpsc channels
- Aggregations on unbounded streams correctly rejected (require windows)

</details>

<details>
<summary>Session - 2026-01-22 (F004 Implementation)</summary>

**Accomplished**:
- ✅ Implemented F004 - Tumbling Windows with full functionality
- ✅ Created comprehensive window operator infrastructure
- ✅ Built-in aggregators: Count, Sum, Min, Max, Avg
- ✅ Performance targets met/exceeded

**Notes**:
- Window assignment: ~4.4ns (target < 10ns)
- Accumulator add: < 1ns (target < 100ns)
- Window emit: ~773ns (target < 1μs)

</details>

<details>
<summary>Session - 2026-01-22 (F002 Implementation)</summary>

**Accomplished**:
- ✅ Implemented F002 - Memory-Mapped State Store with full functionality
- ✅ Created `MmapStateStore` with two storage modes (in-memory and persistent)
- ✅ Performance: ~39ns get (12x better than 500ns target)

**Notes**:
- Deferred index persistence to F007 (WAL)
- Two-tier architecture: FxHashMap index + data storage

</details>
