# LaminarDB In-Memory Streaming API - Research & Design Document

> **Purpose**: Research, design decisions, and implementation guidance for LaminarDB's in-memory source/sink streaming API. Use with Claude Code to generate feature specifications.
>
> **Status**: Phase 2 Complete → Ready for Implementation
>
> **Last Updated**: January 2026

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [Research Findings](#4-research-findings)
5. [Design Decisions](#5-design-decisions)
6. [Configuration Model](#6-configuration-model)
7. [API Specification](#7-api-specification)
8. [Implementation Tiers](#8-implementation-tiers)
9. [Cross-Language Strategy](#9-cross-language-strategy)
10. [Future Integration Points](#10-future-integration-points)
11. [Feature Specifications Required](#11-feature-specifications-required)

---

## 1. Executive Summary

### What We're Building

LaminarDB's in-memory streaming API provides **embedded Kafka Streams-like semantics** without external dependencies - the foundational data ingestion and output layer for the streaming SQL engine.

### Core Principles

1. **Zero overhead by default** - Checkpointing disabled unless enabled
2. **Zero configuration for channel types** - Automatically derived from topology
3. **Composable settings** - No artificial tiers, just independent options
4. **Future-proof** - Clean extension points for Kafka, CDC

### Value Proposition

```
Kafka Stack:                LaminarDB:
┌─────────────┐            ┌─────────────────────────┐
│ Kafka Cluster│            │  Single Embedded Library │
├─────────────┤     →      │                         │
│ Kafka Streams│            │  • Automatic topology   │
├─────────────┤            │  • Optional checkpoint  │
│ RocksDB     │            │  • <500ns latency       │
└─────────────┘            └─────────────────────────┘
```

---

## 2. Goals & Non-Goals

### Goals

| Goal | Target |
|------|--------|
| Hot path latency | <500ns p99 |
| Zero allocation on hot path | 0 mallocs |
| Zero config for topology | Auto-derived |
| Cross-language | Java, Python, C |

### Non-Goals (This Phase)

- Distributed processing
- External connectors (Kafka, CDC) - Phase 4+
- User-specified channel types

---

## 3. Architecture Overview

### Three-Ring Integration

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              USER APPLICATION                            │
│  ┌─────────────┐                                      ┌─────────────┐   │
│  │  Producers  │                                      │  Consumers  │   │
│  │  (push)     │                                      │  (subscribe)│   │
│  └──────┬──────┘                                      └──────▲──────┘   │
└─────────┼────────────────────────────────────────────────────┼──────────┘
          │                                                    │
          ▼                                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│  RING 0 - HOT PATH (Zero Allocation)                                    │
│  ┌─────────────┐    ┌─────────────────────┐    ┌─────────────┐         │
│  │   SOURCE    │    │   STREAMING SQL     │    │    SINK     │         │
│  │             │───▶│   ENGINE            │───▶│             │─────────┼─▶
│  │ Auto:       │    │                     │    │ Auto:       │         │
│  │ SPSC→MPSC   │    │ • Windows           │    │ SPSC or     │         │
│  │ on clone    │    │ • Joins             │    │ Broadcast   │         │
│  └─────────────┘    └──────────┬──────────┘    └─────────────┘         │
│                                │                                        │
└────────────────────────────────┼────────────────────────────────────────┘
                                 │ (if checkpoint enabled)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  RING 1 - BACKGROUND (Only if configured)                               │
│  • Checkpoint snapshots        • WAL writes                             │
└─────────────────────────────────────────────────────────────────────────┘
```

### Automatic Channel Derivation

**User never specifies channel type** - it's derived automatically:

```
┌─────────────────────────────────────────────────────────────────┐
│                     AUTOMATIC DERIVATION                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  PRODUCER SIDE (Source):                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Starts as SPSC (fastest)                               │   │
│  │ • Auto-upgrades to MPSC when source.clone() called       │   │
│  │ • Transparent to user                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  CONSUMER SIDE (Sink):                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Query plan analyzed at CREATE MATERIALIZED VIEW        │   │
│  │ • 1 consumer → SPSC                                      │   │
│  │ • N consumers → Broadcast                                │   │
│  │ • Partitioned GROUP BY → Partitioned channels            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Research Findings

### 4.1 Ring Buffer (LMAX Disruptor)

**Key Learnings**:
- Pre-allocated ring eliminates allocation overhead
- Power-of-2 sizing: `seq & mask` instead of `seq % size`
- Cache-line padding prevents false sharing
- Sequence counters for lock-free coordination

```rust
#[repr(align(64))]
pub struct RingBuffer<T> {
    slots: Box<[MaybeUninit<T>]>,
    mask: u64,
    write_seq: CachePadded<AtomicU64>,
    read_seq: CachePadded<AtomicU64>,
}
```

### 4.2 SPSC to MPSC Upgrade Pattern

**Approach**: Start optimized, upgrade when needed

```rust
// Initial: SPSC (no contention)
let source = db.source::<Trade>("trades")?;
source.push(trade)?;  // Single-threaded fast path

// On clone: Auto-upgrade to MPSC
let src2 = source.clone();  // Triggers upgrade
// Now both can push concurrently with CAS coordination
```

### 4.3 Checkpointing

| checkpoint_interval | wal_mode | Behavior | Overhead |
|---------------------|----------|----------|----------|
| `None` | `None` | Pure in-memory | **0%** |
| `Some(10s)` | `None` | Snapshots only | <1% |
| `Some(10s)` | `Some(Async)` | + async WAL | 1-2% |
| `Some(10s)` | `Some(Sync)` | + sync WAL | 5-10% |

---

## 5. Design Decisions

### 5.1 No User-Specified Channel Types

**Removed from API**:
```rust
// ❌ User should NOT specify this
channel: Channel::Mpsc,
channel: Channel::Broadcast,
```

**Automatic instead**:

| Scenario | Derived Channel |
|----------|-----------------|
| Single producer | SPSC |
| `source.clone()` called | MPSC (auto-upgrade) |
| 1 MV reads source | SPSC sink |
| N MVs read source | Broadcast sink |
| Partitioned GROUP BY | Partitioned |

### 5.2 SPSC → MPSC Auto-Upgrade

```rust
pub struct Source<T> {
    inner: Arc<SourceInner<T>>,
}

impl<T: Record> Clone for Source<T> {
    fn clone(&self) -> Self {
        let prev_count = self.inner.producer_count.fetch_add(1, Ordering::SeqCst);
        
        if prev_count == 1 {
            // First clone: one-time upgrade SPSC → MPSC
            self.inner.upgrade_to_mpsc();
        }
        
        Self { inner: self.inner.clone() }
    }
}
```

**Upgrade is transparent**:
- Existing data preserved
- No API change
- Slight one-time cost on first clone

### 5.3 Composable Durability (No Tiers)

```rust
// Just set what you need - behavior is derived
checkpoint_interval: Option<Duration>,  // None = disabled
wal_mode: Option<WalMode>,              // None = disabled
```

### 5.4 Simplified Enums

```rust
pub enum Backpressure {
    Block,       // Default - wait for space
    DropOldest,  // Overwrite oldest unread
    Reject,      // Return error immediately
}

pub enum WaitStrategy {
    Spin,            // Busy spin (lowest latency, highest CPU)
    SpinYield(u32),  // Default: SpinYield(100)
    Park,            // OS wait (lowest CPU)
}

pub enum WalMode {
    Async,  // Background flush
    Sync,   // Flush before ack
}
```

---

## 6. Configuration Model

### 6.1 Database Configuration

```rust
pub struct Config {
    /// Data directory (required if checkpoint/WAL enabled)
    pub data_dir: Option<PathBuf>,
    
    /// Default buffer size (must be power of 2)
    pub buffer_size: usize,  // Default: 65536
    
    /// Checkpoint interval (None = disabled)
    pub checkpoint_interval: Option<Duration>,
    
    /// WAL mode (None = disabled, requires checkpoint)
    pub wal_mode: Option<WalMode>,
    
    /// Pin processing to CPU core
    pub core_affinity: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: None,
            buffer_size: 65536,
            checkpoint_interval: None,  // Disabled
            wal_mode: None,             // Disabled
            core_affinity: None,
        }
    }
}
```

### 6.2 Source Configuration

```rust
pub struct SourceConfig {
    /// Buffer size (0 = use database default)
    pub buffer_size: usize,
    
    /// Backpressure strategy
    pub backpressure: Backpressure,
    
    /// Wait strategy  
    pub wait_strategy: WaitStrategy,
    
    /// Override checkpoint interval
    pub checkpoint_interval: Option<Duration>,
    
    /// Override WAL mode
    pub wal_mode: Option<WalMode>,
    
    /// Watermark for event-time processing
    pub watermark: Option<Watermark>,
    
    // NOTE: No channel field - automatically derived
}

pub struct Watermark {
    pub column: String,
    pub delay: Duration,
}
```

### 6.3 Validation

```rust
impl Config {
    fn validate(&self) -> Result<()> {
        // data_dir required if durability enabled
        if (self.checkpoint_interval.is_some() || self.wal_mode.is_some())
            && self.data_dir.is_none() 
        {
            return Err("data_dir required for checkpoint/WAL");
        }
        
        // WAL requires checkpoint
        if self.wal_mode.is_some() && self.checkpoint_interval.is_none() {
            return Err("wal_mode requires checkpoint_interval");
        }
        
        // Buffer must be power of 2
        if !self.buffer_size.is_power_of_two() {
            return Err("buffer_size must be power of 2");
        }
        
        Ok(())
    }
}
```

---

## 7. API Specification

### 7.1 Rust API

```rust
// ============================================================
// DATABASE
// ============================================================

impl LaminarDB {
    /// Open with defaults (pure in-memory, max performance)
    pub fn open() -> Result<Self>;
    
    /// Open with config
    pub fn open_with_config(config: Config) -> Result<Self>;
    
    /// Execute SQL
    pub fn execute(&self, sql: &str) -> Result<()>;
    
    /// Get source handle
    pub fn source<T: Record>(&self, name: &str) -> Result<Source<T>>;
    
    /// Get sink handle
    pub fn sink<T: Record>(&self, name: &str) -> Result<Sink<T>>;
    
    /// Start/stop
    pub fn start(&self) -> Result<()>;
    pub fn stop(&self) -> Result<()>;
    
    /// Manual checkpoint (no-op if disabled)
    pub fn checkpoint(&self) -> Result<Option<u64>>;
}

// ============================================================
// SOURCE (Auto SPSC→MPSC on clone)
// ============================================================

impl<T: Record> Source<T> {
    /// Push single record
    pub fn push(&self, record: T) -> Result<()>;
    
    /// Push without blocking
    pub fn try_push(&self, record: T) -> Result<(), TryPushError>;
    
    /// Push batch
    pub fn push_batch(&self, records: &[T]) -> Result<()>;
    
    /// Push Arrow batch (zero-copy)
    pub fn push_arrow(&self, batch: RecordBatch) -> Result<()>;
    
    /// Emit watermark
    pub fn watermark(&self, timestamp: i64);
    
    /// Check capacity
    pub fn has_capacity(&self) -> bool;
}

// Clone auto-upgrades to MPSC
impl<T: Record> Clone for Source<T> {}

// ============================================================
// SINK & SUBSCRIPTION
// ============================================================

impl<T: Record> Sink<T> {
    pub fn subscribe(&self) -> Subscription<T>;
}

impl<T: Record> Subscription<T> {
    pub fn poll(&mut self) -> Option<RecordBatch>;
    pub fn recv(&mut self) -> Result<RecordBatch>;
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<RecordBatch>;
}

impl<T: Record> Iterator for Subscription<T> {
    type Item = RecordBatch;
}
```

### 7.2 SQL DDL

```sql
-- ============================================================
-- CREATE SOURCE (no channel specification needed)
-- ============================================================

CREATE SOURCE name (
    column TYPE [NOT NULL],
    ...
    [, WATERMARK FOR column AS column - INTERVAL 'delay']
) [WITH (
    buffer_size = N,
    backpressure = 'block' | 'drop_oldest' | 'reject',
    wait_strategy = 'spin' | 'spin_yield' | 'park',
    checkpoint_interval = 'duration',
    wal_mode = 'async' | 'sync'
)];

-- Examples:

-- Minimal (max performance, all defaults)
CREATE SOURCE trades (
    symbol VARCHAR,
    price DOUBLE,
    ts TIMESTAMP
);

-- With watermark
CREATE SOURCE trades (
    symbol VARCHAR,
    price DOUBLE,
    ts TIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '100ms'
);

-- With checkpointing
CREATE SOURCE trades (...) WITH (
    checkpoint_interval = '10 seconds'
);

-- ============================================================
-- CREATE SINK (channel auto-derived from topology)
-- ============================================================

CREATE SINK name FROM view;

-- Examples:
CREATE SINK ohlc_output FROM ohlc_1min;
```

### 7.3 Usage Examples

```rust
// === Single Producer (SPSC) ===
let db = LaminarDB::open()?;
let source = db.source::<Trade>("trades")?;
source.push(trade)?;  // Fast SPSC path


// === Multiple Producers (Auto MPSC) ===
let source = db.source::<Trade>("trades")?;

let src1 = source.clone();  // Auto-upgrades to MPSC
let src2 = source.clone();

thread::spawn(move || {
    for trade in exchange_a() { src1.push(trade)?; }
});
thread::spawn(move || {
    for trade in exchange_b() { src2.push(trade)?; }
});


// === Consumer ===
let sink = db.sink::<OHLCBar>("ohlc_1min")?;
for batch in sink.subscribe() {
    process(batch);
}
```

---

## 8. Implementation Tiers

### Tier 1: Core (1-2 weeks)

| Feature | Description |
|---------|-------------|
| Ring Buffer | Pre-allocated, lock-free |
| SPSC Channel | Single producer/consumer |
| SPSC→MPSC Upgrade | Auto on clone |
| Source | push/try_push/push_arrow |
| Sink/Subscription | poll/recv |
| SQL DDL | CREATE SOURCE/SINK |

**Constraints for Tier 1**:
- Block backpressure only
- SpinYield wait strategy only
- No checkpointing

### Tier 2: Production (2-3 weeks)

| Feature | Description |
|---------|-------------|
| Broadcast | Multi-consumer (derived from plan) |
| All Backpressure | Block, DropOldest, Reject |
| All Wait Strategies | Spin, SpinYield, Park |
| Checkpointing | Async snapshots |
| WAL | Async and Sync |
| Recovery | Startup from checkpoint |

### Tier 3: Cross-Language (3-4 weeks)

| Feature | Description |
|---------|-------------|
| C ABI | Stable interface |
| Java | JNI bindings |
| Python | PyO3 bindings |

---

## 9. Cross-Language Strategy

### 9.1 C ABI

```c
// laminar.h
typedef struct LaminarDB LaminarDB;
typedef struct LaminarSource LaminarSource;
typedef struct LaminarSink LaminarSink;
typedef struct LaminarSubscription LaminarSubscription;

LaminarDB* laminar_open(const char* config_json);
void laminar_close(LaminarDB* db);

LaminarSource* laminar_source(LaminarDB* db, const char* name);
LaminarError laminar_push(LaminarSource* src, ArrowArray* arr, ArrowSchema* sch);

LaminarSubscription* laminar_subscribe(LaminarSink* sink);
LaminarError laminar_poll(LaminarSubscription* sub, ArrowArray** arr, ArrowSchema** sch);
```

### 9.2 Java

```java
LaminarDB db = LaminarDB.open();

Source<Trade> source = db.source("trades", Trade.class);
source.push(trade);

// Multi-producer: just clone
Source<Trade> src2 = source.clone();  // Auto MPSC

for (OHLCBar bar : db.sink("ohlc", OHLCBar.class).subscribe()) {
    process(bar);
}
```

### 9.3 Python

```python
db = laminardb.open()

source = db.source("trades")
source.push({"symbol": "AAPL", "price": 150.0})

for batch in db.sink("ohlc").subscribe():
    df = batch.to_pandas()
```

---

## 10. Future Integration Points

### Kafka/CDC Sources (Phase 4+)

Same API, different transport:

```sql
-- In-memory (now)
CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP);

-- Kafka (future) - same downstream code
CREATE SOURCE trades FROM KAFKA (
    brokers = 'localhost:9092',
    topic = 'trades'
);

-- CDC (future)
CREATE SOURCE orders FROM CDC (
    connector = 'postgres',
    table = 'orders'
);
```

---

## 11. Feature Specifications Required

### Tier 1 (P0)

| ID | Name | Description |
|----|------|-------------|
| F-STREAM-001 | Ring Buffer | Pre-allocated, lock-free |
| F-STREAM-002 | SPSC Channel | Single producer/consumer |
| F-STREAM-003 | MPSC Upgrade | Auto-upgrade on clone |
| F-STREAM-004 | Source | Push API with auto channel |
| F-STREAM-005 | Sink | Subscribe API |
| F-STREAM-006 | Subscription | Poll/recv iterator |
| F-STREAM-007 | SQL DDL | CREATE SOURCE/SINK |

### Tier 2 (P1)

| ID | Name | Description |
|----|------|-------------|
| F-STREAM-010 | Broadcast | Multi-consumer (plan-derived) |
| F-STREAM-011 | Backpressure | All strategies |
| F-STREAM-012 | Wait Strategies | All strategies |
| F-STREAM-013 | Checkpointing | Async snapshots |
| F-STREAM-014 | WAL | Async/Sync modes |
| F-STREAM-015 | Recovery | Startup restore |

### Tier 3 (P1/P2)

| ID | Name | Description |
|----|------|-------------|
| F-STREAM-020 | C ABI | Stable interface |
| F-STREAM-021 | Java Bindings | JNI |
| F-STREAM-022 | Python Bindings | PyO3 |

---

## Appendix: Configuration Summary

### What Users Configure

| Setting | Default | Description |
|---------|---------|-------------|
| buffer_size | 65536 | Ring buffer slots |
| backpressure | Block | Full buffer behavior |
| wait_strategy | SpinYield(100) | Empty buffer wait |
| checkpoint_interval | None (disabled) | Snapshot frequency |
| wal_mode | None (disabled) | Write-ahead log |
| watermark | None | Event-time processing |

### What's Automatic

| Aspect | How Derived |
|--------|-------------|
| SPSC vs MPSC | Auto-upgrade on `source.clone()` |
| SPSC vs Broadcast (sink) | Query plan analysis |
| Partitioning | GROUP BY keys |

---

*End of Document*
