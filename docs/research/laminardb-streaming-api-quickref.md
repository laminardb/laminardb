# LaminarDB Streaming API - Quick Reference

## Design Philosophy

```
┌─────────────────────────────────────────────────────────────────┐
│                    ZERO CONFIGURATION                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Channel Type    →  AUTOMATIC (never user-specified)            │
│  SPSC vs MPSC    →  Auto-upgrade on source.clone()              │
│  Broadcast       →  Derived from query plan                      │
│  Checkpointing   →  Disabled by default (opt-in)                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## What Users Configure

| Setting | Default | Options |
|---------|---------|---------|
| `buffer_size` | 65536 | Power of 2 |
| `backpressure` | Block | Block, DropOldest, Reject |
| `wait_strategy` | SpinYield(100) | Spin, SpinYield(n), Park |
| `checkpoint_interval` | None | Duration or None |
| `wal_mode` | None | Async, Sync, or None |
| `watermark` | None | Column + delay |

## What's Automatic

| Aspect | How It Works |
|--------|--------------|
| **SPSC → MPSC** | Auto-upgrade when `source.clone()` called |
| **Broadcast sink** | Derived from query plan (multiple MVs) |
| **Partitioning** | Derived from GROUP BY keys |

---

## Durability (Composable, No Tiers)

| checkpoint_interval | wal_mode | Result | Overhead |
|---------------------|----------|--------|----------|
| `None` | `None` | **Pure in-memory** | **0%** |
| `Some(10s)` | `None` | Periodic snapshots | <1% |
| `Some(10s)` | `Async` | + async WAL | 1-2% |
| `Some(10s)` | `Sync` | + sync WAL | 5-10% |

---

## Rust API

### Database
```rust
let db = LaminarDB::open()?;                    // Max performance
let db = LaminarDB::open_with_config(config)?;  // Custom config
```

### Source (Auto SPSC→MPSC)
```rust
let source = db.source::<Trade>("trades")?;

// Single producer - uses fast SPSC
source.push(trade)?;

// Multiple producers - auto upgrades to MPSC
let src2 = source.clone();  // Triggers upgrade
src2.push(trade)?;          // Both can push safely
```

### Sink (Auto channel from plan)
```rust
let sink = db.sink::<OHLCBar>("ohlc")?;

for batch in sink.subscribe() {
    process(batch);
}
```

---

## SQL

### CREATE SOURCE (no channel option)
```sql
-- Minimal
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

-- With checkpoint
CREATE SOURCE trades (...) WITH (
    checkpoint_interval = '10 seconds'
);

-- With WAL
CREATE SOURCE trades (...) WITH (
    checkpoint_interval = '10 seconds',
    wal_mode = 'async'
);
```

### CREATE SINK (channel auto-derived)
```sql
CREATE SINK output FROM ohlc_1min;
```

---

## Auto Channel Derivation Examples

### SPSC → MPSC (Producer Side)
```rust
// Initially SPSC (fastest)
let source = db.source::<Trade>("trades")?;

// Clone triggers one-time upgrade to MPSC
let src1 = source.clone();
let src2 = source.clone();

// Now all three can push concurrently
thread::spawn(move || source.push(t1));
thread::spawn(move || src1.push(t2));
thread::spawn(move || src2.push(t3));
```

### Broadcast (Consumer Side - Query Plan)
```sql
CREATE SOURCE trades (...);

-- Single MV → SPSC sink (fast)
CREATE MATERIALIZED VIEW ohlc AS 
    SELECT ... FROM trades GROUP BY ...;

-- Adding second MV → trades sink becomes Broadcast
CREATE MATERIALIZED VIEW vwap AS 
    SELECT ... FROM trades GROUP BY ...;
```

---

## Configuration Examples

### Maximum Performance (Default)
```rust
let db = LaminarDB::open()?;
```
```sql
CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP);
```

### With Checkpointing
```rust
let db = LaminarDB::open_with_config(Config {
    data_dir: Some("/var/lib/laminar".into()),
    checkpoint_interval: Some(Duration::from_secs(10)),
    ..Default::default()
})?;
```

### Maximum Durability
```rust
let db = LaminarDB::open_with_config(Config {
    data_dir: Some("/var/lib/laminar".into()),
    checkpoint_interval: Some(Duration::from_secs(5)),
    wal_mode: Some(WalMode::Sync),
    ..Default::default()
})?;
```

---

## Cross-Language

### Java
```java
LaminarDB db = LaminarDB.open();

Source<Trade> source = db.source("trades", Trade.class);
source.push(trade);

// Multi-producer: just clone
Source<Trade> src2 = source.clone();  // Auto MPSC

for (Bar bar : db.sink("ohlc", Bar.class).subscribe()) {
    process(bar);
}
```

### Python
```python
db = laminardb.open()
source = db.source("trades")
source.push({"symbol": "AAPL", "price": 150.0})

for batch in db.sink("ohlc").subscribe():
    df = batch.to_pandas()
```

---

## Performance Targets

| Operation | Target |
|-----------|--------|
| SPSC push | <100ns |
| MPSC push (low contention) | <200ns |
| Poll (data ready) | <50ns |
| SPSC→MPSC upgrade | One-time ~1µs |
| Checkpoint overhead | <1% (async) |

---

## Validation Rules

1. `data_dir` required if checkpoint or WAL enabled
2. `wal_mode` requires `checkpoint_interval`
3. `buffer_size` must be power of 2

---

## Future (Phase 4+)

Same API, different transport:
```sql
-- Kafka source
CREATE SOURCE trades FROM KAFKA (
    brokers = 'localhost:9092',
    topic = 'trades'
);

-- CDC source
CREATE SOURCE orders FROM CDC (
    connector = 'postgres',
    table = 'orders'
);
```
