# Production Kafka Demo Plan

> Created: 2026-02-06
> Status: Planning
> Branch: feature/demo

## Goal

Enhance the existing demo into a production-grade market data pipeline using Redpanda/Kafka with Docker Compose. Showcases the full breadth of LaminarDB's streaming SQL capabilities with Avro serialization, checkpointing, and recovery.

## Architecture

```
┌──────────────────────────┐
│    Producer Binary        │  (separate process)
│    laminardb-demo-producer│
│                          │
│  Synthetic market data:  │
│  - Market ticks (5 syms) │
│  - Order events          │
│  - Reference data (dim)  │
│                          │
│  Format: Avro            │
│  Schema Registry: yes    │
└──────┬───────┬───────┬───┘
       │       │       │  Avro to Redpanda
       ▼       ▼       ▼
  [market-   [order-  [reference-
   ticks]    events]    data]
       │       │       │
       ▼       ▼       ▼
┌──────────────────────────────────────────────────────┐
│              LaminarDB Consumer                       │
│                                                      │
│  Sources (Kafka, Avro, Schema Registry):             │
│    market_ticks, order_events, reference_data        │
│                                                      │
│  Streaming SQL Pipelines:                            │
│  ┌────────────────────────────────────────────┐      │
│  │ 1. OHLC Candles        (TUMBLE 5s)         │      │
│  │ 2. Rolling Spreads     (HOP 1s / 10s)      │      │
│  │ 3. Session Activity    (SESSION 30s)        │      │
│  │ 4. ASOF JOIN           orders ⟷ ticks       │      │
│  │ 5. Lookup JOIN         ticks ⟷ reference    │      │
│  │ 6. Stream-Stream JOIN  ticks ⟷ orders       │      │
│  │ 7. LAG/LEAD           price momentum        │      │
│  │ 8. ROW_NUMBER          top-N movers          │      │
│  │ 9. Volume Anomaly      cascading MV          │      │
│  │ 10. Derived Analytics  cascading MV          │      │
│  └────────────────────────────────────────────┘      │
│                                                      │
│  Sinks (Kafka, Avro):                                │
│    ohlc-bars, enriched-orders, alerts, analytics     │
│                                                      │
│  Checkpoint: every 30s, Kafka offset recovery        │
│                                                      │
│  TUI Dashboard: Ratatui (same as embedded mode)      │
└──────────────────────────────────────────────────────┘
```

## Serialization: Avro + Schema Registry

- **Format**: Apache Avro via Confluent Schema Registry
- **Producer**: Register schemas on startup, serialize with Confluent wire format (magic byte + 4-byte schema ID + Avro payload)
- **Consumer**: Resolve schemas from registry, deserialize via `arrow-avro` crate
- **Schema Registry**: Redpanda's built-in schema registry (port 8081)
- LaminarDB already has:
  - `SchemaRegistryClient` with caching and auto-registration
  - `AvroDeserializer` / `AvroSerializer` (Confluent wire format)
  - `arrow-avro` integration for Arrow ↔ Avro conversion

### SQL Source Definition (Avro)

```sql
CREATE SOURCE market_ticks (
    symbol VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    bid DOUBLE NOT NULL,
    ask DOUBLE NOT NULL,
    volume BIGINT NOT NULL,
    side VARCHAR NOT NULL,
    ts BIGINT NOT NULL
) WITH (
    connector = 'kafka',
    brokers = '${KAFKA_BROKERS}',
    topic = 'market-ticks',
    group_id = '${GROUP_ID}',
    format = 'avro',
    schema_registry_url = '${SCHEMA_REGISTRY_URL}',
    offset_reset = 'earliest'
)
WATERMARK FOR ts AS ts - INTERVAL '5' SECOND;
```

## Docker Compose

```yaml
# Redpanda (Kafka-compatible, no JVM/Zookeeper)
# Redpanda Console (topic inspection, schema registry UI)
# Redpanda Schema Registry (port 8081, built-in)
```

Services:
- **redpanda**: Kafka API (9092), Schema Registry (8081), Admin (9644)
- **redpanda-console**: Web UI (8080) for topic/schema inspection
- Topics auto-created via `rpk` in setup script

## SQL Pipelines (10 Streams)

### 1. OHLC Candles — TUMBLE window

```sql
CREATE STREAM ohlc_bars AS
SELECT
    symbol,
    FIRST_VALUE(price) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST_VALUE(price) as close,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume,
    SUM(price * volume) / SUM(volume) as vwap
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
EMIT AFTER WATERMARK;
```

### 2. Rolling Spread — HOP (sliding) window

```sql
CREATE STREAM rolling_spread AS
SELECT
    symbol,
    AVG(ask - bid) as avg_spread,
    MIN(ask - bid) as min_spread,
    MAX(ask - bid) as max_spread,
    COUNT(*) as tick_count
FROM market_ticks
GROUP BY symbol, hop(ts, INTERVAL '1' SECOND, INTERVAL '10' SECOND)
EMIT AFTER WATERMARK;
```

### 3. Session Activity — SESSION window

```sql
CREATE STREAM session_activity AS
SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(volume) as session_volume,
    MIN(price) as session_low,
    MAX(price) as session_high
FROM market_ticks
GROUP BY symbol, session(ts, INTERVAL '30' SECOND)
EMIT AFTER WATERMARK;
```

### 4. ASOF JOIN — Enrich orders with latest tick

```sql
CREATE STREAM enriched_orders AS
SELECT
    o.order_id,
    o.symbol,
    o.side,
    o.quantity,
    o.price as order_price,
    t.price as market_price,
    t.bid,
    t.ask,
    o.price - t.price as slippage
FROM order_events o
ASOF JOIN market_ticks t
    MATCH_CONDITION(o.ts >= t.ts)
    ON o.symbol = t.symbol;
```

### 5. Lookup JOIN — Enrich ticks with reference data

```sql
CREATE STREAM enriched_ticks AS
SELECT
    t.symbol,
    t.price,
    t.volume,
    r.sector,
    r.market_cap_category,
    r.exchange
FROM market_ticks t
JOIN reference_data r ON t.symbol = r.symbol;
```

### 6. Stream-Stream JOIN — Match orders to ticks within time window

```sql
CREATE STREAM order_tick_matches AS
SELECT
    o.order_id,
    o.symbol,
    o.side,
    o.price as order_price,
    t.price as tick_price,
    t.bid,
    t.ask,
    ABS(o.price - t.price) as price_diff
FROM order_events o
JOIN market_ticks t
    ON o.symbol = t.symbol
    AND t.ts BETWEEN o.ts - INTERVAL '2' SECOND AND o.ts + INTERVAL '2' SECOND;
```

### 7. LAG/LEAD — Price momentum

```sql
CREATE STREAM price_momentum AS
SELECT
    symbol,
    price,
    LAG(price, 1, price) OVER (PARTITION BY symbol ORDER BY ts) AS prev_price,
    price - LAG(price, 1, price) OVER (PARTITION BY symbol ORDER BY ts) AS price_delta,
    LEAD(price, 1, price) OVER (PARTITION BY symbol ORDER BY ts) AS next_price,
    ts
FROM market_ticks;
```

### 8. ROW_NUMBER — Top movers (via cascading MV)

```sql
-- First: aggregate per symbol
CREATE MATERIALIZED VIEW symbol_stats AS
SELECT
    symbol,
    MAX(price) - MIN(price) as price_range,
    STDDEV(price) as volatility,
    COUNT(*) as trade_count
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '10' SECOND)
EMIT AFTER WATERMARK;

-- Then: rank (top-N subquery pattern)
CREATE STREAM top_movers AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY price_range DESC) AS rn
    FROM symbol_stats
) sub WHERE rn <= 3;
```

### 9. Volume Anomaly Detection — HAVING clause

```sql
CREATE STREAM volume_alerts AS
SELECT
    symbol,
    SUM(volume) as total_volume,
    COUNT(*) as trade_count,
    SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) as buy_volume,
    SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as sell_volume
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
HAVING SUM(volume) > 5000
EMIT AFTER WATERMARK;
```

### 10. Derived Analytics — Cascading MV from enriched data

```sql
CREATE STREAM sector_analytics AS
SELECT
    sector,
    AVG(price) as avg_price,
    SUM(volume) as total_volume,
    COUNT(*) as tick_count
FROM enriched_ticks
GROUP BY sector, tumble(ts, INTERVAL '10' SECOND)
EMIT AFTER WATERMARK;
```

## Checkpointing

```rust
let db = LaminarDB::builder()
    .config_var("KAFKA_BROKERS", "localhost:9092")
    .config_var("GROUP_ID", "market-analytics")
    .config_var("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    .buffer_size(65536)
    .checkpoint(StreamCheckpointConfig {
        interval: Duration::from_secs(30),
        enabled: true,
    })
    .storage_dir("./data/checkpoints")
    .build()
    .await?;
```

On recovery:
1. Load last checkpoint (operator state snapshots)
2. Restore Kafka consumer offsets from checkpoint
3. Resume processing from last committed position
4. Exactly-once: sink transactions rolled back for uncommitted epoch

TUI keybinding `[c]` triggers manual checkpoint for demo purposes.

## Reference Data (Dimension Table)

Topic: `reference-data` (compacted, keyed by symbol)

| Field | Type | Example |
|-------|------|---------|
| symbol | string | "AAPL" |
| company_name | string | "Apple Inc." |
| sector | string | "Technology" |
| market_cap_category | string | "Large Cap" |
| exchange | string | "NASDAQ" |
| currency | string | "USD" |

Loaded via `CREATE TABLE` DDL with snapshot-mode consumption (requires F-CONN-002):

```sql
CREATE TABLE reference_data (
    symbol VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    sector VARCHAR,
    market_cap_category VARCHAR,
    exchange VARCHAR,
    currency VARCHAR
) FROM KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'reference-data',
    format = 'avro',
    schema_registry_url = '${SCHEMA_REGISTRY_URL}'
);
```

On startup, the table consumer reads to the high water mark (snapshot phase), deduplicates
by primary key, and marks the table as `ready`. The pipeline blocks until all reference
tables are ready, ensuring lookup JOINs have complete data before processing stream events.
Background refresh applies upserts/deletes from new compacted topic messages.

## Producer Binary

Separate binary: `cargo run -p laminardb-demo --bin producer --features kafka`

Features:
- Registers Avro schemas with Schema Registry on startup
- Generates realistic market data with random-walk price model
- Configurable tick rate (`--tps 1000` default)
- Publishes to 3 topics: `market-ticks`, `order-events`, `reference-data`
- Reference data published once as compacted topic
- Graceful shutdown on SIGINT

## TUI Dashboard

Wire up the existing Ratatui TUI to work in Kafka mode (currently embedded-only):
- Same layout: OHLC, order flow, sparklines, alerts, DAG view, order book
- Add new panels for: rolling spread (HOP), session activity, price momentum (LAG), top movers
- Subscription-based polling in render loop (same pattern as embedded)
- Display checkpoint status and last checkpoint timestamp
- Show Kafka consumer lag if available

## Kafka Topics

| Topic | Key | Format | Partitions | Compaction |
|-------|-----|--------|------------|------------|
| market-ticks | symbol | Avro | 5 | delete |
| order-events | symbol | Avro | 5 | delete |
| reference-data | symbol | Avro | 1 | compact |
| ohlc-bars | symbol | Avro | 5 | delete |
| enriched-orders | order_id | Avro | 5 | delete |
| volume-alerts | symbol | Avro | 1 | delete |
| sector-analytics | sector | Avro | 1 | delete |

## Prerequisites: Feature Specs to Implement

The following features must be implemented before the demo reaches production-grade.
All have detailed feature specifications in `docs/features/phase-3/`.

### SQL Features

| Feature Spec | Priority | Effort | What It Enables |
|-------------|----------|--------|-----------------|
| [F-SQL-004: HAVING Clause](../features/phase-3/F-SQL-004-having-clause.md) | P1 | 2-3 days | Native `HAVING` in pipeline #9 (volume anomaly detection), eliminates cascading MV pattern |
| [F-SQL-005: Multi-Way JOINs](../features/phase-3/F-SQL-005-multi-way-joins.md) | P1 | 3-4 days | `A JOIN B JOIN C` in single query, mixed join types (ASOF + lookup) |
| [F-SQL-006: Window Frames](../features/phase-3/F-SQL-006-window-frames.md) | P2 | 4-5 days | `ROWS BETWEEN` for true per-row moving averages and sliding MIN/MAX |

### Connector Infrastructure

| Feature Spec | Priority | Effort | What It Enables |
|-------------|----------|--------|-----------------|
| [F-CONN-001: Checkpoint Recovery Wiring](../features/phase-3/F-CONN-001-checkpoint-recovery-wiring.md) | P0 | 3-5 days | Production checkpoint/recovery: periodic triggers, source offset persistence, sink epoch coordination |
| [F-CONN-002: Reference Tables](../features/phase-3/F-CONN-002-reference-tables.md) | P0 | 3-5 days | `CREATE TABLE ... FROM KAFKA`, snapshot-mode compacted topic consumption, `TableStore` for lookup JOINs |
| [F-CONN-003: Avro Hardening](../features/phase-3/F-CONN-003-avro-hardening.md) | P1 | 2-3 days | Round-trip tests, complex Avro types (arrays/maps/nested), cache eviction, compatibility enforcement |

### Observability & Demo

| Feature Spec | Priority | Effort | What It Enables |
|-------------|----------|--------|-----------------|
| [F-OBS-001: Pipeline Observability](../features/phase-3/F-OBS-001-pipeline-observability.md) | P1 | 2-3 days | `db.metrics()`, per-stream stats, latency tracking, watermark visibility, backpressure state |
| [F-DEMO-006: Kafka TUI Dashboard](../features/phase-3/F-DEMO-006-kafka-tui.md) | P0 | 2-3 days | Shared TUI event loop, `PipelineDataSource` trait, new panels for HOP/SESSION/LAG streams |

### Implementation Order

Recommended build order based on dependencies:

1. **F-CONN-001** (Checkpoint Recovery Wiring) — foundation for production-grade operation
2. **F-CONN-002** (Reference Tables) — needed for lookup JOIN pipeline #5
3. **F-SQL-004** (HAVING) — simplifies pipeline #9 (volume anomaly detection)
4. **F-SQL-005** (Multi-Way JOINs) — enables richer combined pipelines
5. **F-CONN-003** (Avro Hardening) — production Avro robustness
6. **F-OBS-001** (Observability) — metrics for TUI dashboard
7. **F-DEMO-006** (Kafka TUI) — visual showcase
8. **F-SQL-006** (Window Frames) — nice-to-have for advanced analytics

### SQL Features Used in This Demo

- [x] TUMBLE windows (5s candles)
- [x] HOP/SLIDE windows (1s/10s rolling spread)
- [x] SESSION windows (30s activity sessions)
- [x] ASOF JOIN (order enrichment with latest tick)
- [x] Lookup JOIN (reference data enrichment)
- [x] Stream-Stream JOIN (order-tick matching with time bounds)
- [x] LAG/LEAD (price momentum, tick-by-tick delta)
- [x] ROW_NUMBER/RANK (top-N movers per window)
- [x] FIRST_VALUE/LAST_VALUE (OHLC open/close)
- [x] HAVING (volume anomaly detection) — requires F-SQL-004
- [x] Multi-way JOINs — requires F-SQL-005
- [x] CASE WHEN (buy/sell volume split)
- [x] 30+ aggregates (SUM, AVG, STDDEV, COUNT, MIN, MAX, VWAP)
- [x] EMIT AFTER WATERMARK
- [x] Cascading MVs (derived analytics from intermediate streams)
- [x] Watermarks with allowed lateness
- [x] Avro serialization with Schema Registry
- [x] Checkpointing with Kafka offset recovery
- [x] Exactly-once sink delivery
- [x] Reference tables from compacted topics — requires F-CONN-002
- [x] Pipeline observability metrics — requires F-OBS-001

## File Plan

```
examples/demo/
├── Cargo.toml                    # Update: add avro deps
├── docker-compose.yml            # Update: add schema registry port
├── scripts/
│   ├── setup-kafka.sh            # Update: create all topics
│   └── register-schemas.sh       # NEW: register Avro schemas (optional, producer does it)
├── sql/
│   ├── sources_kafka_avro.sql    # NEW: Avro source definitions
│   ├── streams_full.sql          # NEW: All 10 pipelines
│   ├── sinks_kafka_avro.sql      # NEW: Avro sink definitions
│   ├── sources.sql               # Keep: embedded mode
│   ├── streams.sql               # Keep: embedded mode
│   └── sinks.sql                 # Keep: embedded mode
├── schemas/                      # NEW: Avro schema files (.avsc)
│   ├── market_tick.avsc
│   ├── order_event.avsc
│   └── reference_data.avsc
└── src/
    ├── main.rs                   # Update: wire Kafka mode TUI + checkpoint
    ├── producer.rs               # Update: Avro serialization, schema registration
    ├── app.rs                    # Update: new panels for HOP/SESSION/LAG/momentum
    ├── tui.rs                    # Update: new panels
    ├── generator.rs              # Update: reference data generation
    ├── types.rs                  # Update: new output types
    ├── asof_merge.rs             # Keep: application-level fallback
    ├── system_stats.rs           # Keep
    └── lib.rs                    # Keep
```

## Implementation Order

### Phase A: Core Engine Features (2-3 weeks)

Implement the prerequisite feature specs (see "Prerequisites" section above):

1. **F-CONN-001**: Checkpoint Recovery Wiring (3-5 days)
2. **F-CONN-002**: Reference Table Support (3-5 days)
3. **F-SQL-004**: HAVING Clause Execution (2-3 days)
4. **F-SQL-005**: Multi-Way JOIN Support (3-4 days)
5. **F-CONN-003**: Avro Serialization Hardening (2-3 days)
6. **F-OBS-001**: Pipeline Observability API (2-3 days)

### Phase B: Demo Infrastructure (1-2 days)

1. Update `docker-compose.yml` — expose schema registry port 8081
2. Create Avro schema files (`.avsc`) for all topics
3. Update producer to use Avro + Schema Registry
4. Write `sources_kafka_avro.sql` (with `CREATE TABLE` for reference data)
5. Write `sinks_kafka_avro.sql`
6. Update setup script for all topics

### Phase C: SQL Pipelines & Integration (1-2 days)

1. Write `streams_full.sql` with all 10 pipelines (proper HAVING, multi-way JOINs)
2. Add new output types to `types.rs`
3. Wire new subscriptions in `main.rs`
4. Wire checkpoint recovery flow (start → recover → resume from offset)
5. Test each pipeline individually

### Phase D: TUI & Observability (1-2 days)

1. **F-DEMO-006**: Shared TUI event loop, `PipelineDataSource` trait
2. Add new panels: rolling spread, session activity, price momentum, top movers
3. Wire observability metrics into TUI status bar
4. Add checkpoint status display and `[c]` keybinding

### Phase E: Polish (1 day)

1. Error handling and graceful recovery
2. Test checkpoint/recovery: kill consumer, restart, verify resume from offset
3. Test Avro schema evolution (add field, verify backward compatibility)
4. README with setup instructions
5. Full end-to-end walkthrough: producer → Redpanda → consumer → TUI → checkpoint → recovery

## Running

```bash
# 1. Start infrastructure
docker-compose -f examples/demo/docker-compose.yml up -d

# 2. Create topics
bash examples/demo/scripts/setup-kafka.sh

# 3. Start producer (separate terminal)
cargo run -p laminardb-demo --bin producer --features kafka

# 4. Start consumer with TUI
DEMO_MODE=kafka cargo run -p laminardb-demo --features kafka

# 5. Test recovery: Ctrl+C consumer, restart → resumes from checkpoint
```
