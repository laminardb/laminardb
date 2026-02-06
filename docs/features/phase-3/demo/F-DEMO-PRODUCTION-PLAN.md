# F-DEMO-PRODUCTION: Production-Grade Kafka Demo Plan

> **Status**: Draft
> **Created**: 2026-02-06
> **Priority**: P1

## Executive Summary

Extend the existing demo into a **three-component production system**: Standalone Producer, Kafka/Redpanda cluster with Schema Registry, and LaminarDB Consumer with checkpointing and TUI dashboard.

**Key Decisions:**
- Kafka-only (no PostgreSQL/Delta Lake sinks)
- Avro format with Schema Registry (already implemented in laminar-connectors)
- Docker Compose first (full infrastructure)
- Stick to implemented SQL features, document parser gaps

---

## Current Infrastructure (Already Implemented)

| Component | Status | Notes |
|-----------|--------|-------|
| Schema Registry Client | ✅ | Full REST API, caching, compatibility checks |
| Avro Deserializer | ✅ | Confluent wire format, auto-schema fetch |
| Avro Serializer | ✅ | Per-row SOE format with Confluent prefix |
| SQL Parser | ✅ | 30+ aggregates, TUMBLE/HOP/SESSION, EMIT clauses |
| Checkpoint API | ✅ | `db.checkpoint()`, `db.restore_checkpoint()` |
| Producer binary | ✅ | Basic - needs enhancement |
| TUI Dashboard | ✅ | 3 views - needs extension |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     PRODUCTION KAFKA DEMO ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────┐     ┌─────────────────────────┐     ┌────────────────┐  │
│  │    PRODUCER    │     │     KAFKA CLUSTER       │     │    CONSUMER    │  │
│  │    (Rust)      │────▶│     (Redpanda)          │────▶│   (LaminarDB)  │  │
│  │                │     │                         │     │                │  │
│  │ • Market sim   │     │ ┌─────────────────────┐ │     │ • SQL pipelines│  │
│  │ • Avro + SR    │     │ │  Schema Registry    │ │     │ • Checkpointing│  │
│  │ • Config rate  │     │ │  (port 8081)        │ │     │ • Recovery     │  │
│  │ • Burst mode   │     │ └─────────────────────┘ │     │ • Ratatui TUI  │  │
│  │ • Graceful     │     │                         │     │                │  │
│  │   shutdown     │     │ Input Topics:           │     │ Output Topics: │  │
│  │                │     │ • market-ticks (Avro)   │     │ • ohlc-1m      │  │
│  └────────────────┘     │ • order-events (Avro)   │     │ • ohlc-5s      │  │
│         │               │ • book-updates (Avro)   │     │ • alerts       │  │
│         │               │                         │     │ • analytics    │  │
│         │               └─────────────────────────┘     └────────────────┘  │
│         │                          │                           │            │
│         │                          ▼                           │            │
│         │               ┌─────────────────────────┐            │            │
│         └──────────────▶│    Redpanda Console     │◀───────────┘            │
│                         │    (port 8080)          │                         │
│                         │    Topic/Schema browser │                         │
│                         └─────────────────────────┘                         │
│                                                                              │
│  Volumes:                                                                    │
│  • redpanda_data    - Kafka log segments                                    │
│  • checkpoint_data  - LaminarDB checkpoint files                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## SQL Analytics (Using Implemented Features Only)

### Implemented Queries

```sql
-- ═══════════════════════════════════════════════════════════════
-- MULTI-TIMEFRAME OHLC (uses FIRST_VALUE/LAST_VALUE aggregates)
-- ═══════════════════════════════════════════════════════════════

-- 5-second OHLC (existing)
CREATE STREAM ohlc_5s AS
SELECT symbol,
       FIRST_VALUE(price) as open,
       MAX(price) as high,
       MIN(price) as low,
       LAST_VALUE(price) as close,
       SUM(volume) as volume,
       SUM(price * volume) / SUM(volume) as vwap,
       COUNT(*) as tick_count
FROM market_ticks
GROUP BY symbol, TUMBLE(ts, INTERVAL '5' SECOND)
EMIT ON WINDOW CLOSE;

-- 1-minute OHLC
CREATE STREAM ohlc_1m AS
SELECT symbol,
       FIRST_VALUE(price) as open,
       MAX(price) as high,
       MIN(price) as low,
       LAST_VALUE(price) as close,
       SUM(volume) as volume,
       SUM(price * volume) / SUM(volume) as vwap,
       COUNT(*) as tick_count
FROM market_ticks
GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;

-- ═══════════════════════════════════════════════════════════════
-- ROLLING STATISTICS (using HOP/SLIDE windows)
-- ═══════════════════════════════════════════════════════════════

-- 20-period rolling average (slide every 5s, window size 100s = 20 x 5s)
CREATE STREAM rolling_stats AS
SELECT symbol,
       AVG(price) as ma_20,
       STDDEV(price) as stddev_20,
       MIN(price) as min_20,
       MAX(price) as max_20,
       COUNT(*) as sample_count
FROM market_ticks
GROUP BY symbol, HOP(ts, INTERVAL '5' SECOND, INTERVAL '100' SECOND)
EMIT ON WINDOW CLOSE;

-- ═══════════════════════════════════════════════════════════════
-- VOLUME ANALYTICS
-- ═══════════════════════════════════════════════════════════════

-- Buy/sell pressure (5-second windows)
CREATE STREAM volume_pressure AS
SELECT symbol,
       SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) as buy_volume,
       SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as sell_volume,
       SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) as buy_count,
       SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) as sell_count,
       -- Volume imbalance ratio: positive = buy pressure
       (SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) -
        SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END)) /
       NULLIF(SUM(volume), 0) as imbalance_ratio
FROM market_ticks
GROUP BY symbol, TUMBLE(ts, INTERVAL '5' SECOND);

-- ═══════════════════════════════════════════════════════════════
-- ANOMALY DETECTION (threshold-based, using FILTER clause)
-- ═══════════════════════════════════════════════════════════════

-- High volume alerts (volume > 2x average window)
CREATE STREAM volume_anomalies AS
SELECT symbol,
       SUM(volume) as total_volume,
       AVG(volume) as avg_volume,
       MAX(volume) as max_volume,
       COUNT(*) as trade_count
FROM market_ticks
GROUP BY symbol, TUMBLE(ts, INTERVAL '5' SECOND)
HAVING SUM(volume) > 1000;  -- threshold (app-configurable)

-- Large trade detection
CREATE STREAM large_trades AS
SELECT symbol, price, volume, side, ts
FROM market_ticks
WHERE volume > 500;  -- configurable threshold

-- ═══════════════════════════════════════════════════════════════
-- SPREAD ANALYTICS
-- ═══════════════════════════════════════════════════════════════

CREATE STREAM spread_metrics AS
SELECT symbol,
       AVG(ask - bid) as avg_spread,
       MIN(ask - bid) as min_spread,
       MAX(ask - bid) as max_spread,
       STDDEV(ask - bid) as stddev_spread,
       -- Spread as percentage of mid price
       AVG((ask - bid) / ((ask + bid) / 2) * 100) as avg_spread_pct
FROM market_ticks
WHERE ask > bid  -- filter invalid spreads
GROUP BY symbol, TUMBLE(ts, INTERVAL '5' SECOND);

-- ═══════════════════════════════════════════════════════════════
-- ORDER BOOK IMBALANCE
-- ═══════════════════════════════════════════════════════════════

CREATE STREAM book_imbalance AS
SELECT symbol,
       SUM(CASE WHEN side = 'bid' THEN quantity ELSE 0 END) as bid_depth,
       SUM(CASE WHEN side = 'ask' THEN quantity ELSE 0 END) as ask_depth,
       -- Imbalance: 0.5 = balanced, >0.5 = bid heavy, <0.5 = ask heavy
       CAST(SUM(CASE WHEN side = 'bid' THEN quantity ELSE 0 END) AS DOUBLE) /
       NULLIF(CAST(SUM(quantity) AS DOUBLE), 0) as imbalance,
       COUNT(*) as update_count
FROM book_updates
WHERE action != 'delete'
GROUP BY symbol, TUMBLE(ts, INTERVAL '5' SECOND);

-- ═══════════════════════════════════════════════════════════════
-- SESSION-BASED ANALYTICS (gap detection)
-- ═══════════════════════════════════════════════════════════════

-- Trading sessions (gap > 10 seconds = new session)
CREATE STREAM trading_sessions AS
SELECT symbol,
       COUNT(*) as tick_count,
       SUM(volume) as session_volume,
       MIN(price) as session_low,
       MAX(price) as session_high,
       FIRST_VALUE(price) as session_open,
       LAST_VALUE(price) as session_close
FROM market_ticks
GROUP BY symbol, SESSION(ts, INTERVAL '10' SECOND);
```

---

## SQL Parser Gaps

See [SQL-PARSER-GAPS.md](./SQL-PARSER-GAPS.md) for detailed analysis.

| Feature | Use Case | Status | Workaround |
|---------|----------|--------|------------|
| LAG()/LEAD() | Price change detection | Not implemented | App-layer buffering |
| ROW_NUMBER() | Top-N per symbol | Not implemented | App-layer sorting |
| CTEs (WITH) | Query modularization | Not implemented | Multiple streams |
| UNION | Combine alert types | Not implemented | Multiple subscriptions |

---

## Implementation Phases

| Phase | Scope | Days |
|-------|-------|------|
| 1 | Docker infrastructure (compose, Dockerfiles) | 1 |
| 2 | Enhanced producer (Avro, config, graceful shutdown) | 2 |
| 3 | Consumer checkpointing (enable, recovery, display) | 2 |
| 4 | TUI enhancements (new views, header, shortcuts) | 2 |
| 5 | Documentation, polish, testing | 1 |
| **Total** | | **8 days** |

---

## Docker Compose

```yaml
version: '3.8'

services:
  redpanda:
    image: redpandadata/redpanda:v24.1.1
    command:
      - redpanda start
      - --smp 1
      - --memory 1G
      - --reserve-memory 0M
      - --overprovisioned
      - --node-id 0
      - --kafka-addr PLAINTEXT://0.0.0.0:9092,OUTSIDE://0.0.0.0:19092
      - --advertise-kafka-addr PLAINTEXT://redpanda:9092,OUTSIDE://localhost:19092
      - --schema-registry-addr 0.0.0.0:8081
      - --advertise-schema-registry-addr localhost:8081
      - --pandaproxy-addr 0.0.0.0:8082
      - --advertise-pandaproxy-addr localhost:8082
    ports:
      - "19092:19092"  # Kafka (external)
      - "8081:8081"    # Schema Registry
      - "8082:8082"    # HTTP Proxy
      - "9644:9644"    # Admin API
    volumes:
      - redpanda_data:/var/lib/redpanda/data
    healthcheck:
      test: ["CMD", "rpk", "cluster", "health"]
      interval: 10s
      timeout: 5s
      retries: 5

  console:
    image: redpandadata/console:v2.4.5
    ports:
      - "8080:8080"
    environment:
      KAFKA_BROKERS: redpanda:9092
      KAFKA_SCHEMAREGISTRY_ENABLED: "true"
      KAFKA_SCHEMAREGISTRY_URLS: http://redpanda:8081
    depends_on:
      redpanda:
        condition: service_healthy

  init-topics:
    image: redpandadata/redpanda:v24.1.1
    depends_on:
      redpanda:
        condition: service_healthy
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        echo "Creating topics..."
        rpk topic create market-ticks -p 5 -r 1 --brokers redpanda:9092
        rpk topic create order-events -p 5 -r 1 --brokers redpanda:9092
        rpk topic create book-updates -p 5 -r 1 --brokers redpanda:9092
        rpk topic create ohlc-5s -p 5 -r 1 --brokers redpanda:9092
        rpk topic create ohlc-1m -p 5 -r 1 --brokers redpanda:9092
        rpk topic create alerts -p 1 -r 1 --brokers redpanda:9092
        rpk topic create analytics -p 5 -r 1 --brokers redpanda:9092
        echo "Topics created!"
        rpk topic list --brokers redpanda:9092

  producer:
    build:
      context: ../..
      dockerfile: examples/demo/Dockerfile.producer
    environment:
      KAFKA_BROKERS: redpanda:9092
      SCHEMA_REGISTRY_URL: http://redpanda:8081
      PRODUCER_RATE: 100
      PRODUCER_FORMAT: avro
      RUST_LOG: info
    depends_on:
      init-topics:
        condition: service_completed_successfully
    restart: unless-stopped

  consumer:
    build:
      context: ../..
      dockerfile: examples/demo/Dockerfile.consumer
    environment:
      KAFKA_BROKERS: redpanda:9092
      SCHEMA_REGISTRY_URL: http://redpanda:8081
      GROUP_ID: laminardb-demo
      CHECKPOINT_DIR: /data/checkpoints
      CHECKPOINT_INTERVAL_MS: 30000
      RUST_LOG: info
    volumes:
      - checkpoint_data:/data/checkpoints
    depends_on:
      init-topics:
        condition: service_completed_successfully
    stdin_open: true
    tty: true
    restart: unless-stopped

volumes:
  redpanda_data:
  checkpoint_data:
```

---

## File Structure (Final)

```
examples/demo/
├── Cargo.toml
├── docker-compose.yml
├── Dockerfile.producer
├── Dockerfile.consumer
├── README.md
├── schemas/
│   ├── market_tick.avsc
│   ├── order_event.avsc
│   └── book_update.avsc
├── sql/
│   ├── sources.sql
│   ├── sources_kafka_avro.sql    # NEW: Avro format sources
│   ├── streams.sql               # Existing queries
│   ├── streams_advanced.sql      # NEW: Multi-timeframe + rolling
│   ├── sinks.sql
│   └── sinks_kafka.sql
└── src/
    ├── main.rs                   # Consumer with checkpointing
    ├── lib.rs
    ├── app.rs                    # Extended with checkpoint state
    ├── types.rs                  # Extended with new analytics types
    ├── generator.rs              # Enhanced with Avro serialization
    ├── producer.rs               # Enhanced standalone producer
    ├── avro_schemas.rs           # NEW: Avro schema definitions
    ├── asof_merge.rs
    ├── tui.rs                    # Extended with new views
    ├── tui_checkpoint.rs         # NEW: Checkpoint view panel
    ├── tui_analytics.rs          # NEW: Multi-timeframe view
    └── system_stats.rs
```

---

## Deliverables

1. **One-liner startup**: `docker-compose up -d`
2. **Avro + Schema Registry**: Full Confluent wire format support
3. **Checkpointing**: Configurable interval, recovery on restart
4. **Multi-timeframe analytics**: 5s + 1m OHLC, rolling stats
5. **Production TUI**: Checkpoint view, enhanced metrics
6. **Documentation**: README with architecture, config, limitations
