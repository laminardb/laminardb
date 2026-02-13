# LaminarDB Market Data Demo

Real-time financial market data analytics with a Ratatui TUI dashboard. Demonstrates LaminarDB's streaming SQL capabilities: OHLC bars, volume analysis, spread tracking, order book imbalance, and anomaly detection.

## Running

### Embedded Mode (default)

No external dependencies required. Uses synthetic market data:

```bash
cargo run -p laminardb-demo
```

### Kafka Mode

Requires Docker for Redpanda:

```bash
# Start Redpanda
docker-compose -f examples/demo/docker-compose.yml up -d

# Setup Kafka topics
bash examples/demo/scripts/setup-kafka.sh

# Run with Kafka
DEMO_MODE=kafka cargo run -p laminardb-demo --features kafka
```

## What It Shows

The demo creates a streaming pipeline with three sources and seven continuous queries:

**Sources:**
- `market_ticks` -- Simulated price/volume data for multiple symbols
- `order_events` -- Buy/sell order flow
- `book_updates` -- L2 order book changes

**Streaming Queries (5-second tumbling windows):**
1. **OHLC Bars** -- Per-symbol price aggregation with VWAP
2. **Volume Metrics** -- Buy/sell volume split per symbol
3. **Spread Metrics** -- Bid-ask spread statistics
4. **Anomaly Alerts** -- High-volume detection
5. **Book Imbalance** -- Bid/ask depth ratio
6. **Depth Metrics** -- Total quantities per side
7. **ASOF JOIN** -- Enriched orders with latest market data (application-level)

## TUI Dashboard

The dashboard cycles through multiple views:

| Key | Action |
|-----|--------|
| `Tab` | Next view |
| `q` | Quit |

Views include OHLC charts, volume bars, spread analysis, order book depth, and pipeline metrics (events processed, cycle times, watermarks).

## Architecture

```
Generator ──> SourceHandle::push() ──> LaminarDB Pipeline
                                           |
                                    Streaming SQL queries
                                           |
                                    TypedSubscription<T>
                                           |
                                      Ratatui TUI
```

The demo uses `#[derive(FromRecordBatch)]` from `laminar-derive` to automatically convert Arrow RecordBatches into typed Rust structs for display.
