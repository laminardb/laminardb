# Demo Feature Index

## Overview

Production-style market data demo showcasing LaminarDB's streaming SQL capabilities.

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-DEMO-001 | Market Data Pipeline | P0 | ✅ Done | [Link](F-DEMO-001-market-data-pipeline.md) |
| F-DEMO-002 | Ratatui TUI Dashboard | P0 | ✅ Done | [Link](F-DEMO-002-ratatui-tui.md) |
| F-DEMO-003 | Kafka Integration & Docker | P1 | 📝 Draft | [Link](F-DEMO-003-kafka-docker.md) |
| F-DEMO-004 | DAG Pipeline Visualization | P1 | 📝 Draft | [Link](F-DEMO-004-dag-visualization.md) |

## Architecture

```
Generator -> source.push_batch() -> [LaminarDB SQL] -> subscription.poll() -> App State -> Ratatui
```

### Modes

- **Embedded** (default): Zero external deps, data generated in-process
- **Kafka** (optional): Separate producer binary -> Redpanda -> LaminarDB

### Domain

Synthetic market data for 5 symbols (AAPL, GOOGL, MSFT, TSLA, AMZN):
- Market ticks with random-walk prices
- Order events (buy/sell)
- OHLC bar aggregation via SQL
- Volume/spread metrics
- Anomaly detection (high-volume alerts)
