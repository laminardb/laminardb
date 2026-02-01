# F-DEMO-001: Market Data Pipeline

## Overview

| Field | Value |
|-------|-------|
| **ID** | F-DEMO-001 |
| **Title** | Market Data Pipeline |
| **Phase** | 3 (Connectors & Integration) |
| **Priority** | P0 |
| **Status** | Draft |

## Description

Core data pipeline for the market data demo: typed input/output structs, stateful price generator with random-walk model, SQL streaming pipelines for OHLC bars, volume metrics, spread tracking, and anomaly detection.

## Input Types (Record)

### MarketTick
| Field | Type | Description |
|-------|------|-------------|
| symbol | String | Ticker symbol (AAPL, GOOGL, etc.) |
| price | f64 | Current price |
| bid | f64 | Best bid price |
| ask | f64 | Best ask price |
| volume | i64 | Tick volume |
| side | String | "buy" or "sell" |
| ts | i64 | Event time (ms since epoch) |

### OrderEvent
| Field | Type | Description |
|-------|------|-------------|
| order_id | String | Unique order ID |
| symbol | String | Ticker symbol |
| side | String | "buy" or "sell" |
| quantity | i64 | Order quantity |
| price | f64 | Fill price |
| ts | i64 | Event time (ms since epoch) |

## Output Types (FromRow)

### OhlcBar
Aggregated from market ticks per symbol via GROUP BY.

| Field | Type | SQL Expression |
|-------|------|----------------|
| symbol | String | GROUP BY |
| min_price | f64 | MIN(price) |
| max_price | f64 | MAX(price) |
| trade_count | i64 | COUNT(*) |
| total_volume | i64 | SUM(volume) |
| vwap | f64 | SUM(price * volume) / SUM(volume) |

### VolumeMetrics
Buy/sell volume split per symbol.

| Field | Type | SQL Expression |
|-------|------|----------------|
| symbol | String | GROUP BY |
| buy_volume | i64 | SUM(CASE WHEN side='buy') |
| sell_volume | i64 | SUM(CASE WHEN side='sell') |
| net_volume | i64 | buy - sell |
| trade_count | i64 | COUNT(*) |

### SpreadMetrics
Bid-ask spread per symbol.

| Field | Type | SQL Expression |
|-------|------|----------------|
| symbol | String | GROUP BY |
| avg_spread | f64 | AVG(ask - bid) |
| min_spread | f64 | MIN(ask - bid) |
| max_spread | f64 | MAX(ask - bid) |

### AnomalyAlert
High-volume detection per symbol.

| Field | Type | SQL Expression |
|-------|------|----------------|
| symbol | String | GROUP BY |
| trade_count | i64 | COUNT(*) |
| total_volume | i64 | SUM(volume) |

## Generator

Stateful `MarketState` per symbol with:
- Random-walk price model (base price +/- drift)
- Bid/ask spread based on volatility
- Volume distribution (occasional spikes)
- Configurable tick rate

### Symbols & Base Prices
| Symbol | Base Price |
|--------|-----------|
| AAPL | $150.00 |
| GOOGL | $175.00 |
| MSFT | $420.00 |
| TSLA | $250.00 |
| AMZN | $185.00 |

## SQL Pipelines

Four `CREATE STREAM` pipelines using simple GROUP BY (no TUMBLE windows):
1. `ohlc_bars` - OHLC + VWAP per symbol
2. `volume_metrics` - Buy/sell volume split via CASE WHEN
3. `spread_metrics` - Bid-ask spread statistics
4. `anomaly_alerts` - Volume aggregation for threshold detection

## Files

- `src/types.rs` - Record and FromRow structs
- `src/generator.rs` - MarketState + generate_ticks/orders
- `sql/sources.sql` - CREATE SOURCE definitions
- `sql/streams.sql` - CREATE STREAM pipelines
- `sql/sinks.sql` - CREATE SINK definitions
