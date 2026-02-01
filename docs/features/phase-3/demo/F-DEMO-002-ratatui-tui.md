# F-DEMO-002: Ratatui TUI Dashboard

## Overview

| Field | Value |
|-------|-------|
| **ID** | F-DEMO-002 |
| **Title** | Ratatui TUI Dashboard |
| **Phase** | 3 (Connectors & Integration) |
| **Priority** | P0 |
| **Status** | Draft |

## Description

Terminal UI dashboard using Ratatui 0.29 with crossterm backend. Displays real-time market data analytics: OHLC bars, order flow, price sparklines, anomaly alerts, and KPI summary.

## Layout

```
+-- LAMINARDB MARKET DATA DEMO ---------- Running | 02:34 | 12K/s --+
+-- Vol: 1.2M | VWAP: $152.34 | Spread: 0.05 | Trades: 8.4K ------+
+----------------------------+--------------------------------------+
| OHLC BARS                  | ORDER FLOW                           |
| Symbol  O    H    L    C   | Symbol  Buys   Sells  Net           |
| AAPL  152  153  151  152   | AAPL   12.4K  11.2K  +1.2K         |
| GOOGL 174  175  173  174   | GOOGL   8.1K   9.3K  -1.2K         |
+----------------------------+--------------------------------------+
| PRICE (AAPL)               | ALERTS                               |
| (sparkline)                | 12:34 TSLA vol spike: 5.2K/s         |
|                            | 12:33 GOOGL spread widened            |
+----------------------------+--------------------------------------+
| [q] Quit  [Tab] Symbol  [Space] Pause  [c] Checkpoint            |
+-------------------------------------------------------------------+
```

## Widgets

1. **Header**: Title, status, uptime, throughput
2. **KPI Bar**: Total volume, average VWAP, average spread, total trades
3. **OHLC Table**: Per-symbol price bars (styled table)
4. **Order Flow Table**: Buy/sell volume split with net flow
5. **Sparkline**: Price history for selected symbol (Ratatui Sparkline widget)
6. **Alerts List**: Recent anomaly alerts with timestamps
7. **Footer**: Keyboard shortcuts

## Keyboard Controls

| Key | Action |
|-----|--------|
| `q` / `Esc` | Quit |
| `Tab` | Cycle selected symbol for sparkline |
| `Space` | Pause/resume data generation |

## Dependencies

- `ratatui = "0.29"` with `all-widgets` feature (for Sparkline)
- `crossterm = "0.28"`

## Event Loop

5 FPS (200ms poll interval):
1. `terminal.draw(|f| tui::draw(f, &app))`
2. `crossterm::event::poll(200ms)` -> handle keypress
3. If not paused: push ticks, advance watermarks, drain subscriptions
4. `app.tick(elapsed)` to update derived state

## Files

- `src/tui.rs` - Layout + all widget rendering
- `src/app.rs` - App state struct, ingest methods
