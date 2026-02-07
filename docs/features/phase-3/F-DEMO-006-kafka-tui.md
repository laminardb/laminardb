# F-DEMO-006: Kafka Mode TUI Dashboard

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DEMO-006 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (2-3 days) |
| **Dependencies** | F-DEMO-002 (Ratatui TUI), F025/F026 (Kafka Connectors) |

## Summary

Wire the existing Ratatui TUI dashboard to work in Kafka mode. Currently Kafka
mode (`DEMO_MODE=kafka`) produces data to Redpanda and runs the pipeline but
has no interactive terminal dashboard — it just prints stats to stderr. The
embedded mode TUI should work identically in Kafka mode since both use the
same `db.subscribe()` API for consuming pipeline output.

## Motivation

The TUI dashboard is LaminarDB's primary showcase. Without it in Kafka mode,
there is no visual demonstration of the production pipeline. Since
`db.subscribe()` works the same in both modes (stream subscription channels),
the TUI can be shared with minimal changes.

## Current State

- **Embedded mode**: Full TUI with OHLC, order flow, sparklines, alerts, DAG
  view, order book, system stats. Works perfectly.
- **Kafka mode**: Produces initial batch to Redpanda, subscribes to 6 streams,
  but the main loop only prints stats and has no TUI rendering.

The gap is that `run_kafka_mode()` duplicates the event loop logic instead of
reusing `run_loop()`, and it skips terminal setup entirely.

## Design

### Refactor: Shared Event Loop

Extract the TUI event loop into a mode-agnostic function:

```rust
/// Shared TUI event loop for both embedded and Kafka modes.
fn run_tui_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    stats_collector: &mut StatsCollector,
    data_source: &mut dyn PipelineDataSource,
    subscriptions: &Subscriptions,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        terminal.draw(|f| tui::draw(f, app))?;

        if event::poll(Duration::from_millis(200))? {
            // Handle keyboard input (same for both modes)
        }

        if !app.paused {
            data_source.push_cycle(app)?;
            drain_subscriptions(app, subscriptions);
        }

        app.update_system_stats(stats_collector.refresh());
    }
}
```

### PipelineDataSource Trait

Abstract the data generation/push logic:

```rust
trait PipelineDataSource {
    /// Generate and push one cycle of data.
    fn push_cycle(&mut self, app: &mut App) -> Result<(), Box<dyn std::error::Error>>;
}

/// Embedded mode: generate synthetic data and push via SourceHandle
struct EmbeddedDataSource {
    generator: MarketGenerator,
    tick_source: SourceHandle<MarketTick>,
    order_source: SourceHandle<OrderEvent>,
    book_source: SourceHandle<OrderBookUpdate>,
}

/// Kafka mode: generate data and produce to Redpanda topics
struct KafkaDataSource {
    generator: MarketGenerator,
    producer: FutureProducer,
}
```

### Subscriptions Struct

Bundle all subscription handles:

```rust
struct Subscriptions {
    ohlc: TypedSubscription<OhlcBar>,
    volume: TypedSubscription<VolumeMetrics>,
    spread: TypedSubscription<SpreadMetrics>,
    anomaly: TypedSubscription<AnomalyAlert>,
    imbalance: TypedSubscription<BookImbalanceMetrics>,
    depth: TypedSubscription<DepthMetrics>,
    // New streams from production SQL pipelines:
    rolling_spread: Option<TypedSubscription<RollingSpread>>,
    session_activity: Option<TypedSubscription<SessionActivity>>,
    price_momentum: Option<TypedSubscription<PriceMomentum>>,
    enriched_orders: Option<TypedSubscription<EnrichedOrder>>,
    top_movers: Option<TypedSubscription<TopMover>>,
}
```

### New TUI Panels

Add panels for the production pipeline streams:

1. **Rolling Spread** (from HOP window): Per-symbol avg/min/max spread over 10s
2. **Session Activity** (from SESSION window): Active trading sessions
3. **Price Momentum** (from LAG/LEAD): Price delta, direction indicator
4. **Enriched Orders** (from ASOF JOIN): Orders with market context and slippage
5. **Top Movers** (from ROW_NUMBER): Ranked symbols by volatility/range

### Checkpoint Controls

Add TUI controls for checkpoint demonstration:

- `[c]` — Trigger manual checkpoint, display epoch number
- Status bar shows: last checkpoint epoch, time since last checkpoint
- On startup: display "Recovering from epoch N..." if restoring from checkpoint

### Kafka Consumer Lag Display

If available, show Kafka consumer lag in the header:

```
Running | Kafka: 3 sources | Lag: 42 msgs | Ckpt: epoch 7 (30s ago)
```

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `main` (compile) | 1 | Kafka mode compiles and runs with TUI |
| `app` | 3 | New subscription types, checkpoint status display |
| `tui` | 3 | New panels render correctly, checkpoint status |

## Files

- `examples/demo/src/main.rs` — Refactor run_kafka_mode(), shared event loop
- `examples/demo/src/app.rs` — Add checkpoint status, new stream types
- `examples/demo/src/tui.rs` — Add new panels for HOP/SESSION/LAG streams
- `examples/demo/src/types.rs` — New output types (RollingSpread, etc.)
