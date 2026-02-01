# F-DEMO-004: DAG Pipeline Visualization

## Overview

| Field | Value |
|-------|-------|
| **ID** | F-DEMO-004 |
| **Title** | DAG Pipeline Visualization |
| **Phase** | 3 (Connectors & Integration) |
| **Priority** | P1 |
| **Status** | Draft |
| **Dependencies** | F-DEMO-002 (Ratatui TUI), F-DAG-008 (Pipeline Introspection API) |

## Description

Tab-switchable DAG view in the demo TUI. Press `[d]` to toggle between the main dashboard and a pipeline topology visualization. Renders the pipeline graph (sources -> streams -> sinks) using box-drawing characters and Ratatui styled spans. Derives layout from `LaminarDB::pipeline_topology()` (F-DAG-008) -- not hardcoded.

## Layout

```
+-- LAMINARDB MARKET DATA DEMO ---------- Running | 02:34 | 12K/s --+
+-- Vol: 1.2M | VWAP: $152.34 | Spread: 0.05 | Trades: 8.4K ------+
|                                                                    |
|   [market_ticks] SOURCE        [order_events] SOURCE               |
|   7 columns                    6 columns                           |
|        |                                                           |
|     +--+--+--------+                                               |
|     |  |  |        |                                               |
|     v  v  v        v                                               |
|   [ohlc_bars]  [volume_metrics]  [spread_metrics]  [anomaly_alerts]|
|     STREAM       STREAM           STREAM            STREAM         |
|     |            |                |                 |              |
|     v            v                v                 v              |
|   [ohlc_output]  [volume_output]  [spread_output]  [anomaly_output]|
|     SINK          SINK            SINK              SINK           |
|                                                                    |
+-- [q] Quit [Tab] Symbol [Space] Pause [d] Dashboard ----------+
```

## Rendering

- `Paragraph` with styled `Line`/`Span` vectors (no external graph crate)
- Box-drawing: vertical pipes and arrows for edges
- Colors: Cyan = Source, Green = Stream, Yellow = Sink
- Node boxes: name + type label + column count (for sources)
- Static layout -- pipeline structure does not change at runtime
- Layout algorithm: layer-based (sources on top, streams middle, sinks bottom)

## Keyboard Controls

| Key | Action |
|-----|--------|
| `d` | Toggle between dashboard and DAG view |

Footer updates to show `[d] Dashboard` when in DAG view, `[d] Pipeline` when in dashboard view.

## Integration with F-DAG-008

```rust
// After db.start():
let topology = db.pipeline_topology();
app.set_topology(topology);

// In event loop:
KeyCode::Char('d') => app.toggle_dag(),

// In draw():
if app.show_dag {
    draw_dag(f, app);
} else {
    draw_main_content(f, app, main_area);
}
```

## Files

- `src/app.rs` -- Add `show_dag: bool`, `topology: Option<PipelineTopology>`, `toggle_dag()`, `set_topology()`
- `src/tui.rs` -- Add `draw_dag()`, update `draw()` to branch, update footer
- `src/main.rs` -- Add `'d'` keybinding, pass topology after `db.start()`
