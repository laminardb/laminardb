# F012: Late Data Handling

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F012 |
| **Status** | ✅ Done |
| **Priority** | P2 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F010 |
| **Owner** | Claude |

## Summary

Handle events that arrive after their window has closed (late data). Options include dropping, updating, or routing to a side output.

## Goals

- Configurable allowed lateness
- Update closed windows within lateness period
- Side output for late data
- Metrics for late event tracking

## Implementation

### Core Components

**LateDataConfig** - Configuration for handling late events:
```rust
use laminar_core::operator::window::LateDataConfig;

// Drop late events (default)
let config = LateDataConfig::drop();

// Route late events to a named side output
let config = LateDataConfig::with_side_output("late_events".to_string());
```

**LateDataMetrics** - Tracks late event statistics:
```rust
let metrics = operator.late_data_metrics();
println!("Total late: {}", metrics.late_events_total());
println!("Dropped: {}", metrics.late_events_dropped());
println!("To side output: {}", metrics.late_events_side_output());
```

**Output::SideOutput** - New output variant for routing to named sinks:
```rust
pub enum Output {
    Event(Event),
    Watermark(i64),
    LateEvent(Event),           // No side output configured
    SideOutput { name: String, event: Event },  // Routed to named sink
}
```

### Window Operator Integration

```rust
use laminar_core::operator::window::{
    TumblingWindowAssigner, TumblingWindowOperator, CountAggregator, LateDataConfig,
};
use std::time::Duration;

let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));
let mut operator = TumblingWindowOperator::new(
    assigner,
    CountAggregator::new(),
    Duration::from_secs(5), // 5 second allowed lateness
);

// Configure late events to be routed to a side output
operator.set_late_data_config(LateDataConfig::with_side_output("late_events".to_string()));
```

### Late Event Detection

Events are considered "late" when:
1. The current watermark has passed the window's cleanup time (`window_end + allowed_lateness`)
2. Events within the lateness period are processed normally and can update window state

### SQL Syntax

**ALLOW LATENESS** - Configure allowed lateness duration:
```sql
SELECT COUNT(*) FROM events
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
ALLOW LATENESS INTERVAL '5' MINUTE;
```

**LATE DATA TO** - Route late events to a side output:
```sql
SELECT SUM(amount) FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)
LATE DATA TO late_orders;
```

**Combined** - Both clauses together:
```sql
SELECT AVG(temperature) FROM sensors
GROUP BY TUMBLE(reading_time, INTERVAL '1' HOUR)
ALLOW LATENESS INTERVAL '10' MINUTE
LATE DATA TO late_readings;
```

## Completion Checklist

- [x] Allowed lateness working (events within grace period are processed)
- [x] Side output implemented (Output::SideOutput variant)
- [x] Metrics tracking (LateDataMetrics with total/dropped/side_output counters)
- [x] SQL parsing complete (ALLOW LATENESS, LATE DATA TO)

## Files Changed

- `crates/laminar-core/src/operator/mod.rs` - Added `Output::SideOutput` variant
- `crates/laminar-core/src/operator/window.rs` - Added `LateDataConfig`, `LateDataMetrics`, and late event handling logic
- `crates/laminar-core/src/reactor/mod.rs` - Updated `StdoutSink` to handle `SideOutput`
- `crates/laminar-sql/src/parser/statements.rs` - Added `LateDataClause` AST type
- `crates/laminar-sql/src/parser/parser_simple.rs` - Added `parse_late_data_clause()` function

## Tests Added

### laminar-core (11 new tests)
- `test_late_data_config_default`
- `test_late_data_config_drop`
- `test_late_data_config_with_side_output`
- `test_late_data_metrics_initial`
- `test_late_data_metrics_tracking`
- `test_late_data_metrics_reset`
- `test_window_operator_set_late_data_config`
- `test_late_event_dropped_without_side_output`
- `test_late_event_routed_to_side_output`
- `test_event_within_lateness_not_late`
- `test_reset_late_data_metrics`

### laminar-sql (9 new tests)
- `test_late_data_clause_default`
- `test_late_data_clause_with_allowed_lateness`
- `test_late_data_clause_with_side_output`
- `test_late_data_clause_side_output_only`
- `test_parse_allow_lateness`
- `test_parse_late_data_to`
- `test_parse_allow_lateness_with_late_data_to`
- `test_parse_no_late_data_clause`
- `test_parse_late_data_to_with_semicolon`
