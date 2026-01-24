# F010: Watermarks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F010 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F009 |
| **Owner** | TBD |
| **Completed** | 2026-01-24 |

## Summary

Implement watermarks to track event-time progress and trigger window emissions. Watermarks indicate that no events with timestamps earlier than the watermark are expected.

## Goals

- Generate watermarks from event streams
- Propagate watermarks through operators
- Trigger window emissions on watermark advance
- Support bounded out-of-orderness
- Support multiple input sources with watermark alignment
- Handle idle sources

## Implementation

### Watermark Generation Strategies

| Strategy | Use Case |
|----------|----------|
| `BoundedOutOfOrdernessGenerator` | Allow events to be late by fixed duration |
| `AscendingTimestampsGenerator` | Strictly ordered sources (no lateness) |
| `PeriodicGenerator` | Emit watermarks at fixed wall-clock intervals |
| `PunctuatedGenerator` | Emit based on special marker events |
| `SourceProvidedGenerator` | External sources with embedded watermarks |

### Multi-Source Alignment

`WatermarkTracker` handles watermarks from multiple input sources:
- Tracks minimum watermark across all active sources
- Supports idle source detection and marking
- Ensures no late events are missed from any source

### Key Types

```rust
// Watermark struct with helper methods
pub struct Watermark(pub i64);

impl Watermark {
    pub fn is_late(&self, event_time: i64) -> bool;
    pub fn min(self, other: Self) -> Self;
    pub fn max(self, other: Self) -> Self;
}

// Generator trait
pub trait WatermarkGenerator: Send {
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark>;
    fn on_periodic(&mut self) -> Option<Watermark>;
    fn current_watermark(&self) -> i64;
}

// Multi-source tracker
pub struct WatermarkTracker {
    pub fn update_source(&mut self, source_id: usize, watermark: i64) -> Option<Watermark>;
    pub fn mark_idle(&mut self, source_id: usize) -> Option<Watermark>;
    pub fn check_idle_sources(&mut self) -> Option<Watermark>;
}

// Metrics wrapper
pub struct MeteredGenerator<G: WatermarkGenerator> {
    pub fn metrics(&self) -> &WatermarkMetrics;
    pub fn record_late_event(&mut self);
}
```

### Files

| File | Description |
|------|-------------|
| `crates/laminar-core/src/time/watermark.rs` | Watermark generators and tracker |
| `crates/laminar-core/src/time/mod.rs` | Module exports and Watermark/TimerService types |

## Integration Points

- **Reactor**: Generates watermarks from events and passes to operators
- **Window Operators**: Use watermarks to trigger window emissions
- **Late Detection**: Events with timestamps before watermark are marked late

## Example Usage

```rust
use laminar_core::time::{
    BoundedOutOfOrdernessGenerator, WatermarkGenerator, Watermark,
    WatermarkTracker, MeteredGenerator,
};

// Single source with bounded lateness
let mut gen = BoundedOutOfOrdernessGenerator::new(1000); // 1 second
let wm = gen.on_event(5000);
assert_eq!(wm, Some(Watermark::new(4000)));

// Multi-source tracking
let mut tracker = WatermarkTracker::new(2);
tracker.update_source(0, 5000);
tracker.update_source(1, 3000);
assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));

// Metrics collection
let mut metered = MeteredGenerator::new(gen);
metered.on_event(6000);
let metrics = metered.metrics();
println!("Watermark lag: {}ms", metrics.lag());
```

## Completion Checklist

- [x] Watermark generation working (5 strategies)
- [x] Propagation through operators (via OperatorContext)
- [x] Window triggering tested (TumblingWindowOperator)
- [x] Out-of-order handling verified
- [x] Multi-source alignment (WatermarkTracker)
- [x] Idle source detection
- [x] Metrics collection (MeteredGenerator)
- [x] Unit tests passing (39 watermark-related tests)
