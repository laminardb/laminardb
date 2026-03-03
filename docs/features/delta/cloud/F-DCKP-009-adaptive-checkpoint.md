# F-DCKP-009: Adaptive Checkpoint Interval

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-009 |
| **Status** | Done |
| **Priority** | P2 |
| **Phase** | 7c |
| **Effort** | M (3-4 days) |
| **Dependencies** | F-S3-001, F-CRASH-001 |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-storage`, `laminar-server` |
| **Module** | `crates/laminar-storage/src/checkpoint/adaptive.rs` |

## Summary

Auto-tune checkpoint interval based on observed checkpoint costs and a recovery time target. High-overhead checkpoints reduce the interval to keep recovery time bounded. Low-overhead checkpoints allow the interval to grow, saving I/O and cloud costs.

## Goals

- `AdaptiveCheckpointer`: monitors checkpoint duration, changelog rate (EMA), adjusts interval
- Formula: `ideal_interval = target_recovery_time − last_checkpoint_duration`, clamped to `[min, max]`
- Configurable bounds: `[1s, 5m]` default range
- Metric accessors: `interval_ms()`, `recovery_estimate_ms()`, `changelog_rate_bytes_per_sec()`
- Config: `adaptive = true`, `target_recovery_time = "10s"`, `min_interval = "1s"`, `max_interval = "5m"`

## Non-Goals

- Per-pipeline adaptive intervals (global only)
- Machine learning-based prediction

## Technical Design

### Tuning Algorithm

```rust
fn compute_interval(&self) -> Duration {
    let target = self.config.target_recovery_time;
    let overhead = self.last_checkpoint_duration;
    let ideal = target.saturating_sub(overhead);
    clamp_duration(ideal, self.config.min_interval, self.config.max_interval)
}
```

The model assumes `recovery_time ≈ checkpoint_restore_time + interval`. An exponential moving average (α=0.3) of the changelog rate is maintained for metrics but does not affect the interval directly — the interval depends only on the checkpoint overhead and recovery target.

Initial interval is `target / 2`, clamped to `[min, max]`.

### Config

```toml
[checkpoint]
adaptive = true
target_recovery_time = "10s"
min_interval = "1s"
max_interval = "5m"
```

### API

```rust
pub struct AdaptiveConfig {
    pub target_recovery_time: Duration,
    pub min_interval: Duration,
    pub max_interval: Duration,
}

pub struct AdaptiveCheckpointer { /* ... */ }

impl AdaptiveCheckpointer {
    pub fn new(config: AdaptiveConfig) -> Self;
    pub fn record_checkpoint(&mut self, duration: Duration, changelog_bytes: u64);
    pub fn current_interval(&self) -> Duration;
    pub fn recovery_estimate(&self) -> Duration;
    pub fn interval_ms(&self) -> u64;
    pub fn recovery_estimate_ms(&self) -> u64;
    pub fn changelog_rate_bytes_per_sec(&self) -> f64;
}
```

### Files Modified

| File | Change |
|------|--------|
| `crates/laminar-storage/src/checkpoint/adaptive.rs` | New: `AdaptiveCheckpointer`, `AdaptiveConfig`, tuning algorithm, 14 tests |
| `crates/laminar-storage/src/checkpoint/mod.rs` | Add `pub mod adaptive` |
| `crates/laminar-storage/src/lib.rs` | Re-export `AdaptiveCheckpointer`, `AdaptiveConfig` |
| `crates/laminar-server/src/config.rs` | Add `adaptive`, `target_recovery_time`, `min_interval`, `max_interval` fields to `CheckpointSection` |

## Test Plan

- Initial interval = target/2, clamped to [min, max]
- Fast checkpoint (200ms) → longer interval (9.8s)
- Slow checkpoint (8s) → shorter interval (2s)
- Overhead exceeding target → clamps to min
- Interval never below min
- Interval never above max
- Recovery estimate = overhead + interval
- Metric accessors return correct ms values
- Changelog rate EMA initialized on second checkpoint
- Changelog rate EMA smooths over multiple checkpoints
- Default config is sensible
- Multiple checkpoints converge to steady state
- Doc-test example compiles and runs

## Completion Checklist

- [x] `AdaptiveCheckpointer` implemented with `record_checkpoint()` / `current_interval()` API
- [x] `AdaptiveConfig` struct with `target_recovery_time`, `min_interval`, `max_interval`
- [x] Config fields added to `CheckpointSection`: `adaptive`, `target_recovery_time`, `min_interval`, `max_interval`
- [x] Metric accessors: `interval_ms()`, `recovery_estimate_ms()`, `changelog_rate_bytes_per_sec()`
- [x] Tuning algorithm with `saturating_sub` + clamping
- [x] Changelog rate EMA (α=0.3) for throughput tracking
- [x] 14 unit tests + 1 doc-test (all passing)
- [x] Existing 33 server config tests still pass
- [x] Clippy clean (`-D warnings`) on both `laminar-storage` and `laminar-server`
- [x] Feature INDEX.md updated
