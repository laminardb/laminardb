# F060: Cascading Materialized Views

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F060 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F004, F059, F010 |
| **Owner** | TBD |
| **Research** | [Time-Series Financial Research 2026](../../research/laminardb-timeseries-financial-research-2026.md) |

## Summary

Enable materialized views to read from other materialized views, forming a cascading pipeline. Essential for multi-resolution time-series aggregation (e.g., 1s → 1m → 1h OHLC bars).

## Motivation

From research review:
- **Cascading OHLC bars** - The standard pattern for financial analytics
- **No custom types needed** - Just MVs feeding into other MVs
- **Watermark propagation** - Critical for correctness

**Use Case**: Multi-resolution OHLC bars

```sql
-- Base: 1-second bars from raw trades
CREATE MATERIALIZED VIEW ohlc_1s AS
SELECT
    symbol,
    TUMBLE_START(event_time, INTERVAL '1 second') as bar_time,
    FIRST_VALUE(price) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST_VALUE(price) as close,
    SUM(quantity) as volume
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 second');

-- Derived: 1-minute bars from 1-second bars
CREATE MATERIALIZED VIEW ohlc_1m AS
SELECT
    symbol,
    TUMBLE_START(bar_time, INTERVAL '1 minute') as bar_time,
    FIRST_VALUE(open) as open,    -- First 1s bar's open
    MAX(high) as high,             -- Highest high
    MIN(low) as low,               -- Lowest low
    LAST_VALUE(close) as close,   -- Last 1s bar's close
    SUM(volume) as volume
FROM ohlc_1s                       -- Reads from another MV!
GROUP BY symbol, TUMBLE(bar_time, INTERVAL '1 minute');

-- Derived: 1-hour bars from 1-minute bars
CREATE MATERIALIZED VIEW ohlc_1h AS
SELECT
    symbol,
    TUMBLE_START(bar_time, INTERVAL '1 hour') as bar_time,
    FIRST_VALUE(open) as open,
    MAX(high) as high,
    MIN(low) as low,
    LAST_VALUE(close) as close,
    SUM(volume) as volume
FROM ohlc_1m                       -- Cascading dependency
GROUP BY symbol, TUMBLE(bar_time, INTERVAL '1 hour');
```

## Goals

1. MVs can subscribe to other MVs as input sources
2. Proper watermark propagation through MV chains
3. Topological ordering of MV updates (dependencies first)
4. Cycle detection to prevent infinite loops
5. Consistent state during checkpoints

## Non-Goals

- General-purpose MV infrastructure (Phase 3+)
- Historical backfill on MV creation (see F061)
- MV query rewriting/optimization
- MV schema evolution

## Technical Design

### MV Registry

```rust
/// Materialized View definition.
#[derive(Debug, Clone)]
pub struct MaterializedView {
    /// Unique view name
    pub name: String,
    /// SQL definition (for reference)
    pub sql: String,
    /// Input sources (tables or other MVs)
    pub sources: Vec<String>,
    /// Output schema
    pub schema: SchemaRef,
    /// Associated operator ID
    pub operator_id: String,
    /// View state (running, paused, error)
    pub state: MvState,
}

/// MV execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvState {
    /// View is actively processing
    Running,
    /// View is paused (e.g., waiting for dependency)
    Paused,
    /// View encountered an error
    Error,
}

/// Registry for managing materialized views.
pub struct MvRegistry {
    /// All registered MVs by name
    views: HashMap<String, MaterializedView>,
    /// Dependency graph: MV name -> MVs that depend on it
    dependents: HashMap<String, HashSet<String>>,
    /// Reverse dependency graph: MV name -> MVs it depends on
    dependencies: HashMap<String, HashSet<String>>,
    /// Topological order for processing
    topo_order: Vec<String>,
}

impl MvRegistry {
    /// Register a new materialized view.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - View name already exists
    /// - Source MV doesn't exist
    /// - Would create a dependency cycle
    pub fn register(&mut self, view: MaterializedView) -> Result<(), MvError> {
        // Check for duplicate name
        if self.views.contains_key(&view.name) {
            return Err(MvError::DuplicateName(view.name.clone()));
        }

        // Validate sources exist
        for source in &view.sources {
            if !self.views.contains_key(source) && !self.is_base_table(source) {
                return Err(MvError::SourceNotFound(source.clone()));
            }
        }

        // Check for cycles
        if self.would_create_cycle(&view.name, &view.sources) {
            return Err(MvError::CycleDetected(view.name.clone()));
        }

        // Update dependency graphs
        for source in &view.sources {
            self.dependents
                .entry(source.clone())
                .or_default()
                .insert(view.name.clone());
            self.dependencies
                .entry(view.name.clone())
                .or_default()
                .insert(source.clone());
        }

        self.views.insert(view.name.clone(), view);
        self.update_topo_order();

        Ok(())
    }

    /// Get MVs in topological order (dependencies first).
    pub fn topo_order(&self) -> &[String] {
        &self.topo_order
    }

    /// Get all MVs that depend on the given source.
    pub fn get_dependents(&self, source: &str) -> impl Iterator<Item = &str> {
        self.dependents
            .get(source)
            .into_iter()
            .flatten()
            .map(String::as_str)
    }

    fn would_create_cycle(&self, new_name: &str, sources: &[String]) -> bool {
        // DFS to check if any source transitively depends on new_name
        let mut visited = HashSet::new();
        let mut stack = sources.to_vec();

        while let Some(current) = stack.pop() {
            if current == new_name {
                return true;
            }
            if visited.insert(current.clone()) {
                if let Some(deps) = self.dependencies.get(&current) {
                    stack.extend(deps.iter().cloned());
                }
            }
        }

        false
    }

    fn update_topo_order(&mut self) {
        // Kahn's algorithm for topological sort
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        // Initialize in-degrees
        for name in self.views.keys() {
            let deps = self.dependencies.get(name).map_or(0, HashSet::len);
            in_degree.insert(name.clone(), deps);
            if deps == 0 {
                queue.push_back(name.clone());
            }
        }

        // Process
        self.topo_order.clear();
        while let Some(name) = queue.pop_front() {
            self.topo_order.push(name.clone());

            if let Some(dependents) = self.dependents.get(&name) {
                for dep in dependents {
                    if let Some(count) = in_degree.get_mut(dep) {
                        *count -= 1;
                        if *count == 0 {
                            queue.push_back(dep.clone());
                        }
                    }
                }
            }
        }
    }
}
```

### Watermark Propagation

```rust
/// Watermark tracker for cascading MVs.
pub struct CascadingWatermarkTracker {
    /// Current watermark per MV
    watermarks: HashMap<String, i64>,
    /// Registry reference for dependency lookup
    registry: Arc<MvRegistry>,
}

impl CascadingWatermarkTracker {
    /// Update watermark for an MV and propagate to dependents.
    ///
    /// Returns MVs whose watermarks were updated (in dependency order).
    pub fn update_watermark(&mut self, mv_name: &str, watermark: i64) -> Vec<(String, i64)> {
        let mut updated = Vec::new();

        // Update this MV's watermark
        let old = self.watermarks.insert(mv_name.to_string(), watermark);
        if old != Some(watermark) {
            updated.push((mv_name.to_string(), watermark));
        }

        // Propagate to dependents
        self.propagate_watermarks(mv_name, &mut updated);

        updated
    }

    fn propagate_watermarks(&mut self, source: &str, updated: &mut Vec<(String, i64)>) {
        for dependent in self.registry.get_dependents(source) {
            // Dependent's watermark = min of all source watermarks
            let deps = self.registry.dependencies.get(dependent);
            if let Some(deps) = deps {
                let min_watermark = deps
                    .iter()
                    .filter_map(|d| self.watermarks.get(d))
                    .min()
                    .copied()
                    .unwrap_or(i64::MIN);

                let old = self.watermarks.insert(dependent.to_string(), min_watermark);
                if old != Some(min_watermark) {
                    updated.push((dependent.to_string(), min_watermark));
                    // Recursively propagate
                    self.propagate_watermarks(dependent, updated);
                }
            }
        }
    }
}
```

### MV Pipeline Executor

```rust
/// Executor for cascading MV pipelines.
pub struct MvPipelineExecutor {
    /// MV registry
    registry: Arc<MvRegistry>,
    /// Operators per MV
    operators: HashMap<String, Box<dyn Operator>>,
    /// Watermark tracker
    watermarks: CascadingWatermarkTracker,
    /// Output queues per MV (for downstream consumption)
    outputs: HashMap<String, VecDeque<Event>>,
}

impl MvPipelineExecutor {
    /// Process an event from a base source.
    ///
    /// Propagates through all dependent MVs in topological order.
    pub fn process_source_event(
        &mut self,
        source: &str,
        event: Event,
        ctx: &mut OperatorContext,
    ) -> Result<(), MvError> {
        // Queue the event for processing
        self.outputs
            .entry(source.to_string())
            .or_default()
            .push_back(event);

        // Process MVs in topological order
        for mv_name in self.registry.topo_order() {
            self.process_mv_inputs(mv_name, ctx)?;
        }

        Ok(())
    }

    fn process_mv_inputs(
        &mut self,
        mv_name: &str,
        ctx: &mut OperatorContext,
    ) -> Result<(), MvError> {
        let view = self.registry.views.get(mv_name).ok_or_else(|| {
            MvError::ViewNotFound(mv_name.to_string())
        })?;

        // Collect inputs from all sources
        let mut inputs = Vec::new();
        for source in &view.sources {
            if let Some(queue) = self.outputs.get_mut(source) {
                inputs.extend(queue.drain(..));
            }
        }

        // Process through operator
        let operator = self.operators.get_mut(mv_name).ok_or_else(|| {
            MvError::OperatorNotFound(mv_name.to_string())
        })?;

        for input in inputs {
            let outputs = operator.process(&input, ctx);
            for output in outputs {
                if let Output::Event(event) = output {
                    self.outputs
                        .entry(mv_name.to_string())
                        .or_default()
                        .push_back(event);
                }
            }
        }

        Ok(())
    }

    /// Checkpoint all MVs in the pipeline.
    pub fn checkpoint(&self) -> MvPipelineCheckpoint {
        MvPipelineCheckpoint {
            operator_states: self.operators
                .iter()
                .map(|(name, op)| (name.clone(), op.checkpoint()))
                .collect(),
            watermarks: self.watermarks.watermarks.clone(),
        }
    }
}
```

### SQL Integration

```sql
-- Syntax for cascading MV
CREATE MATERIALIZED VIEW <name> AS
<select_stmt>
[EMIT AFTER WATERMARK | EMIT PERIODICALLY <interval> | EMIT ON UPDATE]
[ALLOW LATENESS <interval>];

-- Example: cascading OHLC
CREATE MATERIALIZED VIEW ohlc_1m AS
SELECT
    symbol,
    TUMBLE_START(bar_time, INTERVAL '1 minute') as bar_time,
    FIRST_VALUE(open) as open,
    MAX(high) as high,
    MIN(low) as low,
    LAST_VALUE(close) as close,
    SUM(volume) as volume
FROM ohlc_1s  -- References another MV
GROUP BY symbol, TUMBLE(bar_time, INTERVAL '1 minute')
EMIT AFTER WATERMARK;

-- Drop with cascade
DROP MATERIALIZED VIEW ohlc_1m CASCADE;
-- Drops ohlc_1m and all MVs that depend on it

-- Show dependencies
SHOW DEPENDENCIES FOR ohlc_1h;
-- Returns: ohlc_1h -> ohlc_1m -> ohlc_1s -> trades
```

## Implementation Phases

### Phase 1: MV Registry (3-4 days)

1. Implement `MaterializedView` struct
2. Implement `MvRegistry` with dependency tracking
3. Cycle detection
4. Topological ordering
5. Unit tests for registry operations

### Phase 2: Watermark Propagation (2-3 days)

1. Implement `CascadingWatermarkTracker`
2. Min-watermark propagation logic
3. Tests for multi-level cascading
4. Tests for multi-source MVs

### Phase 3: Pipeline Executor (3-4 days)

1. Implement `MvPipelineExecutor`
2. Topological event processing
3. Output routing between MVs
4. Checkpoint/restore for pipeline state
5. Integration tests

### Phase 4: SQL Integration (2-3 days)

1. Parser support for `CREATE MATERIALIZED VIEW`
2. `FROM <mv_name>` resolution
3. `DROP ... CASCADE` support
4. `SHOW DEPENDENCIES` command

## Test Cases

```rust
#[test]
fn test_simple_cascade() {
    // trades -> ohlc_1s -> ohlc_1m
    let mut registry = MvRegistry::new();

    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
    registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();

    assert_eq!(registry.topo_order(), &["ohlc_1s", "ohlc_1m"]);
}

#[test]
fn test_cycle_detection() {
    let mut registry = MvRegistry::new();

    registry.register(mv("a", vec!["base"])).unwrap();
    registry.register(mv("b", vec!["a"])).unwrap();

    // This would create: base -> a -> b -> a (cycle)
    let result = registry.register(mv("a_v2", vec!["b"]));
    // Actually this is fine because a_v2 != a

    // Real cycle: c depends on itself
    registry.register(mv("c", vec!["a"])).unwrap();
    // Try to make a depend on c (a -> c -> a would be cycle via rename)
}

#[test]
fn test_watermark_propagation() {
    // Setup: trades -> ohlc_1s -> ohlc_1m -> ohlc_1h
    let mut tracker = setup_cascade_tracker();

    // Update base watermark
    let updated = tracker.update_watermark("trades", 60_000);

    // All downstream MVs should update
    assert!(updated.iter().any(|(n, _)| n == "ohlc_1s"));
    assert!(updated.iter().any(|(n, _)| n == "ohlc_1m"));
    assert!(updated.iter().any(|(n, _)| n == "ohlc_1h"));
}

#[test]
fn test_multi_source_watermark() {
    // MV with multiple sources: combined = f(source_a, source_b)
    // combined's watermark = min(source_a.wm, source_b.wm)
    let mut tracker = setup_multi_source_tracker();

    tracker.update_watermark("source_a", 100);
    assert_eq!(tracker.watermarks.get("combined"), Some(&i64::MIN)); // b not yet set

    tracker.update_watermark("source_b", 80);
    assert_eq!(tracker.watermarks.get("combined"), Some(&80)); // min(100, 80)

    tracker.update_watermark("source_b", 120);
    assert_eq!(tracker.watermarks.get("combined"), Some(&100)); // min(100, 120)
}

#[test]
fn test_cascading_ohlc() {
    // Full integration test: trades -> 1s -> 1m OHLC
    let mut executor = setup_ohlc_pipeline();

    // Send trades
    executor.process_source_event("trades", trade(100.0, 1000), &mut ctx);
    executor.process_source_event("trades", trade(105.0, 1500), &mut ctx);
    executor.process_source_event("trades", trade(98.0, 2000), &mut ctx);

    // Advance watermark past 1s window
    executor.advance_watermark("trades", 3000);

    // Check 1s output
    let ohlc_1s = executor.outputs.get("ohlc_1s").unwrap();
    assert_eq!(ohlc_1s.len(), 1);
    // open=100, high=105, low=98, close=98
}
```

## Acceptance Criteria

- [ ] MV registry with dependency tracking
- [ ] Cycle detection on registration
- [ ] Topological ordering of MV processing
- [ ] Watermark propagation through MV chains
- [ ] Multi-source MV watermark (min semantics)
- [ ] Checkpoint/restore of MV pipeline state
- [ ] SQL `CREATE MATERIALIZED VIEW` with MV sources
- [ ] SQL `DROP ... CASCADE` support
- [ ] 15+ unit tests passing
- [ ] Cascading OHLC example working

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Event propagation (per MV level) | < 10μs | Dominated by operator processing |
| Watermark propagation | < 1μs per MV | Simple min computation |
| Registry lookup | O(1) | HashMap |
| Topo sort (on registration) | O(V + E) | One-time on DDL |

## Future Enhancements

1. **Parallel MV Processing**: Process independent MVs concurrently
2. **Incremental Topo Sort**: Update order without full recomputation
3. **MV Hints**: Allow user to specify processing priority
4. **Historical Backfill**: F061 - populate MV from historical data on creation

## References

- [Time-Series Financial Research 2026](../../research/laminardb-timeseries-financial-research-2026.md)
- Flink Cascading Windows
- RisingWave MV Dependencies
