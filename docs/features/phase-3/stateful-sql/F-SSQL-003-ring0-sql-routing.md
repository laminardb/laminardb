# F-SSQL-003: Ring 0 SQL Operator Routing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SSQL-003 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 3 |
| **Effort** | M-XL |
| **Dependencies** | F-SSQL-001, F-SSQL-002, F075, F082 |
| **Owner** | TBD |
| **Created** | 2026-02-25 |
| **Updated** | 2026-02-25 |

## Summary

Route SQL windowed aggregate queries through Ring 0's stateful operators instead of reimplementing windowed aggregation in `StreamExecutor`. Currently, SQL-path queries (`CREATE STREAM ... AS SELECT ... GROUP BY ... EMIT ON WINDOW CLOSE`) execute entirely within DataFusion's micro-batch engine via `StreamExecutor`, duplicating the stateful window logic already implemented in Ring 0 operators (`TumblingWindowOperator`, `SessionWindowOperator`, etc.). This feature detects windowed aggregate SQL queries at `add_query()` time, builds a Ring 0 DAG sub-pipeline using `DataFusionAggregateFactory` from F075, and wires the existing `sql_emit_to_core()` / `emit_clause_to_core()` bridge functions (currently dead code).

## Goals

- SQL windowed aggregation queries produce identical results to equivalent programmatic DAG API queries
- Unify the two execution paths so there is a single source of truth for window semantics
- Wire the dead-code bridge functions `sql_emit_to_core()` and `emit_clause_to_core()`
- Support tumbling windows first (Phase 1), then hopping+session (Phase 2), then full SQL passthrough (Phase 3)
- Ring 0 operators handle checkpointing via `StateStore` (no StreamExecutor checkpoint needed for routed queries)

## Non-Goals

- Replacing DataFusion entirely for the SQL path (projection, filter, join still go through DataFusion)
- Modifying Ring 0 operators (they are already production-ready)
- Supporting non-aggregate SQL queries through Ring 0 (those stay in DataFusion)

## Technical Design

### Architecture

**Ring**: Ring 0 (operators) + Ring 1 (SQL bridge)
**Crates**: `laminar-db`, `laminar-sql`, `laminar-core`

The key architectural change is splitting `StreamExecutor` query handling into two paths:

```
SQL Query
    │
    ▼
Plan Analysis (detect windowed aggregate)
    │
    ├─ YES: Build Ring 0 sub-pipeline
    │        ├─ DataFusionAccumulatorAdapter (F075) for accumulator creation
    │        ├─ TumblingWindowOperator / SessionWindowOperator (Ring 0)
    │        ├─ StateStore for persistence
    │        └─ Results routed back to StreamExecutor output
    │
    └─ NO: Existing DataFusion micro-batch path
           (projection, filter, join, non-windowed aggregation)
```

### Detection Logic

```rust
/// Analyze a SQL query to determine if it should be routed to Ring 0.
///
/// Returns `Some(Ring0Config)` if the query is a windowed aggregate,
/// `None` if it should stay in the DataFusion micro-batch path.
fn detect_ring0_candidate(
    sql: &str,
    emit_clause: &Option<EmitClause>,
    ctx: &SessionContext,
) -> Result<Option<Ring0QueryConfig>, DbError> {
    // 1. Parse SQL to DataFusion LogicalPlan
    // 2. Walk plan tree for Aggregate node
    // 3. Check for window function (TUMBLE, HOP, SESSION in GROUP BY)
    // 4. Extract window parameters, group-by keys, aggregate functions
    // 5. If all conditions met: return Ring0QueryConfig
}

/// Configuration for routing a SQL query to Ring 0.
struct Ring0QueryConfig {
    /// Window type and parameters.
    window: WindowOperatorConfig,
    /// Aggregate functions to create via DataFusionAggregateFactory.
    aggregates: Vec<AggregateSpec>,
    /// Group-by key column indices.
    group_keys: Vec<usize>,
    /// Emit strategy (from EMIT clause or default).
    emit_strategy: laminar_core::operator::window::EmitStrategy,
    /// Source table name for input events.
    source_table: String,
    /// Output schema.
    output_schema: SchemaRef,
}
```

### Pipeline Construction

```rust
impl StreamExecutor {
    /// Build a Ring 0 sub-pipeline for a windowed aggregate query.
    fn build_ring0_pipeline(
        &mut self,
        query_name: &str,
        config: Ring0QueryConfig,
    ) -> Result<(), DbError> {
        // 1. Create DataFusionAccumulatorAdapter for each aggregate
        let accumulators = config.aggregates.iter()
            .map(|spec| DataFusionAggregateFactory::create(spec))
            .collect::<Result<Vec<_>, _>>()?;

        // 2. Create the window operator
        let operator = match config.window.window_type {
            WindowType::Tumbling { size } => {
                TumblingWindowOperator::new(
                    size,
                    config.group_keys,
                    accumulators,
                    config.emit_strategy,
                )
            }
            // Phase 2: Hopping, Session
            _ => return Err(DbError::unsupported("Only tumbling windows supported")),
        };

        // 3. Create a StateStore for this operator
        let state_store = InMemoryStore::new();

        // 4. Register the sub-pipeline
        self.ring0_pipelines.insert(query_name.to_string(), Ring0Pipeline {
            operator: Box::new(operator),
            state_store,
            config,
        });

        Ok(())
    }
}
```

### Execution Flow

```rust
impl StreamExecutor {
    /// Execute a Ring 0-routed query for one micro-batch cycle.
    fn execute_ring0_query(
        &mut self,
        query_name: &str,
        source_batches: &[RecordBatch],
        watermark_ms: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let pipeline = self.ring0_pipelines.get_mut(query_name)
            .ok_or_else(|| DbError::internal("Ring 0 pipeline not found"))?;

        let mut output = Vec::new();

        // 1. Feed source events to the Ring 0 operator
        for batch in source_batches {
            for row_idx in 0..batch.num_rows() {
                let event = Event::from_batch_row(batch, row_idx, &pipeline.config)?;
                let emitted = pipeline.operator.process_event(
                    event,
                    &mut pipeline.state_store,
                )?;
                output.extend(emitted);
            }
        }

        // 2. Advance watermark (may trigger window close emissions)
        let watermark_emitted = pipeline.operator.advance_watermark(
            watermark_ms,
            &mut pipeline.state_store,
        )?;
        output.extend(watermark_emitted);

        // 3. Convert Ring 0 output to RecordBatch
        batches_from_events(output, &pipeline.config.output_schema)
    }
}
```

### Bridge Function Wiring

The existing dead-code bridge functions become live:

```rust
// In add_query():
if let Some(emit_clause) = &parsed_emit {
    let core_emit = emit_clause_to_core(emit_clause)?;  // Currently dead code → now live
    ring0_config.emit_strategy = core_emit;
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DbError::Unsupported` | Non-tumbling window in Phase 1 | Fall back to StreamExecutor EOWC path |
| `DbError::InvalidQuery` | Window function not in GROUP BY | Return error at `add_query` time |
| `DbError::Execution` | Ring 0 operator process_event fails | Propagate to caller; log batch details |
| `DbError::Internal` | Schema mismatch between SQL plan and Ring 0 event format | Fail fast at pipeline construction |

## Test Plan

### Unit Tests

- [ ] `test_detect_tumbling_aggregate_returns_ring0_config`
- [ ] `test_detect_non_windowed_aggregate_returns_none`
- [ ] `test_detect_projection_only_returns_none`
- [ ] `test_ring0_tumbling_sum_matches_eowc_path`
- [ ] `test_ring0_tumbling_multi_aggregate_multi_group`
- [ ] `test_emit_clause_to_core_all_variants`
- [ ] `test_ring0_pipeline_checkpoint_via_state_store`

### Integration Tests

- [ ] End-to-end: SQL tumbling window query produces same results via Ring 0 as via StreamExecutor EOWC
- [ ] End-to-end: Ring 0 checkpoint/restore preserves window state across restart
- [ ] Regression: non-windowed queries still work through DataFusion path

### Benchmarks

- [ ] `bench_ring0_vs_eowc_tumbling` - Target: Ring 0 path 5-10x faster
- [ ] `bench_ring0_throughput` - Target: 500K events/sec/core

## Rollout Plan

1. **Phase 1 (M)**: Tumbling window routing — detection, pipeline construction, execution, tests
2. **Phase 2 (L)**: Hopping + session window support — multi-window assignment, session merge
3. **Phase 3 (XL)**: Full SQL passthrough — complex expressions, HAVING, ORDER BY within windows
4. **Phase 4**: Remove `IncrementalEowcState` fallback for routed queries
5. **Phase 5**: Code review + merge

## Open Questions

- [ ] Should Ring 0-routed queries bypass DataFusion entirely, or use DataFusion for expression evaluation (e.g., `price * quantity`) before feeding to Ring 0 operators?
- [ ] How should we handle queries that mix windowed aggregation with post-aggregation filters (HAVING)?
- [ ] Should Ring 0 sub-pipelines share a `StateStore` instance or have isolated stores?
- [ ] When should we fall back to the StreamExecutor path vs. returning an error for unsupported patterns?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

- This is the most architecturally significant feature in the stateful SQL series. It unifies the two execution paths that have diverged since Phase 1.
- The phased rollout (tumbling → hopping → session → full) allows shipping value incrementally.
- The dead-code bridge functions (`sql_emit_to_core`, `emit_clause_to_core`) at `stream_executor.rs:39-63` were specifically written for this integration.
- Consider using `DataFusionAggregateFactory` from F075 for accumulator creation, ensuring compatibility with all 50+ DataFusion built-in aggregates.

## References

- [F075: DataFusion Aggregate Bridge](../../phase-2/F075-datafusion-aggregate-bridge.md)
- [F082: Streaming Query Lifecycle](../../plan-compiler/F082-streaming-query-lifecycle.md)
- [Gap Analysis: Gap 3 — Ring 0 Operators Not Wired](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)
- [stream_executor.rs — bridge functions](../../../../crates/laminar-db/src/stream_executor.rs)
- [window.rs — TumblingWindowOperator](../../../../crates/laminar-core/src/operator/window.rs)
