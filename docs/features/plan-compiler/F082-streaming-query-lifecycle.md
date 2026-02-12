# F082: Streaming Query Lifecycle

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F082 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2.5 (Plan Compiler) |
| **Effort** | L (4-5 days) |
| **Dependencies** | F081, F005 (DataFusion Integration), F001 (Reactor) |
| **Owner** | TBD |

## Summary

The end-to-end orchestration layer that takes a SQL string and produces a running compiled streaming query. Wires together DataFusion planning, pipeline extraction, Cranelift compilation, Ring 0/Ring 1 bridge setup, and state store initialization into a single `StreamingQuery` object with lifecycle management (start, checkpoint, hot-swap, stop).

## Motivation

Today, executing a streaming SQL query in LaminarDB requires manual wiring:

1. Parse SQL via `parse_streaming_sql()`
2. Plan via `StreamingPlanner::plan()`
3. Extract window/join config from `QueryPlan`
4. Manually create operators, state stores, and the reactor
5. Submit events to the reactor

With the plan compiler, there are more moving parts — compiled pipelines, bridges, Ring 1 operators — that need coordinated lifecycle management. The `StreamingQuery` abstraction handles this complexity:

```rust
let query = StreamingQuery::compile(
    "SELECT symbol, AVG(price) FROM trades
     WHERE exchange = 'NYSE'
     GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE)
     EMIT ON WINDOW CLOSE"
)?;
query.start();
query.submit(event);  // Ring 0 compiled pipeline executes
// ... Ring 1 window triggers on watermark ...
let output = query.poll_output();
```

## Goals

1. `StreamingQuery` — top-level object: compile, start, submit events, poll output, checkpoint, stop
2. `QueryCompilationPlan` — the intermediate representation between SQL parsing and runtime construction
3. Query hot-swap: compile a new query version while the old one is running, then atomically switch
4. Observability: metrics for compilation time, per-pipeline throughput, Ring 0/Ring 1 split
5. Graceful degradation: if Cranelift is unavailable (e.g., `jit` feature not enabled), fall back to pure DataFusion execution

## Non-Goals

- Multi-query optimization (each query compiled independently)
- Distributed query execution (single-node only)
- State migration between incompatible query versions (schema changes require restart)

## Technical Design

### Query Compilation Flow

```
                    ┌─────────────────────────────┐
                    │         SQL String           │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │    DataFusion: parse +       │
                    │    optimize + LogicalPlan    │
                    │    (laminar-sql planner)     │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   PipelineExtractor:         │
                    │   identify pipelines and     │
                    │   pipeline breakers          │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   PipelineCompiler:          │
                    │   Cranelift JIT per pipeline │
                    │   (or fallback to DF)        │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   RuntimeBuilder:            │
                    │   wire compiled pipelines    │
                    │   + bridges + Ring 1 ops     │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     StreamingQuery           │
                    │   (ready to process events)  │
                    └─────────────────────────────┘
```

### StreamingQuery

```rust
/// A compiled streaming query ready for event processing.
///
/// Encapsulates the entire execution pipeline: compiled Ring 0 functions,
/// Ring 0/Ring 1 bridge, and Ring 1 stateful operators.
pub struct StreamingQuery {
    /// Query identifier.
    id: QueryId,
    /// Original SQL (for debugging/display).
    sql: String,
    /// Compiled Ring 0 pipelines.
    compiled_pipelines: Vec<ExecutablePipeline>,
    /// Ring 0 → Ring 1 bridges (one per pipeline that feeds a breaker).
    bridges: Vec<PipelineBridge>,
    /// Ring 1 operators (windows, joins, aggregations).
    ring1_operators: Vec<Box<dyn Operator>>,
    /// Ring 1 bridge consumers.
    bridge_consumers: Vec<BridgeConsumer>,
    /// State store for Ring 1 operators.
    state_store: Box<dyn StateStore>,
    /// Row schemas per pipeline.
    schemas: Vec<Arc<RowSchema>>,
    /// Pre-allocated output row buffer for Ring 0.
    output_row_buffer: Vec<u8>,
    /// Compilation metadata.
    metadata: QueryMetadata,
    /// Query state.
    state: QueryState,
}

/// Unique query identifier.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct QueryId(pub u64);

/// Query runtime state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    /// Compiled but not started.
    Ready,
    /// Actively processing events.
    Running,
    /// Paused (e.g., for checkpoint).
    Paused,
    /// Stopped (terminal).
    Stopped,
}

/// Compilation and runtime metadata.
#[derive(Debug)]
pub struct QueryMetadata {
    /// Time spent in DataFusion planning.
    pub plan_time: std::time::Duration,
    /// Time spent in pipeline extraction.
    pub extract_time: std::time::Duration,
    /// Time spent in Cranelift compilation.
    pub compile_time: std::time::Duration,
    /// Number of compiled pipelines.
    pub compiled_pipeline_count: usize,
    /// Number of fallback (interpreted) pipelines.
    pub fallback_pipeline_count: usize,
    /// Total Ring 1 operators.
    pub ring1_operator_count: usize,
    /// Whether JIT compilation was used.
    pub jit_enabled: bool,
}
```

### Query Compilation API

```rust
impl StreamingQuery {
    /// Compile a SQL query into a StreamingQuery.
    ///
    /// This is the main entry point. Handles the full pipeline:
    /// SQL → DataFusion → PipelineExtractor → Cranelift → runtime wiring.
    pub fn compile(
        sql: &str,
        config: &QueryConfig,
        compiler_cache: &mut CompilerCache,
    ) -> Result<Self, QueryError> {
        let start = std::time::Instant::now();

        // Step 1: Parse and plan via DataFusion
        let plan_start = std::time::Instant::now();
        let streaming_plan = parse_and_plan(sql, config)?;
        let plan_time = plan_start.elapsed();

        // Step 2: Extract pipelines
        let extract_start = std::time::Instant::now();
        let extracted = PipelineExtractor::extract(&streaming_plan.logical_plan)?;
        let extract_time = extract_start.elapsed();

        // Step 3: Compile pipelines
        let compile_start = std::time::Instant::now();
        let mut compiled_pipelines = Vec::new();
        let mut fallback_count = 0;

        for pipeline in &extracted.pipelines {
            let executable = if config.jit_enabled {
                ExecutablePipeline::try_compile(
                    compiler_cache,
                    pipeline,
                    streaming_plan.physical_plan.clone(),
                )
            } else {
                fallback_count += 1;
                ExecutablePipeline::Interpreted {
                    physical_plan: streaming_plan.physical_plan.clone(),
                    reason: CompileError::UnsupportedExpr("JIT disabled".into()),
                }
            };

            if matches!(&executable, ExecutablePipeline::Interpreted { .. }) {
                fallback_count += 1;
            }
            compiled_pipelines.push(executable);
        }
        let compile_time = compile_start.elapsed();

        // Step 4: Create bridges and Ring 1 operators
        let (bridges, consumers) = Self::create_bridges(
            &extracted, &config.batch_policy,
        );
        let ring1_operators = Self::create_ring1_operators(
            &streaming_plan.query_plan, &extracted,
        )?;

        // Step 5: Initialize state store
        let state_store = Self::create_state_store(config)?;

        // Pre-allocate output row buffer
        let max_output_size = extracted.pipelines.iter()
            .map(|p| RowSchema::from_arrow(&p.output_schema).fixed_size + 1024)
            .max()
            .unwrap_or(1024);

        Ok(Self {
            id: QueryId(fxhash::hash64(sql.as_bytes())),
            sql: sql.to_string(),
            compiled_pipelines,
            bridges,
            ring1_operators,
            bridge_consumers: consumers,
            state_store,
            schemas: extracted.pipelines.iter()
                .map(|p| Arc::new(RowSchema::from_arrow(&p.output_schema)))
                .collect(),
            output_row_buffer: vec![0u8; max_output_size],
            metadata: QueryMetadata {
                plan_time,
                extract_time,
                compile_time,
                compiled_pipeline_count: compiled_pipelines.len() - fallback_count,
                fallback_pipeline_count: fallback_count,
                ring1_operator_count: ring1_operators.len(),
                jit_enabled: config.jit_enabled,
            },
            state: QueryState::Ready,
        })
    }
}
```

### Event Processing

```rust
impl StreamingQuery {
    /// Submit a single event for processing.
    ///
    /// Ring 0: runs the compiled pipeline (filter + project + key extract).
    /// If the event passes, it's forwarded to Ring 1 via the bridge.
    #[inline]
    pub fn submit(&mut self, event: &Event) -> Result<(), QueryError> {
        debug_assert_eq!(self.state, QueryState::Running);

        // Deserialize event into the first pipeline's input format
        let input_row = self.deserialize_event(event)?;

        // Execute each compiled pipeline in sequence
        for (i, pipeline) in self.compiled_pipelines.iter().enumerate() {
            match pipeline {
                ExecutablePipeline::Compiled(compiled) => {
                    let action = unsafe {
                        compiled.execute(
                            input_row.as_ptr(),
                            self.output_row_buffer.as_mut_ptr(),
                        )
                    };

                    match action {
                        PipelineAction::Emit => {
                            let output_row = EventRow::from_bytes(
                                &self.output_row_buffer,
                                &self.schemas[i],
                            );
                            self.bridges[i].send_event(
                                &output_row,
                                event.timestamp,
                                self.hash_key(&output_row, i),
                            )?;
                        }
                        PipelineAction::Drop => {} // filtered out
                        PipelineAction::Error => {
                            return Err(QueryError::PipelineError(i));
                        }
                    }
                }

                ExecutablePipeline::Interpreted { physical_plan, .. } => {
                    // Fallback: use DataFusion batch execution
                    self.submit_interpreted(event, physical_plan, i)?;
                }
            }
        }

        Ok(())
    }

    /// Poll for output from Ring 1 operators.
    ///
    /// Drains the bridge consumers, feeds batches to Ring 1 operators,
    /// and collects output.
    pub fn poll_output(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();

        for (i, consumer) in self.bridge_consumers.iter_mut().enumerate() {
            let actions = consumer.drain();

            for action in actions {
                match action {
                    Ring1Action::ProcessBatch(batch) => {
                        // Convert batch rows to Events and feed to Ring 1 operators
                        let events = self.batch_to_events(&batch);
                        for event in events {
                            let mut ctx = OperatorContext::new(
                                event.timestamp,
                                &mut *self.state_store,
                            );
                            let results = self.ring1_operators[i]
                                .process(&event, &mut ctx);
                            outputs.extend(results);
                        }
                    }
                    Ring1Action::AdvanceWatermark(ts) => {
                        // Trigger timer-based emissions (window close, etc.)
                        let mut ctx = OperatorContext::new(
                            ts,
                            &mut *self.state_store,
                        );
                        let timer_outputs = self.ring1_operators[i]
                            .on_watermark(ts, &mut ctx);
                        outputs.extend(timer_outputs);
                    }
                    Ring1Action::Checkpoint(epoch) => {
                        // Ring 1 checkpoint
                        self.checkpoint_ring1(epoch);
                    }
                    Ring1Action::Eof => {
                        // Flush final results
                        outputs.extend(self.flush_ring1(i));
                    }
                }
            }
        }

        outputs
    }
}
```

### Query Hot-Swap

```rust
impl StreamingQuery {
    /// Compile a new version of this query for hot-swap.
    ///
    /// The new query is compiled in the background. When ready,
    /// call `swap()` to atomically switch from old to new.
    pub fn compile_new_version(
        sql: &str,
        config: &QueryConfig,
        cache: &mut CompilerCache,
    ) -> Result<StreamingQuery, QueryError> {
        // Compile the new version
        let mut new_query = StreamingQuery::compile(sql, config, cache)?;
        new_query.state = QueryState::Ready;
        Ok(new_query)
    }

    /// Atomically swap to a new query version.
    ///
    /// Preconditions:
    /// - `new_query` must have compatible schemas (same input format)
    /// - Old query is paused or checkpointed
    ///
    /// The swap transfers Ring 1 state from old to new if schemas are compatible.
    pub fn swap(&mut self, mut new_query: StreamingQuery) -> Result<(), QueryError> {
        // Validate schema compatibility
        if !self.schemas_compatible(&new_query) {
            return Err(QueryError::IncompatibleSchemas);
        }

        // Transfer state from old Ring 1 operators
        let state_snapshot = self.state_store.snapshot();
        new_query.state_store.restore(state_snapshot);

        // Swap internals
        std::mem::swap(&mut self.compiled_pipelines, &mut new_query.compiled_pipelines);
        std::mem::swap(&mut self.bridges, &mut new_query.bridges);
        std::mem::swap(&mut self.bridge_consumers, &mut new_query.bridge_consumers);
        std::mem::swap(&mut self.ring1_operators, &mut new_query.ring1_operators);
        std::mem::swap(&mut self.schemas, &mut new_query.schemas);
        std::mem::swap(&mut self.metadata, &mut new_query.metadata);

        self.sql = new_query.sql;
        self.state = QueryState::Running;

        Ok(())
    }

    fn schemas_compatible(&self, other: &StreamingQuery) -> bool {
        // Check that input schemas match (output may differ)
        self.schemas.first().map(|s| s.schema_id)
            == other.schemas.first().map(|s| s.schema_id)
    }
}
```

### Query Configuration

```rust
/// Configuration for query compilation and execution.
#[derive(Debug, Clone)]
pub struct QueryConfig {
    /// Enable JIT compilation via Cranelift.
    /// When false, all pipelines use DataFusion interpreted execution.
    pub jit_enabled: bool,
    /// Batch formation policy for Ring 0 → Ring 1 bridge.
    pub batch_policy: BatchPolicy,
    /// Backpressure strategy.
    pub backpressure: BackpressureStrategy,
    /// Maximum compiled pipeline cache entries per core.
    pub max_cache_entries: usize,
    /// State store configuration.
    pub state_store: StateStoreConfig,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            jit_enabled: cfg!(feature = "jit"),
            batch_policy: BatchPolicy::default(),
            backpressure: BackpressureStrategy::PauseSource,
            max_cache_entries: 64,
            state_store: StateStoreConfig::InMemory,
        }
    }
}

/// State store backend selection.
#[derive(Debug, Clone)]
pub enum StateStoreConfig {
    /// In-memory BTreeMap (default for development).
    InMemory,
    /// Memory-mapped file.
    Mmap { path: std::path::PathBuf },
    /// RocksDB (requires `rocksdb` feature).
    RocksDb { path: std::path::PathBuf },
}
```

### Observability

```rust
/// Metrics emitted by a running StreamingQuery.
#[derive(Debug, Default)]
pub struct QueryMetrics {
    // --- Ring 0 metrics ---
    /// Events submitted to Ring 0.
    pub ring0_events_in: u64,
    /// Events that passed Ring 0 compiled pipeline (sent to Ring 1).
    pub ring0_events_out: u64,
    /// Events filtered out by Ring 0 compiled pipeline.
    pub ring0_events_dropped: u64,
    /// Ring 0 processing time (nanoseconds, total).
    pub ring0_total_ns: u64,
    /// Ring 0 per-event p99 latency (nanoseconds).
    pub ring0_p99_ns: u64,

    // --- Bridge metrics ---
    /// Events currently buffered in the bridge.
    pub bridge_pending: u64,
    /// Events dropped due to backpressure.
    pub bridge_backpressure_drops: u64,
    /// Number of batches formed and flushed.
    pub bridge_batches_flushed: u64,

    // --- Ring 1 metrics ---
    /// Batches processed by Ring 1 operators.
    pub ring1_batches_processed: u64,
    /// Output events emitted by Ring 1.
    pub ring1_events_out: u64,
    /// Ring 1 processing time (nanoseconds, total).
    pub ring1_total_ns: u64,

    // --- Compilation metrics ---
    /// Number of pipelines compiled via Cranelift.
    pub pipelines_compiled: u64,
    /// Number of pipelines using DataFusion fallback.
    pub pipelines_fallback: u64,
    /// Compilation cache hit rate (0.0 - 1.0).
    pub cache_hit_rate: f64,
}

impl StreamingQuery {
    /// Snapshot current metrics.
    pub fn metrics(&self) -> QueryMetrics {
        QueryMetrics {
            ring0_events_in: self.compiled_pipelines.iter()
                .filter_map(|p| match p {
                    ExecutablePipeline::Compiled(c) => Some(c.stats.events_processed),
                    _ => None,
                })
                .sum(),
            pipelines_compiled: self.metadata.compiled_pipeline_count as u64,
            pipelines_fallback: self.metadata.fallback_pipeline_count as u64,
            ..Default::default()
        }
    }
}
```

### Graceful Degradation (No JIT)

```rust
/// When the `jit` feature flag is disabled, all compilation falls back
/// to DataFusion interpreted execution. The StreamingQuery API remains
/// identical — callers don't need to know whether JIT is available.
///
/// Build without JIT:
///   cargo build --no-default-features
///
/// Build with JIT:
///   cargo build --features jit
///
/// The `jit` feature flag gates:
/// - cranelift dependencies
/// - ExprCompiler, PipelineCompiler, JitContext
/// - CompilerCache (replaced by no-op cache)
///
/// Everything else (PipelineExtractor, Bridge, StreamingQuery) works
/// regardless of the feature flag.

#[cfg(not(feature = "jit"))]
impl CompilerCache {
    pub fn get_or_compile(
        &mut self,
        _pipeline: &Pipeline,
    ) -> Result<Arc<CompiledPipeline>, CompileError> {
        Err(CompileError::UnsupportedExpr("JIT feature not enabled".into()))
    }
}
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Query compilation (simple) | < 10ms |
| Query compilation (complex, 5+ expressions) | < 50ms |
| Event submission (Ring 0, compiled) | < 200ns |
| Event submission (Ring 0, fallback) | < 5µs |
| Query hot-swap | < 1ms (state transfer excluded) |
| Output poll (no pending) | < 100ns |

## Success Criteria

- [ ] End-to-end: SQL string → compiled query → submit events → get correct output
- [ ] Compilation metadata reports accurate timing for each phase
- [ ] JIT disabled: query still works via DataFusion fallback
- [ ] Query hot-swap correctly transfers Ring 1 state
- [ ] Metrics accurately track Ring 0 vs Ring 1 processing
- [ ] 10 representative streaming queries compile and produce correct output
- [ ] Memory usage bounded: no leaks during long-running query execution

## Testing

| Module | Tests | What |
|--------|-------|------|
| `StreamingQuery::compile` | 8 | Simple query, complex query, JIT enabled, JIT disabled, invalid SQL |
| `StreamingQuery::submit` | 10 | Event processing, filter pass/fail, projection, key extraction |
| `StreamingQuery::poll_output` | 8 | Window emission, watermark-driven, checkpoint, EOF flush |
| Hot-swap | 4 | Compatible swap, incompatible schema rejection, state transfer, concurrent events |
| Observability | 4 | Metrics accuracy, compilation timing, Ring 0/Ring 1 split |
| End-to-end | 10 | Real SQL queries: tumbling window avg, session window count, filter+project, multi-column projection, ASOF join |
| Fallback | 4 | JIT failure → fallback, partial compilation, feature flag disabled |

## Files

- `crates/laminar-core/src/compiler/query.rs` — NEW: `StreamingQuery`, `QueryConfig`, `QueryState`
- `crates/laminar-core/src/compiler/metrics.rs` — NEW: `QueryMetrics`, `QueryMetadata`
- `crates/laminar-core/src/compiler/lifecycle.rs` — NEW: Hot-swap logic
- `crates/laminar-core/src/compiler/mod.rs` — MODIFY: Re-export query types
- `crates/laminar-core/Cargo.toml` — MODIFY: Add `jit` feature flag gating cranelift deps
- `crates/laminar-sql/src/datafusion/execute.rs` — MODIFY: Add `compile_streaming_query()` entry point
