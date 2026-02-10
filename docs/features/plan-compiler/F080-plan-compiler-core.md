# F080: Plan Compiler Core

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F080 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2.5 (Plan Compiler) |
| **Effort** | XL (1-2 weeks) |
| **Dependencies** | F079 |
| **Owner** | TBD |

## Summary

The core compilation engine that takes a DataFusion `LogicalPlan` and produces `CompiledPipeline` objects — fused, zero-allocation function pointers that chain filter, projection, and key extraction into a single native function per pipeline segment. Uses Neumann's produce/consume model to keep data in CPU registers across operator boundaries within a pipeline.

## Motivation

A streaming SQL query like:

```sql
SELECT symbol, AVG(price) AS avg_price
FROM trades
WHERE exchange = 'NYSE' AND price > 0
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE)
```

Decomposes into two pipeline segments separated by a pipeline breaker (the GROUP BY aggregation):

**Pipeline 1 (Ring 0 compiled):** `Source → Filter(exchange='NYSE' AND price>0) → Project(symbol, price, event_time) → KeyExtract(symbol)`

**Pipeline breaker (Ring 1):** `TumblingWindow(1min) → AVG(price) → Emit`

Today, Pipeline 1 executes as a chain of DataFusion `ExecutionPlan` nodes with dynamic dispatch at each boundary and `RecordBatch` allocation between nodes. After compilation, Pipeline 1 becomes a single function pointer that takes a raw event and produces a `(key, projected_row)` pair — zero allocations, zero dispatch, values staying in registers.

## Goals

1. `PipelineExtractor` — walks DataFusion `LogicalPlan`, identifies pipeline breakers, extracts linear operator chains
2. `PipelineCompiler` — fuses a chain of compiled expressions into a single Cranelift function using produce/consume
3. `CompiledPipeline` — output artifact: a function pointer + metadata
4. `CompilerCache` — caches compiled pipelines by plan hash
5. Fallback: if any expression in a pipeline fails to compile, the entire pipeline falls back to DataFusion

## Non-Goals

- Compiling stateful operators (windows, joins, aggregations — those stay as Rust operators)
- Multi-query optimization (each query compiled independently)
- Adaptive re-compilation based on runtime statistics (future work)

## Technical Design

### Pipeline Extraction

A **pipeline** is a maximal chain of operators that can be fused without materializing intermediate results. A **pipeline breaker** is an operator that requires full materialization (aggregation, sort, join build side).

```rust
/// A linear chain of operators that can be fused into a single function.
#[derive(Debug)]
pub struct Pipeline {
    /// Unique ID within the query.
    pub id: PipelineId,
    /// Ordered sequence of operations to fuse.
    pub stages: Vec<PipelineStage>,
    /// Input schema (from source or previous pipeline breaker).
    pub input_schema: Arc<arrow::datatypes::Schema>,
    /// Output schema (after all projections/filters).
    pub output_schema: Arc<arrow::datatypes::Schema>,
}

/// A single operation within a pipeline.
#[derive(Debug)]
pub enum PipelineStage {
    /// Filter: discard rows that don't match the predicate.
    Filter { predicate: Expr },
    /// Projection: compute output columns from input columns.
    Project { expressions: Vec<(Expr, String)> },
    /// Key extraction: compute the grouping/partitioning key.
    KeyExtract { key_exprs: Vec<Expr> },
}

/// An operator that breaks the pipeline (requires state/materialization).
#[derive(Debug)]
pub enum PipelineBreaker {
    /// Window aggregation (tumbling, sliding, session).
    WindowAggregate {
        window_config: WindowOperatorConfig,
        aggregate_exprs: Vec<Expr>,
    },
    /// Stream-stream join.
    StreamJoin { join_config: JoinOperatorConfig },
    /// ORDER BY (requires buffering).
    Sort { order_exprs: Vec<Expr> },
    /// Final output to sink.
    Sink,
}

/// Unique pipeline identifier.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct PipelineId(pub u32);
```

### PipelineExtractor

```rust
/// Walks a DataFusion LogicalPlan and extracts compilable pipelines.
///
/// The extractor performs a bottom-up traversal, collecting filter/project
/// operations into pipeline chains and breaking at aggregation/join/sort nodes.
pub struct PipelineExtractor;

/// The complete extracted plan: a DAG of pipelines connected by breakers.
#[derive(Debug)]
pub struct ExtractedPlan {
    /// Pipelines in topological order (source → sink).
    pub pipelines: Vec<Pipeline>,
    /// Pipeline breakers connecting adjacent pipelines.
    pub breakers: Vec<(PipelineId, PipelineBreaker, PipelineId)>,
    /// Source pipeline IDs.
    pub sources: Vec<PipelineId>,
    /// Sink pipeline IDs.
    pub sinks: Vec<PipelineId>,
}

impl PipelineExtractor {
    /// Extract compilable pipelines from a DataFusion LogicalPlan.
    pub fn extract(plan: &LogicalPlan) -> Result<ExtractedPlan, ExtractError> {
        let mut pipelines = Vec::new();
        let mut breakers = Vec::new();
        let mut current_stages = Vec::new();
        let mut current_schema = None;

        Self::walk(
            plan,
            &mut pipelines,
            &mut breakers,
            &mut current_stages,
            &mut current_schema,
        )?;

        // Flush any remaining stages as a final pipeline
        if !current_stages.is_empty() {
            pipelines.push(Pipeline {
                id: PipelineId(pipelines.len() as u32),
                stages: std::mem::take(&mut current_stages),
                input_schema: current_schema.clone().unwrap(),
                output_schema: current_schema.unwrap(),
            });
        }

        Ok(ExtractedPlan {
            sources: vec![PipelineId(0)],
            sinks: vec![PipelineId(pipelines.len() as u32 - 1)],
            pipelines,
            breakers,
        })
    }

    fn walk(
        plan: &LogicalPlan,
        pipelines: &mut Vec<Pipeline>,
        breakers: &mut Vec<(PipelineId, PipelineBreaker, PipelineId)>,
        current_stages: &mut Vec<PipelineStage>,
        current_schema: &mut Option<Arc<arrow::datatypes::Schema>>,
    ) -> Result<(), ExtractError> {
        match plan {
            // --- Passthrough: push filter into current pipeline ---
            LogicalPlan::Filter(filter) => {
                Self::walk(filter.input(), pipelines, breakers, current_stages, current_schema)?;
                current_stages.push(PipelineStage::Filter {
                    predicate: filter.predicate().clone(),
                });
            }

            // --- Passthrough: push projection into current pipeline ---
            LogicalPlan::Projection(proj) => {
                Self::walk(proj.input(), pipelines, breakers, current_stages, current_schema)?;
                let expressions = proj.expr().iter()
                    .zip(proj.schema().field_names())
                    .map(|(e, n)| (e.clone(), n.clone()))
                    .collect();
                current_stages.push(PipelineStage::Project { expressions });
            }

            // --- Pipeline breaker: aggregation ---
            LogicalPlan::Aggregate(agg) => {
                // Recurse into input first
                Self::walk(agg.input(), pipelines, breakers, current_stages, current_schema)?;

                // Add key extraction for GROUP BY
                if !agg.group_expr().is_empty() {
                    current_stages.push(PipelineStage::KeyExtract {
                        key_exprs: agg.group_expr().to_vec(),
                    });
                }

                // Flush current pipeline
                let pre_id = PipelineId(pipelines.len() as u32);
                pipelines.push(Pipeline {
                    id: pre_id,
                    stages: std::mem::take(current_stages),
                    input_schema: current_schema.clone().unwrap(),
                    output_schema: current_schema.clone().unwrap(), // updated below
                });

                // Record the breaker
                let post_id = PipelineId(pipelines.len() as u32 + 1);
                breakers.push((pre_id, PipelineBreaker::WindowAggregate {
                    window_config: WindowOperatorConfig::default(),
                    aggregate_exprs: agg.aggr_expr().to_vec(),
                }, post_id));
            }

            // --- Leaf: table scan (source) ---
            LogicalPlan::TableScan(scan) => {
                *current_schema = Some(Arc::new(
                    scan.source.schema().as_ref().into()
                ));
            }

            // --- Other nodes: attempt passthrough or break ---
            other => {
                // For unrecognized nodes, treat as pipeline breaker
                for input in other.inputs() {
                    Self::walk(input, pipelines, breakers, current_stages, current_schema)?;
                }
            }
        }
        Ok(())
    }
}
```

### PipelineCompiler (Fused Code Generation)

```rust
/// Compiles an extracted Pipeline into a single native function.
///
/// Uses Neumann's produce/consume model: each stage "produces" values
/// that the next stage "consumes." Values stay in CPU registers across
/// stage boundaries — no intermediate materialization.
pub struct PipelineCompiler<'a> {
    jit: &'a mut JitContext,
    expr_compiler: ExprCompiler<'a>,
}

impl<'a> PipelineCompiler<'a> {
    /// Compile a Pipeline into a CompiledPipeline.
    pub fn compile(&mut self, pipeline: &Pipeline) -> Result<CompiledPipeline, CompileError> {
        let input_schema = RowSchema::from_arrow(&pipeline.input_schema);
        let output_schema = RowSchema::from_arrow(&pipeline.output_schema);

        // Build a single Cranelift function that chains all stages.
        //
        // Function signature:
        //   fn(input_row: *const u8, output_row: *mut u8) -> PipelineAction
        //
        // PipelineAction:
        //   0 = Drop (row filtered out)
        //   1 = Emit (row written to output_row)
        //   2 = Error

        let mut sig = self.jit.module.make_signature();
        sig.params.push(AbiParam::new(types::I64)); // input_row ptr
        sig.params.push(AbiParam::new(types::I64)); // output_row ptr
        sig.returns.push(AbiParam::new(types::I8));  // PipelineAction

        let func_name = format!("pipeline_{}", pipeline.id.0);
        let func_id = self.jit.module
            .declare_function(&func_name, Linkage::Local, &sig)?;

        let mut ctx = self.jit.module.make_context();
        ctx.func.signature = sig;

        {
            let mut builder = FunctionBuilder::new(
                &mut ctx.func,
                &mut self.jit.builder_ctx,
            );
            let entry = builder.create_block();
            builder.append_block_params_for_function_params(entry);
            builder.switch_to_block(entry);
            builder.seal_block(entry);

            let input_ptr = builder.block_params(entry)[0];
            let output_ptr = builder.block_params(entry)[1];

            let drop_block = builder.create_block();
            let emit_block = builder.create_block();

            // Chain stages using produce/consume pattern
            for stage in &pipeline.stages {
                match stage {
                    PipelineStage::Filter { predicate } => {
                        // Compile filter predicate
                        let result = self.expr_compiler.compile_expr_inner(
                            &mut builder, predicate, input_ptr,
                        )?;
                        // If false, jump to drop block
                        builder.ins().brif(
                            result.value,
                            builder.current_block().unwrap(),
                            &[],
                            drop_block,
                            &[],
                        );
                    }

                    PipelineStage::Project { expressions } => {
                        // For each output expression, compile and write to output row
                        for (i, (expr, _name)) in expressions.iter().enumerate() {
                            let val = self.expr_compiler.compile_expr_inner(
                                &mut builder, expr, input_ptr,
                            )?;
                            let field = &output_schema.fields[i];
                            let addr = builder.ins().iadd_imm(
                                output_ptr, field.offset as i64,
                            );
                            // Write value to output row
                            match field.data_type {
                                FieldType::Int64 | FieldType::TimestampMicros => {
                                    builder.ins().store(
                                        MemFlags::trusted(), val.value, addr, 0,
                                    );
                                }
                                FieldType::Float64 => {
                                    builder.ins().store(
                                        MemFlags::trusted(), val.value, addr, 0,
                                    );
                                }
                                _ => {} // other types handled similarly
                            }
                            // Update null bitmap if nullable
                            if let Some(null_flag) = val.null_flag {
                                self.write_null_bit(
                                    &mut builder, output_ptr, i, null_flag, &output_schema,
                                );
                            }
                        }
                    }

                    PipelineStage::KeyExtract { key_exprs } => {
                        // Key extraction writes to the first N fields of output
                        for (i, expr) in key_exprs.iter().enumerate() {
                            let val = self.expr_compiler.compile_expr_inner(
                                &mut builder, expr, input_ptr,
                            )?;
                            let field = &output_schema.fields[i];
                            let addr = builder.ins().iadd_imm(
                                output_ptr, field.offset as i64,
                            );
                            builder.ins().store(
                                MemFlags::trusted(), val.value, addr, 0,
                            );
                        }
                    }
                }
            }

            // Emit path: return 1 (Emit)
            let emit_val = builder.ins().iconst(types::I8, 1);
            builder.ins().jump(emit_block, &[]);

            // Drop path: return 0 (Drop)
            builder.switch_to_block(drop_block);
            builder.seal_block(drop_block);
            let drop_val = builder.ins().iconst(types::I8, 0);
            builder.ins().return_(&[drop_val]);

            // Emit block
            builder.switch_to_block(emit_block);
            builder.seal_block(emit_block);
            builder.ins().return_(&[emit_val]);

            builder.finalize();
        }

        self.jit.module.define_function(func_id, &mut ctx)?;
        self.jit.module.clear_context(&mut ctx);
        self.jit.module.finalize_definitions()?;

        let code_ptr = self.jit.module.get_finalized_function(func_id);
        let func: PipelineFn = unsafe { std::mem::transmute(code_ptr) };

        Ok(CompiledPipeline {
            id: pipeline.id,
            func,
            input_schema: Arc::new(input_schema),
            output_schema: Arc::new(output_schema),
            stats: PipelineStats::default(),
        })
    }
}
```

### CompiledPipeline

```rust
/// The result of pipeline compilation: a native function pointer with metadata.
pub struct CompiledPipeline {
    /// Pipeline identifier.
    pub id: PipelineId,
    /// The compiled function pointer.
    ///
    /// Signature: `fn(input_row: *const u8, output_row: *mut u8) -> u8`
    /// Returns: 0=Drop, 1=Emit, 2=Error
    pub func: PipelineFn,
    /// Input row schema (for deserialization).
    pub input_schema: Arc<RowSchema>,
    /// Output row schema (for Ring 1 handoff).
    pub output_schema: Arc<RowSchema>,
    /// Runtime statistics.
    pub stats: PipelineStats,
}

/// Compiled pipeline function signature.
///
/// - `input_row`: pointer to the input EventRow bytes
/// - `output_row`: pointer to pre-allocated output EventRow bytes
/// - Returns: `PipelineAction` as u8
pub type PipelineFn = unsafe extern "C" fn(*const u8, *mut u8) -> u8;

/// Pipeline execution result.
#[repr(u8)]
pub enum PipelineAction {
    /// Row was filtered out.
    Drop = 0,
    /// Row was processed and written to output.
    Emit = 1,
    /// An error occurred during processing.
    Error = 2,
}

/// Runtime statistics for a compiled pipeline.
#[derive(Debug, Default)]
pub struct PipelineStats {
    pub events_processed: u64,
    pub events_emitted: u64,
    pub events_dropped: u64,
    pub total_ns: u64,
}

impl CompiledPipeline {
    /// Execute the compiled pipeline on a single event row.
    ///
    /// # Safety
    /// - `input` must point to a valid EventRow matching `input_schema`
    /// - `output` must point to a buffer of at least `output_schema.fixed_size` bytes
    #[inline(always)]
    pub unsafe fn execute(&self, input: *const u8, output: *mut u8) -> PipelineAction {
        match (self.func)(input, output) {
            0 => PipelineAction::Drop,
            1 => PipelineAction::Emit,
            _ => PipelineAction::Error,
        }
    }
}
```

### CompilerCache

```rust
/// Caches compiled pipelines by plan hash to avoid recompilation.
///
/// Each core in the thread-per-core model has its own `CompilerCache`
/// to avoid cross-core sharing of JIT code (cache coherency).
pub struct CompilerCache {
    /// Map from plan hash → compiled pipeline.
    cache: FxHashMap<u64, Arc<CompiledPipeline>>,
    /// JIT context for this core.
    jit: JitContext,
    /// Maximum cache entries before eviction.
    max_entries: usize,
}

impl CompilerCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: FxHashMap::default(),
            jit: JitContext::new(),
            max_entries,
        }
    }

    /// Get or compile a pipeline.
    pub fn get_or_compile(
        &mut self,
        pipeline: &Pipeline,
    ) -> Result<Arc<CompiledPipeline>, CompileError> {
        let hash = self.hash_pipeline(pipeline);

        if let Some(compiled) = self.cache.get(&hash) {
            return Ok(Arc::clone(compiled));
        }

        // Compile
        let mut compiler = PipelineCompiler {
            jit: &mut self.jit,
            expr_compiler: ExprCompiler {
                jit: &mut self.jit,
                schema: &RowSchema::from_arrow(&pipeline.input_schema),
            },
        };
        let compiled = Arc::new(compiler.compile(pipeline)?);

        // Evict if at capacity (LRU or random eviction)
        if self.cache.len() >= self.max_entries {
            // Simple: remove oldest entry
            if let Some(&key) = self.cache.keys().next() {
                self.cache.remove(&key);
            }
        }

        self.cache.insert(hash, Arc::clone(&compiled));
        Ok(compiled)
    }

    fn hash_pipeline(&self, pipeline: &Pipeline) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = fxhash::FxHasher::default();
        format!("{:?}", pipeline.stages).hash(&mut hasher);
        hasher.finish()
    }
}
```

### Fallback Mechanism

```rust
/// A pipeline that can be either compiled or interpreted.
///
/// When compilation succeeds, uses the fast JIT path.
/// When compilation fails, falls back to DataFusion execution.
pub enum ExecutablePipeline {
    /// Compiled: zero-allocation native function.
    Compiled(CompiledPipeline),
    /// Interpreted: DataFusion RecordBatch execution.
    Interpreted {
        /// The DataFusion physical plan for this pipeline segment.
        physical_plan: Arc<dyn ExecutionPlan>,
        /// Why compilation failed.
        reason: CompileError,
    },
}

impl ExecutablePipeline {
    /// Try to compile; fall back to interpreted on failure.
    pub fn try_compile(
        cache: &mut CompilerCache,
        pipeline: &Pipeline,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        match cache.get_or_compile(pipeline) {
            Ok(compiled) => Self::Compiled((*compiled).clone()),
            Err(e) => {
                tracing::warn!(
                    pipeline_id = pipeline.id.0,
                    error = %e,
                    "Pipeline compilation failed, falling back to DataFusion"
                );
                Self::Interpreted {
                    physical_plan,
                    reason: e,
                }
            }
        }
    }
}
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Pipeline extraction | < 1ms for typical streaming query |
| Pipeline compilation | < 5ms for filter + project + key extract |
| Per-event execution (filter + project) | < 100ns |
| Per-event execution (filter only) | < 50ns |
| Per-event execution (10-column projection) | < 200ns |
| Cache hit lookup | < 50ns (FxHashMap) |
| Memory per compiled pipeline | < 4KB code + metadata |

## Success Criteria

- [ ] `PipelineExtractor` correctly identifies pipelines and breakers for 10+ representative streaming queries
- [ ] Compiled pipelines produce identical output to DataFusion interpreted execution for all test queries
- [ ] Compilation time < 5ms for queries with up to 10 expressions
- [ ] Per-event execution < 100ns for filter + project pipelines
- [ ] Zero heap allocations in compiled code (validated by `HotPathGuard`)
- [ ] Fallback to DataFusion works transparently for unsupported expressions
- [ ] CompilerCache correctly deduplicates identical plans

## Testing

| Module | Tests | What |
|--------|-------|------|
| `PipelineExtractor` | 12 | Simple filter, projection, filter+project, aggregation breaker, join breaker, multi-pipeline, empty pipeline |
| `PipelineCompiler` | 10 | Filter-only, project-only, filter+project, key extract, all-types projection, null handling |
| `CompiledPipeline` | 8 | Execute filter/project, Drop vs Emit, error path, stats tracking |
| `CompilerCache` | 6 | Cache hit, cache miss, eviction, hash collision, concurrent access |
| Correctness | 10 | Round-trip: SQL → DataFusion → compile → run → verify output matches DataFusion |
| Fallback | 4 | Unsupported expr → fallback, partial compilation, mixed mode |

## Files

- `crates/laminar-core/src/compiler/pipeline.rs` — NEW: `Pipeline`, `PipelineStage`, `PipelineBreaker`, `CompiledPipeline`
- `crates/laminar-core/src/compiler/extractor.rs` — NEW: `PipelineExtractor`, `ExtractedPlan`
- `crates/laminar-core/src/compiler/compiler.rs` — NEW: `PipelineCompiler`
- `crates/laminar-core/src/compiler/cache.rs` — NEW: `CompilerCache`
- `crates/laminar-core/src/compiler/fallback.rs` — NEW: `ExecutablePipeline`
