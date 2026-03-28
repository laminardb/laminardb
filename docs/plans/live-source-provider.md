# LiveSourceProvider Implementation

## Problem

The OperatorGraph currently re-creates MemTables and re-plans SQL from scratch every execution cycle. This causes:
1. MemTable register/deregister churn (~100μs per source per cycle)
2. SQL re-planning overhead (~100μs per query per cycle) — required because DataFusion's optimizer bakes stale table statistics into the cached LogicalPlan
3. Unnecessary data copying (MemTable takes ownership of cloned batches)

## Solution

Replace MemTable-per-cycle with a `LiveSourceProvider` — a `TableProvider` registered once at startup that returns fresh data on each `scan()` call. Same pattern as the existing `ReferenceTableProvider` for lookup tables.

## Design

```rust
// crates/laminar-db/src/table_provider.rs

pub(crate) struct LiveSourceProvider {
    schema: SchemaRef,
    current_batches: Arc<parking_lot::Mutex<Vec<RecordBatch>>>,
}
```

### How it works

1. **Registered once** in the SessionContext during pipeline startup (alongside ReferenceTableProviders for lookup tables)
2. **Each cycle**, the OperatorGraph swaps new batches into `current_batches` instead of creating a MemTable
3. **On scan()**, the provider takes the current batches, wraps them in a MemTable, and delegates scan — identical to how ReferenceTableProvider works with TableStore
4. **The LogicalPlan can be cached again** because the table provider is stable — only the data changes between scans. DataFusion's optimizer sees the same table with the same schema each time. The physical plan is re-created per scan() call, picking up fresh data.

### Key insight: why plan caching works with LiveSourceProvider but not with MemTable

With MemTable: the table is deregistered and re-registered each cycle. DataFusion's catalog sees a NEW table each time. The LogicalPlan references the OLD table's statistics.

With LiveSourceProvider: the table is registered ONCE. The LogicalPlan references a stable table. Physical planning calls scan() which returns fresh data. Statistics come from the current data, not from plan creation time.

## Changes

### 1. Add LiveSourceProvider to table_provider.rs

```rust
pub(crate) struct LiveSourceProvider {
    schema: SchemaRef,
    current_batches: Arc<parking_lot::Mutex<Vec<RecordBatch>>>,
}

impl LiveSourceProvider {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            current_batches: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    /// Swap in new batches for the next scan. Called by OperatorGraph each cycle.
    pub fn set_batches(&self, batches: Vec<RecordBatch>) {
        *self.current_batches.lock() = batches;
    }

    /// Clear batches after the cycle completes.
    pub fn clear(&self) {
        self.current_batches.lock().clear();
    }
}

impl TableProvider for LiveSourceProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Temporary }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>,
                  filters: &[Expr], limit: Option<usize>)
        -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
    {
        let batches = self.current_batches.lock().clone();
        let data = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let mem = MemTable::try_new(self.schema.clone(), data)?;
        mem.scan(state, projection, filters, limit).await
    }
}
```

### 2. Modify OperatorGraph

Add field:
```rust
live_sources: FxHashMap<Arc<str>, Arc<LiveSourceProvider>>,
```

In `pipeline_lifecycle.rs`, after creating the context:
```rust
for (name, schema) in &source_schemas {
    let provider = Arc::new(LiveSourceProvider::new(schema.clone()));
    ctx.register_table(name, Arc::clone(&provider) as Arc<dyn TableProvider>)?;
    graph.register_live_source(Arc::from(name.as_str()), provider);
}
```

Replace `register_source_tables`:
```rust
fn update_source_data(&self, source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
    for (name, batches) in source_batches {
        if let Some(provider) = self.live_sources.get(name) {
            provider.set_batches(batches.clone());
        }
    }
    // Clear sources with no data this cycle
    for (name, provider) in &self.live_sources {
        if !source_batches.contains_key(name) {
            provider.clear();
        }
    }
}
```

Replace `finish_cycle` source cleanup:
```rust
fn finish_cycle(&mut self) {
    // Clear live source data (no deregistration needed)
    for provider in self.live_sources.values() {
        provider.clear();
    }
    // Intermediate tables still need deregistration
    for name in self.cycle_intermediates.drain(..) {
        let _ = self.ctx.deregister_table(&name);
    }
    // No more registered_sources cleanup needed
}
```

### 3. Restore LogicalPlan caching in SqlQueryOperator

With LiveSourceProvider, the LogicalPlan can be cached again:
```rust
QueryState::CachedPlan(_) => {
    let QueryState::CachedPlan(ref plan) = self.state else { unreachable!() };
    let plan = plan.clone();
    self.execute_cached_plan_with_invalidation(&plan).await
}
```

The plan references the same table provider. Physical planning calls scan() which returns fresh data. No stale statistics because the table identity is stable.

**Important**: This needs verification. If DataFusion caches statistics at the LogicalPlan level (not just physical), re-planning may still be needed. Test by running the harness with cached plan + LiveSourceProvider and checking for row loss.

### 4. Remove `registered_sources` tracking

The `registered_sources: Vec<String>` field and its cleanup in `finish_cycle` become unnecessary for live sources. Keep it only for intermediate tables.

## Files to modify

- `crates/laminar-db/src/table_provider.rs` — add LiveSourceProvider (~50 lines)
- `crates/laminar-db/src/operator_graph.rs` — replace register_source_tables with update_source_data, add live_sources field, simplify finish_cycle
- `crates/laminar-db/src/pipeline_lifecycle.rs` — register LiveSourceProviders at startup
- `crates/laminar-db/src/operator/sql_query.rs` — restore LogicalPlan caching (if verification passes)

## Verification

1. Run the lookup join harness — 0 missing events
2. Run the passthrough harness — 0 missing events
3. Benchmark: compare cycle time with/without LiveSourceProvider (expect ~200μs improvement per cycle from eliminating MemTable churn + plan re-creation)
