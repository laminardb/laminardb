# F005B: Advanced DataFusion Streaming Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F005B |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (1 week) |
| **Dependencies** | F005, F006B, F011B |
| **Owner** | TBD |
| **Created** | 2026-01-25 |
| **Updated** | 2026-01-25 |

## Summary

Extend DataFusion integration with streaming-specific UDFs and LogicalPlan creation from parsed streaming SQL. This bridges the gap between the SQL parser (F006B) and DataFusion execution, enabling end-to-end streaming query execution.

## Background

F005 implemented the basic DataFusion integration:
- ✅ `StreamingTableProvider` - table provider for streaming sources
- ✅ `StreamingScanExec` - execution plan for streaming scans
- ✅ `StreamBridge` - push-to-pull bridge
- ✅ Filter pushdown

However, streaming SQL extensions require additional work:
- ❌ TUMBLE/HOP/SESSION window functions as DataFusion UDFs
- ❌ WATERMARK function registration
- ❌ LogicalPlan creation from parsed `StreamingStatement`

## Goals

- Register streaming window functions (TUMBLE, HOP, SESSION) as DataFusion UDFs
- Register WATERMARK function for watermark extraction
- Create DataFusion LogicalPlan from parsed StreamingStatement
- Enable end-to-end streaming query execution via SQL

## Non-Goals

- Physical plan optimization for streaming (Phase 3)
- Distributed query execution (out of scope)
- Custom optimizer rules (future enhancement)

## Technical Design

### 1. Window Function UDFs

Register TUMBLE, HOP, SESSION as DataFusion UDFs that return window bounds:

```rust
/// Register streaming window functions with DataFusion.
pub fn register_window_functions(ctx: &SessionContext) {
    // TUMBLE(time_column, interval) -> window_start, window_end
    ctx.register_udtf("tumble", Arc::new(TumbleUdtf::new()));

    // HOP(time_column, slide_interval, window_interval) -> window_start, window_end
    ctx.register_udtf("hop", Arc::new(HopUdtf::new()));

    // SESSION(time_column, gap_interval) -> window_start, window_end
    ctx.register_udtf("session", Arc::new(SessionUdtf::new()));
}

/// TUMBLE window function implementation.
struct TumbleUdtf;

impl TableFunctionImpl for TumbleUdtf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // Parse time_column and interval from args
        // Return a provider that adds window_start, window_end columns
    }
}
```

### 2. Watermark Function

Register a WATERMARK function for accessing the current watermark:

```rust
/// WATERMARK() -> current watermark timestamp
pub struct WatermarkUdf {
    watermark_tracker: Arc<WatermarkTracker>,
}

impl ScalarUDFImpl for WatermarkUdf {
    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let watermark = self.watermark_tracker.current();
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
            Some(watermark.as_millis() as i64),
            None,
        )))
    }
}
```

### 3. LogicalPlan Creation

Create DataFusion LogicalPlan from parsed StreamingStatement:

```rust
impl StreamingPlanner {
    /// Create a DataFusion LogicalPlan from a QueryPlan.
    pub fn to_logical_plan(
        &self,
        plan: &QueryPlan,
        ctx: &SessionContext,
    ) -> Result<LogicalPlan, PlanningError> {
        // 1. Parse the underlying SQL statement
        let statement = &plan.statement;

        // 2. Create LogicalPlan via DataFusion
        let state = ctx.state();
        let logical_plan = state.statement_to_plan(statement.clone()).await?;

        // 3. Apply streaming transformations
        let logical_plan = self.apply_window_config(logical_plan, &plan.window_config)?;
        let logical_plan = self.apply_join_config(logical_plan, &plan.join_config)?;
        let logical_plan = self.apply_emit_strategy(logical_plan, &plan.emit_clause)?;

        Ok(logical_plan)
    }

    /// Apply window configuration to the logical plan.
    fn apply_window_config(
        &self,
        plan: LogicalPlan,
        config: &Option<WindowOperatorConfig>,
    ) -> Result<LogicalPlan, PlanningError> {
        let Some(config) = config else {
            return Ok(plan);
        };

        // Replace GROUP BY with window aggregation
        // Add window_start, window_end columns
        match config.window_type {
            WindowType::Tumbling => self.apply_tumbling_window(plan, config),
            WindowType::Sliding => self.apply_sliding_window(plan, config),
            WindowType::Session => self.apply_session_window(plan, config),
        }
    }
}
```

### 4. End-to-End Query Execution

```rust
/// Execute a streaming SQL query.
pub async fn execute_streaming_sql(
    sql: &str,
    ctx: &SessionContext,
    planner: &mut StreamingPlanner,
) -> Result<SendableRecordBatchStream, Error> {
    // 1. Parse SQL with streaming extensions
    let statements = StreamingParser::parse_sql(sql)?;

    // 2. Plan the statement
    let plan = planner.plan(&statements[0])?;

    // 3. Create logical plan
    let logical_plan = match plan {
        StreamingPlan::Query(query_plan) => {
            planner.to_logical_plan(&query_plan, ctx)?
        }
        StreamingPlan::Standard(stmt) => {
            ctx.state().statement_to_plan(*stmt).await?
        }
        _ => return Err(Error::UnsupportedFeature("DDL execution".into())),
    };

    // 4. Create physical plan and execute
    let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;
    let stream = physical_plan.execute(0, ctx.task_ctx())?;

    Ok(stream)
}
```

## Implementation Phases

### Phase 1: Window UDFs (2-3 days)

1. Create `datafusion/window_udf.rs` module
2. Implement `TumbleUdtf`, `HopUdtf`, `SessionUdtf`
3. Update `register_streaming_functions()` to register UDTFs
4. Add tests for window function registration

### Phase 2: Watermark Function (1 day)

1. Create `datafusion/watermark_udf.rs` module
2. Implement `WatermarkUdf` as ScalarUDF
3. Add tests for watermark function

### Phase 3: LogicalPlan Creation (2-3 days)

1. Implement `to_logical_plan()` in planner
2. Add window plan transformation
3. Add join plan transformation
4. Add emit strategy handling
5. Integration tests for end-to-end execution

## Testing Strategy

### Unit Tests

```rust
#[test]
fn test_tumble_udtf_registration() {
    let ctx = create_streaming_context();
    register_window_functions(&ctx);

    // Verify TUMBLE is registered
    assert!(ctx.udaf("tumble").is_ok());
}

#[tokio::test]
async fn test_to_logical_plan_with_window() {
    let ctx = create_streaming_context();
    let mut planner = StreamingPlanner::new();

    let sql = "SELECT COUNT(*) FROM events GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)";
    let statements = StreamingParser::parse_sql(sql).unwrap();
    let plan = planner.plan(&statements[0]).unwrap();

    if let StreamingPlan::Query(query_plan) = plan {
        let logical_plan = planner.to_logical_plan(&query_plan, &ctx).await.unwrap();
        // Verify window aggregation node exists
    }
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_end_to_end_tumbling_window() {
    let ctx = create_streaming_context();
    register_streaming_functions(&ctx);

    // Register events source
    let source = create_test_source();
    ctx.register_table("events", Arc::new(source)).unwrap();

    // Execute streaming SQL
    let sql = "SELECT window_start, COUNT(*) as cnt
               FROM events
               GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)";

    let mut planner = StreamingPlanner::new();
    let stream = execute_streaming_sql(sql, &ctx, &mut planner).await.unwrap();

    // Verify results
}
```

## Completion Checklist

- [ ] Window UDFs (TUMBLE, HOP, SESSION) registered
- [ ] WATERMARK UDF implemented
- [ ] `to_logical_plan()` implemented
- [ ] Window plan transformation working
- [ ] Join plan transformation working
- [ ] Emit strategy handling working
- [ ] End-to-end streaming SQL execution working
- [ ] Unit tests (>80% coverage)
- [ ] Integration tests
- [ ] Documentation updated

## References

- [DataFusion UDF Guide](https://datafusion.apache.org/library-user-guide/adding-udfs.html)
- [DataFusion UDTF Implementation](https://datafusion.apache.org/library-user-guide/adding-udtfs.html)
- F005: DataFusion Integration (basic)
- F006B: Production SQL Parser
- F011B: EMIT Clause Extension
