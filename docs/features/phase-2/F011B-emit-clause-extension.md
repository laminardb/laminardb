# F011B: EMIT Clause Extension

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F011B |
| **Status** | ✅ Complete |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F011, F063 |
| **Blocks** | F023 |
| **Owner** | Claude |
| **Research** | [Emit Patterns Research 2026](../../research/emit-patterns-research-2026.md) |
| **Completed** | 2026-01-25 |

## Summary

Extend the F011 EMIT Clause with three additional strategies identified in the 2026 emit patterns research: `OnWindowClose`, `Changelog`, and `Final`. These are critical for exactly-once sinks and CDC pipelines.

## Motivation

The 2026 emit patterns research identified **critical gaps** in LaminarDB's emit strategy support:

| Strategy | Industry Status | LaminarDB (F011) | Gap |
|----------|-----------------|------------------|-----|
| EMIT ON UPDATE | Standard | ✅ Implemented | - |
| EMIT ON WATERMARK | Standard | ✅ Implemented | - |
| EMIT PERIODIC | Common | ✅ Implemented | - |
| **EMIT ON WINDOW CLOSE** | **Critical** | ❌ **Missing** | **Blocks F023** |
| **EMIT CHANGES** | **Critical** | ❌ **Missing** | **Blocks CDC** |
| **EMIT FINAL** | Common | ❌ **Missing** | Nice-to-have |

**Key Quote from Research:**
> "EMIT ON WINDOW CLOSE is essential for append-only sinks (Kafka, S3, Delta Lake) but not implemented... This is a CRITICAL BLOCKING GAP for F023 Exactly-Once Sinks."

## Goals

1. Add `OnWindowClose` emit strategy for append-only sinks
2. Add `Changelog` emit strategy for Z-set weighted output
3. Add `Final` emit strategy to suppress intermediate results
4. SQL syntax support for all new strategies
5. Optimizer rules for emit strategy propagation

## Non-Goals

- Full emit strategy optimizer (Phase 3)
- Automatic emit strategy selection based on sink type
- Emit batching configuration (separate enhancement)

## Technical Design

### Extended EmitStrategy Enum

```rust
/// Emit strategy for streaming operators.
///
/// Controls when and how window/aggregate results are emitted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EmitStrategy {
    // === Existing (F011) ===

    /// Emit final results when watermark passes window end (default).
    ///
    /// Most efficient strategy - emits once per window after allowed lateness.
    /// May emit retractions if late data arrives within lateness bounds.
    #[default]
    OnWatermark,

    /// Emit intermediate results at fixed intervals.
    ///
    /// Useful for dashboards needing periodic updates before window close.
    /// Final result still emitted on watermark.
    Periodic(Duration),

    /// Emit updated results after every state change.
    ///
    /// Lowest latency but highest overhead. Each event triggers emission.
    /// Best for real-time dashboards and alerting.
    OnUpdate,

    // === New (F011B) ===

    /// Emit ONLY when watermark passes window end. No intermediate emissions.
    ///
    /// **Critical for append-only sinks** (Kafka, S3, Delta Lake, Iceberg).
    /// Unlike `OnWatermark`, this NEVER emits before window close, even with
    /// late data retractions. Late data is buffered until next window close.
    ///
    /// Key difference from OnWatermark:
    /// - OnWatermark: May emit retractions for late data
    /// - OnWindowClose: Buffers late data, only emits final result
    OnWindowClose,

    /// Emit changelog records with Z-set weights.
    ///
    /// Every emission includes operation type and weight:
    /// - Insert (+1 weight)
    /// - Delete (-1 weight)
    /// - Update (retraction pair: -1 old, +1 new)
    ///
    /// Required for:
    /// - CDC pipelines
    /// - Cascading materialized views (F060)
    /// - Downstream consumers that need to track changes
    Changelog,

    /// Suppress ALL intermediate results, emit only finalized.
    ///
    /// Similar to OnWindowClose but also suppresses:
    /// - Periodic emissions (even if Periodic was set elsewhere)
    /// - Late data retractions (drops late data entirely)
    ///
    /// Use for BI reporting where only final, exact results matter.
    Final,
}

impl EmitStrategy {
    /// Returns true if this strategy emits intermediate results.
    pub fn emits_intermediate(&self) -> bool {
        matches!(self, Self::OnUpdate | Self::Periodic(_))
    }

    /// Returns true if this strategy requires changelog support.
    pub fn requires_changelog(&self) -> bool {
        matches!(self, Self::Changelog)
    }

    /// Returns true if this strategy is suitable for append-only sinks.
    pub fn is_append_only_compatible(&self) -> bool {
        matches!(self, Self::OnWindowClose | Self::Final)
    }

    /// Returns true if late data should generate retractions.
    pub fn generates_retractions(&self) -> bool {
        matches!(self, Self::OnWatermark | Self::OnUpdate | Self::Changelog)
    }
}
```

### SQL Syntax Extensions

```sql
-- Existing (F011)
EMIT ON WATERMARK           -- Default
EMIT ON UPDATE              -- Every change
EMIT EVERY INTERVAL '10' SECOND  -- Periodic

-- New (F011B)
EMIT ON WINDOW CLOSE        -- Only when window closes (append-only)
EMIT CHANGES                -- Changelog with +/- weights
EMIT FINAL                  -- Suppress all intermediate

-- Examples

-- Kafka sink (append-only) - use EMIT ON WINDOW CLOSE
CREATE MATERIALIZED VIEW sales_per_minute AS
SELECT
    window_start,
    SUM(amount) as total
FROM TUMBLE(sales, event_time, INTERVAL '1 minute')
GROUP BY window_start
EMIT ON WINDOW CLOSE;  -- No duplicates to Kafka

-- CDC pipeline - use EMIT CHANGES
CREATE MATERIALIZED VIEW customer_totals AS
SELECT
    customer_id,
    SUM(amount) as total
FROM orders
GROUP BY customer_id
EMIT CHANGES;  -- Outputs +I/-D/+U/-U records

-- BI report - use EMIT FINAL
CREATE MATERIALIZED VIEW daily_report AS
SELECT
    DATE(event_time) as day,
    COUNT(*) as order_count,
    SUM(amount) as revenue
FROM orders
GROUP BY DATE(event_time)
EMIT FINAL;  -- Only final, exact numbers
```

### Parser Updates

```rust
// In crates/laminar-sql/src/parser/statements.rs

/// SQL EMIT clause variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitClause {
    // Existing
    AfterWatermark,                    // EMIT AFTER WATERMARK, EMIT ON WATERMARK
    OnWindowClose,                     // EMIT ON WINDOW CLOSE
    Periodically { interval: Expr },   // EMIT EVERY INTERVAL '...'
    OnUpdate,                          // EMIT ON UPDATE

    // New (F011B)
    Changes,                           // EMIT CHANGES
    Final,                             // EMIT FINAL
}

impl EmitClause {
    /// Converts SQL AST emit clause to core EmitStrategy.
    pub fn to_emit_strategy(&self) -> EmitStrategy {
        match self {
            Self::AfterWatermark => EmitStrategy::OnWatermark,
            Self::OnWindowClose => EmitStrategy::OnWindowClose,
            Self::Periodically { interval } => {
                // Parse interval to Duration
                let duration = parse_interval(interval);
                EmitStrategy::Periodic(duration)
            }
            Self::OnUpdate => EmitStrategy::OnUpdate,
            Self::Changes => EmitStrategy::Changelog,
            Self::Final => EmitStrategy::Final,
        }
    }
}

// In crates/laminar-sql/src/parser/parser_simple.rs

fn parse_emit_clause(&mut self) -> Result<Option<EmitClause>, ParserError> {
    if !self.parse_keyword(Keyword::EMIT) {
        return Ok(None);
    }

    if self.parse_keywords(&[Keyword::ON, Keyword::WATERMARK])
        || self.parse_keywords(&[Keyword::AFTER, Keyword::WATERMARK])
    {
        Ok(Some(EmitClause::AfterWatermark))
    } else if self.parse_keywords(&[Keyword::ON, Keyword::WINDOW, Keyword::CLOSE]) {
        Ok(Some(EmitClause::OnWindowClose))
    } else if self.parse_keywords(&[Keyword::ON, Keyword::UPDATE]) {
        Ok(Some(EmitClause::OnUpdate))
    } else if self.parse_keyword(Keyword::CHANGES) {
        Ok(Some(EmitClause::Changes))
    } else if self.parse_keyword(Keyword::FINAL) {
        Ok(Some(EmitClause::Final))
    } else if self.parse_keyword(Keyword::EVERY) {
        let interval = self.parse_interval()?;
        Ok(Some(EmitClause::Periodically { interval }))
    } else {
        Err(ParserError::ExpectedKeyword(
            "WATERMARK, WINDOW CLOSE, UPDATE, CHANGES, FINAL, or EVERY".to_string()
        ))
    }
}
```

### Emit Strategy Propagation (Optimizer Rule)

```rust
/// Optimizer rule: Propagate emit strategy through query plan.
///
/// Ensures consistency between query emit strategy and sink requirements.
pub struct EmitStrategyPropagationRule;

impl OptimizerRule for EmitStrategyPropagationRule {
    fn name(&self) -> &str {
        "emit_strategy_propagation"
    }

    fn rewrite(&self, plan: &LogicalPlan, ctx: &OptimizerContext) -> Result<LogicalPlan> {
        // Walk plan bottom-up
        let sink_type = self.detect_sink_type(plan)?;

        match sink_type {
            SinkType::AppendOnly => {
                // Force OnWindowClose for append-only sinks
                self.enforce_emit_strategy(plan, EmitStrategy::OnWindowClose)
            }
            SinkType::Upsert => {
                // Allow any strategy for upsert sinks
                Ok(plan.clone())
            }
            SinkType::Changelog => {
                // Force Changelog emit for CDC sinks
                self.enforce_emit_strategy(plan, EmitStrategy::Changelog)
            }
        }
    }
}

/// Sink types for emit strategy selection.
#[derive(Debug, Clone, Copy)]
pub enum SinkType {
    /// Append-only sinks (Kafka, S3, Parquet, Iceberg)
    AppendOnly,
    /// Upsert-capable sinks (databases, Redis)
    Upsert,
    /// Changelog sinks (CDC pipelines, MV subscriptions)
    Changelog,
}
```

### Window Operator Integration

```rust
impl<A: Aggregator> TumblingWindowOperator<A> {
    /// Handles emission based on strategy.
    fn emit_window(
        &mut self,
        window: &WindowId,
        ctx: &mut OperatorContext,
        is_final: bool,
    ) -> OutputVec {
        match self.emit_strategy {
            EmitStrategy::OnWatermark => {
                // Existing: emit on watermark, may retract
                self.emit_with_potential_retraction(window, ctx)
            }
            EmitStrategy::OnWindowClose => {
                // NEW: Only emit when is_final=true
                if is_final {
                    self.emit_final_only(window, ctx)
                } else {
                    OutputVec::new() // Buffer, don't emit
                }
            }
            EmitStrategy::Changelog => {
                // NEW: Emit with changelog wrapper
                self.emit_as_changelog(window, ctx, is_final)
            }
            EmitStrategy::Final => {
                // NEW: Only emit when is_final=true, no retractions
                if is_final {
                    self.emit_final_no_retraction(window, ctx)
                } else {
                    OutputVec::new()
                }
            }
            EmitStrategy::OnUpdate => {
                // Existing: emit immediately
                self.emit_intermediate(window, ctx)
            }
            EmitStrategy::Periodic(_) => {
                // Existing: handled by timer
                self.emit_intermediate(window, ctx)
            }
        }
    }

    /// Emits result wrapped in ChangelogRecord.
    fn emit_as_changelog(
        &mut self,
        window: &WindowId,
        ctx: &mut OperatorContext,
        is_final: bool,
    ) -> OutputVec {
        let result = self.compute_result(window);

        // Check if we need to emit retraction
        if let Some((old, new)) = self.retraction_gen.check_retraction(window, &result) {
            // Emit retraction pair
            let (before, after) = ChangelogRecord::retraction(old, new, ctx.current_time());
            OutputVec::from_iter([
                Output::Changelog(before),
                Output::Changelog(after),
            ])
        } else {
            // First emission or same result
            let record = ChangelogRecord::insert(result, ctx.current_time());
            OutputVec::from_iter([Output::Changelog(record)])
        }
    }
}
```

## Implementation Phases

### Phase 1: Core Types (1-2 days)

1. Add `OnWindowClose`, `Changelog`, `Final` to `EmitStrategy` enum
2. Add helper methods (`emits_intermediate`, `requires_changelog`, etc.)
3. Update existing tests
4. Add tests for new variants

### Phase 2: SQL Parser (1-2 days)

1. Add `Changes` and `Final` to `EmitClause` enum
2. Update parser to recognize new keywords
3. Implement `to_emit_strategy()` conversion
4. Parser tests for new syntax

### Phase 3: Window Operator Integration (2-3 days)

1. Update `TumblingWindowOperator` for new strategies
2. Update `SlidingWindowOperator` for new strategies
3. Implement `emit_as_changelog()` method
4. Implement `emit_final_only()` and `emit_final_no_retraction()`
5. Integration tests

### Phase 4: Optimizer Rule (1-2 days)

1. Implement `EmitStrategyPropagationRule`
2. Detect sink type from query plan
3. Enforce appropriate emit strategy
4. Add to optimizer rule chain

## Test Cases

```rust
#[test]
fn test_emit_strategy_on_window_close() {
    let mut operator = create_tumbling_operator(Duration::from_secs(60));
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    // Process events
    let outputs = operator.process(&event1, &mut ctx);
    assert!(outputs.is_empty()); // No intermediate emission

    // Advance watermark past window end
    let outputs = operator.on_timer(window_end_timer, &mut ctx);
    assert_eq!(outputs.len(), 1); // Final emission only
}

#[test]
fn test_emit_strategy_changelog() {
    let mut operator = create_tumbling_operator(Duration::from_secs(60));
    operator.set_emit_strategy(EmitStrategy::Changelog);

    // First emission
    let outputs = operator.on_timer(window_end_timer, &mut ctx);
    assert_eq!(outputs.len(), 1);
    match &outputs[0] {
        Output::Changelog(record) => {
            assert_eq!(record.weight, 1);
            assert_eq!(record.operation, CdcOperation::Insert);
        }
        _ => panic!("Expected changelog output"),
    }

    // Late data causes retraction
    let outputs = operator.process(&late_event, &mut ctx);
    assert_eq!(outputs.len(), 2); // -old, +new
    match (&outputs[0], &outputs[1]) {
        (Output::Changelog(before), Output::Changelog(after)) => {
            assert_eq!(before.weight, -1);
            assert_eq!(after.weight, 1);
        }
        _ => panic!("Expected changelog pair"),
    }
}

#[test]
fn test_emit_strategy_final() {
    let mut operator = create_tumbling_operator(Duration::from_secs(60));
    operator.set_emit_strategy(EmitStrategy::Final);

    // Process events - no intermediate
    let outputs = operator.process(&event1, &mut ctx);
    assert!(outputs.is_empty());

    // Late data - dropped, no retraction
    operator.advance_watermark(window_end + 1000);
    let outputs = operator.process(&late_event, &mut ctx);
    assert!(outputs.is_empty()); // Late data dropped

    // Only final emission on window close
    let outputs = operator.on_timer(final_timer, &mut ctx);
    assert_eq!(outputs.len(), 1);
}

#[test]
fn test_sql_parse_emit_changes() {
    let sql = r#"
        CREATE MATERIALIZED VIEW totals AS
        SELECT customer_id, SUM(amount)
        FROM orders
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let stmt = parse_sql(sql).unwrap();
    assert_eq!(stmt.emit_clause, Some(EmitClause::Changes));
}

#[test]
fn test_sql_parse_emit_final() {
    let sql = r#"
        CREATE MATERIALIZED VIEW report AS
        SELECT DATE(ts), COUNT(*)
        FROM events
        GROUP BY DATE(ts)
        EMIT FINAL
    "#;

    let stmt = parse_sql(sql).unwrap();
    assert_eq!(stmt.emit_clause, Some(EmitClause::Final));
}

#[test]
fn test_emit_strategy_append_only_compatible() {
    assert!(EmitStrategy::OnWindowClose.is_append_only_compatible());
    assert!(EmitStrategy::Final.is_append_only_compatible());
    assert!(!EmitStrategy::OnUpdate.is_append_only_compatible());
    assert!(!EmitStrategy::Changelog.is_append_only_compatible());
}
```

## Acceptance Criteria

- [ ] `EmitStrategy` enum extended with `OnWindowClose`, `Changelog`, `Final`
- [ ] SQL parser recognizes `EMIT ON WINDOW CLOSE`, `EMIT CHANGES`, `EMIT FINAL`
- [ ] `TumblingWindowOperator` handles all emit strategies
- [ ] `SlidingWindowOperator` handles all emit strategies
- [ ] `EmitStrategyPropagationRule` implemented
- [ ] Integration with F063 (Changelog/Retraction) for `Changelog` strategy
- [ ] 15+ unit tests passing
- [ ] SQL parsing tests for new syntax

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Emit strategy check | < 5ns | Simple enum match |
| OnWindowClose buffering | 0 extra alloc | Reuses existing window state |
| Changelog wrapping | < 50ns | ChangelogRecord creation |
| Final late data drop | < 10ns | Early return |

## Dependencies

```
F011 (EMIT Clause) ──► F011B (Extension) ──► F023 (Exactly-Once Sinks)
                             │
F063 (Changelog/Retraction) ─┘
```

## References

- [Emit Patterns Research 2026](../../research/emit-patterns-research-2026.md)
- [F011: EMIT Clause](../phase-1/F011-emit-clause.md)
- [F063: Changelog and Retraction Support](F063-changelog-retraction.md)
- [RisingWave EMIT ON WINDOW CLOSE](https://docs.risingwave.com/processing/emit-on-window-close)
- [Flink Changelog Mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/)
