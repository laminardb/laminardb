# F006B: Production SQL Parser

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F006B |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 1.5 (Between Phase 1 and 2) |
| **Effort** | L (2-3 weeks) |
| **Dependencies** | F006, F004, F016, F019, F020 |
| **Owner** | TBD |
| **ADR** | ADR-003 |

## Summary

Replace the POC SQL parser with a production-ready implementation that properly parses streaming SQL (CREATE SOURCE/SINK, window functions, joins) and generates executable Ring 0 operator configurations.

## Problem Statement

The current parser in `crates/laminar-sql/src/parser/` is a proof-of-concept with critical issues:

| Issue | Location | Impact |
|-------|----------|--------|
| CREATE SOURCE hardcoded | `parser_simple.rs:61-67` | Returns `name="events"`, empty columns/watermark |
| CREATE SINK hardcoded | `parser_simple.rs:71-76` | Returns `name="output_sink"`, empty options |
| Window args hardcoded | `window_rewriter.rs:88-100` | Always returns `5 MINUTES` regardless of SQL |
| Window rewrite empty | `window_rewriter.rs:47-54` | `rewrite_select()` is no-op |
| Planner stubbed | `planner/mod.rs:22-25` | Contains `todo!()` - panics when called |

## Goals

- Parse all streaming SQL constructs with actual values (not hardcoded)
- Generate correct operator configurations for windows and joins
- Integrate with DataFusion for standard SQL optimization
- Provide clear error messages with location info
- Achieve <1ms parse time for typical queries

## Success Criteria

- [ ] CREATE SOURCE parses columns, watermarks, WITH options
- [ ] CREATE SINK parses target, query, WITH options
- [ ] Window functions extract actual time column and intervals
- [ ] JOIN queries detect key columns and time bounds
- [ ] Query planner produces executable StreamingPlan
- [ ] All 56 existing SQL tests pass
- [ ] 30+ new tests for production parsing
- [ ] Performance: <1ms parse, <1MB AST

---

## Implementation Phases

### Phase 1: CREATE SOURCE/SINK Parsing (2-3 days)

**Goal**: Fix hardcoded DDL statements to parse actual SQL content.

#### Files to Modify

| File | Changes |
|------|---------|
| `parser/parser_simple.rs` | Replace hardcoded parse_create_source/sink |

#### Implementation Details

**Current Broken Code** (`parser_simple.rs:60-76`):
```rust
// HARDCODED - completely ignores actual SQL!
StreamingStatement::CreateSource(CreateSourceStatement {
    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
    columns: vec![],              // Always empty!
    watermark: None,              // Never parsed!
    with_options: HashMap::new(), // Always empty!
    or_replace: false,
    if_not_exists: false,
})
```

**New Functions to Add**:
```rust
impl StreamingParser {
    /// Parse CREATE SOURCE with columns, watermark, and WITH options
    fn parse_create_source(sql: &str) -> Result<CreateSourceStatement, ParserError> {
        // 1. Extract source name after CREATE SOURCE [IF NOT EXISTS]
        // 2. Parse column definitions from parentheses
        // 3. Look for WATERMARK FOR <col> AS <expr> in column list
        // 4. Parse WITH ('key' = 'value', ...) clause
    }

    /// Parse CREATE SINK with target and WITH options
    fn parse_create_sink(sql: &str) -> Result<CreateSinkStatement, ParserError> {
        // 1. Extract sink name after CREATE SINK
        // 2. Parse FROM <table> or FROM (<subquery>)
        // 3. Parse WITH options
    }

    /// Parse column definitions including watermark
    fn parse_column_definitions(sql: &str) -> Result<(Vec<ColumnDef>, Option<WatermarkDef>), ParserError> {
        // Leverage sqlparser by transforming to CREATE TABLE
        let create_table = format!("CREATE TABLE temp {}", columns_part);
        let stmt = Parser::parse_sql(&GenericDialect {}, &create_table)?;
        // Extract ColumnDef from parsed CREATE TABLE
    }

    /// Parse WITH ('key' = 'value', ...) into HashMap
    fn parse_with_options(sql: &str) -> Result<HashMap<String, String>, ParserError> {
        // Find WITH clause, parse key-value pairs
    }
}
```

#### Test Cases

```sql
-- Test 1: Basic source with columns
CREATE SOURCE events (
    id BIGINT,
    user_id VARCHAR,
    event_time TIMESTAMP
);
-- Expected: name="events", 3 columns, no watermark

-- Test 2: Source with watermark
CREATE SOURCE orders (
    order_id BIGINT,
    amount DECIMAL(10,2),
    order_time TIMESTAMP,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'orders', 'bootstrap.servers' = 'localhost:9092');
-- Expected: name="orders", 3 columns, watermark on order_time, 3 WITH options

-- Test 3: IF NOT EXISTS
CREATE SOURCE IF NOT EXISTS clicks (...);
-- Expected: if_not_exists=true

-- Test 4: Sink with table source
CREATE SINK output_sink FROM processed_orders WITH ('connector' = 'kafka');
-- Expected: name="output_sink", from=Table("processed_orders"), 1 WITH option

-- Test 5: Sink with query source
CREATE SINK alerts FROM (SELECT * FROM events WHERE severity > 5);
-- Expected: from=Query(SELECT ...)
```

#### Acceptance Criteria
- [ ] Source name correctly extracted
- [ ] All column definitions parsed with types
- [ ] Watermark expression parsed correctly
- [ ] WITH options extracted to HashMap
- [ ] IF NOT EXISTS / OR REPLACE flags detected
- [ ] 5 new test cases passing

---

### Phase 2: Window Function Extraction (3-4 days)

**Goal**: Parse TUMBLE/HOP/SESSION with actual parameters from SQL.

#### Files to Modify

| File | Changes |
|------|---------|
| `parser/window_rewriter.rs` | Fix extract_window_function, implement rewrite_select |

#### Implementation Details

**Current Broken Code** (`window_rewriter.rs:88-100`):
```rust
// HARDCODED - ignores actual SQL arguments!
"TUMBLE" => Ok(Some(WindowFunction::Tumble {
    time_column: Box::new(Expr::Identifier(Ident::new("event_time"))), // Fixed!
    interval: Box::new(Expr::Identifier(Ident::new("5 MINUTES"))),     // Fixed!
}))
```

**New Functions to Add**:
```rust
impl WindowRewriter {
    /// Extract window function with actual arguments
    pub fn extract_window_function(expr: &Expr) -> Result<Option<WindowFunction>, ParseError> {
        match expr {
            Expr::Function(func) => {
                let name = func.name.0.last()?.to_string().to_uppercase();
                let args = match &func.args {
                    FunctionArguments::List(list) => &list.args,
                    _ => return Ok(None),
                };

                match name.as_str() {
                    "TUMBLE" => Self::parse_tumble_args(args),
                    "HOP" | "SLIDE" => Self::parse_hop_args(args),
                    "SESSION" => Self::parse_session_args(args),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Parse TUMBLE(time_col, interval) → 2 args
    fn parse_tumble_args(args: &[FunctionArg]) -> Result<Option<WindowFunction>, ParseError> {
        if args.len() != 2 {
            return Err(ParseError::WindowError(
                "TUMBLE requires 2 arguments: (time_column, interval)".into()
            ));
        }
        let time_column = Self::extract_column_expr(&args[0])?;
        let interval = Self::extract_interval_expr(&args[1])?;
        Ok(Some(WindowFunction::Tumble { time_column, interval }))
    }

    /// Parse HOP(time_col, slide, size) → 3 args
    fn parse_hop_args(args: &[FunctionArg]) -> Result<Option<WindowFunction>, ParseError> {
        if args.len() != 3 {
            return Err(ParseError::WindowError(
                "HOP requires 3 arguments: (time_column, slide_interval, window_size)".into()
            ));
        }
        // Extract all 3 arguments
    }

    /// Parse SESSION(time_col, gap) → 2 args
    fn parse_session_args(args: &[FunctionArg]) -> Result<Option<WindowFunction>, ParseError>;

    /// Convert INTERVAL expression to Duration
    fn parse_interval_to_duration(expr: &Expr) -> Result<Duration, ParseError> {
        match expr {
            Expr::Interval(interval) => {
                let value: u64 = parse_interval_value(&interval.value)?;
                let unit = interval.leading_field.unwrap_or(DateTimeField::Second);
                Ok(match unit {
                    DateTimeField::Second => Duration::from_secs(value),
                    DateTimeField::Minute => Duration::from_secs(value * 60),
                    DateTimeField::Hour => Duration::from_secs(value * 3600),
                    DateTimeField::Day => Duration::from_secs(value * 86400),
                    _ => return Err(ParseError::WindowError("Unsupported interval unit".into())),
                })
            }
            _ => Err(ParseError::WindowError("Expected INTERVAL expression".into())),
        }
    }

    /// Rewrite SELECT to add window_start/window_end columns (currently empty!)
    fn rewrite_select(select: &mut Select) {
        // 1. Find window function in GROUP BY
        // 2. Extract window parameters
        // 3. Add window_start, window_end to projection
        // 4. Replace window function in GROUP BY with window_start, window_end
    }
}
```

#### Test Cases

```sql
-- Test 1: Tumbling window with minutes
SELECT COUNT(*) FROM events GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);
-- Expected: time_column="event_time", interval=300s

-- Test 2: Tumbling window with hours
SELECT SUM(amount) FROM orders GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR);
-- Expected: time_column="order_time", interval=3600s

-- Test 3: Sliding (hop) window
SELECT AVG(value) FROM readings GROUP BY HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);
-- Expected: time_column="ts", slide=60s, size=300s

-- Test 4: Session window
SELECT * FROM clicks GROUP BY SESSION(click_time, INTERVAL '30' MINUTE);
-- Expected: time_column="click_time", gap=1800s

-- Test 5: Window with additional GROUP BY columns
SELECT user_id, COUNT(*) FROM events
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR), user_id;
-- Expected: Window extracted, user_id preserved in GROUP BY

-- Test 6: Invalid - wrong number of args
SELECT * FROM events GROUP BY TUMBLE(ts);
-- Expected: Error "TUMBLE requires 2 arguments"
```

#### Acceptance Criteria
- [ ] TUMBLE extracts time_column and interval from actual SQL
- [ ] HOP extracts time_column, slide, and size
- [ ] SESSION extracts time_column and gap
- [ ] INTERVAL parsing handles SECOND/MINUTE/HOUR/DAY
- [ ] Error on invalid argument count
- [ ] 6 new test cases passing

---

### Phase 3: EMIT/Late Data Integration (2-3 days)

**Goal**: Connect parsed EMIT and LATE DATA clauses to operator configuration.

#### Files to Modify/Create

| File | Changes |
|------|---------|
| `parser/statements.rs` | Add conversion methods |
| `translator/mod.rs` | **NEW** - Module for SQL→operator translation |
| `translator/window_translator.rs` | **NEW** - Window operator config builder |

#### Implementation Details

**Add to `statements.rs`**:
```rust
impl EmitClause {
    /// Convert to laminar-core EmitStrategy
    pub fn to_emit_strategy(&self) -> Result<EmitStrategy, ParseError> {
        match self {
            EmitClause::AfterWatermark | EmitClause::OnWindowClose =>
                Ok(EmitStrategy::OnWatermark),
            EmitClause::Periodically { interval } => {
                let duration = WindowRewriter::parse_interval_to_duration(interval)?;
                Ok(EmitStrategy::Periodic(duration))
            }
            EmitClause::OnUpdate => Ok(EmitStrategy::OnUpdate),
        }
    }
}

impl LateDataClause {
    /// Convert to LateDataConfig
    pub fn to_late_data_config(&self) -> LateDataConfig {
        match &self.side_output {
            Some(name) => LateDataConfig::with_side_output(name.clone()),
            None => LateDataConfig::drop(),
        }
    }

    /// Extract allowed lateness as Duration
    pub fn to_allowed_lateness(&self) -> Result<Duration, ParseError> {
        match &self.allowed_lateness {
            Some(expr) => WindowRewriter::parse_interval_to_duration(expr),
            None => Ok(Duration::ZERO),
        }
    }
}
```

**Create `translator/window_translator.rs`**:
```rust
/// Complete configuration for instantiating a window operator
pub struct WindowOperatorConfig {
    pub window_type: WindowType,
    pub time_column: String,
    pub size: Duration,
    pub slide: Option<Duration>,      // For sliding windows
    pub gap: Option<Duration>,        // For session windows
    pub aggregations: Vec<AggregationSpec>,
    pub allowed_lateness: Duration,
    pub emit_strategy: EmitStrategy,
    pub late_data_config: LateDataConfig,
}

pub enum WindowType {
    Tumbling,
    Sliding,
    Session,
}

impl WindowOperatorConfig {
    /// Build config from parsed streaming statement
    pub fn from_continuous_query(
        query: &Statement,
        emit_clause: Option<&EmitClause>,
        late_data_clause: Option<&LateDataClause>,
    ) -> Result<Option<Self>, ParseError> {
        // 1. Find window function in GROUP BY
        // 2. Extract aggregations from SELECT
        // 3. Build complete config
    }

    /// Instantiate the appropriate window operator
    pub fn to_operator(&self) -> Result<Box<dyn Operator>, Error> {
        match self.window_type {
            WindowType::Tumbling => {
                let assigner = TumblingWindowAssigner::new(self.size);
                let aggregator = self.build_aggregator()?;
                Ok(Box::new(TumblingWindowOperator::new(
                    assigner, aggregator, self.allowed_lateness
                )))
            }
            WindowType::Sliding => {
                let assigner = SlidingWindowAssigner::new(self.size, self.slide.unwrap());
                let aggregator = self.build_aggregator()?;
                Ok(Box::new(SlidingWindowOperator::new(
                    assigner, aggregator, self.allowed_lateness
                )))
            }
            WindowType::Session => {
                // Session windows not yet implemented (F017)
                Err(Error::NotImplemented("Session windows"))
            }
        }
    }
}
```

#### Test Cases

```sql
-- Test 1: EMIT AFTER WATERMARK (default)
SELECT COUNT(*) FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) EMIT AFTER WATERMARK;
-- Expected: emit_strategy=OnWatermark

-- Test 2: EMIT PERIODICALLY
SELECT SUM(amount) FROM orders GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) EMIT PERIODICALLY INTERVAL '10' SECOND;
-- Expected: emit_strategy=Periodic(10s)

-- Test 3: EMIT ON UPDATE
SELECT COUNT(*) FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) EMIT ON UPDATE;
-- Expected: emit_strategy=OnUpdate

-- Test 4: Late data with side output
SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
ALLOW LATENESS INTERVAL '5' MINUTE LATE DATA TO late_events;
-- Expected: allowed_lateness=300s, late_data_config=SideOutput("late_events")

-- Test 5: Full config
SELECT COUNT(*) FROM events
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT PERIODICALLY INTERVAL '30' SECOND
ALLOW LATENESS INTERVAL '2' MINUTE;
-- Expected: Full WindowOperatorConfig with all fields set
```

#### Acceptance Criteria
- [ ] EmitClause.to_emit_strategy() works for all variants
- [ ] LateDataClause.to_late_data_config() handles side output
- [ ] WindowOperatorConfig combines all settings
- [ ] Can instantiate TumblingWindowOperator from config
- [ ] Can instantiate SlidingWindowOperator from config
- [ ] 5 new test cases passing

---

### Phase 4: Join Query Parsing (3-4 days)

**Goal**: Extract join keys and time bounds from JOIN clauses.

#### Files to Create

| File | Purpose |
|------|---------|
| `parser/join_parser.rs` | **NEW** - Join analysis and extraction |
| `translator/join_translator.rs` | **NEW** - Join operator config builder |

#### Implementation Details

**Create `parser/join_parser.rs`**:
```rust
/// Analysis result for a JOIN clause
pub struct JoinAnalysis {
    pub join_type: JoinType,
    pub left_table: String,
    pub right_table: String,
    pub left_key_column: String,
    pub right_key_column: String,
    pub time_bound: Option<Duration>,  // None for lookup joins
    pub is_lookup_join: bool,
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Analyze a SELECT statement for join information
pub fn analyze_join(select: &Select) -> Result<Option<JoinAnalysis>, ParseError> {
    let from = &select.from;
    if from.is_empty() || from[0].joins.is_empty() {
        return Ok(None);
    }

    let join = &from[0].joins[0];
    let left_table = extract_table_name(&from[0].relation)?;
    let right_table = extract_table_name(&join.relation)?;

    let (left_key, right_key, time_bound) = analyze_join_constraint(&join.join_operator)?;

    Ok(Some(JoinAnalysis {
        join_type: map_join_type(&join.join_operator),
        left_table,
        right_table,
        left_key_column: left_key,
        right_key_column: right_key,
        time_bound,
        is_lookup_join: time_bound.is_none(),
    }))
}

/// Extract key columns and time bound from ON clause
fn analyze_join_constraint(
    constraint: &JoinConstraint
) -> Result<(String, String, Option<Duration>), ParseError> {
    match constraint {
        JoinConstraint::On(expr) => {
            // Analyze expression tree for:
            // 1. Equality: a.col = b.col
            // 2. Temporal: p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR
            analyze_on_expression(expr)
        }
        JoinConstraint::Using(cols) => {
            // Simple case: JOIN ... USING (column)
            Ok((cols[0].to_string(), cols[0].to_string(), None))
        }
        _ => Err(ParseError::JoinError("Unsupported join constraint".into())),
    }
}

/// Extract time bound from temporal predicates
fn extract_temporal_bound(expr: &Expr) -> Result<Option<Duration>, ParseError> {
    // Pattern 1: p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR
    // Pattern 2: p.ts >= o.ts AND p.ts <= o.ts + INTERVAL '1' HOUR
    // Pattern 3: ABS(p.ts - o.ts) <= INTERVAL '30' MINUTE
    match expr {
        Expr::Between { low, high, .. } => {
            // Extract interval from high - low
        }
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            // Look for time bounds in compound predicate
        }
        _ => Ok(None),
    }
}
```

**Create `translator/join_translator.rs`**:
```rust
/// Configuration for join operator instantiation
pub enum JoinOperatorConfig {
    StreamStream {
        left_key: String,
        right_key: String,
        time_bound: Duration,
        join_type: stream_join::JoinType,
    },
    Lookup {
        stream_key: String,
        lookup_key: String,
        join_type: lookup_join::LookupJoinType,
        cache_ttl: Duration,
    },
}

impl JoinOperatorConfig {
    /// Build from join analysis
    pub fn from_analysis(analysis: &JoinAnalysis) -> Result<Self, Error> {
        if analysis.is_lookup_join {
            Ok(JoinOperatorConfig::Lookup {
                stream_key: analysis.left_key_column.clone(),
                lookup_key: analysis.right_key_column.clone(),
                join_type: map_to_lookup_join_type(analysis.join_type),
                cache_ttl: Duration::from_secs(300), // Default 5 min
            })
        } else {
            Ok(JoinOperatorConfig::StreamStream {
                left_key: analysis.left_key_column.clone(),
                right_key: analysis.right_key_column.clone(),
                time_bound: analysis.time_bound.unwrap(),
                join_type: map_to_stream_join_type(analysis.join_type),
            })
        }
    }

    /// Instantiate the appropriate join operator
    pub fn to_stream_join_operator(&self) -> Result<StreamJoinOperator, Error> {
        match self {
            JoinOperatorConfig::StreamStream { left_key, right_key, time_bound, join_type } => {
                Ok(StreamJoinOperator::new(
                    left_key.clone(),
                    right_key.clone(),
                    *time_bound,
                    *join_type,
                ))
            }
            _ => Err(Error::InvalidConfig("Not a stream-stream join")),
        }
    }

    pub fn to_lookup_join_operator(&self, loader: Arc<dyn TableLoader>) -> Result<LookupJoinOperator, Error> {
        match self {
            JoinOperatorConfig::Lookup { stream_key, lookup_key, join_type, cache_ttl } => {
                let config = LookupJoinConfig::builder()
                    .stream_key_column(stream_key)
                    .lookup_key_column(lookup_key)
                    .join_type(*join_type)
                    .cache_ttl(*cache_ttl)
                    .build();
                Ok(LookupJoinOperator::new(config))
            }
            _ => Err(Error::InvalidConfig("Not a lookup join")),
        }
    }
}
```

#### Test Cases

```sql
-- Test 1: Inner stream-stream join with time bound
SELECT o.*, p.status FROM orders o
INNER JOIN payments p ON o.order_id = p.order_id
AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR;
-- Expected: StreamStream, time_bound=3600s, Inner

-- Test 2: Left outer join
SELECT o.*, p.status FROM orders o
LEFT JOIN payments p ON o.order_id = p.order_id
AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '30' MINUTE;
-- Expected: StreamStream, time_bound=1800s, Left

-- Test 3: Symmetric time bound
SELECT * FROM stream_a a JOIN stream_b b
ON a.key = b.key AND ABS(a.ts - b.ts) <= INTERVAL '5' MINUTE;
-- Expected: StreamStream, time_bound=300s

-- Test 4: Lookup join (no time bound)
SELECT o.*, c.name, c.tier FROM orders o
JOIN customers c ON o.customer_id = c.id;
-- Expected: Lookup, is_lookup_join=true

-- Test 5: Full outer join
SELECT * FROM stream_a a FULL OUTER JOIN stream_b b
ON a.id = b.id AND b.ts BETWEEN a.ts - INTERVAL '10' MINUTE AND a.ts + INTERVAL '10' MINUTE;
-- Expected: StreamStream, time_bound=600s, Full
```

#### Acceptance Criteria
- [ ] Key columns extracted from ON clause
- [ ] Time bound extracted from BETWEEN/comparison predicates
- [ ] Join type correctly identified (Inner/Left/Right/Full)
- [ ] Lookup vs stream-stream distinguished by presence of time bound
- [ ] Can instantiate StreamJoinOperator from config
- [ ] Can instantiate LookupJoinOperator from config
- [ ] 5 new test cases passing

---

### Phase 5: Query Planner Integration (4-5 days)

**Goal**: Replace `todo!()` with actual StreamingPlan generation.

#### Files to Modify/Create

| File | Changes |
|------|---------|
| `planner/mod.rs` | Replace todo!() with implementation |
| `planner/streaming_plan.rs` | **NEW** - Plan representation |
| `planner/operator_builder.rs` | **NEW** - Operator instantiation |

#### Implementation Details

**Current Broken Code** (`planner/mod.rs:22-25`):
```rust
pub fn plan(&self, _statement: &StreamingStatement) -> Result<LogicalPlan, PlanningError> {
    todo!("Implement streaming query planning (F006)")  // PANICS!
}
```

**Create `planner/streaming_plan.rs`**:
```rust
/// Streaming-aware query plan that can be executed
pub enum StreamingPlan {
    /// Standard SQL executed via DataFusion
    DataFusion(LogicalPlan),

    /// Window aggregation with Ring 0 operator
    WindowAggregation {
        input: Box<StreamingPlan>,
        config: WindowOperatorConfig,
    },

    /// Stream-stream join with Ring 0 operator
    StreamJoin {
        left: Box<StreamingPlan>,
        right: Box<StreamingPlan>,
        config: JoinOperatorConfig,
    },

    /// Lookup join with Ring 0 operator
    LookupJoin {
        input: Box<StreamingPlan>,
        config: JoinOperatorConfig,
        table_name: String,
    },

    /// Source definition (DDL)
    CreateSource(CreateSourceStatement),

    /// Sink definition (DDL)
    CreateSink(CreateSinkStatement),
}
```

**Update `planner/mod.rs`**:
```rust
pub struct StreamingPlanner {
    catalog: Arc<dyn Catalog>,  // For table metadata
}

impl StreamingPlanner {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Main entry point - plan a streaming statement
    pub fn plan(&self, statement: &StreamingStatement) -> Result<StreamingPlan, PlanningError> {
        match statement {
            StreamingStatement::Standard(stmt) => self.plan_standard(stmt),
            StreamingStatement::CreateSource(src) => Ok(StreamingPlan::CreateSource(src.clone())),
            StreamingStatement::CreateSink(sink) => Ok(StreamingPlan::CreateSink(sink.clone())),
            StreamingStatement::CreateContinuousQuery { query, emit_clause, late_data_clause, .. } => {
                self.plan_continuous_query(query, emit_clause.as_ref(), late_data_clause.as_ref())
            }
        }
    }

    /// Plan a continuous query - detect windows and joins
    fn plan_continuous_query(
        &self,
        query: &Statement,
        emit_clause: Option<&EmitClause>,
        late_data_clause: Option<&LateDataClause>,
    ) -> Result<StreamingPlan, PlanningError> {
        // 1. Check for window functions in GROUP BY
        if let Some(window_config) = WindowOperatorConfig::from_query(query, emit_clause, late_data_clause)? {
            let input = self.plan_input_source(query)?;
            return Ok(StreamingPlan::WindowAggregation {
                input: Box::new(input),
                config: window_config,
            });
        }

        // 2. Check for joins
        if let Some(join_analysis) = analyze_join(query)? {
            let config = JoinOperatorConfig::from_analysis(&join_analysis)?;
            if join_analysis.is_lookup_join {
                let input = self.plan_input_source(query)?;
                return Ok(StreamingPlan::LookupJoin {
                    input: Box::new(input),
                    config,
                    table_name: join_analysis.right_table,
                });
            } else {
                let left = self.plan_table_source(&join_analysis.left_table)?;
                let right = self.plan_table_source(&join_analysis.right_table)?;
                return Ok(StreamingPlan::StreamJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    config,
                });
            }
        }

        // 3. Fall back to DataFusion for standard SQL
        let logical_plan = self.to_datafusion_plan(query)?;
        Ok(StreamingPlan::DataFusion(logical_plan))
    }
}
```

**Create `planner/operator_builder.rs`**:
```rust
/// Builds Ring 0 operators from StreamingPlan
pub struct OperatorBuilder {
    table_loaders: HashMap<String, Arc<dyn TableLoader>>,
}

impl OperatorBuilder {
    /// Build operator chain from plan
    pub fn build(&self, plan: &StreamingPlan) -> Result<Vec<Box<dyn Operator>>, Error> {
        match plan {
            StreamingPlan::WindowAggregation { config, .. } => {
                Ok(vec![config.to_operator()?])
            }
            StreamingPlan::StreamJoin { config, .. } => {
                Ok(vec![Box::new(config.to_stream_join_operator()?)])
            }
            StreamingPlan::LookupJoin { config, table_name, .. } => {
                let loader = self.table_loaders.get(table_name)
                    .ok_or(Error::TableNotFound(table_name.clone()))?;
                Ok(vec![Box::new(config.to_lookup_join_operator(Arc::clone(loader))?)])
            }
            StreamingPlan::DataFusion(_) => {
                // DataFusion plans are executed separately
                Ok(vec![])
            }
            _ => Ok(vec![]),
        }
    }
}
```

#### Test Cases

```sql
-- Test 1: Window query → WindowAggregation plan
SELECT COUNT(*) FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
-- Expected: StreamingPlan::WindowAggregation with TumblingWindowOperator

-- Test 2: Stream-stream join → StreamJoin plan
SELECT * FROM orders o JOIN payments p ON o.id = p.order_id
AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR;
-- Expected: StreamingPlan::StreamJoin with StreamJoinOperator

-- Test 3: Lookup join → LookupJoin plan
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;
-- Expected: StreamingPlan::LookupJoin with LookupJoinOperator

-- Test 4: Standard SQL → DataFusion plan
SELECT * FROM events WHERE id > 100;
-- Expected: StreamingPlan::DataFusion

-- Test 5: End-to-end operator instantiation
-- Plan → OperatorBuilder → Working operator
```

#### Acceptance Criteria
- [ ] StreamingPlanner.plan() no longer panics
- [ ] Window queries produce WindowAggregation plan
- [ ] Join queries produce StreamJoin or LookupJoin plan
- [ ] Standard SQL falls back to DataFusion
- [ ] OperatorBuilder can instantiate operators from plan
- [ ] 5 new test cases passing

---

### Phase 6: Aggregator Detection (2-3 days)

**Goal**: Parse SELECT aggregations and map to operator aggregators.

#### Files to Create

| File | Purpose |
|------|---------|
| `parser/aggregation_parser.rs` | **NEW** - Aggregation extraction |

#### Implementation Details

```rust
/// Specification for a single aggregation
pub struct AggregationSpec {
    pub function: AggregationType,
    pub column: Option<String>,  // None for COUNT(*)
    pub alias: Option<String>,
}

pub enum AggregationType {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// Extract aggregations from SELECT projection
pub fn extract_aggregations(projection: &[SelectItem]) -> Result<Vec<AggregationSpec>, ParseError> {
    projection.iter()
        .filter_map(|item| extract_from_item(item).transpose())
        .collect()
}

fn extract_from_item(item: &SelectItem) -> Result<Option<AggregationSpec>, ParseError> {
    let (expr, alias) = match item {
        SelectItem::UnnamedExpr(e) => (e, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.to_string())),
        _ => return Ok(None),
    };

    match expr {
        Expr::Function(func) => {
            let name = func.name.0.last()?.to_string().to_uppercase();
            let function = match name.as_str() {
                "COUNT" => AggregationType::Count,
                "SUM" => AggregationType::Sum,
                "MIN" => AggregationType::Min,
                "MAX" => AggregationType::Max,
                "AVG" => AggregationType::Avg,
                _ => return Ok(None),
            };
            let column = extract_column_from_args(&func.args)?;
            Ok(Some(AggregationSpec { function, column, alias }))
        }
        _ => Ok(None),
    }
}

impl AggregationSpec {
    /// Convert to laminar-core Aggregator
    pub fn to_aggregator(&self, schema: &Schema) -> Result<Box<dyn Aggregator>, Error> {
        let column_index = self.column.as_ref()
            .map(|name| schema.index_of(name))
            .transpose()?;

        Ok(match self.function {
            AggregationType::Count => Box::new(CountAggregator::new()),
            AggregationType::Sum => Box::new(SumAggregator::new(column_index.unwrap_or(0))),
            AggregationType::Min => Box::new(MinAggregator::new(column_index.unwrap_or(0))),
            AggregationType::Max => Box::new(MaxAggregator::new(column_index.unwrap_or(0))),
            AggregationType::Avg => Box::new(AvgAggregator::new(column_index.unwrap_or(0))),
        })
    }
}
```

#### Test Cases

```sql
-- Test 1: COUNT(*)
SELECT COUNT(*) FROM events GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);
-- Expected: [AggregationSpec { Count, None, None }]

-- Test 2: SUM with alias
SELECT SUM(amount) as total FROM orders GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
-- Expected: [AggregationSpec { Sum, Some("amount"), Some("total") }]

-- Test 3: Multiple aggregations
SELECT
    COUNT(*) as count,
    SUM(amount) as total,
    AVG(amount) as average,
    MIN(amount) as minimum,
    MAX(amount) as maximum
FROM orders GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR);
-- Expected: 5 AggregationSpecs

-- Test 4: Aggregator instantiation
-- Each AggregationSpec → working Aggregator instance
```

#### Acceptance Criteria
- [ ] COUNT/SUM/MIN/MAX/AVG detected from SELECT
- [ ] Column names extracted correctly
- [ ] Aliases preserved
- [ ] Can instantiate correct Aggregator type
- [ ] 4 new test cases passing

---

## Verification

### Run Tests

```bash
# All SQL tests
cargo test -p laminar-sql --lib

# Specific phase tests
cargo test -p laminar-sql parse_create_source
cargo test -p laminar-sql window_function
cargo test -p laminar-sql join_analysis
cargo test -p laminar-sql streaming_plan

# Integration test
cargo test -p laminar-sql integration
```

### Check Quality

```bash
cargo clippy -p laminar-sql -- -D warnings
cargo fmt -p laminar-sql --check
```

### Performance Benchmark

```bash
# Parser performance (target: <1ms)
cargo bench -p laminar-sql parse_benchmark
```

---

## File Summary

### Modified Files

| File | Phase |
|------|-------|
| `crates/laminar-sql/src/parser/parser_simple.rs` | 1 |
| `crates/laminar-sql/src/parser/window_rewriter.rs` | 2 |
| `crates/laminar-sql/src/parser/statements.rs` | 3 |
| `crates/laminar-sql/src/planner/mod.rs` | 5 |
| `crates/laminar-sql/src/lib.rs` | 3, 4, 5, 6 |

### New Files

| File | Phase |
|------|-------|
| `crates/laminar-sql/src/parser/join_parser.rs` | 4 |
| `crates/laminar-sql/src/parser/aggregation_parser.rs` | 6 |
| `crates/laminar-sql/src/translator/mod.rs` | 3 |
| `crates/laminar-sql/src/translator/window_translator.rs` | 3 |
| `crates/laminar-sql/src/translator/join_translator.rs` | 4 |
| `crates/laminar-sql/src/planner/streaming_plan.rs` | 5 |
| `crates/laminar-sql/src/planner/operator_builder.rs` | 5 |

---

## Dependencies

```
Phase 1 (CREATE SOURCE/SINK)
    │
    ├───> Phase 2 (Window Functions)
    │         │
    │         └───> Phase 3 (EMIT/Late Data)
    │                   │
    │                   └───────────────────┐
    │                                       │
    └───> Phase 4 (Joins) ──────────────────┤
                                            │
                                            v
                                    Phase 5 (Planner)
                                            │
                                            v
                                    Phase 6 (Aggregators)
```

- Phase 1 is independent - **start here**
- Phases 2 and 4 can run in parallel after Phase 1
- Phase 3 depends on Phase 2
- Phase 5 requires Phases 2, 3, and 4
- Phase 6 can parallel with Phase 4, required by Phase 5
