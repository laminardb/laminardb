# SQL Parser Production Improvements

## Current Limitations

The current F006 implementation uses a simplified parser that has several critical limitations:

### 1. **Hardcoded Values**
- Source/sink names are hardcoded ("events", "output_sink")
- Column definitions are not actually parsed
- Watermark expressions are not properly parsed
- WITH options are not extracted from the SQL

### 2. **String-Based Parsing**
- Uses `starts_with()` and `contains()` for keyword detection
- No proper tokenization or AST traversal
- Case-sensitive in some areas
- Prone to false positives (e.g., keywords in strings)

### 3. **Window Function Integration**
- Window functions (TUMBLE, HOP, SESSION) are not integrated into SELECT parsing
- GROUP BY with window functions doesn't actually transform the query
- Window rewriter is incomplete and non-functional

### 4. **Missing Features**
- No support for nested queries
- No support for CTEs (Common Table Expressions)
- No support for multiple sources/joins in streaming queries
- EMIT clause parsing is rudimentary
- No validation of streaming SQL semantics

## Required Improvements

### Phase 1 Completion (Immediate)

1. **Proper sqlparser Integration**
   ```rust
   // Extend sqlparser's Parser with custom keywords
   impl<'a> Parser<'a> {
       fn parse_create_source(&mut self) -> Result<Statement, ParserError>
       fn parse_watermark_clause(&mut self) -> Result<WatermarkDef, ParserError>
       fn parse_emit_clause(&mut self) -> Result<EmitClause, ParserError>
   }
   ```

2. **Window Function Recognition**
   - Properly integrate window functions into DataFusion's query planning
   - Transform GROUP BY TUMBLE(...) into appropriate operators
   - Add window_start/window_end column generation

3. **Comprehensive Testing**
   - Test all SQL edge cases
   - Validate error messages
   - Performance benchmarks for parsing

### Phase 2 Enhancements (Production-Ready)

1. **Custom SQL Dialect**
   ```rust
   pub struct LaminarDialect;

   impl Dialect for LaminarDialect {
       // Define streaming keywords: TUMBLE, HOP, SESSION, WATERMARK, EMIT
       // Support INTERVAL syntax properly
   }
   ```

2. **Query Validation**
   - Validate streaming semantics (e.g., aggregations require windows)
   - Check watermark column exists and is timestamp
   - Validate connector options

3. **Query Optimization**
   - Pushdown predicates through windows
   - Optimize window assignments
   - Merge adjacent windows when possible

4. **Integration with DataFusion**
   - Custom LogicalPlan nodes for streaming operations
   - Custom PhysicalPlan implementations
   - Proper explain plans for debugging

### Example Production Query Support

```sql
-- Complex streaming query that should be supported
CREATE SOURCE orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_time TIMESTAMP,
    amount DECIMAL(10,2),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'order-processor',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE SINK high_value_orders
FROM (
    WITH order_windows AS (
        SELECT
            window_start,
            window_end,
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM orders
        GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR), customer_id
    )
    SELECT
        window_start,
        window_end,
        customer_id,
        order_count,
        total_amount,
        avg_amount
    FROM order_windows
    WHERE total_amount > 1000
        AND order_count > 5
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost/analytics',
    'table-name' = 'high_value_customers',
    'username' = 'analytics',
    'password' = '${env:DB_PASSWORD}'
)
EMIT AFTER WATERMARK;
```

## Implementation Plan

### Step 1: Refactor Current Parser (1 week)
- [ ] Create proper AST visitor for streaming keywords
- [ ] Implement tokenizer extensions for streaming SQL
- [ ] Add proper error handling with line/column info
- [ ] Create comprehensive test suite

### Step 2: Integrate with DataFusion (2 weeks)
- [ ] Create custom LogicalPlan nodes
- [ ] Implement planner rules for streaming operations
- [ ] Add window function support to GROUP BY
- [ ] Integrate EMIT clause into execution

### Step 3: Production Hardening (1 week)
- [ ] Add SQL injection protection
- [ ] Performance optimization
- [ ] Comprehensive error messages
- [ ] Documentation and examples

## Alternative: Use Existing Streaming SQL Parser

Consider adopting or learning from:
1. **Apache Calcite** - Has streaming SQL extensions
2. **Flink SQL Parser** - Mature streaming SQL implementation
3. **RisingWave Parser** - Rust-based, PostgreSQL-compatible

## Decision Required

Before proceeding with Phase 2, we need to decide:
1. Build a production parser from scratch
2. Extend sqlparser significantly
3. Fork and modify an existing streaming SQL parser
4. Create a new parser using a parser generator (LALRPOP, pest)

## Performance Considerations

- Parser should handle 10K+ queries/second
- AST generation < 1ms for typical queries
- Memory usage < 1MB per query AST
- Support for prepared statements to amortize parsing cost