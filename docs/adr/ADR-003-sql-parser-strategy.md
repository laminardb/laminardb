# ADR-003: SQL Parser Strategy for Production

## Status
Proposed

## Context

The current F006 SQL parser implementation is a proof-of-concept that demonstrates streaming SQL capabilities but is not suitable for production use. Key limitations include:

- Hardcoded values instead of actual parsing
- String matching instead of proper tokenization
- No integration with DataFusion's query planning
- Missing critical SQL features (CTEs, subqueries, joins)
- No validation or error handling

LaminarDB promises "Full SQL Support" as a key differentiator from competitors like Kafka Streams (which has no SQL support).

## Decision Drivers

1. **Production Requirements**
   - Must parse complex streaming SQL queries correctly
   - Must provide clear error messages with line/column information
   - Must integrate seamlessly with DataFusion
   - Must support all promised SQL features

2. **Performance Requirements**
   - Parser throughput: >10K queries/second
   - Parse time: <1ms for typical queries
   - Memory usage: <1MB per query AST

3. **Maintenance Considerations**
   - Code maintainability
   - Ability to add new SQL features
   - Community support and documentation

## Considered Options

### Option 1: Extend sqlparser-rs
**Pros:**
- Already used by DataFusion
- Active community and maintenance
- Written in Rust
- Supports many SQL dialects

**Cons:**
- Not designed for streaming SQL
- Would require significant modifications
- Might need to fork for streaming features

### Option 2: Build Custom Parser
**Pros:**
- Full control over syntax and features
- Optimized for streaming SQL
- No unnecessary features

**Cons:**
- Significant development effort
- Maintenance burden
- Risk of bugs and edge cases

### Option 3: Use Parser Generator (LALRPOP/pest)
**Pros:**
- Grammar-based, easier to maintain
- Clear specification of syntax
- Good error reporting

**Cons:**
- Additional build complexity
- Learning curve
- May be less flexible

### Option 4: Fork RisingWave's Parser
**Pros:**
- Already handles streaming SQL
- PostgreSQL compatible
- Written in Rust
- Production tested

**Cons:**
- Different architecture assumptions
- Potential licensing issues
- Divergence maintenance

## Decision

**Extend sqlparser-rs with streaming SQL capabilities**

This approach:
1. Maintains compatibility with DataFusion
2. Leverages existing parsing infrastructure
3. Benefits from sqlparser community improvements
4. Allows incremental enhancement

Implementation strategy:
1. Create a `StreamingSqlDialect` that adds keywords: TUMBLE, HOP, SESSION, WATERMARK, EMIT
2. Extend the parser with streaming-specific statement types
3. Create a translation layer to DataFusion's LogicalPlan
4. Add comprehensive tests for streaming SQL

## Consequences

### Positive
- Faster time to production-ready parser
- Compatibility with DataFusion ecosystem
- Benefit from sqlparser updates
- Clear upgrade path

### Negative
- Some limitations from sqlparser architecture
- Need to contribute changes upstream
- May need custom modifications for advanced features

### Risks
- sqlparser may not accept streaming SQL extensions
- Performance overhead from generic parser
- Complex integration with DataFusion

## Implementation Plan

### Phase 1: Basic Integration (2 weeks)
- Create StreamingSqlDialect
- Add streaming keywords
- Parse CREATE SOURCE/SINK
- Basic window function support

### Phase 2: Full Integration (3 weeks)
- Complete window function parsing
- EMIT clause support
- Watermark definitions
- Integration with DataFusion logical plans

### Phase 3: Production Hardening (2 weeks)
- Comprehensive error messages
- Performance optimization
- SQL injection prevention
- Prepared statement support

## Alternatives for Future Consideration

If sqlparser proves insufficient, we can:
1. Fork sqlparser and create laminar-sqlparser
2. Build a streaming-specific parser layer on top
3. Contribute streaming SQL as a first-class feature to sqlparser

## References

- [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)
- [DataFusion SQL](https://arrow.apache.org/datafusion/user-guide/sql/index.html)
- [Flink SQL](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)
- [RisingWave SQL](https://docs.risingwave.com/docs/current/sql-references/)