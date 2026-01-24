# F006B: Production SQL Parser

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F006B |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 1.5 (Between Phase 1 and 2) |
| **Effort** | L (2-3 weeks) |
| **Dependencies** | F006 |
| **Owner** | TBD |

## Summary

Upgrade the proof-of-concept SQL parser from F006 to a production-ready implementation that can handle complex streaming SQL queries with proper error handling, validation, and DataFusion integration.

## Goals

- Properly parse all streaming SQL constructs
- Integrate window functions into DataFusion query planning
- Provide clear error messages with line/column information
- Support complex queries with CTEs, subqueries, and joins
- Achieve >10K queries/second parsing throughput

## Success Criteria

- [ ] Parse complex streaming SQL queries correctly
- [ ] All hardcoded values replaced with actual parsing
- [ ] Window functions properly integrated with GROUP BY
- [ ] Comprehensive error messages with location info
- [ ] Performance: <1ms parse time for typical queries
- [ ] Memory: <1MB per query AST
- [ ] 100% test coverage for SQL constructs

## Technical Design

Based on ADR-003, extend sqlparser-rs:

```rust
// Custom dialect for streaming SQL
pub struct StreamingSqlDialect;

impl Dialect for StreamingSqlDialect {
    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        matches!(kw,
            Keyword::TUMBLE |
            Keyword::HOP |
            Keyword::SESSION |
            Keyword::WATERMARK |
            Keyword::EMIT
        ) || self.base_dialect.is_reserved_for_identifier(kw)
    }
}

// Parser extensions
impl Parser {
    pub fn parse_watermark(&mut self) -> Result<WatermarkDef, ParserError> {
        self.expect_keyword(Keyword::WATERMARK)?;
        self.expect_keyword(Keyword::FOR)?;
        let column = self.parse_identifier()?;
        self.expect_keyword(Keyword::AS)?;
        let expression = self.parse_expr()?;
        Ok(WatermarkDef { column, expression })
    }

    pub fn parse_window_function(&mut self) -> Result<WindowFunction, ParserError> {
        // Proper parsing of TUMBLE/HOP/SESSION with arguments
    }
}
```

## Implementation Plan

### Week 1: Parser Extensions
- [ ] Create StreamingSqlDialect with keywords
- [ ] Extend parser for CREATE SOURCE/SINK
- [ ] Add watermark parsing
- [ ] Add EMIT clause parsing

### Week 2: Window Function Integration
- [ ] Parse window functions in expressions
- [ ] Transform GROUP BY window functions
- [ ] Generate window_start/window_end columns
- [ ] Create DataFusion LogicalPlan nodes

### Week 3: Production Hardening
- [ ] Comprehensive error handling
- [ ] Performance optimization
- [ ] SQL injection prevention
- [ ] Integration tests with DataFusion

## Test Cases

```sql
-- Complex query that must parse correctly
CREATE SOURCE clicks (
    user_id BIGINT,
    page_id VARCHAR,
    click_time TIMESTAMP,
    WATERMARK FOR click_time AS click_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'user-clicks'
);

WITH hourly_stats AS (
    SELECT
        window_start,
        window_end,
        page_id,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(*) as total_clicks
    FROM clicks
    GROUP BY TUMBLE(click_time, INTERVAL '1' HOUR), page_id
),
top_pages AS (
    SELECT *
    FROM hourly_stats
    WHERE unique_users > 100
    ORDER BY total_clicks DESC
    LIMIT 10
)
SELECT
    window_start,
    window_end,
    page_id,
    unique_users,
    total_clicks,
    total_clicks / unique_users as clicks_per_user
FROM top_pages
EMIT ON WINDOW CLOSE;
```

## Risks

- sqlparser may not accept our streaming extensions
- Performance overhead from generic parser
- Complex integration with DataFusion's optimizer

## Dependencies

- sqlparser-rs 0.60+
- DataFusion 52.0+
- Comprehensive test suite from F006