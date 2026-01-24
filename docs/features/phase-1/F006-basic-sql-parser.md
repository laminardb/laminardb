# F006: Basic SQL Parser

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F006 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F005 |
| **Owner** | Completed |

## Summary

Extend DataFusion's SQL parser to support LaminarDB-specific streaming SQL extensions including window functions, watermarks, EMIT clauses, and CREATE SOURCE/SINK statements.

## Goals

- Parse streaming DDL (CREATE SOURCE, CREATE SINK)
- Support window function syntax (TUMBLE, HOP, SESSION)
- Handle EMIT clause for output control
- Parse watermark definitions

## Technical Design

```rust
pub enum StreamingStatement {
    CreateSource {
        name: String,
        columns: Vec<ColumnDef>,
        watermark: Option<WatermarkDef>,
        with_options: HashMap<String, String>,
    },
    CreateSink {
        name: String,
        columns: Vec<ColumnDef>,
        with_options: HashMap<String, String>,
    },
}

pub struct WatermarkDef {
    pub column: String,
    pub expression: Expr,
}
```

## Completion Checklist

- [x] DDL parsing implemented (POC level)
- [x] Window functions parsed (structure only)
- [x] EMIT clause supported (basic detection)
- [x] Unit tests passing

## Production Status

**⚠️ IMPORTANT**: The current implementation is a proof-of-concept and NOT production-ready.

See:
- [SQL Parser Improvements Plan](F006-sql-parser-improvements.md)
- [ADR-003: SQL Parser Strategy](../../adr/ADR-003-sql-parser-strategy.md)

Production implementation is required before Phase 2 features can be built.
