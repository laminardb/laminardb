# F-POPT-007: LogicalPlan Passthrough

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-007 |
| **Status** | Draft |
| **Priority** | P1 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F005, F006 |
| **Crate** | `laminar-sql` |
| **Module** | `planner/mod.rs` |
| **Audit Ref** | C9 |

## Summary

The SQL planner serializes the parsed AST back to SQL text
(`plan.statement.to_string()`), then DataFusion re-parses it. Eliminate
this round-trip by constructing a `LogicalPlan` directly from the parsed
AST.

## Problem

`planner/mod.rs:568` — Full SQL text round-trip per query plan. The custom
parser produces an AST, converts it to SQL text, then DataFusion's parser
re-tokenizes and re-parses the same SQL. This doubles parsing cost and
loses semantic information (e.g., streaming extensions).

## Proposed Fix

Build a `LogicalPlan` directly from the LaminarDB AST nodes:
1. Map LaminarDB `SELECT` → DataFusion `LogicalPlan::Projection`
2. Map LaminarDB `WHERE` → `LogicalPlan::Filter`
3. Map LaminarDB window/streaming clauses → custom `ExtensionPlan` nodes
4. Pass the `LogicalPlan` directly to DataFusion's optimizer

This avoids the parse round-trip and preserves streaming-specific semantics
that are lost in the SQL text representation.

## Trade-offs

- Significant refactor of the planner module
- Must keep parity with DataFusion's SQL semantics for standard SQL
- Streaming extensions (`EMIT`, `WATERMARK`, windows) need custom plan nodes
- Alternative: cache the DataFusion parse result (simpler, less impactful)
