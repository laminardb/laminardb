# F005: DataFusion Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F005 |
| **Status** | ✅ Done |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F001 |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-25 |

## Summary

Integrate Apache DataFusion as the SQL query engine for LaminarDB. DataFusion provides query parsing, planning, and optimization while we provide streaming-specific extensions and execution.

## Goals

- Use DataFusion for SQL parsing and planning
- Implement custom streaming table providers
- Support zero-copy data exchange via Arrow
- Enable predicate and projection pushdown

## Technical Design

### Custom Table Provider

```rust
pub struct StreamingTableProvider {
    schema: SchemaRef,
    source: Arc<dyn StreamSource>,
}

#[async_trait]
impl TableProvider for StreamingTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingScanExec::new(
            self.source.clone(),
            self.schema.clone(),
            projection.cloned(),
        )))
    }
}
```

## Implementation Status

The basic DataFusion integration is complete:

- ✅ `StreamingTableProvider` - table provider for streaming sources
- ✅ `StreamingScanExec` - execution plan for streaming scans
- ✅ `StreamBridge` - push-to-pull bridge (tokio channel-based)
- ✅ `ChannelStreamSource` - concrete source implementation
- ✅ Filter pushdown working (DataFusion handles this via TableProvider)
- ✅ Projection pushdown working
- ✅ Full test coverage (8 tests in datafusion module)

**Advanced features** (streaming UDFs, LogicalPlan creation) are tracked in [F005B](../phase-2/F005B-advanced-datafusion-integration.md).

## Completion Checklist

- [x] Table provider implemented (`StreamingTableProvider`)
- [x] Streaming execution plan (`StreamingScanExec`)
- [x] Filter pushdown working
- [x] Push-to-pull bridge (`StreamBridge`)
- [ ] Benchmarks passing (tracked separately)
