# F-SSQL-006: Dynamic Watermark Filter Pushdown

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SSQL-006 |
| **Status** | Done |
| **Priority** | P3 |
| **Phase** | 3 |
| **Effort** | M |
| **Dependencies** | F-SSQL-005 |
| **Owner** | -- |
| **Created** | 2026-02-25 |
| **Updated** | 2026-02-28 |

## Summary

Model the pipeline watermark as a `DynamicPhysicalExpr` (DataFusion v50+) that sources can use to skip data older than the current watermark before it enters the query plan. Currently, late data is read from sources, flows through the entire plan, and is only discarded at the window operator level. With dynamic filter pushdown, `StreamingScanExec` can apply a `ts >= watermark` filter at the scan level, avoiding unnecessary deserialization, expression evaluation, and network I/O for data that will be immediately dropped.

## Goals

- Implement `WatermarkDynamicFilter` as a `DynamicPhysicalExpr`
- Push watermark-based filters down to `StreamingScanExec` via `with_dynamic_filters()`
- Skip late data at the scan level before it enters the plan
- Use monotonically increasing `generation()` to track watermark advances
- Zero overhead when watermark hasn't advanced (generation check is a single atomic load)

## Non-Goals

- Changing watermark semantics or the allowed-lateness policy
- Filtering data in Ring 0 operators (they already handle late data correctly)
- Pushing arbitrary filters to sources (only watermark-based temporal filters)
- Supporting sources that don't have a declared event-time column

## Technical Design

### Architecture

**Ring**: Ring 1 (scan + filter pushdown)
**Crate**: `laminar-sql`
**Files**: new `crates/laminar-sql/src/datafusion/watermark_filter.rs`, modified `crates/laminar-sql/src/datafusion/exec.rs`

### Data Structures

```rust
/// Dynamic filter that represents the current watermark position.
///
/// Sources use this to skip data with event timestamps below the
/// watermark, avoiding unnecessary processing of late data.
pub struct WatermarkDynamicFilter {
    /// Shared watermark value (ms since epoch). Updated by the pipeline.
    watermark: Arc<AtomicI64>,
    /// Monotonically increasing generation counter.
    /// Incremented each time the watermark advances.
    generation: Arc<AtomicU64>,
    /// The event-time column this filter applies to.
    time_column: String,
    /// The data type of the time column (Timestamp or Int64).
    time_type: DataType,
}
```

### API/Interface

```rust
impl DynamicPhysicalExpr for WatermarkDynamicFilter {
    /// Returns the current generation (incremented on watermark advance).
    ///
    /// Sources compare this with their cached generation to decide
    /// whether to re-evaluate the filter.
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Evaluate the filter: returns a `BooleanArray` where `true` means
    /// the row's timestamp >= current watermark.
    fn evaluate(
        &self,
        batch: &RecordBatch,
    ) -> datafusion_common::Result<ArrayRef> {
        let watermark_ms = self.watermark.load(Ordering::Acquire);
        let time_col = batch.column_by_name(&self.time_column)
            .ok_or_else(|| DataFusionError::Internal(
                format!("Time column '{}' not found", self.time_column)
            ))?;

        // Compare: ts >= watermark
        let watermark_scalar = ScalarValue::TimestampMillisecond(
            Some(watermark_ms), None
        );
        let watermark_array = watermark_scalar.to_array_of_size(batch.num_rows())?;
        let mask = arrow::compute::kernels::cmp::gt_eq(time_col, &watermark_array)?;
        Ok(Arc::new(mask))
    }

    /// Returns the columns this filter references.
    fn references(&self) -> Vec<Column> {
        vec![Column::new_unqualified(&self.time_column)]
    }
}
```

### Watermark Advance Hook

```rust
impl WatermarkDynamicFilter {
    /// Called by the pipeline when the watermark advances.
    pub fn advance_watermark(&self, new_watermark_ms: i64) {
        let old = self.watermark.load(Ordering::Acquire);
        if new_watermark_ms > old {
            self.watermark.store(new_watermark_ms, Ordering::Release);
            self.generation.fetch_add(1, Ordering::Release);
        }
    }
}
```

### StreamingScanExec Integration

```rust
impl StreamingScanExec {
    /// Apply dynamic filters to this scan.
    ///
    /// When a `WatermarkDynamicFilter` is pushed down, the scan
    /// applies it to each batch before yielding rows.
    pub fn with_dynamic_filters(
        self,
        filters: Vec<Arc<dyn DynamicPhysicalExpr>>,
    ) -> Self {
        Self {
            dynamic_filters: filters,
            ..self
        }
    }
}

// In the stream implementation:
impl Stream for StreamingScanStream {
    fn poll_next(...) -> Poll<Option<Result<RecordBatch>>> {
        // ... get next batch ...

        // Apply dynamic filters
        if !self.dynamic_filters.is_empty() {
            let mut mask = None;
            for filter in &self.dynamic_filters {
                let filter_mask = filter.evaluate(&batch)?;
                mask = Some(match mask {
                    None => filter_mask,
                    Some(existing) => arrow::compute::and(&existing, &filter_mask)?,
                });
            }
            if let Some(mask) = mask {
                batch = arrow::compute::filter_record_batch(&batch, &mask)?;
                if batch.num_rows() == 0 {
                    continue; // Skip entirely filtered batches
                }
            }
        }

        Poll::Ready(Some(Ok(batch)))
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DataFusionError::Internal` | Time column not found in batch | Propagate error; source schema mismatch |
| No error | Watermark not yet initialized (i64::MIN) | Filter passes all rows (no data skipped) |

## Test Plan

### Unit Tests

- [ ] `test_watermark_filter_skips_late_data`
- [ ] `test_watermark_filter_passes_on_time_data`
- [ ] `test_watermark_filter_generation_increments_on_advance`
- [ ] `test_watermark_filter_no_advance_no_generation_change`
- [ ] `test_watermark_filter_passes_all_when_uninitialized`
- [ ] `test_streaming_scan_with_dynamic_filter_applied`
- [ ] `test_streaming_scan_empty_batch_after_filter_skipped`

### Integration Tests

- [ ] End-to-end: source with watermark filter skips late data, downstream operator sees only on-time data
- [ ] End-to-end: watermark advances mid-stream, filter starts skipping more data
- [ ] Performance: filter applied at scan level reduces downstream operator input by expected amount

### Benchmarks

- [ ] `bench_watermark_filter_overhead_per_batch` - Target: < 100ns for generation check (no-op path)
- [ ] `bench_watermark_filter_applied` - Target: < 5μs per 1K-row batch

## Rollout Plan

1. **Phase 1**: `WatermarkDynamicFilter` struct + `DynamicPhysicalExpr` impl + unit tests
2. **Phase 2**: `StreamingScanExec::with_dynamic_filters()` + integration
3. **Phase 3**: Wire into pipeline watermark tracking (connect to `Arc<AtomicI64>`)
4. **Phase 4**: Benchmarks + optimization
5. **Phase 5**: Code review + merge

## Open Questions

- [ ] Should the filter be pushed down automatically by a physical optimizer rule, or wired manually in `StreamExecutor`?
- [ ] Should we support configurable "slack" (e.g., `ts >= watermark - allowed_lateness`) to avoid filtering data within the lateness window?
- [ ] Should generation tracking use a global counter or per-source counters?
- [ ] Is `DynamicPhysicalExpr` stable in DataFusion v52, or is it still experimental?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

- The `Arc<AtomicI64>` for watermark sharing already exists in the pipeline — it's the same one used by `WATERMARK()` UDF in `watermark_udf.rs`. No new shared state is needed, just a reference.
- The generation counter avoids re-evaluating the filter when the watermark hasn't changed. This is important because `poll_next()` is called at high frequency and the watermark typically changes much less often.
- For sources with no event-time column (e.g., reference tables), the filter is simply not pushed down.
- DataFusion's `DynamicPhysicalExpr` was introduced in v50 for semi-join filter pushdown and generalized in v52.

## References

- [DataFusion DynamicPhysicalExpr](https://docs.rs/datafusion/latest/datafusion/physical_expr/trait.DynamicPhysicalExpr.html)
- [Gap Analysis: Opportunity 2 — Dynamic Filter Pushdown](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)
- [StreamingScanExec](../../../../crates/laminar-sql/src/datafusion/exec.rs)
- [watermark_udf.rs — shared Arc<AtomicI64>](../../../../crates/laminar-sql/src/datafusion/watermark_udf.rs)
- [F-SSQL-005: Cooperative Scheduling](F-SSQL-005-cooperative-scheduling.md)
