# F061: Historical Backfill

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F061 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F060, F031 (Delta Lake), F032 (Iceberg) |
| **Owner** | TBD |
| **Research** | [Time-Series Financial Research 2026](../../research/laminardb-timeseries-financial-research-2026.md) |

## Summary

Enable unified queries that combine live streaming data with historical data from cold storage. Support automatic backfill of materialized views from historical data on creation.

## Motivation

From research review - **Gap 7: Real-Time + Historical Query Unification**:

> Most systems either do real-time OR historical well, not both. Switching between streaming views and historical queries requires different tools. No unified query model for "give me live data + backfill from history."

**Use Cases:**

1. **MV Backfill**: Create MV, automatically populate from 7 days of history
2. **Hybrid Queries**: Query both live stream and historical in one SQL statement
3. **Replay**: Re-process historical data through streaming pipeline

```sql
-- Unified query: live stream + historical backfill
SELECT * FROM trades
WHERE symbol = 'AAPL'
  AND event_time >= NOW() - INTERVAL '1 hour'  -- Historical
UNION ALL
SELECT * FROM trades_stream                      -- Live
WHERE symbol = 'AAPL';

-- Or with automatic materialization
CREATE MATERIALIZED VIEW aapl_vwap AS
SELECT
    TUMBLE_START(event_time, INTERVAL '1 minute') as bar_time,
    SUM(price * quantity) / SUM(quantity) as vwap
FROM trades
WHERE symbol = 'AAPL'
GROUP BY TUMBLE(event_time, INTERVAL '1 minute')
WITH (
    historical_backfill = '7 days',  -- Backfill from storage on creation
    emit = 'on_watermark'
);
```

## Goals

1. Query both live and historical data in unified SQL
2. Automatic MV backfill from cold storage on creation
3. Configurable backfill duration
4. Progress tracking for long backfills
5. Resume backfill after failure

## Non-Goals

- Complex merge logic (deduplication handled separately)
- Real-time cold storage queries (historical is batch)
- Time-travel queries (point-in-time snapshots)

## Technical Design

### Hybrid Query Execution

```rust
/// Query execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    /// Stream-only: process live events
    StreamOnly,
    /// Batch-only: process historical data
    BatchOnly,
    /// Hybrid: backfill from historical, then switch to live
    Hybrid {
        /// How far back to look in historical data
        backfill_duration: Duration,
    },
}

/// Hybrid query executor.
pub struct HybridQueryExecutor {
    /// Stream source for live data
    stream_source: Box<dyn Source>,
    /// Batch source for historical data
    historical_source: Box<dyn HistoricalSource>,
    /// Query mode
    mode: QueryMode,
    /// Current phase
    phase: ExecutionPhase,
    /// Boundary timestamp (switch from historical to live)
    boundary_ts: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPhase {
    /// Reading historical data
    Historical,
    /// Transitioning from historical to live
    Transition,
    /// Processing live stream
    Live,
}

impl HybridQueryExecutor {
    /// Execute query in hybrid mode.
    pub async fn execute(&mut self, ctx: &mut QueryContext) -> Result<(), QueryError> {
        match self.mode {
            QueryMode::StreamOnly => self.execute_stream(ctx).await,
            QueryMode::BatchOnly => self.execute_batch(ctx).await,
            QueryMode::Hybrid { backfill_duration } => {
                // Phase 1: Historical backfill
                self.boundary_ts = now_ms() - backfill_duration.as_millis() as i64;
                self.phase = ExecutionPhase::Historical;

                self.execute_historical_phase(ctx).await?;

                // Phase 2: Transition (process buffered live events)
                self.phase = ExecutionPhase::Transition;
                self.execute_transition(ctx).await?;

                // Phase 3: Live streaming
                self.phase = ExecutionPhase::Live;
                self.execute_stream(ctx).await
            }
        }
    }

    async fn execute_historical_phase(&mut self, ctx: &mut QueryContext) -> Result<(), QueryError> {
        // Start buffering live events during backfill
        self.stream_source.start_buffering();

        // Read and process historical data
        while let Some(batch) = self.historical_source.next_batch().await? {
            // Filter to backfill window
            let filtered = self.filter_time_range(
                batch,
                self.boundary_ts,
                now_ms() - BUFFER_MARGIN_MS,
            )?;

            if filtered.num_rows() > 0 {
                ctx.process_batch(filtered).await?;
            }

            // Update progress
            ctx.report_progress(self.historical_source.progress());
        }

        Ok(())
    }

    async fn execute_transition(&mut self, ctx: &mut QueryContext) -> Result<(), QueryError> {
        // Process buffered live events, deduplicating with historical
        let buffered = self.stream_source.drain_buffer();

        for event in buffered {
            // Skip events already processed from historical
            if event.timestamp < self.boundary_ts {
                continue;
            }

            ctx.process_event(event).await?;
        }

        Ok(())
    }
}
```

### MV Backfill Configuration

```rust
/// Backfill configuration for materialized views.
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// How far back to load historical data
    pub duration: Duration,
    /// Batch size for historical reads
    pub batch_size: usize,
    /// Parallelism for backfill processing
    pub parallelism: usize,
    /// Whether to continue from last checkpoint on failure
    pub resume_on_failure: bool,
    /// Progress reporting interval
    pub progress_interval: Duration,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(86400), // 1 day
            batch_size: 10_000,
            parallelism: 4,
            resume_on_failure: true,
            progress_interval: Duration::from_secs(10),
        }
    }
}

/// Backfill progress tracking.
#[derive(Debug, Clone)]
pub struct BackfillProgress {
    /// Total bytes to process (estimated)
    pub total_bytes: u64,
    /// Bytes processed so far
    pub processed_bytes: u64,
    /// Total rows processed
    pub processed_rows: u64,
    /// Current timestamp being processed
    pub current_timestamp: i64,
    /// Target timestamp (end of backfill)
    pub target_timestamp: i64,
    /// Start time of backfill
    pub start_time: Instant,
    /// Estimated completion time
    pub estimated_completion: Option<Instant>,
}

impl BackfillProgress {
    pub fn percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.processed_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    pub fn rows_per_second(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            0.0
        } else {
            self.processed_rows as f64 / elapsed
        }
    }
}
```

### Historical Source Trait

```rust
/// Source for reading historical data from cold storage.
#[async_trait]
pub trait HistoricalSource: Send + Sync {
    /// Get the time range of available historical data.
    fn time_range(&self) -> (i64, i64);

    /// Read next batch of historical data.
    ///
    /// Returns None when all data has been read.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SourceError>;

    /// Seek to a specific timestamp for resume.
    async fn seek(&mut self, timestamp: i64) -> Result<(), SourceError>;

    /// Get current progress (0.0 - 1.0).
    fn progress(&self) -> f64;
}

/// Delta Lake historical source.
pub struct DeltaLakeHistoricalSource {
    table_path: String,
    start_ts: i64,
    end_ts: i64,
    current_file_index: usize,
    files: Vec<String>,
    reader: Option<ParquetReader>,
}

#[async_trait]
impl HistoricalSource for DeltaLakeHistoricalSource {
    fn time_range(&self) -> (i64, i64) {
        (self.start_ts, self.end_ts)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SourceError> {
        // Read from current Parquet file
        if let Some(ref mut reader) = self.reader {
            if let Some(batch) = reader.next_batch().await? {
                return Ok(Some(batch));
            }
        }

        // Move to next file
        self.current_file_index += 1;
        if self.current_file_index >= self.files.len() {
            return Ok(None);
        }

        // Open next file
        self.reader = Some(
            ParquetReader::open(&self.files[self.current_file_index]).await?
        );

        self.next_batch().await
    }

    async fn seek(&mut self, timestamp: i64) -> Result<(), SourceError> {
        // Find file containing timestamp using partition metadata
        // ...
        Ok(())
    }

    fn progress(&self) -> f64 {
        if self.files.is_empty() {
            1.0
        } else {
            self.current_file_index as f64 / self.files.len() as f64
        }
    }
}
```

### SQL Syntax

```sql
-- Create MV with historical backfill
CREATE MATERIALIZED VIEW symbol_stats AS
SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(quantity) as total_volume,
    AVG(price) as avg_price
FROM trades
GROUP BY symbol
WITH (
    historical_backfill = INTERVAL '7' DAY,
    backfill_batch_size = 10000,
    backfill_parallelism = 4
);

-- Check backfill progress
SHOW BACKFILL STATUS FOR symbol_stats;
-- Returns:
-- | progress | processed_rows | rows_per_sec | eta        |
-- |----------|----------------|--------------|------------|
-- | 45.2%    | 4,520,000      | 125,000      | 2 minutes  |

-- Cancel backfill (keeps partial progress)
CANCEL BACKFILL FOR symbol_stats;

-- Resume backfill
RESUME BACKFILL FOR symbol_stats;

-- Hybrid query (explicit)
SELECT * FROM trades
WHERE event_time >= NOW() - INTERVAL '1 hour'
  WITH (source = 'hybrid');  -- Uses both historical and live
```

## Implementation Phases

### Phase 1: Historical Source Interface (3-4 days)

1. Define `HistoricalSource` trait
2. Implement `DeltaLakeHistoricalSource`
3. Implement `IcebergHistoricalSource` (if F032 done)
4. Time range filtering
5. Unit tests for historical reading

### Phase 2: Hybrid Executor (3-4 days)

1. Implement `HybridQueryExecutor`
2. Phase transition logic (historical → live)
3. Event buffering during backfill
4. Deduplication at boundary
5. Tests for phase transitions

### Phase 3: MV Backfill (3-4 days)

1. Add `BackfillConfig` to MV definition
2. Implement background backfill process
3. Progress tracking and reporting
4. Checkpoint/resume for long backfills
5. Integration tests

### Phase 4: SQL Integration (2-3 days)

1. Parse `WITH (historical_backfill = ...)` clause
2. `SHOW BACKFILL STATUS` command
3. `CANCEL/RESUME BACKFILL` commands
4. Integration tests

## Test Cases

```rust
#[test]
fn test_hybrid_executor_phases() {
    // Verify correct phase transitions
    let mut executor = HybridQueryExecutor::new(
        mock_stream(),
        mock_historical(),
        QueryMode::Hybrid { backfill_duration: Duration::from_secs(3600) },
    );

    assert_eq!(executor.phase, ExecutionPhase::Historical);

    // Process all historical
    while executor.has_historical() {
        executor.step();
    }

    assert_eq!(executor.phase, ExecutionPhase::Transition);

    // Process buffered
    executor.drain_buffer();

    assert_eq!(executor.phase, ExecutionPhase::Live);
}

#[test]
fn test_mv_backfill_progress() {
    // Create MV with 1-hour backfill
    let mv = create_mv_with_backfill(Duration::from_secs(3600));

    // Check progress updates
    let progress = mv.backfill_progress();
    assert!(progress.percentage() >= 0.0);
    assert!(progress.percentage() <= 100.0);
}

#[test]
fn test_backfill_resume() {
    // Start backfill
    let mut mv = create_mv_with_backfill(Duration::from_days(7));
    mv.start_backfill();

    // Simulate crash at 50%
    let checkpoint = mv.checkpoint();
    drop(mv);

    // Resume from checkpoint
    let mut mv2 = restore_mv(checkpoint);
    mv2.resume_backfill();

    // Should continue from ~50%
    assert!(mv2.backfill_progress().percentage() >= 45.0);
}

#[test]
fn test_boundary_deduplication() {
    // Events at boundary should not be duplicated
    let mut executor = setup_hybrid_executor();

    // Historical ends at t=1000
    // Live buffer contains events from t=900 to t=1100

    executor.execute();

    // Event at t=950 should appear once (from historical)
    // Event at t=1050 should appear once (from live buffer)
    // No duplicates
}
```

## Acceptance Criteria

- [ ] `HistoricalSource` trait defined
- [ ] Delta Lake historical source implemented
- [ ] Hybrid query executor working
- [ ] Phase transitions (historical → transition → live)
- [ ] Event buffering during backfill
- [ ] Boundary deduplication
- [ ] MV backfill configuration
- [ ] Progress tracking and reporting
- [ ] Checkpoint/resume for long backfills
- [ ] SQL syntax for backfill configuration
- [ ] 12+ unit tests passing

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Historical read throughput | > 100K rows/sec | Parallel Parquet reading |
| Phase transition | < 1s | Buffer drain + switch |
| Backfill overhead | < 5% | Compared to batch-only |

## References

- [Time-Series Financial Research 2026](../../research/laminardb-timeseries-financial-research-2026.md)
- Flink Batch/Streaming Unification
- RisingWave Historical Backfill
- Delta Lake Time Travel
