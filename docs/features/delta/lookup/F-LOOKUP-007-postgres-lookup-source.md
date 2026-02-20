# F-LOOKUP-007: PostgresLookupSource (SourceDirect + Pushdown)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-007 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-LOOKUP-002 (LookupSource trait), F-LOOKUP-003 (Predicate types) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/lookup/postgres_source.rs` |

## Summary

`PostgresLookupSource` implements the `LookupSource` trait for direct Postgres queries. This is the Tier 2 (cold) data source used when both foyer cache tiers miss. It provides full predicate pushdown via parameterized SQL WHERE clauses, full projection pushdown via SELECT column lists, and efficient batch lookups via `WHERE id = ANY($1)`.

This source is the primary backend for the **SourceDirect** lookup strategy, where no full materialization occurs and every cache miss results in a Postgres query. It is also used as the Tier 2 fallback for **Replicated** and **Partitioned** strategies when both cache tiers miss (rare, but handles cold-start and evicted entries).

Connection pooling uses `deadpool-postgres` for efficient connection reuse. Prepared statements are cached per connection for repeated queries. The source declares full pushdown capabilities since Postgres can handle all `Predicate` variants natively.

## Goals

- Implement `LookupSource` trait for Postgres
- Full predicate pushdown: translate all `Predicate` variants to SQL WHERE clauses
- Full projection pushdown: SELECT only requested columns
- Batch key lookup via `WHERE primary_key = ANY($1)` (up to 1000 keys per batch)
- Connection pooling with `deadpool-postgres`
- Prepared statement caching for repeated query patterns
- Configurable connection pool size, timeouts, and retry behavior
- Health check via simple SELECT query
- Metrics: query count, latency histogram, error count

## Non-Goals

- Write operations (lookup sources are read-only)
- CDC/replication (see F-LOOKUP-006 and F027)
- Schema introspection or DDL (handled at SQL layer)
- Query planning or optimization (we generate parameterized SQL)
- Postgres-specific features (partitioning, materialized views)
- SSL/TLS configuration (handled by connection config, not this module)
- Multi-table joins at source (one lookup = one table)

## Technical Design

### Architecture

**Ring**: Ring 1 / Ring 2 (Async) -- network I/O
**Crate**: `laminar-connectors`
**Module**: `laminar-connectors/src/lookup/postgres_source.rs`

The `PostgresLookupSource` is the most feature-rich source implementation because Postgres supports all pushdown capabilities. The query generation pipeline translates `Predicate` values to parameterized SQL, avoiding SQL injection while maximizing pushdown.

```
Cache Miss Flow:
┌──────────────────────────┐
│ LookupCacheHierarchy     │
│   .fetch(key)            │
│   → Ring 0 miss          │
│   → Ring 1 disk miss     │
│   → Tier 2: Postgres     │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────┐
│ PostgresLookupSource.query(keys, predicates, projection) │
│                                                          │
│  1. Build SQL:                                           │
│     SELECT {projection}                                  │
│     FROM {table}                                         │
│     WHERE primary_key = ANY($1)                          │
│       AND {predicate_1}                                  │
│       AND {predicate_2}                                  │
│                                                          │
│  2. Execute via connection pool                          │
│     (prepared statement, parameterized)                  │
│                                                          │
│  3. Deserialize rows → Vec<Option<Vec<u8>>>              │
└──────────────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────┐
│ deadpool-postgres         │
│ Connection Pool           │
│  - max 16 connections     │
│  - idle timeout 60s       │
│  - prepared stmt cache    │
└────────────┬─────────────┘
             │
             ▼
       ┌───────────┐
       │  Postgres  │
       │  Server    │
       └───────────┘
```

### API/Interface

```rust
use crate::lookup::predicate::{ColumnId, Predicate, ScalarValue};
use crate::lookup::source::{LookupSource, SourceCapabilities};
use crate::lookup::LookupError;
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::types::ToSql;

/// Configuration for the Postgres lookup source.
#[derive(Debug, Clone)]
pub struct PostgresLookupSourceConfig {
    /// Postgres connection string.
    /// Format: "host=localhost port=5432 dbname=mydb user=laminar password=..."
    pub connection_string: String,

    /// Table name to query (schema-qualified).
    /// Example: "public.customers"
    pub table_name: String,

    /// Primary key column name(s).
    /// Used for WHERE clause in key lookups.
    pub primary_key_columns: Vec<String>,

    /// All column names in order (for projection mapping).
    /// Column index in this vec matches ColumnId.
    pub column_names: Vec<String>,

    /// Maximum connections in the pool.
    /// Default: 16.
    pub max_pool_size: usize,

    /// Connection idle timeout.
    /// Idle connections are closed after this duration.
    /// Default: 60 seconds.
    pub idle_timeout: std::time::Duration,

    /// Query timeout.
    /// Individual queries are cancelled after this duration.
    /// Default: 5 seconds.
    pub query_timeout: std::time::Duration,

    /// Maximum number of keys per batch lookup.
    /// Default: 1000. Postgres handles arrays up to ~10K efficiently.
    pub max_batch_size: usize,

    /// Connection string for read replicas (optional).
    /// If set, read queries are routed to replicas.
    pub read_replica_connection_string: Option<String>,
}

impl Default for PostgresLookupSourceConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            table_name: String::new(),
            primary_key_columns: Vec::new(),
            column_names: Vec::new(),
            max_pool_size: 16,
            idle_timeout: std::time::Duration::from_secs(60),
            query_timeout: std::time::Duration::from_secs(5),
            max_batch_size: 1000,
            read_replica_connection_string: None,
        }
    }
}

/// Postgres implementation of the LookupSource trait.
///
/// Provides full predicate and projection pushdown via parameterized
/// SQL queries. Uses connection pooling for efficient connection reuse.
///
/// # Pushdown Capabilities
///
/// | Capability | Supported | Implementation |
/// |-----------|-----------|----------------|
/// | Predicate pushdown | Yes | WHERE clause with $N params |
/// | Projection pushdown | Yes | SELECT column list |
/// | Batch lookup | Yes | WHERE pk = ANY($1) |
/// | Range scan | Yes | WHERE pk BETWEEN $1 AND $2 |
///
/// # Example Query Generation
///
/// Given:
/// - keys: [42, 43, 44]
/// - predicates: [Eq(region_col, "APAC"), Gt(credit_limit_col, 1000000)]
/// - projection: [id_col, name_col, region_col]
///
/// Generates:
/// ```sql
/// SELECT id, name, region
/// FROM customers
/// WHERE id = ANY($1)
///   AND region = $2
///   AND credit_limit > $3
/// ```
pub struct PostgresLookupSource {
    /// Connection pool.
    pool: Pool,
    /// Configuration.
    config: PostgresLookupSourceConfig,
    /// Cached SQL template for key-only lookups.
    key_only_sql: String,
    /// Metrics.
    metrics: std::sync::Mutex<PostgresSourceMetrics>,
}

/// Metrics for the Postgres lookup source.
#[derive(Debug, Clone, Default)]
pub struct PostgresSourceMetrics {
    /// Total queries executed.
    pub queries: u64,
    /// Total rows returned.
    pub rows_returned: u64,
    /// Total query errors.
    pub errors: u64,
    /// Sum of query durations in microseconds (for average calculation).
    pub total_duration_us: u64,
    /// Maximum query duration in microseconds.
    pub max_duration_us: u64,
    /// Pool connections in use.
    pub pool_active: usize,
    /// Pool connections idle.
    pub pool_idle: usize,
}

impl PostgresLookupSource {
    /// Create a new Postgres lookup source with the given configuration.
    ///
    /// Opens a connection pool and validates connectivity.
    pub async fn new(config: PostgresLookupSourceConfig) -> Result<Self, LookupError> {
        let pg_config: tokio_postgres::Config = config.connection_string.parse()
            .map_err(|e| LookupError::ConfigError {
                reason: format!("Invalid connection string: {}", e),
            })?;

        let pool_config = Config {
            ..Default::default()
        };
        let pool = pool_config.create_pool(Some(Runtime::Tokio1), pg_config.clone().into())
            .map_err(|e| LookupError::Internal(format!("Pool creation failed: {}", e)))?;

        // Pre-generate SQL template for key-only lookups
        let all_columns = config.column_names.join(", ");
        let pk = config.primary_key_columns.join(", ");
        let key_only_sql = format!(
            "SELECT {} FROM {} WHERE {} = ANY($1)",
            all_columns, config.table_name, pk
        );

        Ok(Self {
            pool,
            config,
            key_only_sql,
            metrics: std::sync::Mutex::new(PostgresSourceMetrics::default()),
        })
    }

    /// Build a SQL query from keys, predicates, and projection.
    ///
    /// Returns (sql_string, parameter_values) for parameterized execution.
    fn build_query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> (String, Vec<Box<dyn ToSql + Sync + Send>>) {
        // SELECT clause
        let select_cols = if projection.is_empty() {
            self.config.column_names.join(", ")
        } else {
            projection
                .iter()
                .map(|&col_id| self.config.column_names[col_id as usize].as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };

        // FROM clause
        let from = &self.config.table_name;

        // WHERE clause
        let mut where_clauses = Vec::new();
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let mut param_idx = 1usize;

        // Key lookup: WHERE pk = ANY($1)
        if !keys.is_empty() {
            let pk = &self.config.primary_key_columns[0];
            where_clauses.push(format!("{} = ANY(${})", pk, param_idx));
            // Convert key bytes to appropriate type
            // (actual conversion depends on PK column type)
            param_idx += 1;
        }

        // Predicate pushdown: AND pred_col op $N
        for pred in predicates {
            let (clause, pred_params) = crate::lookup::predicate::predicate_to_sql(
                pred,
                param_idx,
                &self.config.column_names,
            );
            where_clauses.push(clause);
            param_idx += pred_params.len().max(1);
        }

        let where_str = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let sql = format!("SELECT {} FROM {}{}", select_cols, from, where_str);
        (sql, params)
    }
}

impl LookupSource for PostgresLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<Vec<u8>>>, LookupError> {
        let start = std::time::Instant::now();

        // Get connection from pool
        let client = self.pool.get().await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.table_name.clone(),
                source: Box::new(e),
            })?;

        // Build parameterized SQL
        let (sql, params) = self.build_query(keys, predicates, projection);

        // Execute query with timeout
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();

        let rows = tokio::time::timeout(
            self.config.query_timeout,
            client.query(&sql, &param_refs),
        )
        .await
        .map_err(|_| LookupError::Timeout {
            table_id: self.config.table_name.clone(),
            elapsed: self.config.query_timeout,
        })?
        .map_err(|e| LookupError::SourceError {
            table_id: self.config.table_name.clone(),
            source: Box::new(e),
        })?;

        // Convert rows to result format
        let duration = start.elapsed();
        let result = if keys.is_empty() {
            // Scan mode: return all rows as Some
            rows.iter()
                .map(|row| Some(self.serialize_row(row)))
                .collect()
        } else {
            // Key lookup mode: align results with input keys
            self.align_results_with_keys(keys, &rows)
        };

        // Update metrics
        if let Ok(mut m) = self.metrics.lock() {
            m.queries += 1;
            m.rows_returned += rows.len() as u64;
            m.total_duration_us += duration.as_micros() as u64;
            m.max_duration_us = m.max_duration_us.max(duration.as_micros() as u64);
        }

        Ok(result)
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            supports_predicate_pushdown: true,
            supports_projection_pushdown: true,
            supports_batch_lookup: true,
            supports_range_scan: true,
            max_batch_size: self.config.max_batch_size,
            max_predicate_count: 20,  // Postgres handles many predicates well
            estimated_single_lookup_ms: 2.0,
            estimated_batch_lookup_ms: 5.0,
        }
    }

    fn source_name(&self) -> &str {
        "postgres"
    }

    fn estimated_row_count(&self) -> Option<u64> {
        // Could query pg_class.reltuples, but that requires an extra query.
        // Return None and let the caller estimate from other signals.
        None
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        let client = self.pool.get().await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.table_name.clone(),
                source: Box::new(e),
            })?;
        client.query_one("SELECT 1", &[]).await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.table_name.clone(),
                source: Box::new(e),
            })?;
        Ok(())
    }
}

impl PostgresLookupSource {
    /// Serialize a Postgres row to bytes.
    ///
    /// Uses a compact binary format: [col_count, col1_len, col1_bytes, ...]
    /// This is a temporary format; production will use Arrow IPC.
    fn serialize_row(&self, _row: &tokio_postgres::Row) -> Vec<u8> {
        todo!("Row serialization: extract columns, encode to bytes")
    }

    /// Align query results with input keys.
    ///
    /// The SQL query may return rows in a different order than the input
    /// keys, and some keys may not have matching rows. This method
    /// produces a Vec aligned 1:1 with the input keys.
    fn align_results_with_keys(
        &self,
        _keys: &[&[u8]],
        _rows: &[tokio_postgres::Row],
    ) -> Vec<Option<Vec<u8>>> {
        todo!("Build HashMap<key, row>, then map input keys to results")
    }

    /// Get current metrics snapshot.
    pub fn metrics(&self) -> PostgresSourceMetrics {
        self.metrics.lock().unwrap().clone()
    }

    /// Get pool status.
    pub fn pool_status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            size: status.size,
            available: status.available,
            max_size: status.max_size,
        }
    }
}

/// Connection pool status for monitoring.
#[derive(Debug, Clone)]
pub struct PoolStatus {
    /// Current pool size (active + idle).
    pub size: usize,
    /// Available (idle) connections.
    pub available: usize,
    /// Maximum pool size.
    pub max_size: usize,
}
```

### Data Structures

```rust
/// Prepared statement cache key.
///
/// Different combinations of predicates and projections produce
/// different SQL queries. We cache prepared statements by their
/// query signature to avoid re-preparing on every call.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct QuerySignature {
    /// Whether keys are included in the WHERE clause.
    has_keys: bool,
    /// Number of predicates.
    predicate_count: usize,
    /// Predicate variant tags (for signature matching).
    predicate_types: Vec<u8>,
    /// Projected column IDs.
    projection: Vec<ColumnId>,
}
```

### Algorithm/Flow

#### Query Generation Pipeline

```
Input: keys=[42,43,44], predicates=[Eq(3,"APAC"), Gt(5,1000000)], projection=[0,1,3]

Step 1: SELECT clause
  projection = [0,1,3] → column_names[0], column_names[1], column_names[3]
  → "SELECT id, name, region"

Step 2: FROM clause
  → "FROM customers"

Step 3: WHERE clause
  a. Keys → "id = ANY($1)"                    param $1 = [42,43,44]
  b. Eq(3, "APAC") → "region = $2"             param $2 = 'APAC'
  c. Gt(5, 1000000) → "credit_limit > $3"      param $3 = 1000000

Step 4: Assemble
  → "SELECT id, name, region FROM customers WHERE id = ANY($1) AND region = $2 AND credit_limit > $3"

Step 5: Execute with params [$1=[42,43,44], $2='APAC', $3=1000000]

Step 6: Align results with input keys
  Query returns: [(42, "Alice", "APAC"), (44, "Charlie", "APAC")]
  Input keys: [42, 43, 44]
  Result: [Some(row_42), None, Some(row_44)]
  (key 43 not found or filtered out by predicates)
```

#### Connection Pool Lifecycle

```
1. On PostgresLookupSource::new():
   a. Parse connection string
   b. Create deadpool-postgres pool with max_pool_size
   c. Validate connectivity with health check
2. On query():
   a. pool.get().await → acquire connection (or wait)
   b. Execute query
   c. Connection returned to pool on drop
3. On idle timeout:
   a. Pool closes idle connections
4. On shutdown:
   a. Pool drains all connections
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| Connection refused | Postgres down, wrong host/port | Retry with backoff, check config |
| Authentication failure | Wrong credentials | Fail permanently, log clear error |
| Table not found | Wrong table_name in config | Fail permanently, log clear error |
| Column not found | Projection references invalid column | Fail query, log column names |
| Query timeout | Slow query, missing index | Return timeout error, suggest index |
| Connection pool exhausted | All connections in use, high load | Backpressure, increase pool size |
| Network error mid-query | Connection dropped during execution | Retry query on new connection |
| Type mismatch | ScalarValue type vs column type | Fail query, log types |
| Too many parameters | Batch size exceeds Postgres limit | Chunk batch (max_batch_size) |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| Single key lookup | 1-5ms | Indexed PK, local Postgres | `bench_postgres_single_key` |
| Batch 100 keys | 5-15ms | WHERE pk = ANY($1), indexed | `bench_postgres_batch_100` |
| Batch 1000 keys | 10-30ms | WHERE pk = ANY($1), indexed | `bench_postgres_batch_1000` |
| With predicate pushdown | +1-2ms | Additional WHERE clauses | `bench_postgres_with_predicates` |
| With projection pushdown | -10-30% bytes | Fewer columns selected | `bench_postgres_with_projection` |
| Connection pool acquire | < 100μs | Pool has idle connections | `bench_postgres_pool_acquire` |
| Query generation | < 5μs | Build SQL from predicates | `bench_postgres_query_generation` |
| Health check | < 50ms | SELECT 1 | `bench_postgres_health_check` |

## Test Plan

### Unit Tests

- [ ] `test_postgres_config_default` -- verify defaults
- [ ] `test_postgres_build_query_keys_only` -- WHERE pk = ANY($1)
- [ ] `test_postgres_build_query_predicates_only` -- WHERE pred1 AND pred2
- [ ] `test_postgres_build_query_keys_and_predicates` -- combined
- [ ] `test_postgres_build_query_with_projection` -- SELECT subset
- [ ] `test_postgres_build_query_all_predicate_types` -- all 9 variants
- [ ] `test_postgres_build_query_empty_params` -- SELECT * FROM table
- [ ] `test_postgres_capabilities` -- all flags true
- [ ] `test_postgres_source_name` -- returns "postgres"
- [ ] `test_query_signature_hash_equality` -- same sig, same hash
- [ ] `test_query_signature_different_predicates` -- different hash

### Integration Tests (requires Postgres testcontainer)

- [ ] `test_postgres_source_connect` -- pool creation, health check
- [ ] `test_postgres_source_single_key_lookup` -- fetch one row
- [ ] `test_postgres_source_batch_key_lookup` -- fetch 100 rows
- [ ] `test_postgres_source_key_not_found` -- returns None
- [ ] `test_postgres_source_predicate_pushdown_eq` -- WHERE col = val
- [ ] `test_postgres_source_predicate_pushdown_in` -- WHERE col = ANY(...)
- [ ] `test_postgres_source_predicate_pushdown_range` -- WHERE col > val
- [ ] `test_postgres_source_projection_pushdown` -- SELECT subset
- [ ] `test_postgres_source_scan_with_predicates` -- no keys, filter only
- [ ] `test_postgres_source_full_scan` -- no keys, no predicates
- [ ] `test_postgres_source_query_timeout` -- exceeds deadline
- [ ] `test_postgres_source_connection_failure` -- pool error
- [ ] `test_postgres_source_concurrent_queries` -- parallel lookups
- [ ] `test_postgres_source_result_alignment` -- results match key order

### Benchmarks (requires Postgres)

- [ ] `bench_postgres_single_key` -- Target: 1-5ms
- [ ] `bench_postgres_batch_100` -- Target: 5-15ms
- [ ] `bench_postgres_batch_1000` -- Target: 10-30ms
- [ ] `bench_postgres_with_predicates` -- Target: +1-2ms overhead
- [ ] `bench_postgres_query_generation` -- Target: < 5μs

## Rollout Plan

1. **Step 1**: Add `deadpool-postgres` and `tokio-postgres` deps to `laminar-connectors/Cargo.toml`
2. **Step 2**: Define `PostgresLookupSourceConfig` in `laminar-connectors/src/lookup/postgres_source.rs`
3. **Step 3**: Implement `PostgresLookupSource::new()` with pool setup
4. **Step 4**: Implement `build_query()` SQL generation
5. **Step 5**: Implement `LookupSource::query()` with parameterized execution
6. **Step 6**: Implement result alignment for batch key lookups
7. **Step 7**: Implement health check
8. **Step 8**: Unit tests for SQL generation
9. **Step 9**: Integration tests with Postgres testcontainer
10. **Step 10**: Benchmarks
11. **Step 11**: Code review and merge

## Open Questions

- [ ] Should we use `deadpool-postgres` or `bb8-postgres` for connection pooling? Both are mature. deadpool has more features (managed connections, runtime selection); bb8 is simpler.
- [ ] Should prepared statements be explicitly managed or rely on Postgres's implicit caching? Explicit prepared statements avoid re-parsing but require lifecycle management.
- [ ] Should we support read replicas transparently? The config has a `read_replica_connection_string` field but the routing logic needs design.
- [ ] How should we handle Postgres schema differences (e.g., column types that don't map cleanly to ScalarValue)? Should there be a type mapping layer?
- [ ] Should batch key lookups use `= ANY($1::type[])` or `IN ($1, $2, ..., $N)`? ANY with array is more efficient for large batches but requires array type knowledge.

## Completion Checklist

- [ ] `PostgresLookupSourceConfig` with defaults
- [ ] `PostgresLookupSource` implementation
- [ ] `LookupSource::query()` implemented
- [ ] `build_query()` SQL generation for all predicate types
- [ ] Result alignment for batch key lookups
- [ ] Connection pooling with deadpool-postgres
- [ ] Health check implementation
- [ ] `PostgresSourceMetrics` for monitoring
- [ ] `PoolStatus` for pool monitoring
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing (testcontainer)
- [ ] Benchmarks meet targets
- [ ] Documentation with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-LOOKUP-002: LookupSource Trait](./F-LOOKUP-002-lookup-source-trait.md) -- trait being implemented
- [F-LOOKUP-003: Predicate Types](./F-LOOKUP-003-predicate-types.md) -- predicate translation
- [F-LOOKUP-001: LookupTable Trait](./F-LOOKUP-001-lookup-table-trait.md) -- parent abstraction
- [deadpool-postgres](https://docs.rs/deadpool-postgres/) -- connection pooling
- [tokio-postgres](https://docs.rs/tokio-postgres/) -- async Postgres client
- [PostgreSQL Prepared Statements](https://www.postgresql.org/docs/current/sql-prepare.html) -- statement caching
