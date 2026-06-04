# Plan: Production-Ready Lookup Sources at Petabyte / MPP Scale

- **Status:** Proposed (not started).
- **Date:** 2026-05-28
- **Scope:** Make the lookup-source connector path production-grade for Delta Lake,
  Iceberg, Postgres, and MongoDB, and scale on-demand enrichment lookups to dimension
  tables that do not fit memory (petabyte class) via distributed MPP execution.
- **Target (decided 2026-05-28):**
  - **Topology:** Full distributed MPP — key-shard the join probe side across nodes,
    reusing the existing vnode + shuffle substrate.
  - **Priority PB backend:** Delta / Iceberg **clustered on the lookup key** (file-skipping
    + manifest pruning carry per-key cost), eventually extended to indexed OLTP.
  - **Consistency:** Eventually-consistent cache (TTL + negative caching + CDC
    invalidation). No strong read-through; no point-in-time per-probe guarantee on the
    on-demand path (the versioned/temporal-join path keeps its own semantics).

---

## Problem

There are two lookup abstractions:

- **`ReferenceTableSource`** (`crates/laminar-connectors/src/reference.rs:44`) —
  `poll_snapshot()` + `poll_changes()`. Hydrates the **entire** table into the in-memory
  `TableStore` and registers it as one concatenated `RecordBatch`
  (`RegisteredLookup::Snapshot`). This is `cache_mode = full` (default), explicitly bounded
  to small tables (`crates/laminar-db/src/table_cache_mode.rs:18`, "< 10M rows").
- **`LookupSource`** (`crates/laminar-core/src/lookup/source.rs:77`) — on-demand point
  lookup `query(keys, predicates, projection)` behind a foyer cache
  (`RegisteredLookup::Partial`). This is `cache_mode = partial | none` — the only model that
  can address tables larger than memory.

### Backend coverage (today)

| Backend | Streaming source | Full-snapshot ref (`full`) | Incremental refresh | On-demand lookup (`partial`/`none`) | Pushdown |
|---|---|---|---|---|---|
| Delta Lake | yes | yes | yes (version diff, cap 10/poll) | yes (`DeltaLookupSource`) | no (infra exists, unused) |
| Iceberg | yes | yes | yes (snapshot incremental) | **no** | no |
| Postgres | yes (CDC) | weak (`SELECT *`, NoTls, no pool) | CDC-adapter only | **no** | no |
| MongoDB | yes (change stream) | **no** | **no** | **no** | no |

Only Delta has a working on-demand path. The README
(`crates/laminar-connectors/README.md:21`) overstates Postgres ("pool-backed lookup source
with predicate pushdown — Implemented"); none of those three properties hold.

### Why it cannot scale today (evidence)

1. **On-demand lookups block the single compute thread.** The pipeline runs on one
   `new_current_thread()` runtime (`crates/laminar-db/src/pipeline_lifecycle.rs:1596`).
   `execute_cycle().await` (`pipeline/streaming_coordinator.rs:554`) →
   `SqlQueryOperator` `physical_plan::collect().await` (`operator/sql_query.rs:283`) →
   `PartialLookupJoinExec` awaits `source.query_batch().await` **inline**
   (`crates/laminar-sql/src/datafusion/lookup_join_exec.rs:1287`). A cache miss = an
   S3/Postgres round trip that parks the only task on the compute thread: no other operator
   runs, watermarks freeze, all sources backpressure. There is **no timeout** on the query.
   (Contrast: `AiInferenceOperator` was deliberately async-decoupled to a Ring-1 worker for
   exactly this reason.)
2. **Per-key full-scan loop.** `DeltaLookupSource::query`
   (`crates/laminar-connectors/src/lookup/delta_lookup.rs:164-220`) loops over keys issuing
   one `WHERE pk = x LIMIT 1` + `execute_stream` per key. It advertises
   `supports_batch_lookup: true` (`:227`) but does the opposite, and ignores
   `_predicates`/`_projection`. At PB scale, unless the table is clustered on the key, each
   key is a full scan.
3. **No MPP.** Every join operator is `Partitioning::UnknownPartitioning(1)`
   (`lookup_join_exec.rs:440,733,1053,1645`). Cross-instance hash shuffle
   (`crates/laminar-sql/src/datafusion/cluster_repartition.rs`, vnode-based over
   `crates/laminar-core/src/state/vnode.rs`) **exists but is aggregation-only** (Partial →
   FinalPartitioned) and gated behind `cluster`. Joins do not repartition.
4. **No negative caching.** Misses returning `None` are never cached
   (`lookup_join_exec.rs:1291-1296`); every event with a non-existent key re-queries the
   source forever.
5. **Memory-only, entry-count-bounded cache, no TTL.** `FoyerMemoryCacheConfig`
   (`crates/laminar-core/src/lookup/foyer_cache.rs:58`) bounds by entry count (on-demand
   default 65,536, `pipeline_lifecycle.rs:1111`), not bytes; no disk tier
   (`foyer_cache.rs:169` "No slower storage tiers are wired yet"); no TTL/refresh.
6. **Source errors silently corrupt output.** On query error the operator serves cache-only
   results (`lookup_join_exec.rs:1298-1305`): INNER drops matched rows, LEFT emits NULLs as
   if absent. No retry/backpressure/metric.

## Goals

- On-demand enrichment never blocks the compute thread; a slow/hung source degrades
  throughput (backpressure), never correctness or liveness.
- A single node sustains high lookup throughput against an out-of-memory dimension via
  bounded async I/O concurrency + a byte-bounded RAM cache (SSD tier deferred; see Phase 2).
- The cluster scales lookup throughput ~linearly by key-sharding the probe side across
  nodes, reusing the proven vnode + shuffle + barrier substrate.
- Petabyte dimension support via **externalized, clustered** lakehouse tables with
  file-skipping/manifest pruning so per-key cost is O(few files), not O(table).
- All four backends (Delta, Iceberg, Postgres, MongoDB) usable as on-demand lookup sources
  with predicate/projection pushdown, retries, and per-table observability.
- Eventually-consistent freshness: TTL + negative caching + CDC-driven invalidation.

## Non-goals (anti-over-engineering boundaries)

- **No internal materialization/sharding of the dimension's petabytes.** The bytes stay in
  the lakehouse/store; LaminarDB holds only caches + in-flight requests. We are not building
  a sharded internal state store for dimension data.
- **No strong/read-through consistency** on the on-demand path. Stale-within-TTL is
  accepted (decided). The versioned/temporal-join path is out of scope here.
- **No thread-per-core revival.** The compute thread stays single-threaded per node;
  concurrency comes from async I/O off the compute thread + cross-node MPP. (TPC was
  deleted; we do not resurrect it.)
- **No bespoke connection-pool/HTTP framework.** Reuse `deadpool`/object-store/existing
  clients.
- **No new query language surface.** `CREATE LOOKUP TABLE ... WITH (cache_mode=...)` plus a
  small set of `lookup.*` options is the entire user-facing API.
- **No eager prefetch/speculation.** Misses are fetched on demand, coalesced; we do not
  predict future keys.

## Architectural decisions

- **AD-1 — On-demand lookup join becomes a `GraphOperator`, async-decoupled.** The
  `partial`/`none` lookup join moves out of the DataFusion plan into an `OperatorGraph`
  node modeled on `AiInferenceOperator` (`crates/laminar-db/src/operator/ai_inference.rs`):
  `process()` serves cache hits inline, hands misses to a Ring-1 worker over a bounded
  channel (spawned on the **main** runtime handle at start, never the compute handle),
  drains completed results on a later cycle, holds the output watermark behind the oldest
  in-flight batch (`watermark_hold`), and backpressures via `wants_input` at an in-flight
  cap. In-flight batches are checkpointed and re-ingested on restore (cache/source dedup
  makes replay safe). The `full` (snapshot) join stays in DataFusion — it is a pure
  in-memory hash probe with no I/O and no reason to move.
- **AD-2 — Lookup-join repartition is a cache-affinity optimization, not a correctness
  requirement.** Any node can look up any key, so cross-node key-sharding is for cache
  hit-rate + bounded total cache memory + per-node throughput, not for correctness. This
  lets MPP roll out incrementally: per-node-independent lookups first (each node caches what
  it sees), key-affinity shuffle second.
- **AD-3 — Eventually-consistent cache.** Positive entries carry a TTL; negative
  (tombstone) entries carry a shorter TTL; CDC `poll_changes` invalidates/updates entries
  (`update_partial_cache_from_batch` already exists, `pipeline_callback.rs:1278`).
- **AD-4 — Petabyte = externalize + file-skip.** Per-key cost is bounded by
  requiring/validating that the lakehouse table is partitioned/clustered (Z-order) on the
  lookup key, and by pushing equality/IN predicates into the scan so manifest + file stats
  prune. The store is the cold tier; the hot working set is held in a byte-bounded RAM cache.

---

## Track A — Single-node correctness & coverage

### Phase 0 — Stop the bleeding (urgent, small) — DONE 2026-05-28
- Wrap the source query in `tokio::time::timeout` (mirror `INFERENCE_TIMEOUT`) so a hung
  source cannot wedge the pipeline. (`lookup_join_exec.rs` fetch path.)
- Replace silent cache-only degradation (`lookup_join_exec.rs:1298-1305`) with
  **error propagation**: a source error or timeout returns `Err` from the operator stream →
  `execute_cycle` discards pending offsets and replays (`streaming_coordinator.rs:561-564`),
  i.e. backpressure/at-least-once, never silent row loss or NULL-filling.
- **Deliberately NOT in Phase 0** (anti-over-engineering): inline retry-with-backoff (would
  *lengthen* the compute-thread block — retry moves to Phase 1, off-thread) and Prometheus
  metrics (the operator moves into laminar-db in Phase 1 where prom access is natural; wiring
  metrics into the laminar-sql free function now is throwaway plumbing).
- **Exit:** a source outage replays instead of corrupting; a hung source is bounded by the
  timeout. (Full non-blocking + retry + metrics arrive in Phase 1.)

### Phase 1 — Async-decouple the on-demand lookup join (keystone, AD-1)

**DONE 2026-05-28 — fully wired + tested.** `partial`/`none` lookup joins now route to the
async-decoupled `LookupEnrichOperator` (`operator/lookup_enrich.rs`): cache hits inline, misses
to a Ring-1 worker (30s fetch timeout), `watermark_hold` + `wants_input` backpressure,
checkpoint/restore of in-flight, negative-cache tombstones, retry-on-error (no NULL-fill).
Routing via `detect_lookup_enrich_query` (`sql_analysis.rs`) + the full residual-projection
rewriter with collision-aware suffixing (`{col}_{lookup_table}`), shared with the operator's
`output_schema`. `create_operator` branch; partial-table set + main runtime handle threaded
through `OperatorGraph` from `pipeline_lifecycle`. 7 tests (4 operator + 3 detector incl.
collision in SELECT/WHERE), clippy clean, 721 lib tests green. `full`/snapshot lookups keep the
DataFusion path. v1 limits: single join step, single-column key, stream-on-left, INNER/LEFT,
stream schema known (else falls through to DataFusion).

**Implemented wiring (was the spec; kept for the record):**
1. **Partial-table metadata into `OperatorGraph`.** Add `partial_lookup_tables:
   HashMap<String, Vec<String>>` (table → column names) + setter; populate in
   `pipeline_lifecycle` from `table_regs` where `refresh == RefreshMode::Manual` **before** the
   `add_query` loop (mirrors exactly the on-demand phase that registers those tables as
   `Partial`, ~`:1104`). Reuse the existing `ai_handle` (main runtime handle) for the worker
   (rename field `ai_handle` → `main_runtime_handle` for clarity; private, ~3 sites).
2. **`detect_lookup_enrich_query(sql, &partial_cols, &source_schemas)`** in `sql_analysis.rs`,
   mirroring `detect_asof_query`: `analyze_joins` → find a plain equi-join (not asof/temporal,
   no `time_bound`) whose left **or** right table ∈ `partial_cols`. Lookup side = that table;
   stream side = the other. Build `LookupEnrichConfig { table_name, key_columns=[stream key],
   join_type: Inner|LeftOuter }`. Return `None` (→ DataFusion fallback) for join types other
   than Inner/Left.
3. **Residual-projection rewriter** `build_lookup_projection_sql` + `rewrite_lookup_expr`
   mirroring `build_projection_sql`/`rewrite_expr` (recursive over Identifier /
   CompoundIdentifier / BinaryOp / UnaryOp / Nested / Function / Cast / `_ => to_string()`).
   **Collision rule (must match the operator exactly):** stream col → bare name; lookup col →
   bare name unless its name ∈ stream column names → `{col}_{lookup_table}`. Reads from
   `__lookup_enrich_tmp`.
4. **Operator `output_schema` must apply the SAME suffix rule** (it has both schemas at
   runtime): a lookup field whose name collides with a stream field is renamed
   `{name}_{lookup_table}`. This is the one coordination point — rewriter (plan-time) and
   operator (runtime) must agree, so factor the rule into a shared helper.
5. **`create_operator` branch** routing `LookupEnrichConfig` → `LookupEnrichOperator::new(...,
   lookup_registry, main_runtime_handle)`; **`add_query`** calls the detector and threads the
   config + `projection_sql`. `full`/snapshot tables (not in `partial_cols`) keep the existing
   DataFusion `LookupJoinExec` path.
6. **Test:** end-to-end MV with a partial lookup join (projected SELECT) enriches correctly,
   incl. a key-name-collision case exercising the suffix rule.

Until wired, `clippy -D warnings` flags the module as dead — do not commit before the branch
lands.

**Verified integration (2026-05-28):** routing follows the existing **dedicated-join
pattern** (asof/temporal/interval), NOT an AI-style AST rewrite. A `partial`/`none` lookup
join today is detected by the `lookup_join_rewrite` optimizer rule
(`planner/lookup_join.rs`) → `LookupJoinNode` → `PartialLookupJoinExec`, all inside a
`SqlQueryOperator`'s DataFusion plan. We hoist it to a dedicated `GraphOperator` so misses can
be served across cycles (DataFusion's pull model cannot "serve hits now, fetch misses later").

Reuse (no hand-rolling, no new infra):
- Worker = `ai_worker.rs` shape (`run_worker` loop, bounded submit/result `mpsc`); swap
  `provider.infer_batch` → `LookupSourceDyn::query_batch`.
- Operator = `AiInferenceOperator` shape (`PendingBatch`, `ingest`/`apply_result`/`submit`/
  `flush_unsubmitted`, `watermark_hold`, `wants_input`, `checkpoint`/`restore`,
  `estimated_state_bytes`).
- State = the existing `PartialLookupState` from `lookup_registry` (foyer cache +
  `key_sort_fields` + `source`); key encoding via the existing `RowConverter`; row assembly
  lifted from `probe_partial_batch_with_fallback` (stream cols + lookup cols, inner-drop /
  left-NULL); residual via `ProjectingJoinState` like the other join operators.

Work (one cohesive change; cannot be partially landed without dead/unwired code):
1. Planner: emit a `LookupEnrichConfig` (table, join keys, join type) + residual
   `projection_sql` from the lookup-join detection, mirroring asof/temporal config extraction.
2. `operator/lookup_enrich.rs` + `lookup_worker.rs`.
3. `create_operator` branch routing `partial`/`none` lookup joins to the new operator
   (passing `lookup_registry` + the main runtime `Handle`, already plumbed for AI). `full`
   stays on DataFusion `LookupJoinExec` (pure in-memory probe).
4. Off-thread bounded retry + Prometheus metrics (deferred here from Phase 0; prom access is
   natural in laminar-db).
5. Negative caching (problem #4): tombstone = zero-row batch in the foyer cache; S3-FIFO
   eviction gives bounded staleness — no explicit TTL machinery in v1 (full TTL is Phase 2).
- **Exit:** misses run off the compute thread; cache hits stay inline (< 500ns); a slow
  source reduces throughput, never freezes watermarks; in-flight survives checkpoint/restore.

### Phase 2 — Cache semantics
- **DONE 2026-05-28 — byte-weighted bound.** `FoyerMemoryCache` is now byte-weighted
  (`with_weighter` = `RecordBatch::get_array_memory_size`, `capacity_bytes` budget, 64 MiB
  default), mirroring the AI result cache. The user's `cache_memory` budget flows through as
  bytes (no lossy entry-count conversion); the dead `cache_max_entries` registration field was
  removed. Closes problem #5 (entry-count bound could OOM on wide rows).
- **TTL — DONE 2026-05-29.** `FoyerMemoryCacheConfig.ttl: Option<Duration>` + lazy expiry on
  `get_cached`: entries store `inserted_at: Instant` (new private `CachedBatch` value type), and a
  read past the TTL drops the entry and reports a miss so the caller re-fetches. Wired end-to-end:
  `CREATE LOOKUP TABLE ... WITH ('cache.ttl' = '<secs>')` → `LookupTableProperties.cache_ttl` →
  new `TableRegistration.cache_ttl: Option<Duration>` (db.rs) → `FoyerMemoryCacheConfig.ttl`
  (pipeline_lifecycle). `None` = entries live until byte-eviction / CDC invalidation (prior
  behaviour). Also fixed a latent server bug: the server's generated DDL emitted `cache_memory`/
  `cache_ttl` (underscore) but the parser keys on the dotted `cache.memory`/`cache.ttl`, so both
  were silently dropped — server now emits the dotted form. 3 new cache tests (zero-ttl, hit→expire,
  no-ttl-survives); 12 foyer + 271 core + 721 db tests green, clippy `-D warnings` clean.
- **Exit:** cache memory is byte-bounded (done); TTL-based lazy expiry (done).
- **Deferred (not v1):** foyer hybrid RAM→SSD tier. The clustered store + file-skipping
  (Phase 3) *is* the cold tier; a local SSD tier is a warm-key latency optimization to add
  only if RAM-miss → store latency proves too high in practice.

### Phase 3 — Vectorized + file-skipping source path (AD-4)
- **DONE 2026-05-29 — batched fetch + clustering warning.** `DeltaLookupSource::query`
  (`crates/laminar-connectors/src/lookup/delta_lookup.rs`) now issues **one** batched scan per
  chunk of ≤`MAX_KEYS_PER_QUERY` (1024) keys instead of one `WHERE pk = x LIMIT 1` + `execute_stream`
  per key (closes problem #2: N keys = ⌈N/1024⌉ pruned scans, was N full-scans). A single-column PK
  folds into `"pk" IN (...)`; a composite PK becomes an OR of per-key AND-groups; both de-dup keys
  to keep SQL compact. Results are realigned to the input key order by re-encoding each fetched
  row's PK columns with the **same** `RowConverter` used to decode the input keys (`index_batch_by_key`),
  so a key always maps to its own row regardless of scan order; duplicate input keys each resolve.
  `open()` now emits a best-effort `tracing::warn!` if none of the key columns is a Delta partition
  column (Z-ORDER isn't visible in metadata, so warning-not-error) via the new
  `delta_io::get_partition_columns`. 5 tests (open/miss + batched-align/dup/empty), clippy clean
  (`-D warnings`, delta-lake feature), 554 connectors lib tests green.
- **Projection pushdown — DONE 2026-05-30.** A per-table projection (a *superset* of every column
  any query references, plus the key — the cache is shared per table so it can't be per-query) is
  computed in `pipeline_lifecycle` via `compute_lookup_projection` (`sql_analysis.rs`), which unions
  referenced columns using sqlparser's exhaustive `visit_expressions` (the `visitor` feature) and
  bails to "fetch all" on a wildcard / subquery / unparseable shape (can't under-count safely). It
  rides in a new `PartialLookupState.projection: Vec<ColumnId>`; the operator projects its
  `lookup_schema = schema.project(projection)` and the worker passes it to `query_batch`. All four
  backends honor it (Delta `select_columns`, Iceberg scan `.select`, Postgres `SELECT <cols>`, Mongo
  projection doc) via the shared `laminar_core::lookup::source::projection_names`, returning the
  projected schema; `KeyAligner` realigns by PK *name*, so the key being in the projection is all
  that's required. `supports_projection_pushdown` now `true`. Tests: 3 analysis (union / wildcard-bail
  / full-collapse) + 1 operator coordination (projected schema in, no spare column out).
- **Predicate pushdown — deferred (deliberate).** For a *keyed point-lookup* the key IN-list already
  prunes files/rows, so a residual predicate only filters the ~1 row fetched — and `PushdownAdapter`
  already evaluates such predicates locally. Extracting lookup-side predicates from SQL for that
  marginal gain isn't worth the surface; `supports_predicate_pushdown` stays `false`.
- **Per-backend live projection tests — deferred** (same constraint as Phase 4: real
  Delta/Iceberg/PG/Mongo need gated dev-deps / live services). The shared `projection_names` + the
  operator-contract test cover the wiring.
- **Manifest-prune verification — DONE 2026-05-29.** `test_batch_query_prunes_partition_files`
  creates a Delta table partitioned on the key (8 keys → 8 partition files), verifies cross-partition
  correctness through `DeltaLookupSource`, then runs the generated `WHERE "id" IN (2, 5)` shape and
  asserts the `next` provider's `count_files_scanned` metric is `< 8` — i.e. the IN-list genuinely
  skips non-matching partition files (per-key cost = O(matching files), not O(table)). Note: the
  registered provider is delta-rs's **`next` `DeltaScan`** (`table_provider().build()`), whose
  metric is `count_files_scanned` (no `files_pruned`); pruning shows as scanned < total.
- **Exit:** N missed keys = ⌈N/chunk⌉ scans (done); per-key cost is O(matching files) on a
  key-clustered table, **verified** by the prune test; clustering is warned (not enforced).

### Phase 4 — Backend coverage (all four as on-demand `LookupSource`)
- **DONE 2026-05-29 — all three new backends land.** All reuse the Phase-1 operator + the
  Phase-3 batched-fetch + key-realignment-by-PK-re-encoding contract:
  - **Iceberg** (`lookup/iceberg_lookup.rs`): `IcebergLookupSource` — native scan
    `with_filter(Reference::is_in(...))` (composite → OR of `equal_to` AND-groups, NULL →
    `is_null`); reloads the table per fetch for snapshot freshness; Arrow→`Datum` conversion.
    `register_lookup_source("iceberg", ...)`.
  - **Postgres** (`lookup/postgres_lookup.rs`): real pooled `LookupSource` replacing the
    no-pool/`NoTls`/`SELECT *` reference stub — `deadpool` pool + parameterized
    `WHERE pk = ANY($1)`; schema from a prepared zero-row probe; native pg type map with
    rich types (numeric/date/timestamp/uuid/json) rendered as text. `register_lookup_source("postgres", ...)`.
    Fixes the README mismatch (README updated). **Server-auth TLS DONE** (`rustls` via
    `tokio-postgres-rustls`): `sslmode` = disable (plaintext) | require/verify-ca/verify-full
    (all = verified TLS, chain+hostname) + optional `sslrootcert` (CA PEM; else webpki roots).
    `prefer` is rejected (a static pooled connector can't do its plaintext fallback). No insecure
    skip-verify; mTLS client certs not yet wired. (The CDC control-plane connect + sink still
    `NoTls` — a separate, still-open gap; the helper here is local to the lookup, not yet shared.)
  - **MongoDB** (`mongodb/lookup.rs`): new `MongoLookupSource` — `find({ pk: { $in: [...] } })`,
    projects each document into the table's **declared** Arrow schema. Required threading the
    declared schema into the factory: `LookupSourceFactory::build` + `create_lookup_source`
    now take `Option<SchemaRef>` (schema-bearing backends ignore it; Mongo requires it).
    `register_lookup_source("mongodb", ...)`.
- v1 limits (all three): single-column key (matches the operator). Each chunks/IN-lists per
  fetch. Pure logic (type/predicate/param construction) is unit-tested (Iceberg 6, Postgres 6,
  Mongo 5); live-service integration tests are future work (Iceberg needs a REST catalog,
  Postgres/Mongo need a live server).
- `arrow-row` is now pulled by the `iceberg`/`postgres-cdc`/`mongodb-cdc` features (was only via
  `delta-lake`→`changelog-collapse`).
- **Exit:** Delta, Iceberg, Postgres, MongoDB all serve on-demand lookups with key-filtered
  pushdown + pooling + server-auth TLS (PG); per-backend live integration tests remain.

### Consolidation + test strategy (2026-05-29) — supersedes the per-phase mechanics above

A principal review flagged AI-generation residue across the four sources; the cleanup landed:
- **`laminar-core::lookup::KeyAligner`** now owns the `RowConverter` and does key decode +
  result realignment. It replaced the per-source `RowConverter`, `index_batch_by_key`, and the
  copy-pasted realign loop (so the earlier "`index_batch_by_key`" / "⌈N/1024⌉ chunk" wording above
  is historical). The alignment invariant is tested **once** in `KeyAligner`, not 4×.
- The `query_count`/`row_count`/`error_count` atomics were **deleted** from all four sources — they
  were never read by any production consumer. (Re-introduce only when wired to Prometheus in Phase 6.)
- **Delta** now builds a typed DataFusion `Expr` (`col(pk).in_list(...)`) instead of a hand-rolled
  `WHERE`-string with manual quote-escaping — type-correct and injection-free, matching Iceberg.
- The inconsistent internal `MAX_KEYS_PER_QUERY` chunking was removed (the operator already bounds
  probe batch size).
- **Type-match guard** (`d67575bf`): the lookup-enrich operator fails fast with a clear message when
  the input join-key type differs from the lookup PK type (previously a cryptic Arrow error deep in
  the hot path on the first batch). Correction to the earlier "silent wrong answer" framing — the
  encoder *errored*; it was just late and cryptic.

**Why no automated end-to-end test (deliberate, not an oversight):** seeding a Delta table from a
laminar-db test requires naming `deltalake::protocol::SaveMode`, i.e. adding `deltalake` as a
laminar-db **dev-dependency** — and Cargo has no feature-gated dev-deps, so it would compile the
whole delta-rs stack on *every* `cargo test -p laminar-db` (default, no features). The sink-seed
alternative is async-commit + worker-timing flaky. Both are disproportionate. The wiring is instead
covered by composition: `detect_lookup_enrich_query` (sql_analysis unit tests) + the operator
behaviour tests against a real `LookupSourceDyn` (`MapSource`: miss→fetch→enrich, inner-drop/left-null,
negative cache) + the type-match guard test + the trivial `create_operator` branch + per-backend
predicate/param unit tests. The only unexercised link is a *real backend's* `open()`+scan through
`start()` — pure integration, left as a gated future test.

**Manual verification recipe (run when touching the on-demand path):**
```sql
-- with: cargo run -p laminar-server --features delta-lake  (point at a key-clustered Delta table)
CREATE LOOKUP TABLE dim (id BIGINT NOT NULL, name VARCHAR, PRIMARY KEY (id))
  WITH ('connector'='delta-lake', 'table.path'='/path/to/dim', 'strategy'='on_demand');
CREATE SOURCE events (id BIGINT, amt BIGINT, ts BIGINT, WATERMARK FOR ts AS ts);
CREATE STREAM out AS SELECT events.id, events.amt, dim.name
  FROM events JOIN dim ON events.id = dim.id;        -- expect enriched rows; misses dropped (INNER)
```

---

## Track B — Distributed MPP scale-out

### Phase 5 — Key-shard the probe side across the cluster (AD-2, behind `cluster`)

**DONE 2026-05-30.** Phase 1 hoisted the lookup join out of DataFusion, and the production
cross-node shuffle is the in-operator `shuffle_pre_agg_batches` (not the test-only
`ClusterRepartitionExec`), so Phase 5 mirrors *that*: a key-shuffle inside
`LookupEnrichOperator::process`. Each row's lookup-key columns hash to a vnode (shared
`laminar_core::shuffle::row_vnodes`); rows this node doesn't own ship to the owner via
`ShuffleSender`, rows peers ship here drain via the new `ShuffleReceiver::drain_vnode_data_for`,
and the local set is enriched. **Forward-only** — any node can look up any key (AD-2: the shuffle
is cache affinity, not correctness), so enriched rows continue downstream on the owner and a
downstream aggregate re-shuffles on its own key. Single-node is a pass-through. Wired via
`create_operator` → `attach_cluster_shuffle` with the same `ClusterShuffleConfig` the agg path uses.

Stage (a) (per-node-independent lookups) was already free — every node runs its own operator over
its source partition. Stage (b) (this work) adds the key-affinity shuffle so each node caches only
its owned key range.

**Per-stage addressing.** The `ShuffleReceiver` is shared per node, so a lookup shuffle and an agg
shuffle in one pipeline (enrich → aggregate) would drain the same queue and cross-feed mismatched
schemas. Fixed by tagging `VnodeData` with the operator/MV name and demuxing by stage in
`drain_vnode_data_for`; this also fixes the latent agg-agg cross-feed. The agg path moved to the
staged send/drain. Row→vnode hashing and slicing are now shared (`shuffle::routing`) across all
three shuffle paths instead of copy-pasted.

**Consistency = agg-path parity, not stronger.** Best-effort per-cycle drain, fail-the-cycle on an
unassigned vnode (never silent drop), inline `send_to` await (a socket write, TCP-backpressured).
**No barrier alignment** — in-flight-on-wire rows at a checkpoint are the same at-least-once gap the
agg shuffle has; strict cross-node alignment is a shared open item in the cluster-production-readiness
plan. Rebalance needs no cache migration: the operator reads `owner(v)` fresh, so a moved vnode
re-routes and the new owner rebuilds its cache.

Test: `cluster_key_shuffle_routes_remote_keys_to_owner` — two operators over loopback shuffle;
asserts each key enriches only on its owner (node 2's via the shuffle). Clippy clean with/without
`cluster`.

- **Exit:** key-sharded lookup-join MV across N nodes with cache affinity; barrier-consistency is
  best-effort (agg parity), strict alignment deferred.
- **Deferred:** full cluster e2e through DDL (needs a feature-gated dev-dep connector, as in Phase 4);
  strict barrier alignment; inbound-backpressure tuning under a slow owner.

### Phase 6 — Ops hardening
- **Core metrics DONE 2026-05-30** (the lookup path previously emitted *zero* metrics). Four
  per-table (`table` label) series on the central `EngineMetrics` (server-scraped `/metrics`),
  incremented by `LookupEnrichOperator` — registry threaded via `OperatorGraph.prom` →
  `create_operator` → operator (the same path checkpoint/window state use):
  `lookup_cache_hits_total`, `lookup_cache_misses_total` (batch-aggregated `inc_by`, not per-row),
  `lookup_source_errors_total` (worker fetch error/timeout), `lookup_in_flight_rows` (gauge, set per
  `process`). Hit ratio + miss QPS are derived in PromQL — not stored. 1 test (cold→miss, warm→hit
  through the async worker). This wires the observability the consolidation deliberately deferred
  (the deleted per-source counters were unwired; these go to the real scrape surface).
- **Deferred (not core):** source fetch p50/p99 histogram, negative-hit ratio, cache-bytes gauge,
  cross-node shuffle depth (Phase 5 only) — add when a latency/eviction question actually arises.
- Capacity/runbook docs: clustering requirement, cache sizing, per-node throughput model,
  and a corrected connector capability table (replaces the inaccurate README claims).
- **Only if needed:** a per-source circuit breaker — defer unless timeout + bounded retry +
  backpressure (Phase 0) + negative caching (Phase 1) prove insufficient against a down source.

---

## Sequencing & risk

- **Critical path:** 0 → 1 → 2/3 → 4, with 5 layered on top once 1–3 are solid. Phase 1 is
  the keystone for *both* tracks: per-node lookups must be async-decoupled before sharding
  them across nodes, or each node simply stalls in parallel.
- **Lowest risk first:** Phase 0 is small and removes the liveness/correctness landmines.
- **Highest risk:** Phase 5 (distributed join repartition) — mitigated by AD-2 (correctness
  doesn't depend on it) and by reusing the already-proven aggregation shuffle substrate
  rather than building new transport.
- **Dependency:** Track B inherits the maturity of `cluster`; it should not gate
  Track A shipping.

## Open questions

- Indexed-OLTP / dedicated-KV tier as a second PB backend (deferred per scoping; revisit
  after Delta/Iceberg clustered path proves out).
- Whether `full`-mode tables above a size threshold should auto-promote to `partial` rather
  than OOM.
- Cross-node cache coherence on CDC invalidation (which node(s) hold a key) — likely
  "invalidate-on-own-shard only," consistent with AD-2 affinity.
