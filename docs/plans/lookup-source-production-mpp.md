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
   FinalPartitioned) and gated behind `cluster-unstable`. Joins do not repartition.
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
- New `crates/laminar-db/src/operator/lookup_enrich.rs` `GraphOperator` + a
  `crates/laminar-db/src/lookup_worker.rs` Ring-1 worker (clone the `ai_worker.rs` shape:
  `WorkItem`/`WorkResult`/`WorkerContext`, bounded submit/result channels).
- Router change: `partial`/`none` lookup joins planned as this graph node instead of
  `PartialLookupJoinExec`; `full` stays on the DataFusion `LookupJoinExec`.
- Implement `watermark_hold` (oldest in-flight ingest watermark), `wants_input` (in-flight
  cap → source backpressure), `checkpoint`/`restore` of in-flight + unsubmitted batches,
  `estimated_state_bytes`.
- Negative caching with its own TTL (closes problem #4).
- **Exit:** misses run off the compute thread; cache hits stay inline (< 500ns); a slow
  source reduces throughput, never freezes watermarks; in-flight survives checkpoint/restore.

### Phase 2 — Cache semantics
- Byte-weighted foyer bound via `with_weighter` (reuse the AI result-cache migration);
  `lookup.cache.max-bytes` replaces entry-count default. (Closes problem #5 memory bound.)
- TTL on positive entries → lazy re-fetch on next probe after expiry (no background refresh
  machinery); CDC invalidation already wired.
- **Exit:** cache memory is byte-bounded; stale entries expire and re-fetch on demand.
- **Deferred (not v1):** foyer hybrid RAM→SSD tier. The clustered store + file-skipping
  (Phase 3) *is* the cold tier; a local SSD tier is a warm-key latency optimization to add
  only if RAM-miss → store latency proves too high in practice.

### Phase 3 — Vectorized + file-skipping source path (AD-4)
- Rewrite `DeltaLookupSource::query` as a single `WHERE pk IN (<keys>)` (or join against an
  ephemeral keys batch) per fetch; wire `predicates`/`projection` through `PushdownAdapter`
  (`crates/laminar-core/src/lookup/source.rs:150`). (Closes problem #2.)
- Validate at `CREATE LOOKUP TABLE` time that the table is partitioned/clustered on the key;
  emit a loud warning (or error, configurable) if not, since unclustered = full scan/key.
- Verify (test + metric) that IN-list pushdown actually prunes manifests/files.
- **Exit:** N missed keys = 1 pruned scan; per-key cost is O(matching files), documented.

### Phase 4 — Backend coverage (all four as on-demand `LookupSource`)
- **Iceberg:** `IcebergLookupSource` mirroring the Delta IN-list + projection path
  (`scan` with row-filter; lean on partition/metadata pruning). Register via
  `register_lookup_source("iceberg", ...)` (`lakehouse/mod.rs`).
- **Postgres:** real `LookupSource` — pooled client (`deadpool-postgres`), TLS (drop the
  hardcoded `NoTls`, `postgres_reference.rs:77`), parameterized `WHERE pk = ANY($1)`, and a
  proper type map (Decimal128 / Date / Timestamp / binary / arrays — not the current
  everything-to-Utf8 at `:222`). Fixes the README mismatch.
- **MongoDB:** new `MongoLookupSource` — `$in` multi-get on the indexed key with projection;
  register a lookup factory. (Closes the only "zero coverage" backend; clean fit for point
  lookups.)
- All reuse the Phase 1 operator + Phase 3 batched-fetch contract.
- **Exit:** Delta, Iceberg, Postgres, MongoDB all serve on-demand lookups with pushdown,
  pooling, retries; integration tests per backend.

---

## Track B — Distributed MPP scale-out

### Phase 5 — Key-shard the probe side across the cluster (AD-2, behind `cluster-unstable`)
- Insert a join-side repartition analogous to `ClusterRepartitionExec`: hash the stream
  rows' lookup-key columns over `VnodeRegistry` (xxh3 % vnode_count,
  `state/vnode.rs`), route to the node owning the key's vnode via `ShuffleSender`, receive
  via `ShuffleReceiver`, align checkpoint barriers with `BarrierTracker`
  (`crates/laminar-core/src/shuffle/`).
- Per-node: the Phase-1 operator + Phase-2 tiered cache + Phase-3 IN-list source. Each node
  owns a key range → cache affinity (hot key cached on one node, not replicated N×), bounded
  total cache, and N× aggregate lookup throughput.
- Rollout staging (per AD-2): (a) per-node-independent lookups (no shuffle, each node caches
  what its source partitions feed it) ships first and already multiplies throughput;
  (b) key-affinity shuffle adds cache efficiency.
- Rebalance/fencing reuse the existing control plane (`crates/laminar-db/src/rebalance.rs`,
  vnode `assignment_version` fence).
- **Exit:** a lookup-join MV runs across N nodes, key-sharded, barrier-consistent, with
  cache affinity; throughput scales with node count; rebalance preserves correctness.

### Phase 6 — Ops hardening
- Per-lookup-table Prometheus metrics: hit ratio, miss QPS, source p50/p99, in-flight,
  error rate, cache bytes, negative-hit ratio, cross-node shuffle depth.
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
- **Dependency:** Track B inherits the maturity of `cluster-unstable`; it should not gate
  Track A shipping.

## Open questions

- Indexed-OLTP / dedicated-KV tier as a second PB backend (deferred per scoping; revisit
  after Delta/Iceberg clustered path proves out).
- Whether `full`-mode tables above a size threshold should auto-promote to `partial` rather
  than OOM.
- Cross-node cache coherence on CDC invalidation (which node(s) hold a key) — likely
  "invalidate-on-own-shard only," consistent with AD-2 affinity.
