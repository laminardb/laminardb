# Changelog

## [0.28.0]

### Changed

- The official-release backend search is narrowed to one carry candidate: canonical `rocksdb`
  0.24.0 with `librocksdb-sys 0.17.3+10.4.2` and bundled RocksDB 10.4.2 may enter a bounded,
  fail-fast adapter/conformance cycle. It is a community Rust binding rather than a Meta-authored
  official binding, and it is not production-qualified. No runtime dependency or adapter is added
  by this decision; `[LDB-4007]` and `[LDB-0013]` remain fail-closed.
- Current TidesDB releases are rejected after native 9.3.14 and a separate short-return contract
  test exposed false-success commit paths; PR 664 is not an eligible released artifact. A bounded
  stock Fjall 3.1.8 fault child now empirically confirms that its worker-error/drop path waits
  indefinitely. A small worker-accounting patch closes that exact branch, but remains unreleased
  and does not provide a bounded/fallible close or complete maintenance-error surface; no production
  fork is carried. Redb 4.1.0 remains stopped on resource/lifecycle evidence. These are watch/re-
  entry subjects, not runtime fallbacks. The stopped redb construction job is removed from required
  CI.
- Managed aggregate candidates are now planned and initialized before checkpoint recovery in
  embedded, single-node, and cluster pipelines. Catalog-bridged and intermediate source schemas are
  registered before that planning boundary.
- Managed per-vnode checkpoint participants are graph-authoritative. Capture uses the exact local
  ownership roster, global state is scoped to vnode zero, required empty state is represented by a
  named FULL payload whose decoded state is empty, and restore/revoke reject missing, unexpected,
  duplicate-within-link, or placement-invalid state before activation. Implemented local keyed,
  windowed, and stateful-join candidates that reach cluster admission remain fail-closed with
  `[LDB-4007]`. The physical-route boundary is family-specific: bounded interval joins other than
  `INNER` fail in planning, while temporal and lookup translation can still coerce unsupported join
  types into a physical candidate which cluster admission then rejects with `[LDB-4007]`.
- Managed SQL aggregate vnode transitions now decode and validate complete restore chains into
  a prepared mutation plan with private replacement collections, prepare all applicable
  participants, abort all attempted participants on prepublication failure, publish under exact
  graph authority through unit-returning hooks, and retire displaced state after publication locks
  are released. Preparation reserves only checked net final live-map growth; exact-limit,
  max-plus-one, equal-cardinality, and rollback tests prove that a rejected transition does not
  mutate logical rows/bookkeeping or reserve avoidable growth. Capacity successfully reserved for
  publication may still remain after a later abort.
  Cluster graph initialization also rejects a cached `Rejected` capability or post-initialization
  descriptor drift. This does not add a backend or widen operator, source, sink, or delivery
  admission; `[LDB-4007]` remains unchanged.
- Built-in checkpoint backends now bind each vnode partial to its immediate parent plus checked
  transitive raw-payload bytes and physical-artifact count. Cluster restore preflights the requested
  subset under the existing global-singleton compatibility envelope before its first body GET,
  single-flights successful parent seal reads, verifies exact child-to-parent lineage and decoded
  base identity before following a parent, and validates a verified raw-body receipt at the
  immutable staging boundary. The raw rkyv `VnodePartial` suffix is unchanged, but the outer object
  wrapper is now `LDBVP3` version 3 with a fixed 164-byte header and checkpoint seals are version 8.
  V2 wrappers and version-7 seals require an explicit state/checkpoint reset until a migration
  bridge exists. This is not a hot-state backend, complete restore-memory/request/pause budget,
  delivery or exactly-once change, keyed/window/join/MV admission, latency/RSS result, or soak
  result; `[LDB-4007]` and `[LDB-0013]` remain closed.
- Cluster recovery capsules now carry a versioned vnode-restore contract. Every sealed participant
  readiness record attests the same current-profile payload and ancestry limits; the leader first
  validates the complete readiness roster, then metadata-walks every required vnode through exact
  parent seals to a root, recomputes checked cluster-wide payload/artifact totals, and persists the
  capsule only if those totals match the bounded contract. Recovery repeats that full metadata
  proof before any vnode body read, requires the target runtime to derive exactly the same limits,
  seeds body loading with the validated seal inventory set, and preflights each acquired subset
  against the committed totals. A post-seal validation failure publishes Abort when durable
  authority is available; an unresolved Abort requires recovery. Neither outcome can promote that
  attempt as a later delta/reference parent or acknowledge its source cut. Promotion is now an
  infallible in-memory step only after the exact durable Commit wins.
  Recovery capsules advance from version 5 to 6 and participant readiness keys/payloads from v5 to
  v6; older cluster cuts require an explicit state/checkpoint reset or new namespace. Changing the
  staged-byte or retained-chain configuration across a live cut has the same reset boundary until
  a versioned capability-superset rule exists. The limits are explicitly tagged
  `global_singleton_compatibility`: they close the Commit-domain raw-lineage authority gap but are
  not themselves a memory reservation, keyed admission, hot-state backend, or production resource/
  latency result. `[LDB-4007]` and `[LDB-0013]` remain unchanged.
- Cluster vnode restore now atomically reserves each acquired subset's declared raw lineage bytes
  and artifact count before its first body GET. The non-cloneable charge follows loaded bodies into
  the exact pending transition; body reads share a 32-request worker pool and one absolute deadline/
  cancellation scope. For transitions that own restore input, publication, terminal poison, failed
  launch, and graph replacement release only the exact abandoned transition, preserving any newer
  staged work. Revoke-only final-owner exits own no such charge and retain their durable staging
  authority after an indeterminate failure. Cancellation after a
  backend read starts drops the read future, request permit, collected bodies, and charge. This
  adds no steady-state record-path lock or I/O and does not cover wrapper/seal metadata, allocator/
  response overhead, decoder or decoded-state RSS, publication pause, a hot-state backend, keyed/
  window/join admission, or exactly-once. `[LDB-4007]` and `[LDB-0013]` remain unchanged.
- Legacy vnode restore now validates the raw-rkyv outer archive through a checked borrowed view and
  enforces the committed `global_singleton_compatibility` one-entry ceiling before allocating owned
  operator names or payload vectors. Sealed-chain traversal and graph preflight both use this
  bounded decoder; corrupt or over-profile heads and parents fail before callbacks, live-state
  mutation, or activation. The persisted bytes are unchanged and record processing has no new work.
  Checked archive traversal, unaligned-copy memory, inner aggregate/Arrow decoding, complete RSS,
  and publication pause remain open; no backend or stateful admission is added.
- Incremental aggregate state now binds one typed, immutable key-group count at construction in
  embedded, single-node, and cluster modes. Local runtimes bind one key group; cluster lazy
  initialization and DDL preflight use the exact registry/checkpoint topology, including global
  aggregates that retain the complete count while mapping their sole key to vnode zero. Full/delta
  capture and the production managed vnode transition reject a different count before ownership
  bookkeeping, payload decode, or live-state mutation. The former optional delta count is now only
  a baseline-activation flag, removing duplicate routing authority without adding a record-path
  hash, allocation, lock, or I/O. No wire/fingerprint, backend, delivery, or admission rule changes;
  `[LDB-4007]` and `[LDB-0013]` remain unchanged.
- Replacement cluster processes now start fenced and use the audited recovery-successor path to
  recertify durable vnode owners. Pristine vnode-zero bootstrap remains distinct from stale owner
  or drain state, and a stale-process drain terminal is settled before successor recovery.
- A passive replacement process outside a recovery round's frozen owner quorum now clears any
  stale recovery target and rebuilds its assignment-closed connector runtime from the latest
  durable checkpoint. Data-owning participants still restore the exact released epoch, and the
  strict `[LDB-6041]` rejection of a requested epoch behind a newer durable Commit remains intact
  (`142dadf7`). The triggering three-node ALO engineering rerun failed after its surviving owners
  reached checkpoint 5: the restarted ownerless node repeatedly tried Release epoch E after the
  owners had committed E+1 and timed out. Its subject SHA-256 was
  `91fe9d2c2ef734e1fad4f148da9e69a2eebed92057a74b70702b9b77fb490b10`, with logs at
  `target/tmp/soak-989812-1785220503643922100`; it is retained failure evidence, not a pass. The
  post-`142dadf7` rerun passed the engineering gate in 339.97 seconds with three kill/rejoin rounds
  (43.721/36.441, 37.089/30.020, and 34.586/31.053 seconds), all 132,899 expected IDs observed,
  7,898 at-least-once duplicates, and all 514 observed stalls at or below 1,024 ms with no
  deadline or SLO violation. It used server SHA-256
  `089197fa82d20cc9c83118036d8820d3168f6c3752125f05c6677ac6418f81d5`, harness SHA-256
  `b88df679bc4acc93739a4d4462bf208ac9c3b684bc58afc3f09a62a5d096b7b6`, and logs
  `target/tmp/soak-996548-1785223178826043700`; its end-of-steady-soak progress was checkpoint
  170/epoch 170 and its frozen durable input cut was checkpoint 187/epoch 187 at a 400 rps target.
  This is engineering evidence
  only: the independent immutable release-candidate soak is **NOT RUN**, and a deterministic
  Release-E/Commit-E+1 passive connector regression remains open.
- A separate attempted soak launch against an executable whose SHA-256 began `5ee...` stopped at
  the harness's configured-digest mismatch guard. No LaminarDB subject ran, so that event is
  preflight protection rather than runtime or soak evidence.
- Assignment-adoption reports now distinguish process/fence adoption from installed vnode-state
  readiness. Startup and recovery publish `vnode_state_ready = false`; successor assignment
  publication requires `true` from every exact participant after its registry fence and installed
  state binding match. The report is refreshed through bounded control storage every two seconds,
  and all pre-publication waits share one absolute checkpoint deadline. This is a current-state
  scan, not an atomic lease across the final compare-and-swap; observability, cost, race injection,
  large-state deadline validation, and independent release-candidate soak evidence remain open.
- The operator-graph checkpoint/state ABI is version 5. The raw rkyv `VnodePartial` payload layout
  remains unchanged, but its outer wrapper/seal and version-4 graph checkpoints are intentionally
  incompatible as described above; upgrade requires an explicit state/checkpoint reset until a
  separate migration bridge is designed.
- Key-group topology is mode-scoped: embedded and single-node use one group, while cluster uses
  optional `server.key_groups` (default 256). Checkpoints and assignment certificates now bind the
  partitioning ABI so incompatible recovery or shuffle peers fail closed.
- `SnapshotAdoption` now reports `vnodes_requiring_restore`, `restored_vnode_count`, and
  `restore_epoch`. The former acquisition/rehydration names were inaccurate when a retained vnode
  required restore because the process could not prove an exact installed-state binding.

### Removed

- Removed the public `VnodeRehydrator` and `VnodeRehydration` APIs. Direct callers could restore
  from a backend-selected seal without the committed recovery-capsule authority used by cluster
  recovery. Vnode restore is now an internal, authority-pinned lifecycle operation; there is no
  public replacement in 0.28.
- Removed the public `RehydratedVnode` type and `LaminarDB::rehydrated_vnode_state()` inspection
  method. They exposed partial staged chain bytes without the assignment, process, pipeline, and
  committed-cut authority now required for safe publication. The replacement transition remains an
  internal cluster lifecycle detail; there is no public replacement in 0.28. This is an explicit
  source-breaking change for applications upgrading from v0.27.x to v0.28.0.
- Removed the experimental tiered-state feature and configuration. Its demotion path could clear
  vnode dirtiness before a checkpoint was durable, then replace newer live groups with bytes from
  the prior durable checkpoint after an attempt failed. It is not a safe keyed-state foundation.
- Removed WebSocket source-server, replay, connector-owned checkpoint, and connector-owned
  event-time options. WebSocket sources are client-only and best-effort; event time is declared
  with SQL `WATERMARK FOR` and decoded against an explicit timestamp schema.

## [0.22.0]

### Added

- **Postgres wire protocol, production hardening**: full TLS path on
  the SUBSCRIBE listener: `pgwire_tls_min_version` to pin TLS 1.3
  (`bcab5471`, #12), optional mTLS via `pgwire_tls_client_ca` using
  `WebPkiClientVerifier` (`88e2af46`, #10), in-place hot reload of the
  `TlsAcceptor` via a `notify` watcher with debounce (`2cad3d54`, #11),
  and `pg_authid`-style pre-hashed `md5{32-hex}` passwords in
  `pgwire_users` so plaintext never has to live in `laminardb.toml`
  (`a3376408`, #7).
- **Extended-query + binary format** on the pgwire listener (`95fa8a74`).
- **SQL-level cursors**: `DECLARE c CURSOR FOR SUBSCRIBE …` +
  `FETCH n FROM c` for libpq clients with `\set FETCH_COUNT n`
  (`ab49408e`).
- **Explicit window-edge columns**: `tumble_end`, `hop_end`,
  `cumulate_end` UDFs return the window-end timestamp directly
  (`0a5b862e`).

### Changed

- **`DbState` collapse** (#381): removed the dual representation that
  carried both the in-flight `LaminarDB` handle and a snapshot of its
  contents. Trims docs/restated state.

## [0.21.0]

### Added

- **SUBSCRIBE over the Postgres wire protocol** (#369): `psql`, JDBC,
  asyncpg, and any libpq client can tail a stream or materialized view.
  Includes the listener itself, MD5 password auth, TLS, and a
  stream-side schema layer for `WHERE` push-down.
- **Pgwire hardening** (#371): connection caps, per-client throttling,
  certificate expiry warnings.
- **`RETAIN HISTORY` + `AS OF EPOCH`** (#373): `CREATE STREAM … WITH
  ('retain_history' = '64mb')` keeps a bounded ring of recent committed
  epochs in memory; `SUBSCRIBE … AS OF EPOCH n` resumes from epoch `n`
  so a reconnecting client doesn't miss rows produced during the
  disconnect.
- **Materialized view result storage and queryability** (#319): MV
  output is persisted via `mv_store` and exposed through a DataFusion
  `TableProvider`, so `SELECT * FROM mv_name` works after the pipeline
  has produced results.
- **Push-based WebSocket stream subscriptions** (#329): replaces the
  earlier poll loop on `/ws/{name}`.
- **Self-join pre-filtering** (#321): predicate analysis on
  `FROM a JOIN a …` cuts N² buffer growth before the join executes.
- **OTel demo + connector cleanup** (#317): schema auto-discovery,
  cleaner connector registration surface.
- **Production-grade Prometheus metrics** (#339): pipeline counters,
  cycle duration percentiles, checkpoint telemetry, sink error/timeout
  counters, per-connector gauges.
- **Throttled checkpoint barrier retries** (#327): back off rather
  than spin when a sink times out; clears `sink_timed_out` on success.

### Removed (dead-code waves)

- **#315**: Window operator unification deleted ≈13K LOC of DAG-only
  window operators.
- **#316**: Removed ≈23K LOC of dead DAG / checkpoint / storage paths
  (`core/dag/`, `core/state/`, `storage/checkpoint/`, `incremental/`,
  `wal.rs`). The single `StreamingCoordinator` tokio task is now the
  only execution path.
- **#320 / #326**: Removed ≈5K LOC from `laminar-db`, including the
  old `StreamExecutor` (4,652 LOC).

These deletions are not backwards-compatible. There were no public
APIs touched, but checkpoints from 0.20.x cannot be restored on 0.21+.

## [0.20.2]

### Added

- **Production-grade Prometheus metrics** (#339): expanded `/metrics`
  with pipeline counters, cycle duration percentiles, checkpoint
  telemetry, sink error/timeout counters, and per-connector gauges.
- **Temporal probe join DDL round-trip**: `CREATE STREAM` and
  `CREATE MATERIALIZED VIEW` now preserve the raw query text between
  `AS` and `EMIT` via a `query_sql` field on the AST, so custom
  streaming syntax (e.g. `TEMPORAL PROBE JOIN ... LIST (...)`,
  `RANGE FROM ... TO ... STEP ...`) survives the DDL parse / replay
  cycle used by `SHOW CREATE` and hot reload.

### Changed / Removed

- **Removed `WatermarkDynamicFilter`** (#338): the dead dynamic
  filter module was deleted. Watermark-based row filtering now runs
  through the standard streaming scan path.
- **Moved `examples/microstructure/pipeline.sql`** to use `TIMESTAMP`
  for the event-time column `"T"` (matches the v0.20.1 timestamp
  migration).
- **Deleted stale planning docs**: `docs/features/INDEX.md` and the
  `docs/plans/` directory were removed. `docs/ROADMAP.md` is the
  canonical feature tracker.

### Fixed

- **Watermark fallback wiring** (#337): `max.out.of.orderness.ms` is
  now respected when event-time extraction falls back to default
  generators. The attestation gate on the fallback path was removed.
- **Session window cardinality + CDC / sink shutdown** (#335):
  session windows cap per-key sessions to prevent unbounded state
  growth; sinks and CDC sources shut down cleanly when the pipeline
  stops.

## [0.20.1]

### Breaking: Timestamp column migration

Event-time columns throughout LaminarDB now use Arrow `Timestamp(_)`
instead of `Int64`-as-epoch-millis. The `interval_rewriter` is gone;
DataFusion handles `Timestamp ± INTERVAL` arithmetic natively.

**Schema changes in connector-produced columns:**
- OTel `_laminar_received_at`: `Int64` → `Timestamp(Nanosecond)`
- Postgres CDC `_ts_ms`: `Int64` → `Timestamp(Millisecond)`
- Kafka metadata `_timestamp`: `Int64` → `Timestamp(Millisecond)`
- MongoDB CDC `_wall_time_ms`: `Int64` → `Timestamp(Millisecond)`
- Files `_metadata.file_modification_time`: `Int64` → `Timestamp(Millisecond)`
- WebSocket `event.time.field` extraction: now reads any `Timestamp(_)`

**User-facing SQL changes:**
- Event-time DDL must declare `TIMESTAMP`, not `BIGINT`. Hand-written
  sources that used `ts BIGINT, WATERMARK FOR ts` need to switch to
  `ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL 'N' SECOND`.
- `INTERVAL` arithmetic is now native on `TIMESTAMP` columns.
  `ts BETWEEN t - INTERVAL '10' SECOND AND t + INTERVAL '10' SECOND`
  works. The old `t - 10000` numeric-arithmetic trick is no longer
  required.
- `tumble()` / `hop()` / `session()` UDFs accept any `Timestamp(_)`
  precision (Second / Millisecond / Microsecond / Nanosecond).
  Return type remains `Timestamp(Millisecond)`.

**Removed APIs:**
- `laminar_sql::parser::interval_rewriter` module
- `laminar_core::time::TimestampFormat` enum
- `EventTimeExtractor::from_column(name, format)`: now takes only `name`
- `laminar_db::db::infer_timestamp_format`
- `laminar_db::sql_analysis::infer_ts_format_from_batch`

**New APIs:**
- `laminar_core::time::cast_to_millis_array`: shared helper that
  normalises any `Timestamp(_)` array to `TimestampMillisecondArray`
  via Arrow's cast kernel. Used by the window UDFs, event-time
  extractor, interval-join key helper, and the WebSocket parser.

**Fixed:**
- `TUMBLE`/`HOP`/`SESSION` over `Timestamp(Nanosecond)` no longer
  errors with "Unsupported timestamp type" at runtime.
- Interval joins over `Timestamp(Nanosecond)` columns no longer
  error when extracting join keys.
- OTel `_laminar_received_at` unit-inference bug. Windows that
  used to be silently 1,000,000× smaller than declared (nanos
  treated as millis) now use real wall-clock durations.
- `WATERMARK FOR` in TOML config composes with connector schema
  auto-discovery. Columnless sources with a watermark clause are
  validated against the discovered schema instead of bailing out.

**Migration notes:**
- Existing checkpoints from v0.20.0 are **not** compatible. They
  reference Int64 timestamp columns in serialized operator state.
  Wipe `./data/checkpoints` before starting the server on v0.20.1.
- Hand-written SQL with `ts BIGINT` needs to change to `ts TIMESTAMP`
  if the column is used in a watermark or window. Plain `BIGINT`
  columns unrelated to event time are unchanged.

## [0.20.0] and earlier: notable additions

### Added: MongoDB CDC Source & Sink (`mongodb-cdc` feature, PR #255)

Landed in the 0.20.x line via PR #255. The feature provides MongoDB
change-stream source and write-sink connectors, CDC envelope types,
full-document modes, time-series sink configuration, and CDC replay writes.

The experimental `ResumeTokenStore` and `LargeEventReassembler` APIs later
proved unsafe or unwired and were removed. Resume tokens are no longer
persisted independently of the engine checkpoint, and split-large-event
handling is not advertised by the connector.
