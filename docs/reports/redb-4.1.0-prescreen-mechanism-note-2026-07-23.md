# redb 4.1.0 prescreen exact-source mechanism note

Status: static, non-gating input to the proposed bounded prescreen. This is not a completed
`state-backend-mechanism-mapping/v1` record, a probe result, qualification evidence, candidate
admission, backend selection, or production approval. Cycle 16 later built and ran a separately
labelled construction-only workspace; no approval-bearing prescreen harness or workload has run.

## Exact subject

The source reviewed on 2026-07-23 is the crates.io archive with this closed identity:

| Field | Frozen value |
|---|---|
| crate | `redb =4.1.0` |
| archive length | 188,200 bytes |
| archive SHA-256 | `8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839` |
| packaged VCS revision | `6ed1f981ba4deab0b2adbdd7bccb46ec409b2191` |
| packaged MSRV | Rust 1.89 |
| license | MIT OR Apache-2.0 |

Primary provenance is the [crates.io 4.1.0 release](https://crates.io/crates/redb/4.1.0) and its
[packaged upstream revision](https://github.com/cberner/redb/tree/6ed1f981ba4deab0b2adbdd7bccb46ec409b2191).

The proposed prescreen build is narrower: release mode,
`1.95.0-x86_64-unknown-linux-gnu`, target `x86_64-unknown-linux-gnu`,
`default-features = false`, no features, and redb's default Linux file backend. It does not enable
`cache_metrics` or `logging` and does not supply a custom `StorageBackend`. The eventual pre-run
approval must bind the exact lockfile, source archive, SBOM, binary, target, compiler and linker
flags, and canonical applied-configuration bytes. The construction workspace's lock and binaries
are not a substitute for that still-unbuilt, approval-bound artifact set.

The archive digest and length were independently recomputed from the local Cargo registry cache.
The VCS revision, MSRV, features, optional dependencies, and license were read from packaged
`Cargo.toml` and `.cargo_vcs_info.json`. A different archive, revision, feature set, backend, target,
toolchain, or relevant build flag is a different subject.

## Static execution model

The exact runtime source contains no redb-created worker thread or async maintenance executor. The
only `thread::spawn` occurrences under `src/` are inside the `#[cfg(test)]` module in
`src/tree_store/page_store/cached_file.rs`; optional runtime dependencies are disabled by the
frozen feature set. This supports only the narrow claim that this exact redb configuration has no
redb-managed asynchronous maintenance worker. It does not claim that the containing process, Linux
kernel, filesystem, or storage device performs no asynchronous work.

The work redb does expose is synchronous and must remain charged to the operation that encounters
it:

- `Database::begin_write` reaches
  `transaction_tracker.rs::TransactionTracker::start_write_transaction`, whose mutex and
  condition variable wait while the single database-wide writer is live. There is no try, timeout,
  or cancellation acquisition API.
- `transactions.rs::WriteTransaction::durable_commit` processes freed pages, mutates allocator and
  system trees, optionally serializes quick-repair allocator state, and calls the page manager's
  commit path before returning.
- A normal write transaction starts with `ShrinkPolicy::Default`. In
  `tree_store/page_store/page_manager.rs::commit_inner`, the ordinary commit path calls
  `try_shrink` unless the policy is `Never`; when the threshold is met it updates the layout and
  resizes the file after the durability flush. File truncation is therefore possible foreground
  commit work, not only explicit compaction or shutdown work.
- `tree_store/page_store/cached_file.rs` performs cache locking, eviction, write-buffer draining,
  file writes, and the backend sync inline. Cache-budget pressure can therefore add synchronous
  file I/O and lock delay to a caller.
- Dropping an incomplete `WriteTransaction` synchronously calls its abort/rollback path unless the
  thread is panicking or storage has failed.
- Open may perform repair/recovery and allocator reconstruction. `Database::compact` is an explicit
  foreground loop of relocation and repeated commits. A clean `Database::drop` calls
  `ensure_allocator_state_table_and_trim`, which makes a quick-repair, maximum-shrink commit before
  closing the storage.

Mutex/RwLock contention, allocation/free-page work, cache eviction, file growth or truncation,
write-buffer flush, `sync_data`, repair, abort, compact, and close can all block a calling thread.
The prescreen must separately record scheduled-to-dispatch, writer-acquisition, candidate service,
and end-to-end latency. Calling the mechanism N/A cannot erase any of those populations.

## Cache counter boundary

With `cache_metrics` disabled, `cached_file.rs::PagedCachedFile::cache_stats` returns a literal
`CacheStats` whose eviction, hit, miss, and used-byte fields are all zero. Those values mean
**unsupported/non-observed in this build**, not “observed zero activity.” A result for the frozen
build must omit candidate cache counters rather than publish those literals as measurements.

Enabling `cache_metrics` would create a distinct build identity and require a separately approved
observer-effect decision. `WriteTransaction::stats` traverses data and system trees and counts
allocator state; it is prohibited during measured attempts. If collected after an attempt, it is
an offline diagnostic and not a sample of the measured hot path.

## Proposed mechanism arms, still unproved

The exact-source proposal for the amended candidate-specific mapping is:

| Mapping arm | Proposed value | Exact meaning and remaining proof |
|---|---|---|
| `background_maintenance_debt` | `not_applicable` | redb has no candidate-managed asynchronous compaction/flush worker in the frozen build. Deferred reclamation and any work performed by a later foreground operation remain charged, and a completed bounded native mechanism probe must confirm the source-derived execution model before a formal mapping may use N/A. |
| `engine_pressure_stalls` | `not_applicable` | redb exposes no distinct background-debt pressure controller or stall state in this configuration. This says nothing about the global writer wait, locks, cache-budget I/O, allocation, resize, sync, or recovery blocking; those remain observable in mandatory common and adapter timing evidence. A completed bounded native probe is still required. |
| redb cache counters | unsupported in this build | literal zero-filled `CacheStats` is not evidence; omit it. |
| common process/storage observations | externally observed | cgroup CPU/memory/I/O, process RSS/PSS/FDs, XFS project quota/allocation, `/proc` dirty/writeback, and block-device completion/write evidence remain mandatory where the native protocol requires them. |
| adapter latency and admission | externally observed | adapter queue depth/admission plus writer-acquisition, service, and end-to-end timing must cover the global writer and every synchronous charged path. |

The kernel's dirty/writeback queues, filesystem work, device queueing, controller cache, and physical
completion are outside redb's proposed N/A arms. They are common external evidence, not zeros and not
candidate-internal observations. Docker Desktop/WSL can exercise framing and smoke mechanics but
cannot establish the native XFS/device evidence or complete this bounded mechanism probe.

No `bounded_probe_proof` exists today. A future native reviewed result may bind a completed bounded
probe artifact under `state-backend-redb-prescreen-result/v1`; the Docker result shape structurally
forbids that artifact and cannot encode `PRESCREEN_PASS`. Even native `PRESCREEN_PASS` only funds an
additive candidate profile, formal mechanism and persistence mapping, and adapter review. It cannot
donate C1/C2/C3, qualification, selection, fault/endurance, checkpoint, exactly-once, source/sink,
soak, or production evidence.

## Persistence boundary

For the frozen default Linux backend, `CachedFile::flush` drains its userspace write buffer and
reaches the file backend's `std::fs::File::sync_data`. One-phase Immediate commit performs the final
flush after publishing the primary header; the optional two-phase form performs an earlier flush as
well. Quick repair saves allocator state and forces the protocol's two-phase behavior. When an
ordinary commit decides to shrink, `commit_inner` performs the file resize after that final flush
and shows no subsequent sync in the same commit path; crash behavior at that ordering is therefore
an empirical question, not a durability inference from the earlier `sync_data` call.

This source path is not proof of directory durability, rename/create ordering, host page-cache loss,
device volatile-cache or power-loss behavior, or storage-controller persistence. It also does not
create a distributed checkpoint, source-offset cut, sink transaction, or exactly-once delivery
boundary. Those require the separately specified physical truth table and distributed checkpoint/
source/sink protocol. redb exposes no distinct standard directory-sync or `persist_all` primitive
that this note can promote by inspection.

## Source anchors

The static claims above were traced through these packaged files and symbols:

- `Cargo.toml`, `Cargo.toml.orig`, and `.cargo_vcs_info.json` for identity, features, dependencies,
  MSRV, and license;
- `src/db.rs`: `Database::begin_write`, `Database::compact`,
  `ensure_allocator_state_table_and_trim`, and `impl Drop for Database`;
- `src/transaction_tracker.rs`: `TransactionTracker::start_write_transaction` and
  `end_write_transaction`;
- `src/transactions.rs`: `WriteTransaction::commit_inner`, `durable_commit`, `stats`, and
  `impl Drop for WriteTransaction`;
- `src/tree_store/page_store/page_manager.rs`: `ShrinkPolicy`, `commit_inner`, `try_shrink`, and
  open/repair paths;
- `src/tree_store/page_store/cached_file.rs`: `cache_stats`, cache eviction, write-buffer draining,
  `flush`, and the test-only thread spawns; and
- `src/tree_store/page_store/file_backend/optimized.rs` and `fallback.rs`: `set_len` and
  `sync_data` forwarding to `std::fs::File`.

The detached pre-run approval must hash this note's exact bytes along with the protocol and every
other input. Schema validation alone does not verify any descriptor or owner attestation and cannot
authorize a command; an external verifier and an implemented fail-closed harness gate would be
required before execution. Neither exists; the construction-only command deliberately cannot accept
approval or emit a prescreen result.
