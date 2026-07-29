# RocksDB 0.24.0 adapter-entry source closure

- **Date:** 2026-07-29
- **Scope:** first-four-hour source, provenance, lifecycle, and vnode-cleanup gate from the
  [official-release selection](official-release-state-backend-selection-2026-07-29.md)
- **Verdict:** **STOP BEFORE DEPENDENCY OR ADAPTER**
- **Production/admission:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Execution:** no candidate, qualification, A/B, soak, observer, or certification run

## Outcome

The exact released path selected for entry was:

| Component | Immutable identity |
|---|---|
| Rust wrapper | `rocksdb 0.24.0`; archive SHA-256 `ddb7af00d2b17dbd07d82c0063e25411959748ff03e8d4f96134c2ff41fce34f`; release commit `bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4` |
| Native sys crate | `librocksdb-sys 0.17.3+10.4.2`; archive SHA-256 `cef2a00ee60fe526157c9023edab23943fae1ce2ab6f4abb2a807c1746835de9`; same release commit |
| Bundled engine | RocksDB 10.4.2; tag `v10.4.2`; commit `410c5623195ecbe4699b9b5a5f622c7325cec6fe` |
| Delta endpoint | RocksDB 11.1.2; tag `v11.1.2`; peeled commit `3b446089141659fad25328c5ea3e7ed283df46e4` |

The released API fails the first absolute adapter-entry contract. Vnode-prefix range compaction is
synchronous, has no deadline, returns `()`, and discards the native `Status`. Laminar therefore
cannot tell success from I/O failure or prove that logical vnode deletion reached quota-visible
physical reclamation. The same release also lacks a bounded, fallible in-process close path.

The stop rule was explicit: the first required fork, native patch, inadequate failure surface, or
unbounded lifecycle ends the cycle before code. Accordingly this cycle adds no Cargo dependency,
feature, lockfile entry, adapter, runtime selector, configuration, or test executable. It does not
silently fall back to TidesDB, Fjall, redb, or bounded memory. The eligible released local-spill set
is now empty.

## Decisive released-API failures

### Vnode cleanup cannot report or bound physical reclamation

The required sequence is: retain a consistent old snapshot, atomically install a vnode-prefix
range tombstone and ownership metadata, prove absence in a new view while the old snapshot remains
stable, release the snapshot, flush, reclaim the deleted range, and observe both completion and
quota-visible byte reduction without harming adjacent vnodes.

The exact release cannot complete that sequence honestly:

| Released surface | Source result | Contract consequence |
|---|---|---|
| `compact_range`, `compact_range_opt`, `compact_range_cf`, `compact_range_cf_opt` | [`rocksdb` 0.24.0 returns `()`](https://github.com/rust-rocksdb/rust-rocksdb/blob/bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4/src/db.rs#L1922-L2004). The bundled [C shim calls `CompactRange` and discards its `Status`](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/c.cc#L1908-L1976). | A native failure is indistinguishable from success. The synchronous call has no deadline or cancellation token. |
| `wait_for_compact` | Fallible and optionally timed, but it waits for queued/background work only after the synchronous manual-compaction call has returned. A timeout ends waiting; it does not stop that earlier call or reconstruct its discarded status. | It cannot wrap or repair range compaction. |
| `delete_file_in_range[_cf]` | Its safe API documents that it leaves all L0 and mixed-boundary files, can leave keys in the target range, and may invalidate snapshots created before the call. | It cannot guarantee prefix reclamation and is incompatible with the held-snapshot check. |
| `drop_cf` | Fallible, but it would require a column family per vnode rather than the selected prefix layout. | Thousands of vnode CFs would be a new storage architecture, not a small adapter; it still supplies no bounded physical-purge completion. |

Logical range deletion and fallible flush are present. They are insufficient: an LSM can retain the
deleted bytes until compaction, so disk quota and rebalance cleanup require an observable physical
reclamation policy rather than a successful tombstone write.

### In-process lifecycle is neither bounded nor fallible

The Rust wrapper's database `Drop` calls void `rocksdb_close`; the bundled C close deletes the
native object, whose destructor may wait for background flush, compaction, purge, or recovery and
does not return close status to Rust. `cancel_all_background_work(wait)` is also void. With
`wait=true` it may wait without a deadline; with `wait=false` it is not a proof that all native work
has stopped. `wait_for_compact` timing out does not cancel the work.

A parent process can deadline and terminate a child that owns RocksDB, quarantine its local root,
and restore a fresh root from a Laminar checkpoint. That proves process containment only. It cannot
make an in-process library call bounded, and embedded mode cannot terminate its host process as a
storage cleanup mechanism. Putting every state access behind a sidecar would introduce IPC into the
hot path and require a separate architecture decision; it is outside this no-speculation slice.

The local root remains disposable and is never checkpoint authority, but disposability does not
remove leaked-resource, shutdown-liveness, or host-availability obligations.

## 10.4.2 to 11.1.2 delta screen

The official release history and the 426-commit tag range were screened before applying the API
veto. The delta alone would not have stopped the deliberately narrow profile: plain `DB`, default
leveled compaction, one-shot initialization, no advanced transaction/recovery modes, and a fresh
root after abnormal shutdown or engine-version change. It does impose important exclusions:

| Later fix | Relevance to the frozen profile | Disposition |
|---|---|---|
| [`3aa706c2`](https://github.com/facebook/rocksdb/commit/3aa706c2bf3e2544def2d3fb7d581e35873d2c21) enforces shared `WriteBufferManager` limits during WAL recovery | In 10.4.2, reopening a dirty root can replay beyond the shared budget and OOM when other DBs consume it. | Fresh-root restore after abnormal/ambiguous close is mandatory. Any warm dirty-root recovery requirement is a hard stop. |
| [`cb43abb1`](https://github.com/facebook/rocksdb/commit/cb43abb1f125313c93ba1bd0b99d0bb77d883400) fixes silent corruption in round-robin compaction | Reachable only with `compaction_pri=kRoundRobin`, not the upstream default. | Freeze default leveled compaction with `kMinOverlappingRatio`; reject round-robin. |
| [`2dc6d6f7`](https://github.com/facebook/rocksdb/commit/2dc6d6f765c3d4645e0f283dc63d55e0acfabc2e) preserves the underlying table-builder I/O error during an empty-output flush | 10.4.2 reports failure, but can misclassify it as corruption. | Every non-OK maintenance result must poison the root; do not retry by parsed subtype. |
| [`3d993787`](https://github.com/facebook/rocksdb/commit/3d99378791ce6aac15672f8c0b72ebcf1c08d903) fixes a table-cache handle leak after compaction failure | Relevant to injected post-verification/metadata faults and reinforces the lifecycle risk. | Would require fail-stop process containment, root quarantine, and a fault test; it does not repair the missing compaction result. |
| [`cb8bc56d`](https://github.com/facebook/rocksdb/commit/cb8bc56d14786b66ccb2923643ea70c4500474a9) fixes the C create-CF failure handle | Relevant only when initialization fails and the same process retries. | Fixed CF creation would be one-shot and terminal; no in-process retry. |
| [`2afb3879`](https://github.com/facebook/rocksdb/commit/2afb38791779be19c3fbe90aea487dede7da107c) fixes a `DeleteScheduler` wake-up hang | Reachable with native rate-limited file deletion. | That facility would be forbidden. |

Best-efforts recovery, `TransactionDB`, FIFO compaction, WAL TTL deletion, remote/resumable
compaction, MultiScan, external-SST ingestion, read-only/secondary access to a live root, BlobDB,
and native CF export are outside the frozen surface. No relevant plain-snapshot or ordered-iterator
correctness fix was identified. No security fix was disclosed in the reviewed release history;
normal dependency, license, and SBOM checks would still be required before distribution.

This classification does not rescue the adapter. The released wrapper's cleanup and lifecycle API
fails independently and first.

## Build provenance result

Bundled provenance is feasible on Windows and Linux, but was not built because the source veto
stopped execution:

- pin `rocksdb =0.24.0` with `default-features = false` and only `bindgen-runtime`;
- require `ROCKSDB_COMPILE=1`;
- reject `ROCKSDB_LIB_DIR`, `ROCKSDB_INCLUDE_DIR`, and `ROCKSDB_STATIC`, and freeze or clear
  `ROCKSDB_CXX_STD`;
- verify the exact lock/checksums, generated `OPTIONS-*` engine version `10.4.2`, and absence of a
  dynamically imported system RocksDB library; and
- exclude FreeBSD from this exact provenance profile because the sys build script selects the
  system installation there.

This path requires a C++17 toolchain and libclang. The wrapper is Apache-2.0; the sys/native bundle
requires its combined Apache/MIT/BSD notice and SBOM review. Provenance controls cannot add a
missing result or deadline to the public API.

## Decision and re-entry

RocksDB 10.4.2 through `rocksdb` 0.24.0 is removed as an adapter-entry candidate. It remains useful
research/reference evidence, not a runtime dependency or fallback. The managed working-state
architecture remains engine-neutral and still needs a qualified local-spill implementation for the
intended broad-state production profile.

An official release may re-enter only when the exact released path provides, without a fork, git
dependency, direct private FFI, or local native patch:

1. a fallible, deadline/cancellation-aware way to reclaim a vnode prefix and observe completion;
2. a bounded/fallible lifecycle usable in-process in embedded, single-node, and cluster modes, or a
   separately accepted process-isolated architecture whose IPC hot-path cost is qualified;
3. bounded dirty-root recovery, or an explicit fresh-root-only policy proven across every abnormal
   exit; and
4. the existing atomic batch, consistent snapshot, portable fresh-root restore, resource/error,
   Windows/WSL2, full-disk, latency, failover, ALO/EO, qualification, and independent immutable-RC
   soak gates.

Until then, backend-neutral core work may continue. No stateful cluster operator is admitted and
no production-readiness claim is made.
