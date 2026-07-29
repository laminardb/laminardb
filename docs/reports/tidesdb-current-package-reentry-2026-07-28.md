# TidesDB empirical entry validation

- **Date:** 2026-07-28
- **Scope:** executable backend-entry evidence; no Laminar dependency or adapter
- **Released Rust subject:** official `tidesdb` 0.11.1 at
  `e2febbc548e7f0158d1c09ea487aa0bb7c343616`, bundling native 9.3.6
- **Current native subject:** TidesDB 9.3.14 at
  `6fe1e83104b70255a694239d360a14bae51d0c70`
- **Upstream patch:** [TidesDB PR 664](https://github.com/tidesdb/tidesdb/pull/664), commit
  `b80e424ae98540c61be81d83c85f03f43d93b1d0`; open, DCO passed, CI/review pending
- **Historical product-line decision:** retained TidesDB as the preferred integration direction on
  2026-07-28
- **Current selection status:** superseded by the
  [2026-07-29 official-release selection](official-release-state-backend-selection-2026-07-29.md);
  current TidesDB releases are rejected and are not a runtime fallback
- **Entry decision:** both exact subjects remain unadmitted; TidesDB is not qualification-eligible
- **Admission:** `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Conclusion

The released Rust package passed its 71 default-feature library tests and the ordinary transaction,
iteration, and clean-reopen scenarios exercised here. The bounded-capacity run produced useful
failure observations, not a pass. These results cover bundled native 9.3.6, not current native
9.3.14, and are entry evidence rather than qualification.

Unmodified native 9.3.14 reproduced a false-success commit in unified-memtable mode: a deterministic
allocation failure returned success while a fresh transaction observed zero of 1,024 requested
keys. Unified mode is default-off and is excluded from the proposed first Laminar profile. A
separate test-instrumented short-return experiment showed the common caller defect: both classic
and unified transaction paths accepted six inserted entries out of seven as success. That
experiment proves incorrect handling of the documented batch return contract; it does not
establish the natural incidence of a short return in classic mode.

A local prototype changed the four transaction batch call sites to require the exact inserted
count and made the reproduced unified allocation failure return an error. Deterministic native
regressions now cover all four callers and the 1,024-operation allocation fault. They pass focused
ASan/UBSan and optimized release builds. A broad sanitizer CTest was stopped at its 15-minute
timebox after five of eleven executables passed and while an unrelated cache benchmark was still
running; it has no complete-suite verdict. Full upstream CI remains required before merge.

The appropriately small next implementation is therefore the native count/error patch plus focused
regressions. Laminar does not add an atomicity protocol or depend on an unfixed package. After a
fixed native successor to 9.3.14 and matching official Rust release exist, Laminar can add one
serialized owner lane that reuses a transaction after successful commits. An ambiguous or possibly
partial commit outcome must use Laminar's existing indeterminate-apply recovery path.

## Results

| Exact subject and scenario | Observation | Meaning |
|---|---|---|
| Official `tidesdb` 0.11.1, `cargo test --locked --lib` with default features | 71 passed, 0 failed | Default-feature Rust library baseline passed; not the complete native/all-feature suite |
| Bundled native 9.3.6, one 4,096-key FULL-sync transaction | Five sampled point reads and complete ordered iteration passed before and after clean reopen | Ordinary live and clean-reopen smoke passed |
| Same smoke on ten fresh tmpfs roots | Commit 14.2–21.2 ms; destructor 85–97 ms; reopen 18.7–21.8 ms | Smoke timing only; not physical-disk or tail-latency evidence |
| 16 MiB tmpfs capacity exhaustion | Commit returned a generic I/O error; Rust `Drop` returned in 56–66 ms, while native logging reported flush-before-close failure | Destructor returned within the 30-second deadline; successful native close was not observable |
| Immediate reuse of two capacity-exhausted roots after releasing a 4 MiB reserve | Open returned success but discovered an empty CF catalog despite retained CF/manifest/WAL/SST files | Those roots were unsafe for immediate reuse; permanent data loss was not established |
| Unmodified native 9.3.14, unified/default-off mode, 1,024-key transaction, one-shot allocation failure | Commit returned success and a fresh `READ_COMMITTED` transaction saw 0/1,024; reproduced 5/5 | Exact native subject failed the false-acknowledgement gate |
| Native 9.3.14 with test-only short-result injection | The fifth entry was skipped; classic and unified commits returned success and a fresh reader saw 6/7 | Caller-contract counterexample, not a natural-incidence measurement |
| PR 664 patch, focused ASan/UBSan Debug | Existing partial-batch contract test, four classic/unified stack/heap short-count cases, and unified allocation fault passed | Patch-specific sanitizer evidence; not a full suite |
| PR 664 patch, focused optimized Release | 13 test functions passed: partial-batch, basic/reset, normal classic/unified batch, and both new fault groups | Normal and fault paths passed in the optimized build |
| Patched native 9.3.14, broad ASan/UBSan CTest | Block-manager, skip-list, compression, Bloom-filter, and manifest executables passed; run was stopped during clock-cache at 15 minutes | Deliberately incomplete; no broad-suite pass claimed |

## Released-package environment and limits

The wrapper tests ran in Docker Desktop's Linux engine 29.6.2 using Debian 12 and Rust/Cargo
1.85.1. The image was `rust:1.85-bookworm` at digest
`sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4`.
The source was mounted read-only. Default compression features were enabled; native object storage
was disabled.

The clean-reopen harness committed 4,096 vnode-shaped records in one FULL-sync transaction. Its ten
timing repetitions used tmpfs. The capacity scenario deliberately configured `min_disk_space(1)`,
one flush worker, one compaction worker, a 64 KiB write buffer, and one 32 KiB uncompressed value per
transaction. The two reserved-space reproductions reached 207 and 224 successful commits before the
I/O error. Rust `Drop` cannot return `tidesdb_close`'s native result, so the observation is bounded
destructor return, not successful close.

The custom wrapper and exploratory fault harnesses lived in disposable directories outside this
repository and were not preserved as reproducible artifacts. Their results must not be promoted to
qualification evidence. The focused tests on the native patch branch are the durable regressions;
the complete upstream matrix remains outstanding.

The native patch was tested in Docker Desktop's Linux engine with Debian 12, GCC 12.2, and native
S3 disabled. The sanitizer build used TidesDB's `TIDESDB_WITH_SANITIZER=ON` ASan/UBSan configuration;
the optimized build used `CMAKE_BUILD_TYPE=Release`, sanitizers off, and the default Snappy/LZ4/Zstd
backends. These local lanes are regression evidence only, not backend qualification or soak.

## Native defect and minimum patch

Native 9.3.14 assigns one commit sequence to every transaction operation. That sequence coordinates
visibility only for versions actually installed. `skip_list_put_batch` explicitly permits a
non-negative result smaller than the requested count, but all four classic/unified stack/heap
transaction callers accept every non-negative result. Commit can therefore mark the shared sequence
committed even when an intended operation is absent.

The minimum native patch is:

1. require `inserted_count == requested_count` at all four transaction batch call sites;
2. return `TDB_ERR_MEMORY` from the reproduced unified batch-preparation allocation failure instead
   of taking the broken fallback; and
3. add deterministic short-count and allocation-failure regressions while retaining existing
   normal-path transaction coverage.

The prototype scanned the first 128 commit-thread allocation positions in the tested unified
configuration. Only four positions injected reachable failures through the configured allocator;
the separate short-count test covered the batch return contract. Results were:

- WAL-buffer allocation failure: error, 0/1,024 visible;
- dedup-hash allocation failure: success, 1,024/1,024 visible;
- batch-entry allocation failure: error, 0/1,024 visible; and
- prefixed-key arena allocation failure: error, 0/1,024 visible.

The short-count prototype returned an error while six physically inserted entries remained in the
live memtable. Rollback is outside this patch. A future Laminar owner must stop using that local root
after an ambiguous or short-apply commit error and enter the existing
`StatefulOperatorPartialApply` recovery path. No per-key readback, two-phase commit, checksum,
scrubber, or new root-poison state machine is required.

There is also a source-identified concurrency question: native `READ_COMMITTED` advances from the
global sequence without consulting commit status while the sequence is allocated before apply. It
needs a focused concurrent-reader regression before TidesDB's general runtime atomicity claim is
accepted. The proposed first Laminar profile would avoid this path structurally by permitting no
concurrent TidesDB reads on the owner lane; checkpoint freeze must be serialized through that lane.

## Rust release path

Published wrapper 0.11.1 declares only feature `v9_3_6` and source crate
`tidesdb-src-v9-3-6`. Its generic version parser does not make an undeclared `v9_3_14` feature
available. A dry run of the official `scripts/sync-versions.py` showed the mechanical manifest,
build-script, CI, and source-crate updates required for a new version; it did not prove ABI or
correctness compatibility. The wrapper build also accepts an exact ambient `pkg-config` library
before using bundled source, so qualification must verify the linked native artifact.

The release sequence is:

1. merge [TidesDB PR 664](https://github.com/tidesdb/tidesdb/pull/664) after its complete upstream
   matrix and review pass;
2. publish a fixed native successor to 9.3.14;
3. publish the corresponding official `tidesdb-src-vX-Y-Z` and `tidesdb` feature/release; and
4. repeat the complete package-entry gate against those immutable artifacts.

Only then should Laminar pin the exact wrapper and native feature with default features disabled.
Native S3 would remain disabled in the proposed first profile; Laminar's provider-neutral Rust
`object_store` checkpoint path retains S3, Azure, GCS, and local transport authority.

## Product boundary

TidesDB is disposable worker-local capacity and latency infrastructure. Laminar retains vnode
ownership, checkpoint decisions, portable checkpoint encoding, rebalance fencing, source offsets,
sink transactions, and exactly-once composition. Production remains blocked until the official
release pair, Laminar adapter/all-mode integration, failover/ALO/EO regressions, performance and
resource qualification, and an independent immutable-release soak all pass.
