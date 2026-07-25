# Distributed keyed state Cycle 37 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** aggregate-v1 journal/checkpoint transitions frozen in a normative contract and
  disconnected test-only oracle; TidesDB remote-provider boundary independently revalidated
- **Backend/candidate/provider executed:** no
- **Runtime backend, adapter, public state trait, schema, or admission change:** none
- **Bounded memory:** reference/conformance-only
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

The [managed-state artifact contract](../architecture-decisions/managed-state-artifact-format-v1.md)
now defines the initial aggregate-v1 mutation-generation and checkpoint transition. Parent authority
requires the exact sealed inventory plus durable terminal Commit. Seal followed by Abort retains
dirty generations but is not admitted ancestry, so the first later changed capture emits FULL.
Unchanged state may still reference an older admitted nonempty BODY, and a later DELTA may parent an
immediately preceding admitted REFERENCE. DecisionInDoubt blocks mutation, re-emission, and new
attempts until the exact create-once outcome is observed, whether ambiguity began before or after
seal. Abort may resolve without a seal and retains generations; Commit still requires the exact
validated seal. Every allocated attempt ID is burned, including outcome-less numeric gaps.

The private oracle is nested under the existing `artifact_v1` tests. It models one PUT-only
aggregate namespace with owned `BTreeMap`s and literal expectations. It checks atomic per-batch
prefix arithmetic, cross-batch coalescing, deterministic ordering, immutable capture re-emission,
ordinary one-live-capture exclusion, admitted ancestry, outcome-less allocator gaps, pre-seal
ambiguous Abort, sealed ambiguous Commit, sealed-Abort gaps, exact Commit-gated generation release,
post-freeze mutation isolation, and one existing aggregate/V2 codec seam. It is deliberately
disconnected from runtime state and admission.

A primary-source recheck of TidesDB `v9.3.14` and `tidesdb-rs v0.11.1` confirms that shipped remote
implementations are filesystem plus S3-compatible. There is no native Azure Blob/ADLS or native GCS
connector and no Rust `object_store` injection. A low-level synchronous C callback table could host
a custom connector, but that is unimplemented integration work. Native remote support therefore has
zero local-backend selection weight and remains disabled; this does not penalize a hypothetical
local-only candidacy. LaminarDB's local/S3/GCS/Azure artifact and checkpoint compatibility remains a
hard gate and sole distributed recovery authority.

## Six-pass cycle review

### 1. AI slop, evidence, and consistency

**Pass after independent review and correction.** The first standalone oracle draft was rejected
before commit because it duplicated codec fixtures and grew to 903 lines. It was deleted and
replaced by one private nested model with literal results and a narrow reuse of existing codecs.
Reviews then found and fixed five substantive issues: zero-row appends could create an invalid empty
state or dirty no-op; the bridge initially relied on a coincidentally matching dummy parent digest;
the ordinary live-capture exclusion lacked a direct vector; pre-seal ambiguous Abort was incorrectly
forbidden; and burned-ID wording omitted outcome-less allocator gaps. The final documentation also
distinguishes structural parent decoding from production admission, limits retention obligations to
nonempty generations, and consistently treats sealed-Abort as non-admitted.

### 2. Overengineering, hot path, and latency

**Pass for the reference slice.** No generic store facade, async service, lock, public trait,
backend adapter, restore installer, or manifest consumer was added. The oracle's map scans, owned
values, and clones are intentionally unsuitable for production and prove neither constant-time
freeze nor bounded memory. Production work must use vnode-local journals, bounded immutable
generations, off-event-loop materialization, and measured p99/p99.9 hot-victim behavior; no
object-store request enters the record hot path.

### 3. Unused code and dependencies

**Pass.** The rejected standalone draft is absent. The surviving module is reachable only through
the existing test module and all transition paths relevant to its contract are exercised. No Cargo
dependency, feature, public symbol, runtime branch, backend code, provider configuration, fixture,
schema, workflow, or cloud resource was added.

### 4. Production readiness, delivery, exactly once, and soak

**NO-GO, correctly fail-closed.** This model does not implement restore-before-activate, old-owner
fencing, bounded production state, rebalance, timers, joins, source cuts, sink publication,
ambiguous external commits, or end-to-end exactly once. A worker-local backend remains disposable
and cannot replace LaminarDB's checkpoint/coordinator authority. Backend qualification, integrated
fault campaigns, source/sink certification, multi-process recovery, and the independently operated
unchanged-release product soak remain mandatory before any production claim.

### 5. Documentation and research hygiene

**Pass.** The normative transition rules live in the artifact contract; the phase plan and owner
packet point to the selected nested test seam and no longer offer an unused tool alternative.
TidesDB's dated prescreen remains relevant because it records exact rejection and re-entry evidence;
its provider section already distinguishes shipped filesystem/S3-compatible support, the C callback
seam, the Rust binding limitation, and LaminarDB's Azure/GCS authority. No research document was
removed because no retained document became irrelevant.

The oracle proves immutable semantic-view re-emission. Existing deterministic codec and golden
tests separately prove byte stability; the oracle does not claim an independent byte/digest proof.

### 6. Tests and empirical boundary

**Pass for this admission-neutral reference slice; no backend or product evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo test -p laminar-db --lib --no-default-features checkpoint_oracle`: 3 passed.
- `cargo clippy -p laminar-db --lib --tests --no-default-features -- -D warnings`: pass.
- `cargo test -p laminar-db --lib --no-default-features`: 1,261 passed; one profiling test ignored.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicitly ignored non-gating throughput/RSS observation.
- `git diff --check`, changed-document relative Markdown links, and exact qualification registries:
  pass.
- No candidate, backend, Docker/WSL workload, object-store provider API, cloud resource, cluster
  soak, or independent production soak ran.

## Cycle 38 entry boundary

**Superseded by the Cycle 38 project-owner direction.** No DKS GitHub approval workflow existed, and
the documented PF4/PF5 protected-receipt ceremony was removed as unnecessary for validation-only
work. Cycle 38 also makes TidesDB the preferred conditional local-spill candidate and leaves
RocksDB/Fjall as immutable v4 references. The next work is the bounded TidesDB remediation/source-
closure and successor-contract design for the still-rejected official Rust path, followed only by
genuinely reusable validation primitives. Candidate source construction, adapter work, execution,
backend selection, runtime admission, and production claims remain separately closed.
