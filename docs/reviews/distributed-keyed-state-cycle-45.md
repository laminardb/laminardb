# Distributed keyed state Cycle 45 review

- **Date:** 2026-07-25
- **Scope:** ASOF empty-buffer schema checkpoint completeness and restore hardening
- **Cycle outcome:** checkpoint v2 preserves learned right schema after complete eviction
- **Admission/backend outcome:** unchanged; no TidesDB dependency, runtime backend, or cluster
  capability was added
- **Production verdict:** **NO-GO** pending the remaining lifecycle, delivery, backend, fault, and
  independent-soak gates

## Result and boundary

The ASOF operator's learned right schema is logical state: a LEFT join needs it to emit right-side
null columns after every retained right row has been evicted. Cycle 44 proved that checkpoint
compaction removed those rows and v1 then restored neither a buffer nor the schema. The red
regression reproduced the exact sequence: ingest quotes, evict all at watermark 200, checkpoint,
restore, then process left-only trades. Before the change the restored operator returned no batch;
the live operator emitted two rows with nullable `quote_ts` and `bid` columns.

Checkpoint v2 leaves the rkyv `AsofBufferCheckpoint` v1 body unchanged and appends:

```text
[v1 rkyv buffer body][optional schema-only Arrow IPC][schema length: u32 LE][version: u8 = 2]
```

The appendix exists only when the compacted buffer is empty and a right schema was learned. When a
retained batch exists it is the sole schema authority. Schema capture uses a bounded header-only IPC
writer. The source schema is capped at 256 KiB retained memory, the appendix at 512 KiB on wire, and
the decoded schema at 1 MiB. Byte-level frame preflight rejects declared Arrow bodies that exceed the
available payload before Arrow may allocate. The reader also requires schema-only or exactly-one-
batch streams with explicit EOS and rejects malformed/trailing IPC and dual schema authorities.

Restore decodes into locals, verifies the configured key/time columns, and checks that every
persisted index points to exactly one in-range row whose hash and timestamp match, and requires each
equal-key/timestamp tie vector to remain strictly increasing. Only then does it replace the buffer,
watermark, and schema together. Checkpointability is enforced before first ingest and at restore, so
accepted state cannot enter a permanent empty-buffer checkpoint failure loop. First admission runs
the exact bounded schema-only encoder and framing preflight once. Later pointer-distinct schemas
recheck retained-memory size before structural equality because logical equality does not include
allocation capacity; pointer-identical batches keep the immediate fast path. In-limit non-empty v1
checkpoints derive the schema from their batch. Empty v1 LEFT checkpoints are ambiguous and fail
recovery-closed; empty v1 INNER checkpoints remain compatible because null-extension never depends
on the missing schema, but they cannot preserve historical learned-schema drift constraints.

This closes one operator-local replay invariant. It does not detect a dropped or panicking
`process` future after synchronous mutation, provide sticky attempt/root poison, distribute ASOF
state by vnode, fence ownership, coordinate source offsets or sink transactions, qualify TidesDB,
or provide independent soak evidence. The operator codec relies on the outer checkpoint-size and
pipeline-identity envelopes rather than adding a second whole-body cap or query fingerprint.
`[LDB-4007]` and `[LDB-0013]` remain authoritative.

## AI slop review

**Pass.** The code follows one reproduced failure and one explicit v2 wire layout. No speculative
backend trait, object-store path, generic checkpoint framework, admission bypass, or duplicate state
authority was added. Compatibility decisions are executable rather than prose-only.

## Overengineering review

**Pass with a deliberate cold-path hardening boundary.** The schema codec and index validation are
local to ASOF. Dedicated caps mirror the already reviewed shuffle schema limits. Strict index checks
stay in this cycle because an accepted reordered tie vector changes restored ASOF results, while an
overdeclared Arrow body can allocate before ordinary decode errors. The larger test matrix is
therefore persisted-format correctness, not a runtime abstraction. Per-cycle work with no right
input does no schema validation. The exact bounded schema encode runs once when learning a schema;
incoming batches use Arc pointer equality before the memory/structural checks required for a
pointer-distinct schema.

## Unused-code review

**Pass.** Both checkpoint versions have live restore dispatch. The schema appendix encoder/decoder,
framing parser, retained-schema accessor, logical-empty check, and index verifier are all consumed by
the production checkpoint path. Test-only cloning and wire constructors remain under `cfg(test)`.

## Production-readiness review

**NO-GO.** Returned-error recovery classification and empty-buffer schema restore are now stronger,
but cancellation/panic after synchronous mutation can still bypass returned-error classification.
There is still no admitted vnode-keyed state owner, fresh-root poison lifecycle, fenced rebalance,
portable backend artifact, complete source/sink delivery matrix, qualified TidesDB package, or
independent multi-process soak. No cluster capability descriptor changed.

## Documentation review

**Pass.** ADR-008, both implementation plans, and the validation report record the same narrow
result and the same remaining blockers. Historical v1 limitations and conditional migration are
stated explicitly. Backend research remains reference evidence; no stale document was made current
and no research file became removable in this cycle.

## Test review

**Pass after three independent read-only audits.** Initial audits rejected declared-body allocation,
noncanonical retained IPC/index state, late checkpointability failure, unconditional checkpoint
schema encoding, missing INNER behavior, and the lack of a literal v1 fixture. The final recovery
audit additionally found pointer-distinct capacity amplification and inconsistent bounded-encoder
checks between admission and restore. Those findings were fixed before all three reviewers returned
PASS. Final results against the post-fix source are:

- `cargo test -p laminar-db --lib operator::asof_join::tests::`: 26 passed;
- `cargo test -p laminar-db --lib`: 1,287 passed and one profiling test ignored;
- `cargo test -p laminar-db --lib --all-features`: 1,770 passed and two explicit tests ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass;
- `cargo fmt --all -- --check` and `git diff --check`: pass;
- local links in the five changed/new Markdown documents: zero missing; and
- Cargo manifests and lockfile contain no TidesDB dependency.

The first all-feature and Clippy commands exhausted their Windows command windows while vendored
OpenSSL compilation continued. Those attempts were not counted as passes; each exact command was
rerun cleanly from the completed cache to obtain the results above. Production soak was
intentionally not run because admission and backend qualification remain closed.

## Cycle 46 review plan

Audit the cancellation/panic boundary before adding another recovery mechanism:

1. **AI slop:** reproduce cancellation after ASOF mutation at the real graph/coordinator boundary;
2. **Overengineering:** identify the smallest owner that can poison an ambiguous attempt without a
   disconnected future backend trait or operator-specific framework;
3. **Unused code:** require a live checkpoint/publication exclusion consumer for every poison bit;
4. **Production readiness:** prove fresh-graph restore is mandatory before reuse and preserve stronger
   shuffle/halt dispositions;
5. **Documentation:** keep cancellation recovery separate from vnode ownership, backend, and delivery
   claims; and
6. **Tests:** cover drop/panic, no post-ambiguity checkpoint/output, prior-cut restore, and unchanged
   normal hot-path latency before any admission change.
