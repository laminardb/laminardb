# redb 4.1.0 bounded state-backend prescreen v1

- **Identity:** `state-backend-redb-prescreen/v1`
- **Status:** Cycle 33 validation-only prescreen directions frozen: the engineering recommendation
  rejects the Cycle 32 Linux/x86-64 `N19` assembly ABI in favour of a versioned sequenced-packet
  redesign, with owner acceptance still absent; GCP and conditional AWS remain unselected provider
  finalists;
  and the attempt-exclusive-VM Docker successor has a source-derived dummy runtime/API proposal plus
  BPF/gate/holder proof predicates, but no eligible binary/host tuple or mechanism evidence; only
  redb-free validation work is authorized, while blocked source authorities/platform proofs,
  complete target/preflight schemas and collectors, strict later run/result wires, live
  provider/storage/finalization verifiers, native supervisor/child/actuator/oracle, reviewed build,
  owner approvals, and execution remain absent
- **Evidence class:** `NOT C2/C3 QUALIFICATION EVIDENCE`
- **Scope:** decide whether a redb adapter is worth adding to the backend qualification bake-off
- **Production/admission effect:** none; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Existing synthetic contracts:** [pre-run descriptor-root schema](../../tools/state-backend-qual/schema/redb-prescreen-approval-v1.schema.json),
  [reviewed-result descriptor-root schema](../../tools/state-backend-qual/schema/redb-prescreen-result-v1.schema.json),
  and [exact-source mechanism note](../reports/redb-4.1.0-prescreen-mechanism-note-2026-07-23.md).
  The two JSON schemas are regression inputs only and must be replaced by the payload/protected-review
  contracts below before approval or execution.

## Decision boundary

redb 4.1.0 is not a qualification candidate. This prescreen answers four cheaper questions before
LaminarDB pays for a third adapter:

1. does one database-wide writer create unacceptable acquisition or victim tails for the proposed
   state traffic;
2. what latency cost comes from one-phase Immediate, two-phase Immediate, and quick-repair commits;
3. do process-crash outcomes preserve atomic cross-table state and Immediate's return boundary; and
4. does quick repair materially bound reopen time without introducing correctness or resource
   failures?

`PRESCREEN_PASS` only funds the candidate mechanism mapping, persistence mapping, and adapter design
review. `PRESCREEN_NO_GO` means repeatable, valid architectural or performance evidence does not
justify that investment. `DEFER` means the experiment or environment could not decide.
`REJECT_EXACT_PIN` applies only to a correctness failure in the exact pinned build on a valid quiet
target. None supplies C1/C2/C3, fault, endurance, checkpoint, source/sink, exactly-once, selection,
or production evidence. The prescreen never selects redb by comparing its limits with Fjall or
RocksDB results.

### Cycle 21 protocol-freeze resolutions

This revision closes the design ambiguities above, but it does not make the prescreen approval- or
execution-ready. It freezes a candidate packet layout, non-circular protected-review policy,
deterministic fixture/schedule, completion rule, separate decision and safety bounds, retained-
evidence boundary, five-hour budget, and disposition precedence. The existing descriptor-root
schemas cannot express that packet and remain synthetic regression inputs only.

Before a pre-run owner approval can exist, validation-only work must implement and independently
review strict approval/result payload and protected-review-receipt schemas; exact-byte, protected-
review, locator, plan/result, oracle, and classifier verification; raw bounded wire schemas and
goldens; and a redb-free fail-closed verifier. A separately authorized construction stage must then
build and review the native supervisor, child, actuator, oracle and verifier and populate their exact
source, lockfile, SBOM, build and binary descriptors. Only those as-built bytes can enter an owner
approval. The current instruction does not authorize that construction or any candidate run.

The only executable redb code remains the separate `construction-only-no-decision` lane. No formal
disposition can be produced from this document, the old schemas, their fixtures, or that lane.

The four physical bases cannot be created by the current instruction. A later explicit
`fixture-construction-no-decision` authorization must first approve the as-built generator, recipe,
source/build and target scratch location. That bounded stage may open redb only to create the four
fixed fixture files and their logical/file digests; it emits no smoke/native disposition and every
qualification/selection/production field remains false. Independent verification then supplies the
fixture descriptors for the two-owner pre-run payload. Fixture generation is therefore outside the
five-hour decision campaign without being an unapproved hidden pre-run. The Cycle 16 construction
lane does not inherit this authority.

### Fixed packet root and locators

Every formal descriptor is `(role, locator, byte_length, sha256, media_type)`. `locator` is a UTF-8,
forward-slash, packet-root-relative path: no empty component, `.`, `..`, drive/UNC prefix, backslash,
symlink, hard link, device, socket, FIFO or path escape is allowed. Each retained applicable
singleton appears exactly once at the fixed locator in this table; an expected raw leaf may instead
reconcile as unavailable under Cycle 25. The terminal-finding row is conditionally expected
only after the native control journal durably records a terminal-stop-pending frame. The Docker
launch-ledger and control rows are mandatory for every stage-one-admitted Docker run. Later
repeatable retained-artifact
roles require a separate closed registry and cardinality. The verifier opens relative to an already-open
role-assigned root handle with no-follow semantics, verifies regular-file identity before and after
streaming, caps bytes before allocation, and rejects aliases or extra decision-bearing files.

| Role | Fixed locator | Required before |
|---|---|---|
| protocol | `contract/protocol.md` | protocol review |
| exact-source mechanism note | `contract/redb-mechanism-note.md` | protocol review |
| wire schemas | `contract/wire-schemas.tar.zst` | construction approval |
| literal goldens | `contract/literal-goldens.tar.zst` | construction approval |
| fixture recipe and expected digests | `contract/fixture-recipe.json` | construction approval |
| execution plan | `contract/execution-plan.json` | protocol review |
| candidate configuration | `contract/candidate-configuration.json` | construction approval |
| target identity policy | `contract/target-identity.json` | protocol review |
| preflight/noise policy | `contract/preflight-policy.json` | protocol review |
| seed and slot schedule | `contract/schedule.json` | protocol review |
| clock/cgroup/cache-reset policy | `contract/clock-isolation-policy.json` | protocol review |
| trigger/adaptive-delay policy | `contract/trigger-delay-policy.json` | protocol review |
| deadline/resource/artifact bounds | `contract/bounds.json` | protocol review |
| protected-review policy | `contract/protected-review-policy.json` | pre-run approval |
| exact redb 4.1.0 crate archive | `subject/redb-4.1.0.crate` | fixture construction |
| complete formal source | `build/source.tar.zst` | pre-run approval |
| Cargo lockfile | `build/Cargo.lock` | pre-run approval |
| SBOM | `build/sbom.spdx.json` | pre-run approval |
| reproducible build receipt | `build/build-manifest.json` | pre-run approval |
| fixture generator | `build/redb-prescreen-fixture-generator` | fixture construction |
| supervisor | `build/redb-prescreen-supervisor` | pre-run approval |
| candidate child | `build/redb-prescreen-child` | pre-run approval |
| crash actuator | `build/redb-prescreen-actuator` | pre-run approval |
| independent oracle | `build/redb-prescreen-oracle` | pre-run approval |
| external verifier/classifier | `build/redb-prescreen-verifier` | pre-run approval |
| 64-MiB physical base | `fixtures/base-64m.redb` | pre-run approval |
| 256-MiB physical base | `fixtures/base-256m.redb` | pre-run approval |
| 1-GiB physical base | `fixtures/base-1g.redb` | pre-run approval |
| 4-GiB physical base | `fixtures/base-4g.redb` | pre-run approval |
| approval payload | `approval/payload.json` | dispatch |
| pre-run protected-review receipt | `approval/protected-review.json` | dispatch |
| reviewed smoke result | `evidence/prior-smoke-result.json` | native dispatch only |
| actual target observation | `evidence/actual-target.json` | run start |
| actual preflight cut | `evidence/preflight-cut.json` | run start |
| run-start binding | `result/run-start-binding.json` | campaign start |
| native campaign-control journal | `result/campaign-control.bin` | native evidence close |
| terminal finding | `result/terminal-finding.json` | native terminal-pending closure |
| Docker launch ledger | `result/docker-launch-ledger.bin` | Docker evidence close |
| Docker smoke control | `result/docker-smoke-control.json` | Docker evidence close |
| raw run manifest | `result/raw-run-manifest.json` | result review |
| evidence-close manifest | `result/evidence-close-manifest.json` | cleanup precondition |
| crash-recoverable cleanup journal | `result/cleanup-journal.bin` | cleanup recovery |
| cleanup report | `result/cleanup-report.json` | result classification |
| retained artifact index | `result/artifact-index.json` | result review |
| validator report | `result/validator-report.json` | result review |
| independent oracle report | `result/oracle-report.json` | result review |
| bounded mechanism-probe report | `result/mechanism-probe.json` | native result review |
| derived result payload | `result/payload.json` | owner result review |
| post-run protected-review receipt | `result/protected-review.json` | immutable closure publication |

The closed role registry assigns every contract/subject/build/fixture target to the immutable
approval-input root and every `approval/`, `evidence/` and `result/` object to the separate bounded
result root. The logical locator namespace is shared, but packet content cannot select or substitute
a root handle. The final result index covers only the result root; the approval-input version is
bound and reverified separately.

The approval payload binds every pre-run contract, subject, build, physical-fixture and target row,
but excludes `approval/payload.json`, its protected-review receipt, and every result row. The pre-run
receipt binds the approval payload's exact length and SHA-256. The raw run manifest binds the exact
run-start binding, including its approval payload/receipt pair and copied dispatch context. Only the
later live run-provenance verifier can authenticate that context or prove capability consumption.
The result payload binds that complete approval packet plus every applicable run/result evidence row,
but excludes `result/payload.json` and its post-run receipt. The post-run receipt binds the result
payload's exact length and SHA-256. Neither payload binds itself or the receipt that is necessarily
created afterward. The redb archive is the fixed packet row `subject/redb-4.1.0.crate`, not an
unresolved external descriptor. A prior smoke result is accepted only when the verifier
proves that it is a non-synthetic, reviewed-and-stored `DOCKER_SMOKE_PASS` over the identical
subject, source/build, schemas, goldens, 64-MiB fixture and Docker-smoke plan and returns the opaque
`DOCKER_SMOKE_PREREQUISITE_VERIFIED_NO_DECISION` capability. An opaque or synthetic descriptor
never satisfies that prerequisite, and the capability does not replace the separate native pre-run
approval.

## Frozen source and build scope

The only subject is:

- crate `redb =4.1.0`;
- SHA-256 of the exact `.crate` archive
  `8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`; and
- packaged/upstream revision `6ed1f981ba4deab0b2adbdd7bccb46ec409b2191`.

The future formal harness must live in an isolated, unpublished prescreen crate. Its exact lockfile,
source archive, SBOM, feature set, target, `1.95.0-x86_64-unknown-linux-gnu` toolchain, compiler
flags, and binary SHA-256 must be recorded. Neither the LaminarDB root workspace nor runtime crates
gain a redb dependency. A different archive, source revision, feature set, or relevant build flag is
a different subject and cannot inherit the result.

One redb file contains four byte-key/byte-value tables named `state`, `timer`, `join_left`, and
`join_right`. The builder cache is 8 GiB. Measured attempts prohibit `WriteTransaction::stats`,
`Database::compact`, savepoints, and any other full-tree or exclusive maintenance call. Every
transaction uses 32-byte keys and 992-byte values, for 1,024 logical key-plus-value bytes per
mutation. A transaction's mutations are distributed deterministically across all four tables.

The three exact transaction modes are:

| Mode | redb 4.1.0 calls | Purpose |
|---|---|---|
| `I1` | `set_durability(Immediate)`, `set_two_phase_commit(false)`, `set_quick_repair(false)` | Immediate one-phase baseline |
| `I2` | `set_durability(Immediate)`, `set_two_phase_commit(true)`, `set_quick_repair(false)` | isolate two-phase commit cost |
| `QR` | `set_durability(Immediate)`, `set_two_phase_commit(false)`, `set_quick_repair(true)` | save allocator state; redb forces two-phase commit during commit |

The future formal harness must assert and record the closed setter sequence before each attempt;
pinned-source review verifies that `QR` forces two-phase commit. `Durability::None` is outside this
protocol because it does not satisfy the persistence question.

### Deterministic fixture and operation recipe

Logical bytes always mean key bytes plus value bytes; redb page/file overhead is physical. The clean
bases contain exactly 65,536 entries for 64 MiB, 262,144 entries for 256 MiB, 1,048,576 entries for
1 GiB, and 4,194,304 entries for 4 GiB, divided equally across the four tables. The 64-MiB base has
16,384 entries per table and `P = 2,048` entries per table/vnode. Four same-ordinal table entries form
one logical entity. Fixture creation uses a fresh exclusive create-new regular file. The generator calls
`Database::builder()`, then exactly one Builder setter, `set_cache_size(8_589_934_592)`, then
`create_file`; it uses no other Builder setter. It is built with `redb =4.1.0`,
`default-features=false`, and the exact bound archive, source, lockfile, SBOM, toolchain, target,
profile, and flags. The generator configuration and build receipt bind that sequence and reject any
environment or feature drift.

For each table/vnode with `P = entries_per_table / 8`, the initial live set is every vnode-local slot
from 1 through `P-1`, inclusive, plus filler slot `P`; slot 0 is deliberately absent. Initial values
use generation/seed zero, transaction ordinal `q = v*P+s`, entity index zero, and put code. Fixture
mutation order is four non-interleaved phases:

1. put the complete initial set in ascending vnode, slot, then table order;
2. delete every entity whose slot is 8 modulo 16 in the inclusive vnode-local range 3 through `P`,
   in that same order;
3. reinsert that exact deleted set in that same order; and
4. overwrite every entity whose slot is 9 modulo 16 in the same inclusive range and order.

Each phase is partitioned independently into consecutive transactions of exactly 4,096 mutations
(4 MiB of charged logical request bytes), except its one final transaction when fewer remain; a
four-table entity is never split because the boundary is divisible by four. Each transaction is
opened, configured in this exact order with `set_durability(Immediate)`,
`set_two_phase_commit(false)`, and `set_quick_repair(false)`, populated, committed, and dropped before
the next begins. There is no empty transaction or cross-phase coalescing. All churn intents use seed
zero and entity index zero. Phase-2 delete intents and phase-3 reinsert values use generation 1 and
`q = (1_u64 << 56) | (v*P+s)`, differing only by operation code delete versus put. Phase-4 overwrite
values use generation 2, put code, and `q = (2_u64 << 56) | (v*P+s)`. Deletes pass only the key to
redb, while the fully generated delete-intent value is retained by the redb-free oracle.

This restores the exact entry count while creating deterministic allocated/free-page churn. After
the final commit the generator drops the database, records a pre-read file digest, reopens with the
same builder settings read-only, streams the canonical table/key/value digest, drops the read-only
database, and records a post-read file digest. The two file digests MUST be identical. The approved
payload binds the generator binary, canonical digest, and that single physical digest; a copied
fixture must match the physical digest before opening and the canonical digest after scanning.

The canonical scan digest is SHA-256 over `"LDB-REDB-SCAN-V1\0"`, followed by each table in the
fixed order `state`, `timer`, `join_left`, `join_right`: `u8(table_id) || u64_be(row_count)` and then
each entry in redb byte-key order as `u32_be(key_len) || key || u32_be(value_len) || value`. Key and
value lengths must be 32 and 992. The read-only scan must leave the database file digest unchanged;
otherwise fixture verification fails.

Each base divides every table evenly across fixed vnodes 0 through 7, inclusive; all four
table-entry counts are divisible by eight. For table ID `t` in `{0,1,2,3}`, vnode `v`, vnode-local
logical slot `s`, generation
`g`, seed `z`, and transaction ordinal `q`, the 32-byte key is:

```text
u16_be(v) || u64_be(s)
|| first_22_bytes(SHA256("LDB-REDB-KEY-V1\0" || u8(t) || u16_be(v) || u64_be(s)))
```

The 992-byte value begins with `u8(t) || u16_be(v) || u64_be(s) || u32_be(g) || u64_be(z) ||
u64_be(q) || u32_be(entity_index) || u8(operation_code)`. Operation code is `0x01` for put and
`0x02` for delete intent. The remaining 956 bytes are consecutive SHA-256 blocks over
`"LDB-REDB-VALUE-V1\0"` plus that 36-byte header and a `u32_be` block counter beginning at zero,
truncated exactly; delete does not pass the value to redb but retains it in the intent/oracle. The
verifier reimplements this recipe without redb. Integer overflow, duplicate key generation,
unexpected pre-state, or recipe disagreement invalidates the attempt.

Every mutation count is divisible by four. Entity index `e` emits the four table mutations in table
order. Slot selection is evaluated without intermediate `u64` overflow: let
`P = entity_count_per_table / 8`, compute
`s128 = (u128(q)*65_537 + u128(e)*17 + u128(z)) mod u128(P)`, then losslessly convert `s128` to `u64`
and require `s < P`. For generated operation traffic, let `q_low32 = q & 0xffff_ffff`; require
`q_low32 <= 0xffff_fffe`, then set generation to the lossless `u32(q_low32 + 1)`. This masks before
addition and never attempts to convert the lane bits. The entity operation is delete exactly when
the equivalent unsigned-128-bit sum `(u128(q)+u128(e)+u128(z)) mod 16` is zero; otherwise it is a put
of the generated value. In steady/HOLD attempts, `q = (lane_id << 56) | lane_release_ordinal`; each
lane ordinal starts at zero before warmup, continues through measured traffic, and is capped at
`0xffff_fffe`, so `q_low32` equals that ordinal without wrap. Lane IDs are fixed by the schedule and
no `q` is reused within an attempt.

Small crash target transactions contain 128 mutations/32 entities on vnode 0; large-recovery targets
contain 4,096 mutations/1,024 entities on vnode 0. In both, entity 0 inserts absent slot 0, entity 1
overwrites present slot 1, entity 2 deletes present slot 2, and entity indices 3 through 31 (small)
or 3 through 1,023 (large) overwrite the present slot equal to their entity index. Each entity emits
tables 0 through 3 in order and carries that exact entity index. Prime entities 0 through 31
overwrite slots `P/2` through `P/2+31`, respectively, in table order. Prime uses `q=0`; target uses
`q=1`; both use the trial seed and checked generation `q+1`. The delete-intent header for target
entity 2 therefore uses generation 2, the trial seed, `q=1`, entity index 2, and delete code; only its
key reaches redb. Prime and target are separate transactions and each uses the selected mode's exact
setter sequence once before any table opens. The sentinel insert/delete pair keeps pre/post live
entry count equal. The independent oracle precomputes the affected-record intent digest and
expected old/new full-scan digests by streaming the frozen fixture recipe plus prime/target mutations;
the measured child never scans or hashes the whole database before commit.

Logical request bytes charge 1,024 bytes per mutation for offered-load and sample-cap arithmetic,
including a delete's 992-byte expected-value/intent material even though only its 32-byte key is
passed to redb. Actual engine/device bytes remain separately measured diagnostics.

The fixed steady-state vnode/lane-ID assignment is `W0=1`, `W1={2,3}`,
`W2={hot:4,victim:5}`, and `HOLD={holder:6,victim:7}`. Lanes never share a key. In every `HOLD`
repetition both transactions use
the repetition's `I1`, `I2`, or `QR` setter mode; after the controlled hold, both execute one
128-mutation transaction before commit. This freezes the previously ambiguous HOLD durability mode.

### Complete seed and slot schedule

Let seed index `i` enumerate `2026072301`, `2026072302`, `2026072303`; let slot `j` be `0..11`.
For the steady matrix, mode index is `(j + i) mod 3` over `[I1,I2,QR]` and probe index is
`(j + 2*i) mod 4` over `[W0,W1,W2,HOLD]`. The coprime periods enumerate each mode/probe pair exactly
once per seed. Slot ID is `steady/s<seed>/n<two-digit-j>` and cannot be reused or silently rerun.

Small crash slots are ordered by seed, then mode index `(m+i) mod 3` for `m=0..2`, then trigger
`1 + ((k+2*i) mod 6)` for `k=0..5`; IDs are `atomic/s<seed>/<mode>/t<trigger>`. Six reserved
adaptive slots have IDs
`atomic-extra/<mode>/n<0..1>` and may be used only to obtain the required confirmed in-commit count.
Extra 0 has parent `atomic/s2026072301/<mode>/t2`, seed `2026072301`, and zero delay; extra 1 has
parent `atomic/s2026072302/<mode>/t3`, seed `2026072302`, and 50-microsecond delay. Transaction bytes,
mode and every other parent input remain unchanged. Unused reserved IDs are recorded as unused. Large
recovery slots are ordered by seed and the same rotated mode order with IDs
`recovery/s<seed>/<mode>`; there is no adaptive large-trial retry, and a missed confirmed in-commit
kill is `DEFER`. The machine-readable schedule expands these formulas into all 99 baseline slot IDs
plus six reserved IDs, and the semantic verifier requires exact equality rather than trusting counts.

### Separate pre-run authorization

This proposal does not authorize protocol execution. Any formal command that claims the Docker smoke
or native prescreen identity requires the additive
`state-backend-redb-prescreen-approval-payload/v2` and
`state-backend-redb-prescreen-protected-review-receipt/v2` over identical payload bytes. Neither
valid copied content nor the Cycle 26 bytes-only validator authorizes dispatch. The implemented
28-row `/v1` payload/receipt pair can never describe the formal 29-object input set. The receipt is
copied content and is never provider-authenticated merely because its
JSON validates. The unsigned payload contains all
decision-bearing fields and descriptors, plus the two required roles and exact required decision
literal. It contains no concrete principal, review-event ID, review timestamp, or receipt
descriptor; those copied fields can exist only in the later receipt, so the packet is non-circular.

This prescreen reuses the repository's protected-review trust boundary; it does not define an
offline key hierarchy, signature format, owner registry, revocation service, or root-rotation
protocol. The exact `contract/protected-review-policy.json` binds the repository/provider identity,
immutable change and head-revision requirements, the configured workload-owner and operations-owner
review groups, stale-review dismissal, distinct-principal rule, protected execution environment,
and the provider API fields the trusted dispatcher verifies. The pre-run receipt contains exactly:

- schema/version and `pre_run` stage;
- policy role/length/SHA-256;
- provider, repository, immutable change ID, and reviewed head revision;
- approval-payload role/length/SHA-256;
- one copied provider-scoped review-event ID, stable account ID, role, decision, and UTC time for each
  of `workload_owner` and `operations_owner`; and
- the copied dispatch workflow/environment/run identity and verification UTC time.

Content validation requires unequal principal and review-event ID strings, exact approval literals,
and bindings to the head and payload digest, but proves none of those provider facts. Unknown fields,
a locally invented identity, a provider mismatch, mutable branch-only identity, missing protected-
environment context, or a payload/head/configuration change fails content validation closed.

The trusted dispatcher separately queries the live provider and verifies current distinct non-self
owners, event bodies/states/times, head, policy and protected workflow identity. This is the first of
two disjoint gates. It may mint one non-serializable, run-class-specific protected-run admission
capability: `DOCKER_SMOKE_DISPATCH_AUTHORIZATION_VERIFIED` or
`NATIVE_DISPATCH_AUTHORIZATION_VERIFIED`. Issuance also requires and binds the exact
`APPROVAL_INPUT_STORAGE_VERSION_VERIFIED` capability/version. Native admission additionally consumes
the exact verified Docker-prerequisite capability; Docker admission neither consumes nor produces
native authority. A single
consumption of the admission capability may enter the named protected runner, create its bounded
workspace, collect actual target and preflight observations, and freeze the exact run-start binding.
It cannot open redb or start a candidate child, actuator, recovery opener or clean-control process.

The actual-target and preflight-cut envelopes are strict, bounded objects that precede the run-start
binding and never bind that future object. The preflight status is exactly `passed`, `failed`, or
`incomplete`. After the run-start bytes are durably published, reopened and rehashed, the live
dispatcher must reverify their admitted run-attempt context and every target, preflight, schedule and
executable binding. Only status `passed` may mint the disjoint
`DOCKER_SMOKE_CHILD_DISPATCH_AUTHORIZATION_VERIFIED` or
`NATIVE_CHILD_DISPATCH_AUTHORIZATION_VERIFIED` capability, bound to that exact run-start descriptor
and class. That capability is handed off and consumed once into the protected runner's schedule gate.
The gate covers the exact campaign schedule and every candidate-affecting start or database open:
transaction child, opener/scanner, large-recovery clean control and the separate crash actuator.
Report, evidence-close and cleanup processes have only closure authority and cannot use that gate. A
valid failed/incomplete preflight, or failure to obtain the second capability after valid admission,
may close only as a no-candidate `DEFER`. Without verified first-stage admission there is no formal
run-start, manifest or outcome.

Neither stage, its consumption nor the operational gate is reconstructible from packet bytes.
Merely placing matching JSON in a packet never authorizes execution. Both stages must bind the exact
approval payload, receipt, change, head, workflow, job, environment and protected run attempt; be
freshness-bounded and single-use in that same protected attempt; and reject cross-class substitution.
The later live run-provenance verifier independently proves both stages. Until their exact provider
linearization, TOCTOU, freshness, hand-off, restart and replay rules are frozen, all four capabilities
are unconstructible.

For native only, the outer protected runner—not the restartable supervisor worker—first retains a
non-serializable `NATIVE_CAMPAIGN_ROOT_LEASE` over exact run-start, evidence/control-root handles,
dedicated workspace/cgroup, protected run attempt and outer-runner process registry. It creates and
reopens the campaign-control file, then irreversibly narrows that lease to
`NATIVE_CAMPAIGN_CONTROL_LEASE` binding the exact file identity before a worker receives a scoped
single-writer handle. Neither lease grants candidate start/open authority. Loss of the supervisor
worker irrevocably closes the schedule gate; after proving the old worker exited and obtaining
exclusive control, the control lease may mint only `RecoveredClosureOnly` for that exact journal
prefix. Loss of the outer runner or lease makes the run unfinalizable. Packet paths, journal bytes and
copied run identity cannot recreate either lease.

Result review uses the same receipt schema with stage `post_run`, the exact result-payload length and
SHA-256, two copied role-review records, and the retained-evidence content root. A native payload
carries `derived_outcome`; a Docker payload carries `smoke_outcome` and no native outcome. The
receipt binds the exact payload and artifact-index bytes, but neither payload nor receipt contains
future storage or review authority. The live verifiers later cover the immutable closure and current
provider facts. Neither payload nor receipt hashes itself.

The redb-free semantic verifier validates schemas, exact bytes/locators, payload/receipt equality,
and cross-artifact invariants. It reports only content validity and `authorization_unverified`; it
cannot turn a copied receipt into authority even when a protected workflow invokes it. The trusted
dispatcher independently validates the live provider context before it may invoke an execution
entry point, and the candidate child is opened only after both checks succeed. If the
repository cannot enforce and export two role-separated pre-run reviews, dispatch is unavailable:
no campaign runs and no prescreen outcome exists. Missing post-run review instead blocks final
sealing without changing an already-derived outcome. Portable offline cryptographic attestations
would require a separate security ADR,
security-owner approval, threat model, and implementation budget; this prescreen does not invent
them. Any bound input change requires a new payload and two new protected reviews. The separately
user-approved `construction-only-no-decision` lane does not consume this packet, cannot emit a
prescreen disposition, and hard-codes every evidence-eligibility field false.

The applicable approval or result payload binds the fixed packet rows assigned to it above, the
toolchain/target/flags, complete fixture and seed/order schedule, target/preflight/noise rules,
clock/cgroup/cache-reset procedures, trigger
and bounded adaptive-delay rule, every deadline/resource/artifact cap, and all-false qualification/
selection/production/admission fields. Every JSON packet root uses the exact notice
`NOT QUALIFICATION EVIDENCE`;
bounded binary frames use their versioned magic and are covered by a noticed manifest rather than
duplicating prose. Synthetic payloads/receipts are always ineligible. Only the later result
classifier may derive `DEFER`; content validation derives no disposition. Schema validity alone has
no authority, and no prescreen packet can authorize or donate C1/C2/C3 evidence.

### Cycle 22 validation-only successor contract freeze

Cycle 22 may implement only bounded content validation. It does not construct a packet, dereference
the native artifact descriptors, authenticate GitHub, dispatch a workflow, open redb, classify a
candidate result, or seal evidence. Content validity, provider authentication, execution authority,
and final-result sealing are four different states. The local validator can establish only the
first and always reports `authorization_unverified`.

The successor JSON identities are:

| Object | Exact `schema_version` | Schema file | Current implementation scope |
|---|---|---|---|
| protected-review policy | `state-backend-redb-prescreen-protected-review-policy/v1` | `redb-prescreen-protected-review-policy-v1.schema.json` | pre-run content validation |
| approval payload | `state-backend-redb-prescreen-approval-payload/v1` | `redb-prescreen-approval-payload-v1.schema.json` | pre-run content validation |
| result payload | `state-backend-redb-prescreen-result-payload/v1` | none; identity reserved | later validation slice |
| protected-review receipt | `state-backend-redb-prescreen-protected-review-receipt/v1` | `redb-prescreen-protected-review-receipt-v1.schema.json` | pre-run content validation; post-run shape reserved |

The exact implemented `$id` values, in table order with the absent result entry skipped, are
`https://laminardb.dev/schemas/state-backend-redb-prescreen-protected-review-policy-v1.json`,
`https://laminardb.dev/schemas/state-backend-redb-prescreen-approval-payload-v1.json`,
`https://laminardb.dev/schemas/state-backend-redb-prescreen-protected-review-receipt-v1.json`.
The reserved future result-payload `$id` is
`https://laminardb.dev/schemas/state-backend-redb-prescreen-result-payload-v1.json`; no file or
validator currently implements it, and the legacy `redb-prescreen-result-v1.schema.json` is not an
alias for it.

The exact owner decisions are `APPROVE_REDB_PRESCREEN_EXECUTION_V1` before either run class and
`ACCEPT_REDB_PRESCREEN_RESULT_V1` after either run class. The payload digest already binds
`docker_smoke_no_decision` or `native_prescreen_decision`; multiplying decision literals by run
class adds no authority. These strings are requested review decisions, not bearer capabilities. A
matching string in local JSON never authorizes execution or result sealing.

The policy contains only: its identity and notice; a stable policy ID; provider contract
`github-protected-review-export/v1`; repository full name and provider-scoped stable ID; base ref
`refs/heads/main`; the ordered `workload_owner` and `operations_owner` review groups and their
provider-scoped stable IDs; the two decision literals; the required immutable-change, exact-head,
current-membership, approved-state, stale-dismissal, distinct-principal, distinct-event,
no-self-review and no-admin-bypass controls; and the configured dispatcher workflow, job and
protected-environment identities. It contains no URL, token, credential, signature, key,
certificate, trust-root, registry, revocation, reviewer principal, approval event, authorization or
disposition. The policy bytes in a packet are not a trust root: a future dispatcher must byte-match
them to out-of-band trusted repository configuration and query the live provider.

All fields below are required and every object recursively has `additionalProperties=false`; no
successor field is optional unless its union is stated explicitly. The exact policy layout is:

| JSON pointer | Type or exact value |
|---|---|
| `/schema_version` | `state-backend-redb-prescreen-protected-review-policy/v1` |
| `/notice` | `NOT QUALIFICATION EVIDENCE` |
| `/policy_id` | protocol ID |
| `/provider/contract` | `github-protected-review-export/v1` |
| `/provider/repository_full_name` | `laminardb/laminardb` |
| `/provider/repository_id` | provider-scoped ID |
| `/provider/base_ref` | `refs/heads/main` |
| `/review_groups` | exactly two ordered objects |
| `/review_groups/0` | `{role:"workload_owner", group_id:<provider-scoped ID>}` |
| `/review_groups/1` | `{role:"operations_owner", group_id:<provider-scoped ID>}` |
| `/decision_literals/pre_run` | `APPROVE_REDB_PRESCREEN_EXECUTION_V1` |
| `/decision_literals/post_run` | `ACCEPT_REDB_PRESCREEN_RESULT_V1` |
| `/required_controls/immutable_change_id` | `true` |
| `/required_controls/reviews_on_exact_head` | `true` |
| `/required_controls/current_group_membership` | `true` |
| `/required_controls/approved_review_state` | `true` |
| `/required_controls/dismiss_stale_reviews` | `true` |
| `/required_controls/distinct_principals` | `true` |
| `/required_controls/distinct_review_events` | `true` |
| `/required_controls/self_review_allowed` | `false` |
| `/required_controls/admin_bypass_allowed` | `false` |
| `/protected_execution/workflow_file` | `.github/workflows/`-relative safe workflow path |
| `/protected_execution/job_name` | protocol ID |
| `/protected_execution/environment_id` | provider-scoped ID |

`review_groups/0/group_id` and `review_groups/1/group_id` must differ. The workflow path matches
`^\\.github/workflows/[a-z0-9][a-z0-9._-]{0,127}\\.ya?ml$`; it names future trusted code and does
not create that workflow or authorize its execution.

The repository's current [branch-protection setup](../../.github/setup-branch-protection.sh)
requires one CODEOWNERS approval. That is useful merge protection but cannot prove this protocol's
two role-separated approvals.
[GitHub's environment contract](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments)
also advances after any one listed required reviewer approves, so a protected environment is an
execution/secrets boundary, not a two-role proof. Before any future dispatch, a default-branch
trusted dispatcher must export two
current review events over the exact head, resolve live membership in the two configured groups,
and reject shared principals, stale/dismissed/superseded reviews and self-review. If the provider or
repository configuration cannot expose those facts, authorization remains unavailable.

The approval payload contains only: its identity and notice; payload and protocol IDs; one run
class; the exact pre-run decision literal; the two ordered required roles; the exact ordered 28-row
descriptor set below; a separate `prior_smoke_result` that is null for Docker smoke and is the fixed
reviewed-smoke descriptor for native prescreen; and the all-false evidence scope. It contains no
principal, event, review time, receipt descriptor, result, disposition, approval state, execution
flag, or self-hash. The 28 descriptor targets are not opened by the Cycle 22 bytes-only validator.
The exact scope is `prescreen_only=true` and `qualification_eligible`,
`candidate_admission_eligible`, `backend_selection_eligible`, `production_eligible`,
`independent_soak_eligible`, `c1_c2_c3_eligible`, `fault_endurance_eligible`,
`checkpoint_exactly_once_eligible`, and `source_sink_delivery_eligible` all false.

The exact approval-payload layout is:

| JSON pointer | Type or exact value |
|---|---|
| `/schema_version` | `state-backend-redb-prescreen-approval-payload/v1` |
| `/notice` | `NOT QUALIFICATION EVIDENCE` |
| `/payload_id` | protocol ID |
| `/protocol_id` | `state-backend-redb-prescreen/v1` |
| `/run_class` | `docker_smoke_no_decision` or `native_prescreen_decision` |
| `/required_decision_literal` | `APPROVE_REDB_PRESCREEN_EXECUTION_V1` |
| `/required_review_roles` | exactly `["workload_owner","operations_owner"]` |
| `/artifacts` | exactly 28 descriptors in the table order |
| `/prior_smoke_result` | Docker: `null`; native: fixed prior-smoke descriptor |
| `/evidence_scope` | the exact scope above |

Every descriptor has exactly `{role, locator, byte_length, sha256, media_type}`. `byte_length` is a
base-10 JSON integer from 1 through its role cap; `sha256` is 64 lowercase hexadecimal characters
and not all zero. Role, locator and media type equal their table row without aliasing or
normalization. Descriptor 15 additionally has exact length `188200` and SHA-256
`8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`. Descriptor 14's length and
SHA-256 must equal the exact supplied policy bytes. The aggregate cap sums all 28 `byte_length`
values plus the native prior-smoke length when present using checked `u64` arithmetic.

| # | Exact role | Exact locator | Exact media type |
|---:|---|---|---|
| 1 | `redb-prescreen-protocol` | `contract/protocol.md` | `text/markdown; charset=utf-8` |
| 2 | `redb-exact-source-mechanism-note` | `contract/redb-mechanism-note.md` | `text/markdown; charset=utf-8` |
| 3 | `redb-prescreen-wire-schemas` | `contract/wire-schemas.tar.zst` | `application/zstd` |
| 4 | `redb-prescreen-literal-goldens` | `contract/literal-goldens.tar.zst` | `application/zstd` |
| 5 | `redb-prescreen-fixture-recipe` | `contract/fixture-recipe.json` | `application/json` |
| 6 | `redb-prescreen-execution-plan` | `contract/execution-plan.json` | `application/json` |
| 7 | `redb-prescreen-candidate-configuration` | `contract/candidate-configuration.json` | `application/json` |
| 8 | `redb-prescreen-target-identity-policy` | `contract/target-identity.json` | `application/json` |
| 9 | `redb-prescreen-preflight-policy` | `contract/preflight-policy.json` | `application/json` |
| 10 | `redb-prescreen-schedule` | `contract/schedule.json` | `application/json` |
| 11 | `redb-prescreen-clock-isolation-policy` | `contract/clock-isolation-policy.json` | `application/json` |
| 12 | `redb-prescreen-trigger-delay-policy` | `contract/trigger-delay-policy.json` | `application/json` |
| 13 | `redb-prescreen-bounds` | `contract/bounds.json` | `application/json` |
| 14 | `redb-prescreen-protected-review-policy` | `contract/protected-review-policy.json` | `application/json` |
| 15 | `redb-4.1.0-crate-archive` | `subject/redb-4.1.0.crate` | `application/octet-stream` |
| 16 | `redb-prescreen-source` | `build/source.tar.zst` | `application/zstd` |
| 17 | `redb-prescreen-cargo-lock` | `build/Cargo.lock` | `text/plain; charset=utf-8` |
| 18 | `redb-prescreen-sbom` | `build/sbom.spdx.json` | `application/spdx+json` |
| 19 | `redb-prescreen-build-manifest` | `build/build-manifest.json` | `application/json` |
| 20 | `redb-prescreen-fixture-generator` | `build/redb-prescreen-fixture-generator` | `application/octet-stream` |
| 21 | `redb-prescreen-supervisor` | `build/redb-prescreen-supervisor` | `application/octet-stream` |
| 22 | `redb-prescreen-child` | `build/redb-prescreen-child` | `application/octet-stream` |
| 23 | `redb-prescreen-actuator` | `build/redb-prescreen-actuator` | `application/octet-stream` |
| 24 | `redb-prescreen-oracle` | `build/redb-prescreen-oracle` | `application/octet-stream` |
| 25 | `redb-prescreen-verifier` | `build/redb-prescreen-verifier` | `application/octet-stream` |
| 26 | `redb-prescreen-base-256m` | `fixtures/base-256m.redb` | `application/octet-stream` |
| 27 | `redb-prescreen-base-1g` | `fixtures/base-1g.redb` | `application/octet-stream` |
| 28 | `redb-prescreen-base-4g` | `fixtures/base-4g.redb` | `application/octet-stream` |

That exact 28-row layout remains the implemented Cycle 22 synthetic regression contract and is not
silently reinterpreted. The additive formal `state-backend-redb-prescreen-approval-payload/v2`
requires 29
common rows: rows 1--25 above, then `redb-prescreen-base-64m` at
`fixtures/base-64m.redb`, followed by the existing 256-MiB, 1-GiB and 4-GiB roles as rows 27--29.
The 64-MiB fixture role has a 1-GiB declared-byte cap, and the checked aggregate cap remains 20 GiB.
Both run classes bind the same 29 rows, so native prerequisite equality includes the exact 64-MiB
fixture descriptor. At Cycle 25 close no schema accepted that successor; the Cycle 26 slice below
adds copied-content conformance without execution or storage authority.

The 29 descriptor targets form one exact `approval_input_object_set` held in a provider-versioned,
immutable, read-only input root. Its objects—including all four physical bases—remain outside the
result retained set and its 2-GiB accounting; they are not deleted by result cleanup. Before
first-stage admission, a live verifier opens that exact version through pre-authorized handles,
rehashes every target against the payload descriptors, verifies enforced retention, and may return
only the opaque `APPROVAL_INPUT_STORAGE_VERSION_VERIFIED` capability. The future run-start embeds one
copied input-version binding; it is not a standalone packet object, has no role or locator, and adds
no separately indexed or retained artifact. That nested content binds the exact approval-payload
descriptor and its ordered 29 descriptors plus provider/store/version equality identifiers, but no
storage authority. Its exact provider fields and schema remain blocked until the live storage
provider/version/retention contract is selected. Admission binds the live capability and exact
version; post-run storage/provenance verification must reopen the same input version as well as the
separate result-evidence version. No nested binding, descriptor, receipt, provider version string or
`immutable=true` field can construct the capability.

The native-only prior-smoke descriptor is
`(redb-prescreen-reviewed-smoke-result, evidence/prior-smoke-result.json, length, sha256,
application/json)`. It remains outside both the legacy 28-row and formal common 29-row artifact
arrays because it does not exist before Docker smoke. The native payload binds it separately, and the
later semantic slice must prove its subject, build, schemas, goldens, 64-MiB fixture and Docker plan
equal the native payload before dispatch can be considered.

The receipt contains only its fixed fields below. It has no authenticated, approved, authorized,
executable, sealed, valid-signature, expiry or final-disposition field.

| JSON pointer | Type or exact value |
|---|---|
| `/schema_version` | `state-backend-redb-prescreen-protected-review-receipt/v1` |
| `/notice` | `NOT QUALIFICATION EVIDENCE` |
| `/stage` | `pre_run` or `post_run` |
| `/provider` | exactly the policy's `provider` object |
| `/change/change_id` | provider-scoped immutable ID |
| `/change/base_ref` | exactly the policy base ref |
| `/change/head_revision` | 40 lowercase hexadecimal characters |
| `/policy` | exact descriptor for `contract/protected-review-policy.json` |
| `/payload` | stage-specific payload descriptor |
| `/reviews` | exactly two ordered review objects |
| `/protected_execution/workflow_file` | exactly the policy workflow file |
| `/protected_execution/job_name` | exactly the policy job name |
| `/protected_execution/environment_id` | exactly the policy environment ID |
| `/protected_execution/workflow_run_id` | provider-scoped ID |
| `/protected_execution/workflow_run_attempt` | JSON integer from 1 through `u64::MAX` |
| `/protected_execution/workflow_job_id` | provider-scoped ID |
| `/protected_execution/provider_verified_at_utc` | canonical UTC timestamp |
| `/retained_evidence` | pre-run: `null`; post-run: retained-evidence content root |

The pre-run `/payload` tuple is
`(redb-prescreen-approval-payload, approval/payload.json, length, sha256, application/json)`; the
post-run tuple substitutes role `redb-prescreen-result-payload` and locator `result/payload.json`.
The `/policy` tuple uses role `redb-prescreen-protected-review-policy`, its fixed locator, and
`application/json`. Both bind the exact supplied bytes at the applicable cap. Each ordered review
object has exactly `{role, stable_account_id, review_event_id, provider_state, decision_literal,
reviewed_head_revision, reviewed_payload_byte_length, reviewed_payload_sha256, reviewed_at_utc}`.
The roles are workload then operations, `provider_state` is exactly `APPROVED`, the stage selects
the exact decision literal, and each reviewed head and payload binding equals the receipt's change
and payload descriptor. `stable_account_id[0] != stable_account_id[1]` and
`review_event_id[0] != review_event_id[1]`; the two identifier namespaces are not compared. Both
review times are no later than `/protected_execution/provider_verified_at_utc`.

The trusted dispatcher accepts a provider review body only when its exact UTF-8 bytes are
`<decision_literal>\npayload_byte_length=<canonical unsigned decimal>\npayload_sha256=<lowercase
digest>\n`. It obtains those bytes, event state, event ID, account ID, commit ID and submission time
from the live provider, then resolves current team membership separately. This makes both owner
events bind the payload bytes rather than merely a related change. Local validation checks only
that the receipt's two copied bindings equal the exact supplied payload; it cannot authenticate the
review body or event.

For pre-run content validation, `/provider`, `/change/base_ref` and the three configured protected
execution fields equal the policy; the payload's descriptor 14 equals both the receipt policy
descriptor and the exact policy bytes; and the receipt payload descriptor equals the exact approval
payload bytes. The payload deliberately repeats no provider or execution identity. Post-run
equality with result content remains part of the later result-payload slice.

The post-run `/retained_evidence` content root has exactly `{kind, artifact_index}`. `kind` is
`state-backend-redb-prescreen-retained-evidence-root/v1`, and `artifact_index` is the exact
`(redb-prescreen-artifact-index, result/artifact-index.json, length, sha256, application/json)`
descriptor with length from 1 through 16 MiB. The exact artifact-index bytes are the root byte
sequence; its descriptor length and SHA-256 are the root length and digest, so there is no separate
container, archive or root-hash algorithm. The retained index excludes itself,
`result/payload.json`, and `result/protected-review.json`; it lists every other retained artifact in
canonical locator order. This is content identity only. It contains no store, URL, key, version,
retention or `immutable=true` claim. The later post-run slice must define and bind a separately
trusted storage-version/retention record before result sealing can exist. Cycle 22 does not create
or accept a post-run result.

Bootstrap limits are compiled into the validator and never read from packet content: policy JSON
32 KiB, approval payload JSON 64 KiB, result payload JSON 256 KiB, receipt JSON 64 KiB, JSON nesting
depth 16, at most 4,096 decoded nodes per input, provider-scoped IDs at most 256 ASCII bytes,
canonical UTC timestamps exactly 20 ASCII bytes, locators at most 192 UTF-8 bytes, two reviews and
28 pre-run descriptors. Each non-fixture descriptor is at most 256 MiB; the 256-MiB, 1-GiB and
4-GiB fixture roles are capped at 1, 4 and 12 GiB respectively; checked aggregate declared bytes
are at most 20 GiB. The future classifier first applies the conservative Cycle 24 retained-cap
formula, and the later storage verifier checks the exact actual closure total at most 2 GiB.
Declared lengths never drive
allocation in this validation slice. Duplicate keys, unknown fields, placeholders, non-`u64` JSON
numbers, all-zero/uppercase hashes, traversal, backslashes, overflow, stage swaps, wrong ordering
or any authority/PKI/provider-endpoint field fail closed.

A decoded node is each JSON object, array or scalar value including the root; member names do not
count. Root depth is one and every contained value adds one. `protocol ID` means 1..128 ASCII bytes
matching `^[a-z0-9][a-z0-9._/-]{0,127}$`. `provider-scoped ID` means 1..256 ASCII bytes matching
`^[A-Za-z0-9][A-Za-z0-9._:=/-]{0,255}$`. A canonical UTC timestamp is exactly 20 ASCII bytes,
matches `YYYY-MM-DDTHH:MM:SSZ`, satisfies JSON Schema `date-time`, uses seconds `00..59`, and has a
real Gregorian calendar date. Fixed UTC second precision makes chronological comparison bytewise.
The 20-GiB aggregate covers the 28 approval descriptors and a non-null prior-smoke descriptor.
Schema files themselves must pass the Draft 2020-12 meta-schema before their instances are
accepted.

The Cycle 22 API consumes the three exact JSON byte strings directly. Its success value has a
single-variant `Unverified` authority type and false execution/result-sealing accessors; the CLI
prints only content-valid, ineligible and authorization-unverified vocabulary. Exit success means
valid bounded content, not approval, provider verification, dispatch eligibility, a redb finding,
backend selection or production evidence. A race-safe formal packet reader remains a later security
boundary requiring handle-relative no-follow opens and file-identity checks; this bytes-only API
must not be relabelled as that reader.

The sole command is
`state-backend-qual validate-redb-prescreen-pre-run-content <policy-json-path>
<approval-payload-json-path> <receipt-json-path>`. Success exits 0, writes the repository notice then
`VALID_INELIGIBLE_REDB_PRESCREEN_CONTENT stage=pre_run payload=<payload_id>
authorization=authorization_unverified` to stdout, and writes no stderr. Invalid content exits 2 with the notice
on stdout and an `INVALID_REDB_PRESCREEN_CONTENT ` stderr prefix. An unreadable input also exits 2
with the existing `INVALID_INPUT <path>: ` prefix. Wrong arity or any unknown command exits 64 and
prints usage. No output or public type contains `DEFER`, `DOCKER_SMOKE_PASS`,
`DOCKER_SMOKE_INCOMPLETE`, `PRESCREEN_PASS`, `PRESCREEN_NO_GO`, `REJECT_EXACT_PIN`,
provider-authenticated, executable or sealed state.

### Cycle 23 outer post-run binding slice

A strict successor result validator is not yet implementable. The protocol has not frozen the exact
result-payload layout, Docker outcome vocabulary, retained-index entry/destruction model, raw wire
schemas and goldens, classifier inputs, or immutable storage-version and retention contract. The
legacy `state-backend-redb-prescreen-result/v1` schema is synthetic regression input and supplies no
defaults. Cycle 23 therefore validates only the outer post-run receipt and exact byte bindings. It
does not implement `state-backend-redb-prescreen-result-payload/v1` or an artifact-index schema.

The binding API consumes these six caller-supplied byte strings in order:

1. protected-review policy, at most 32 KiB;
2. approval payload, at most 64 KiB;
3. pre-run protected-review receipt, at most 64 KiB;
4. opaque result-payload bytes, from 1 byte through 256 KiB;
5. opaque artifact-index bytes, from 1 byte through 16 MiB; and
6. post-run protected-review receipt, at most 64 KiB.

It first performs the complete Cycle 22 pre-run content validation. It then validates the post-run
receipt against the closed receipt schema and requires all of the following:

- `stage` is `post_run`; provider, policy descriptor, base ref, and configured workflow, job and
  environment equal the supplied policy, and the policy descriptor binds the exact policy bytes;
- the complete post-run `/change` object equals the pre-run receipt's `/change` object, and the
  post-run provider object equals both the policy and pre-run provider objects;
- the post-run payload descriptor is exactly
  `(redb-prescreen-result-payload,result/payload.json,actual length,actual SHA-256,application/json)`;
- the retained-evidence root has the fixed kind and its index descriptor is exactly
  `(redb-prescreen-artifact-index,result/artifact-index.json,actual length,actual SHA-256,application/json)`;
- the two ordered copied review records use the exact post-run decision literal, bind the post-run
  change head and exact result bytes, have unequal copied account-ID strings and unequal event-ID
  strings, and use canonical times no later than the post-run provider-verification time;
- neither copied post-run event-ID string appears in the pre-run receipt; owner account strings may
  repeat across stages; and
- each copied post-run review time is no earlier than the pre-run provider-verification time. This
  also requires the post-run provider-verification time to be no earlier than the pre-run one.

String inequality proves only inequality of copied strings. It does not prove that an account,
event, review, protected run or provider fact exists, that two real principals differ, or that a
review is current. Live provider verification remains a separate unavailable authority boundary.
The post-run workflow run/attempt/job IDs are not required to equal their pre-run values: they
identify the copied export context, while only the configured workflow/job/environment lineage and
change/head are fixed across stages.

The result and index bytes are deliberately opaque in this slice. They are bounded and hashed but
are not decoded as JSON, checked against a schema, dereferenced, or interpreted. Therefore success
does not establish that the bytes conform to the copied `application/json` media-type claim, or
establish their notice, identity, disposition, entry set, locator ordering, entry existence, digests,
destruction records, `retained_artifact_bytes`, final 2-GiB accounting, classification, storage,
retention, or sealing. A fully self-consistent copied and repinned chain can pass only as unverified
binding content. The later strict result/index slice must consume exact bytes, define and check those
facts, and still remain separate from live provider and storage trust.

The public result is `RedbPrescreenPostRunBindingSummary` with only the single-variant
`RedbPrescreenAuthorization::Unverified` authority. Its execution and result-sealing accessors
return false unconditionally. It has no disposition, run class, payload ID, reviewer, event,
provider, artifact root, storage, retention, backend-selection or qualification field and no
conversion into a trusted type.

The sole Cycle 23 command is
`state-backend-qual validate-redb-prescreen-post-run-binding <policy-json-path>
<approval-payload-json-path> <pre-run-receipt-json-path> <opaque-result-payload-path>
<opaque-artifact-index-path> <post-run-receipt-json-path>`. Success exits 0 and writes the notice,
then exactly:

```text
VALID_INELIGIBLE_REDB_PRESCREEN_BINDING stage=post_run authorization=authorization_unverified
```

Invalid binding content exits 2 with `INVALID_REDB_PRESCREEN_BINDING ` on stderr. An unreadable or
over-cap input exits 2 through the existing `INVALID_INPUT <path>: ` path. Wrong arity and every
unknown command exit 64. No `run`, `dispatch`, `approve`, `accept`, `verify`, `classify`, `seal`,
`select` or `qualify` command is added.

At Cycle 23 close, the strict successor remained blocked on the result/finalization and cleanup
hierarchies. Cycle 24 freezes those semantic boundaries below. Schema or classifier implementation
is still blocked on the enumerated exact-wire, evidence-matrix and trusted-provider decisions.

### Cycle 24 result, cleanup and finalization contract

Native result finalization has one operational stop state, one derived content outcome, one local
copied binding state, three post-run live verification capabilities and one registry-held final
state. These names are disjoint and cannot be substituted for one another; the earlier Docker and
native dispatch capabilities are separate pre-run gates.

| Plane | Exact state or value | Authority and effect |
|---|---|---|
| native runtime stop | `TERMINAL_CORRECTNESS_STOP_LATCHED` | Campaign-scoped irreversible stop triggered only by one Cycle 25 validated eligible small-crash finding; authorizes only stopping all remaining candidate/diagnostic execution and closing evidence |
| classifier content | `derived_outcome` = `PRESCREEN_PASS`, `PRESCREEN_NO_GO`, `DEFER`, or `REJECT_EXACT_PIN` | Deterministic redb-free classification of the closed native evidence, before post-run owner review |
| copied review content | `POST_RUN_REVIEW_BINDINGS_COPIED_UNVERIFIED` | Cycle 23 local equality/chronology result; grants no provider authority |
| live run authority | `NATIVE_RUN_PROVENANCE_VERIFIED` | Opaque capability binding the protected runner and exact authorized native execution to the stored manifest/index/payload/receipt bytes |
| live review authority | `POST_RUN_REVIEWS_PROVIDER_VERIFIED` | Opaque capability binding the exact stored receipt and its copied events to current live provider facts and the exact frozen payload |
| live storage authority | `EVIDENCE_STORAGE_VERSION_VERIFIED` | Opaque capability covering the exact immutable approval-input version and the separate exact result-evidence version, with enforced retention for both |
| final native result | `FINAL_PRESCREEN_RESULT_SEALED` | Registry-held state only; its outcome must equal an independently recomputed `derived_outcome` over the exact stored closure |

`TERMINAL_CORRECTNESS_STOP_LATCHED` is never a disposition or a seal. It is not representable by
deserializing result content and cannot be raised by a candidate child. If evidence-close,
cleanup-report or final-index validation before payload freeze cannot validate all evidence required
for the finding, the classifier derives `DEFER`; the historical fact that the campaign stopped
remains unchanged. A cleanup failure also derives `DEFER` for an otherwise pass/no-go campaign. It
may coexist with `REJECT_EXACT_PIN` only when the latched correctness proof remains complete,
attributable and retained despite that failure. After payload freeze, evidence mutation or loss
invalidates the chain and blocks finalization; it never silently rewrites that frozen outcome.

The frozen campaign dependency order has two phases. The five-hour native watchdog covers only the
local campaign phase:

1. finish the campaign or latch the terminal correctness stop, stop every candidate writer and
   close all campaign measurement, oracle and report producers;
2. durably close the evidence set, then perform crash-recoverable cleanup and close its report;
3. construct and independently validate the final artifact index;
4. run the redb-free classifier and freeze a result payload carrying exactly one `derived_outcome`.

The asynchronous governance phase then:

5. obtains the two owner review events over those exact payload bytes and exports the local post-run
   receipt, which remains `POST_RUN_REVIEW_BINDINGS_COPIED_UNVERIFIED`;
6. atomically publishes the exact retained result closure, index, payload and receipt to its approved
   immutable result store, reopens the exact approval-input version used at admission, and obtains
   `EVIDENCE_STORAGE_VERSION_VERIFIED` from a live verifier over that exact version pair;
7. reruns the strict redb-free result/index/artifact verifier and classifier over the exact immutable
   approval-input/result-evidence version pair and requires the recomputed outcome and every
   payload/index/receipt digest to match;
8. obtains `NATIVE_RUN_PROVENANCE_VERIFIED` by verifying the live protected-run identity, the
   admission gate and either exact child-dispatch consumption for an attempted run or authenticated
   non-issuance/non-consumption matching a no-candidate cut, consumption of the exact Docker-
   prerequisite capability and approval-input version, subject, source/build/target and supervisor
   execution against the raw manifest and exact stored
   index/payload/receipt hashes; then re-reads the live review provider to obtain
   `POST_RUN_REVIEWS_PROVIDER_VERIFIED`; and
9. immediately has the trusted registry atomically consume the semantic result and all three live
   capabilities and enter `FINAL_PRESCREEN_RESULT_SEALED` for that exact storage-version pair.

Missing, refused, dismissed or stale owner review, provider drift, failed storage verification,
failed provenance or semantic revalidation, or registry failure blocks finalization. None rewrites a
valid already-derived candidate outcome to `DEFER`. `POST_RUN_REVIEWS_PROVIDER_VERIFIED` must bind
the exact stored receipt length/hash, policy, change/head, payload length/hash, copied event
IDs/bodies/states/times, current distinct non-self role membership, and verifier/protected-workflow
identity. The registry must follow the exact descriptor chain and require one identical manifest,
   input version, index, payload, receipt and retained result object set across semantic validation,
   run provenance, storage authority and the review receipt/capability as applicable. Provider
   storage-version identities are authenticated only by the storage capability and registry, never
   by the redb-free semantic verifier.

The exact provider point-in-time and TOCTOU linearization, capability freshness/lifetime and retry
rules, immutable-storage provider/version/atomic-publish/object-set/retention/admin-bypass proof, and
registry authority, idempotency/conflict and exported-proof semantics are not yet selected. The
three capabilities must eventually be non-serializable, digest-bound, freshness-bounded, single-use
within one finalization attempt; a retry must reverify every live fact. Until those exact rules are
frozen, `FINAL_PRESCREEN_RESULT_SEALED` is unconstructible. Any exported or copied final record is
unverified content until an approved live or cryptographic registry verifier proves it. No packet-
supplied field, digest, URL, ETag, version string, timestamp, approval word or `immutable=true`
value can mint a trusted capability or registry state, and no conversion from the Cycle 22/23
content summaries to a trusted type is permitted.

The indexed `result/validator-report.json` validates the pre-close campaign evidence only. The
step-7 strict verifier runs after immutable closure and records its result in the trusted registry,
not as another indexed packet object; this avoids a final-index/result self-hash cycle.

#### Evidence close and cleanup hierarchy

Let `R` be the exact run-class-specific set of retained result-artifact descriptors that exists after
all applicable evidence producers and independent reports have closed but before transient database
deletion. It excludes the separately immutable approval-input object set and all transient database
copies. Each
descriptor remains exactly `{role, locator, byte_length, sha256, media_type}`. Let `M`, `J`, `C` and
`F` denote the exact evidence-close-manifest, final cleanup-journal, cleanup-report and final-index
byte strings respectively.

1. `result/evidence-close-manifest.json` lists `R`, including the raw run manifest and exact
   validator and oracle envelopes plus, for native only, the mechanism-probe envelope; every
   applicable envelope is present exactly once even when its status is `not_run` or `incomplete`. It
   also stores the Cycle 25 expected-leaf-set digest and full
   retained-valid/retained-invalid/unavailable reconciliation, and lists the complete transient
   database cleanup set with stable identities, workspace locators,
   `post_scan_file_byte_length`, and required pre-cleanup logical and file digests. The manifest is
   written durably, reopened and rehashed before cleanup. It is a content precondition, never bearer
   authority: only the trusted supervisor's already-held, pre-authorized handle-relative scratch-root
   capability can select deletion targets. Manifest identities and locators are equality constraints
   and cannot redirect deletion.
2. `result/cleanup-journal.bin` is initialized only after that check. Its first durable frame binds
   the campaign identity and exact `(role, locator, byte_length, sha256, media_type)` descriptor for
   `M`. Before each destructive action it durably records intent; after the action it durably records
   either verified destruction or a closed failure observation. Recovery first revalidates that
   header against the exact reopened manifest and supervisor-owned handles, then resumes without
   silently omitting an interrupted database.
3. `result/cleanup-report.json` binds the exact close-manifest and final journal bytes and is a
   deterministic projection with exactly one terminal cleanup record per database in the close
   manifest. Its records, rather than duplicated index entries, carry the per-database intent,
   completion/failure and absence evidence.
4. `result/artifact-index.json` is generated after the journal and report close. Its retained entry
   set is exactly `R` plus the descriptors of the evidence-close manifest, final cleanup journal and
   cleanup report. It excludes only itself, `result/payload.json` and
   `result/protected-review.json`.

The final index stores checked summary counts, not a second copy of the cleanup records. At minimum
they are `retained_artifact_count`, `retained_artifact_bytes`, `database_to_clean_count`,
`database_cleanup_record_count`, `destroyed_database_count`, `cleanup_failure_count` and
`database_bytes_to_clean`. Validation requires all of the following:

- artifact entries are strictly ordered by raw UTF-8 locator bytes; locators are globally unique,
  `(role, locator)` pairs are unique, and equal content digests are allowed;
- every applicable indexed retained singleton role occurs exactly once and every retained repeatable
  role has an explicit closed cardinality; separately, every Cycle 25 expected leaf occurs exactly
  once across retained-valid, retained-invalid and unavailable reconciliation entries; missing
  entries add no retained count or bytes, and checked counts equal the applicable lengths and sums;
- the cleanup-report database set equals the close-manifest cleanup set, with exactly one terminal
  `destroyed` or `failed` record per identity, so destroyed and failed records partition that set;
- the index summary counts are recomputed from the bound report; missing or unresolved cleanup is
  never represented as a smaller set;
- `retained_artifact_count = |R| + 3` and `retained_artifact_bytes = checked_sum(R.byte_length) +
  byte_length(M) + byte_length(J) + byte_length(C)`; transient database bytes are excluded from
  that retained total;
- `database_cleanup_record_count = database_to_clean_count = destroyed_database_count +
  cleanup_failure_count`, and `database_bytes_to_clean` is the checked sum of
  `post_scan_file_byte_length` across the close-manifest database set; and
- no indexed artifact changes after final-index publication. Any byte change invalidates the
  index, derived payload, review receipt and later storage closure.

The classifier checks the retained cap before the result and receipt exist by reserving both
maximum future sizes rather than creating a result-size fixed point:

```text
retained_artifact_bytes
+ actual_final_index_byte_length
+ 256 KiB maximum result-payload bytes
+ 64 KiB maximum post-run receipt bytes
<= 2 GiB
```

The live storage verifier later checks the exact actual closure total: all index-listed object
bytes plus the actual final-index, result-payload and post-run-receipt bytes, again at most 2 GiB.

This lifecycle freezes the hash DAG and removes the old pre/post-cleanup index circularity; it does
not yet authorize implementation. The exact evidence-close durability primitive, cleanup-journal
wire/goldens and recovery protocol, scratch-root identity, no-follow deletion, parent-directory
durability, absence proof, retry/deadline/failure codes, database digest cuts, artifact-role
registry/cardinalities and numeric entry/database caps remain freeze blockers. Cycle 25 below freezes
the semantic raw-manifest, terminal-control, conditional-evidence and constructible-`DEFER`
boundaries; their exact schemas, binary framing and literal goldens remain undefined. The exact
Docker/native dispatch-capability and Docker-prerequisite
freshness, single-use hand-off, live Docker run-provenance/replay/TOCTOU verifier and trusted-
consumption rules are likewise unfrozen; none of the four admission/child-dispatch capabilities, the
approval-input storage capability or the Docker-prerequisite capability is currently constructible.

#### Docker smoke result is a separate type

Cycle 25 froze the exact semantic matrix for a then-planned Docker Desktop/WSL result with
`result_kind = docker_smoke_prerequisite` and `smoke_outcome = DOCKER_SMOKE_PASS` or
`DOCKER_SMOKE_INCOMPLETE`. A pass has no incomplete reason; an incomplete result has at least one
closed reason code. Cycle 25 below freezes the exact smoke population, reason vocabulary and
conditional evidence matrix; no implementing schema exists. Cycle 30 supersedes Desktop/WSL as an
eligible producer: these semantics remain superseded, unimplemented design-history/reference
contracts and no current dispatcher can emit either formal outcome. There are no literal
target/preflight/result fixtures to promote. A future native-Linux successor must version every
affected identity rather than reinterpret this producer identity.

Docker content has no native `derived_outcome`, `PRESCREEN_*` outcome, disposition,
`TERMINAL_CORRECTNESS_STOP_LATCHED`, `REJECT_EXACT_PIN` or `FINAL_PRESCREEN_RESULT_SEALED` state. A
smoke failure may retain diagnostics only as `DOCKER_SMOKE_INCOMPLETE`; it cannot reject or select
a backend. Only a trusted verifier may return
`DOCKER_SMOKE_PREREQUISITE_VERIFIED_NO_DECISION`, after proving a non-synthetic
`DOCKER_SMOKE_PASS`; identical subject/source/build/child/schemas/goldens/plan and 64-MiB fixture;
the exact stored Docker run-start, launch ledger, control, raw manifest, report, evidence-close,
cleanup, index,
result and receipt closure; current live owner review and immutable-storage version; live protected
Docker run identity; and consumption of the exact admission and child-dispatch capabilities by that
run. That opaque capability
permits consideration for a separately approved native dispatch and supplies no native result,
qualification or production evidence.

### Cycle 25 run binding, terminal stop and incomplete-evidence contract

Cycle 25 freezes semantic construction dependencies only. It adds no schema, wire, dispatcher,
supervisor, deletion path or execution command. The legacy synthetic result schema and existing
runner wires supply no defaults.

#### Acyclic run roots

A final raw-run manifest cannot retrospectively identify streams that did not already share an
immutable run binding. The construction graph uses descriptive object names to avoid overloading the
timestamp symbols used later:

```text
approval roots + actual target/preflight
  -> run_start
  -> native campaign_control + raw leaves (+ conditional terminal_finding)
     or Docker launch_ledger + raw leaves + docker_control
  -> raw_manifest
  -> validator/oracle/(native mechanism) envelopes
  -> evidence_close -> cleanup_journal -> cleanup_report -> final_index
  -> result payload -> post-run receipt
```

The closed `pre_start_evidence` set is the approval payload and receipt, the prior Docker result for
native, and the actual-target and preflight-cut envelopes. Those objects necessarily precede
`result/run-start-binding.json` and do not bind it. The run-start object descriptor-binds each one,
which prevents a `run_start -> preflight -> run_start` cycle.

The run-start object has reserved identity
`state-backend-redb-prescreen-run-start-binding/v1`. It is strict UTF-8 JSON and binds the exact
approval-payload and pre-run-receipt descriptors, conditional prior-Docker-result descriptor,
protocol/run class/payload ID, copied change/head and protected workflow/job/environment/run-attempt
context, expected admission and child-capability kinds, approved schedule and component-binary
descriptors, actual-target and preflight-cut descriptors, Linux boot ID, dispatcher audit-event
identity, `CLOCK_MONOTONIC_RAW` origin, and the all-false evidence scope. It contains no child PID,
outcome, authorization, authentication, capability-consumption, review, storage or seal claim.

The exact file bytes—not reserialized or nominally canonical JSON—are the hashed run binding. Strict
parsing rejects duplicate keys, trailing data, bad UTF-8, excessive depth/nodes, non-`u64` numbers
and noncanonical identifiers. The second-stage dispatcher reopens and hashes those bytes before it
can mint a child-dispatch capability. Failed/incomplete preflight or unavailable second-stage
authority may still produce a stage-one-admitted no-candidate `DEFER` closure; neither permits a
candidate process launch.

Every run-produced binary leaf needs a new redb-specific wire version whose header contains the exact
32-byte SHA-256 of the run-start bytes; every run-produced JSON leaf binds the exact full run-start
descriptor. Existing latency/resource wires lack that run binding, and existing mechanism wires bind
only their mapping/profile. They remain reference designs and are ineligible redb evidence without
additive versioned headers and new literal goldens.

The raw-run manifest has reserved identity
`state-backend-redb-prescreen-raw-run-manifest/v1`. It binds the exact run-start descriptor in one
dedicated field; separately lists `pre_start_evidence` and stable run-produced raw descriptors; binds
the supervisor, child, actuator, opener/scanner and clean-control process tuples; records the complete
schedule ledger; and carries one content-only campaign cut. The run-start descriptor is not duplicated
in either array. Each descriptor array is strictly ordered by raw UTF-8 locator bytes, locators are
globally unique across both arrays and the dedicated run-start field, and `(role, locator)` pairs are
unique. Native campaign-control and present terminal-finding bytes, or the Docker control object, are
run-produced raw descriptors. The Docker launch ledger is also a mandatory run-produced raw
descriptor. Reports and all evidence-close, cleanup, index, result, review, storage and finalization
objects are excluded. The manifest does not bind itself and contains no outcome.

Process identity is never PID alone. The future wire must include Linux boot ID, PID-namespace
identity, PID, `/proc` start ticks, cgroup identity and exact executable digest. Empty process arrays
are legal only when the schedule and control evidence prove that process class never started.

The native schedule ledger contains exactly 105 approved rows in this dynamic dispatch order: 36
steady rows, 54 small-crash baseline rows, the six reserved rows
`i1/n0, i1/n1, i2/n0, i2/n1, qr/n0, qr/n1`, then nine large-recovery rows. Each row is exactly
`attempted`, `unused_reserved`, or `not_attempted`. `attempted` means the durable release intent for
the slot's first database-opening process was reached, including ambiguous delivery after that cut;
a separate completion field is `complete` or
`incomplete`. `unused_reserved` is legal only after that row's activation predicate was evaluated
false because its mode already had the required confirmed-in-commit coverage. A reserved row reached
only after an earlier cut is `not_attempted`, and a baseline row is never `unused_reserved`.

Validation requires checked
`105 = attempted_count + unused_reserved_count + not_attempted_count`, contiguous
`slot_attempt_ordinal` values from zero for attempted rows, and no reuse or rerun. After
`unused_reserved` rows are removed, attempted rows form a prefix and `not_attempted` rows the suffix.
Each not-attempted row binds the exact final campaign-control stop frame. Slot IDs use lowercase
`i1`, `i2` and `qr`. The native cut is exactly
`candidate_not_started`, `partial_nonterminal`, `terminal_correctness_incomplete`,
`terminal_correctness`, or `campaign_complete`; it is a checked projection of the final control frame,
not a free submitted value.

#### Native campaign control and terminal finding

`result/campaign-control.bin` is mandatory for every stage-one-admitted native run. It has reserved
identity `state-backend-redb-prescreen-campaign-control/v1` and is created through
`NATIVE_CAMPAIGN_ROOT_LEASE` after the exact run-start bytes exist. Only the narrowed control lease
may lend a scoped writer to the supervisor, and this occurs before any candidate-affecting launch. It
is append-only, single-writer, sequence-numbered and hash-chained. Every frame
binds the run-start SHA-256, campaign identity, contiguous sequence, exact prior-frame hash (all zero
only for `CAMPAIGN_OPENED`), a monotonic-RAW offset and one closed frame kind. No frame binds the
future raw manifest.

The journal and outer-runner process registry jointly own a dormant-process release handshake. Before
every candidate-affecting process, the supervisor durably records `PROCESS_START_INTENT` with schedule
row, contiguous `process_intent_sequence`, process role, executable digest and dedicated cgroup. The
outer launch broker
then spawns only an inert bootstrap in that cgroup, retains its pidfd, verifies its full identity and
prevents any database open or actuation. The supervisor durably records `PROCESS_ARMED` with that
identity before it may record `PROCESS_RELEASE_INTENT` with the next contiguous
`process_release_ordinal` and ask the broker to release the one-shot start barrier. The first database-
opening release intent for a row additionally assigns the next contiguous `slot_attempt_ordinal`;
every later actuator/opener/scanner/control process for that row binds the same slot ordinal and its
own process-release ordinal. Release delivery/acknowledgement and pidfd/`waitid` exit are later frames. An
unresolved release intent is conservatively attempted; a bootstrap never armed or released is not.
Slot-start, slot-close and reserved-predicate decisions are also frames.

Recovery resolves every start intent against the outer registry and exact dedicated cgroup, kills
any armed or ambiguously released process, and proves the cgroup contains no unaccounted process
before `STOP_ACKNOWLEDGED` or `EVIDENCE_CUT`. Thus a worker crash cannot leave live candidate or
actuator code outside the journal/process set. Final ordinary stops are `CANDIDATE_NOT_STARTED` or
`PARTIAL_NONTERMINAL_STOP` with a closed cause and evidence binding; normal completion is
`CAMPAIGN_COMPLETED`. Every classifiable journal ends with
`EVIDENCE_CUT`, which binds the final 105-row vector and digest, checked counts, final process/exit
set, checked contiguous process-intent/process-release/slot-attempt sequence counts, stop-frame hash
and campaign cut. The raw manifest is a checked projection of that final frame.

`CANDIDATE_NOT_STARTED` permits exactly `preflight_failed`, `preflight_incomplete`,
`child_dispatch_authorization_unavailable`, `candidate_bootstrap_failed` or
`pre_candidate_safety_bound`.
`PARTIAL_NONTERMINAL_STOP` permits exactly `attempt_incomplete_or_timeout`,
`candidate_error_or_nonreturn`, `candidate_bootstrap_failed`, `target_or_environment_invalid`,
`actuation_invalid`,
`fixture_intent_or_schedule_invalid`, `raw_evidence_incomplete`,
`harness_or_attempt_oracle_invalid`, `safety_bound_reached`, `supervisor_restart`,
`excluded_row_correctness_anomaly` or `terminal_validation_or_pending_persist_failed`. The stop frame
binds the exact evidence that makes its cause legal; no free-form reason is accepted.
`candidate_bootstrap_failed` is required if and only if an inert bootstrap fails before release:
`CANDIDATE_NOT_STARTED` uses it when zero rows are attempted, and `PARTIAL_NONTERMINAL_STOP` uses it
after any slot attempt. `harness_or_attempt_oracle_invalid` excludes bootstrap failures.

At campaign level only one slot may be active. Within that slot at most one released database-opening
process may be live at an instant; the transaction child, reopen/scanner and large-recovery clean-
control instances remain sequential and may all be required for the same slot. Only that slot's
associated actuator may overlap the transaction child as the approved trigger requires; `W1`/`W2`
lanes remain threads inside one child. No next slot may start until every process, scan, attempt-level
oracle check and
terminal check for the prior slot has closed. Missing, malformed, unchainable, torn or non-final
campaign-control bytes make the closure unfinalizable; the journal cannot describe its own failure as
`DEFER`.

The terminal-finding object is strict, content-only
`state-backend-redb-prescreen-terminal-finding/v1`. It binds the run-start and exact control-journal
prefix; redb archive, child and configuration; source schedule row, `slot_attempt_ordinal` and all
relevant `process_release_ordinal` values; fixture-
copy and post-prime old-state witness; target intent and expected old/new digests; final crash-marker
frame; pidfd signal/delivery/exit receipt; target/preflight/quiet-window evidence; canonical reopen/
scan observation; attempt-level independent-oracle witness; observed/committed RAW offsets; and the
all-false evidence scope.

Only small-crash rows `atomic/s<seed>/<i1|i2|qr>/t<1..6>` and legitimately activated
`atomic-extra/<i1|i2|qr>/n<0..1>` are terminal-latch eligible. Steady/HOLD, `recovery/*` and its clean
controls, Docker, and mechanism/resource/latency rows are excluded in v1. A correctness anomaly in an
excluded row therefore closes conservatively as nonterminal `DEFER` pending a separately frozen proof
contract; it cannot be promoted to a terminal code.

The verifier derives the allowed state set from final post-exit markers: before commit entry only
`complete_old` is legal; commit entered without candidate return permits `complete_old` or
`complete_new`; return or acknowledgement permits only `complete_new`. The code/observation
cross-product is exact:

- `RECOVERED_STATE_OUTSIDE_ALLOWED_SET` is legal if and only if the observation is one of
  `complete_old`, `complete_new`, `torn_mixed`, `unexpected_extra`, `unexpected_missing` or
  `noncanonical_duplicate` and is outside that marker-derived set;
- `CANDIDATE_CORRUPTION_ERROR_ON_VALID_REOPEN_OR_SCAN` is legal if and only if the observation is
  exactly `candidate_corruption_error`; and
- `CANDIDATE_REOPEN_OR_SCAN_PANIC_ON_VALID_INPUT` is legal if and only if the observation is exactly
  `candidate_panic` in the candidate reopen/scanner process after valid actuation, with valid-input
  and panic-origin proof.

Any other pairing fails validation. Generic open/scan timeout or error is not corruption.

Latch prerequisites are conjunctive: exact pin/build/configuration and eligible row; verified fixture
copy and primed state; independently generated intent and old/new digests; valid target and quiet
interval; monotonic crash frame; correct child identity and pidfd-targeted `SIGKILL` with delivered/
exit evidence and no unwind/drop; valid trigger/final-marker phase; fresh canonical reopen of the
same file identity; complete all-table scan or a closed corruption/panic witness; and agreement
between the attempt-level redb-free oracle and a separate terminal verifier over durable, bounded,
hash-matched inputs.

That verifier deterministically serializes the only permitted terminal-finding byte vector and
returns `(ExactTerminalFindingBytes, ValidatedTerminalFinding)`. The private, non-deserializable,
non-clone, single-use token binds campaign/run-start hash, eligible row and ordinal, exact evidence
and control-prefix root, invariant code, and the exact finding byte length and SHA-256. Caller-
supplied, parsed or reserialized finding bytes cannot construct or satisfy the token. The only
terminal transition is:

1. the supervisor consumes the token in the first compare-and-swap
   `RUNNING -> STOP_PENDING`, closing the process-dispatch gate;
2. it appends and durably commits `TERMINAL_STOP_PENDING`, binding the token fields, exact finding
   length/hash, evidence root and schedule cut; this frame is the durable owner of conditional
   terminal-finding applicability;
3. it publishes only `ExactTerminalFindingBytes` with create-new/no-follow semantics, syncs file and
   parent, reopens it and requires the token's exact length/hash;
4. it appends and durably commits `TERMINAL_STOP_LATCHED`, binding the pending-frame hash and reopened
   finding descriptor; that commit is the sole linearization point for
   `TERMINAL_CORRECTNESS_STOP_LATCHED`;
5. it forbids every later process-start frame, reconciles every outer-registry start intent, observes
   exit for every released candidate-affecting process including the actuator, proves the exact
   dedicated cgroup empty with no unaccounted process, zero candidate database handles/in-flight
   operations and schedule conservation, then appends and durably commits `STOP_ACKNOWLEDGED` binding
   the exact latch-frame hash and final event/status-vector root; and
6. it appends the final `EVIDENCE_CUT` and proceeds only to reports, evidence close and cleanup.

There is no unlatch, second finding, candidate rerun, diagnostic candidate open or post-cut launch.
Failure after the in-memory gate closes but before `TERMINAL_STOP_PENDING` is durable cannot claim a
terminal finding; a recoverer may append only a generic nonterminal stop and `DEFER` cut if the
remaining journal is valid. Once the pending frame is durable, the finding slot remains expected
regardless of the later cut. Failure to publish the finding, commit the latch or commit the
acknowledgement requires a durable `TERMINAL_STOP_INCOMPLETE` frame naming exactly
`finding_not_published`, `latch_not_persisted` or `acknowledgement_not_persisted`, followed by an
`EVIDENCE_CUT` with `terminal_correctness_incomplete`. If those frames cannot be committed, there is
no outcome. A previously committed latch remains a historical latch even when acknowledgement fails;
only a complete acknowledgement can enable `REJECT_EXACT_PIN`.

Any restart after `CAMPAIGN_OPENED` is no-resume. Only `RecoveredClosureOnly` minted by the still-live
outer `NATIVE_CAMPAIGN_CONTROL_LEASE` after exact run-start/journal-prefix verification, prior-worker
exit, exclusive-root ownership and dedicated-cgroup reconciliation may stop/observe processes, append
a permitted stop/failure/cut frame, close evidence and clean up. It has no launch/open method. A pre-
latch prefix or parsed finding cannot recreate `ValidatedTerminalFinding`. A valid durable latch
narrows that closure capability to private `RecoveredStopOnly` with only stop, acknowledge and
closure methods. Packet/export copies of the control journal and finding are content, never bearer
authority.

No latch is legal for target noise, kernel I/O, a missed or ambiguous actuation, wrong PID or normal
exit, fixture/intent/schedule drift, ring/sample/log loss, spontaneous pre-actuation panic, generic
candidate error/non-return, safety or soft-gate failure, oracle/validator/harness disagreement,
cleanup failure, or review/storage/finalization failure. Those paths use the nonterminal/incomplete
matrix below.

#### Conditional evidence matrix

Every classifiable stage-one-admitted native run has the same structurally valid closure spine:
actual-target and preflight-cut envelopes, run-start, campaign-control, raw manifest, fixed validator,
oracle and mechanism envelopes, evidence-close manifest, cleanup journal/report, final index and
result payload. A missing or malformed spine object is no outcome, not a missing-evidence `DEFER`.

Each fixed report file is a supervisor-owned outer lifecycle envelope. It binds the exact run-start
and raw-manifest descriptors, the expected-leaf-set digest, intended producer executable, optional
producer process/exit receipt, and the canonically ordered exact descriptors of the manifest-listed
evidence and immutable approval-input objects the producer consumed. Inputs must be a subset of the
union of the manifest's pre-start/raw descriptors and the exact 29 descriptors in the run-start-bound
approval-input version. Reports cannot create new supporting objects after the manifest cut or bind
peer/future reports.

`producer_lifecycle` is exactly `not_started`, `failed`, or `completed`. `completed` requires one
strict nested producer report plus a producer process/exit receipt, all binding the same raw manifest
and input descriptors; only that form may contain metrics, status or verdict. `not_started` forbids
producer identity, nested report, metrics, verdict and fabricated zeros and binds the exact manifest-
listed control stop that prevented launch. `failed` requires the envelope's own producer process/exit
receipt but forbids nested report, metrics, status and verdict. The independent validator must be
`completed` and cover every expected leaf, with
an evidence verdict of `valid` or `invalid`; validator non-completion means no outcome. Oracle and
mechanism envelope status is `not_run`, `incomplete` or `complete`, derived from the lifecycle and
nested report. A complete oracle verdict is `no_violation` or `correctness_violation`. A terminal
correctness verdict additionally binds the exact terminal-finding descriptor and invariant code.
Oracle/mechanism producer failure may therefore close as `DEFER` without inventing observations, but
supervisor or independent-validator failure cannot.

The bounded `expected_leaf_set` is derived after the raw manifest closes from the approved run-class
role registry plus exact run-start, validated campaign-control ledger/cut and raw-manifest bytes. It
contains run-class static raw-leaf slots, the required raw leaves for each `attempted` row, and the
terminal-finding slot whenever a durable `TERMINAL_STOP_PENDING` exists. A `not_attempted` row creates
no candidate leaf slots. It excludes pre-start roots, run-start, campaign-control, raw manifest, all
report envelopes, evidence-close manifest, cleanup journal/report, final index, result payload and
post-run receipt. Thus it is an expected pre-evidence-close set and contains no self or future edge.

The evidence-close manifest durably owns the full reconciliation. For each expected leaf, its ordered
`expected_leaf_reconciliation` contains exactly one of:

- `retained_valid` with the actual stable descriptor present in both raw manifest and retained set,
  accepted by the validator;
- `retained_invalid` with that actual stable descriptor and one closed validator structural/binding
  error code; or
- `unavailable` with fixed role, locator and schedule/slot identity, no descriptor, and exactly
  `producer_failed_before_manifest_cut`, `stream_incomplete_or_bound_exceeded_before_manifest_cut`,
  or `object_absent_at_manifest_cut`.

A well-formed candidate correctness violation is `retained_valid`. Stable malformed bytes are
`retained_invalid` and must be retained; they cannot be hidden as unavailable. `unavailable` is legal
only when the raw manifest has no descriptor for that expected slot and no object exists at its
packet locator at the manifest cut after a bounded no-follow absence check. If the raw manifest lists
a descriptor and that object is later absent, unreadable, over-cap, identity-changing or byte-changing
before evidence close, the closure is unfinalizable. Producer staging bytes stay outside the packet
root until atomic publication and grant no descriptor. Both `retained_invalid` and `unavailable`
force `DEFER` and cannot satisfy a terminal proof.

The evidence-close manifest binds exact run-start, raw-manifest and report-envelope descriptors,
stores the expected-leaf-set digest and reconciliation, and lists the complete physically retained
descriptor set. The retained-valid/invalid descriptor set is exactly the intersection of expected
leaf slots and manifest-listed stable raw descriptors; unavailable entries are the remaining expected
leaves. Every report input belongs to the exact manifest-evidence/approval-input union above; approval-
input descriptors remain references into their separately verified immutable version and are not
copied into the retained result set. The retained result set
includes the exact approval payload and receipt, conditional prior-smoke result, actual target and
preflight envelopes, run-start, every manifest-listed raw object, raw manifest and report envelopes.
It excludes every target byte object in the separately immutable 29-object approval-input version and
every transient working database copy. Working copies occur exactly once in the manifest's cleanup
set; immutable approval-input bases never do. An unregistered extra object in the `approval/`,
`evidence/` or `result/` result-closure namespaces fails validation. The
final index still contains exactly that retained set plus evidence-close manifest, final cleanup
journal and cleanup report. Unavailable entries contribute neither retained count nor retained bytes.
Every applicable indexed retained singleton occurs exactly once; separately, every expected leaf
occurs exactly once across the retained-valid/retained-invalid/unavailable reconciliation.

| Campaign cut | Required conditional evidence | Allowed native outcome |
|---|---|---|
| no verified first-stage admission | no formal run-start, raw manifest or result | no outcome, never `DEFER` |
| `candidate_not_started` | exact `CANDIDATE_NOT_STARTED` control frame; every row `not_attempted`; no release/open/actuation frame (unreleased bootstrap failure frames only for that exact cause); oracle/mechanism `not_run`; validator completed | `DEFER` only |
| `partial_nonterminal` | exact attempted prefix after unused-reserved removal and explicit remainder; final `PARTIAL_NONTERMINAL_STOP`; no durable terminal-pending frame; report states bind that stop | `DEFER` only |
| `terminal_correctness_incomplete` | durable terminal-pending plus exact incomplete stage, retained or unavailable finding as applicable, explicit ledger remainder and historical latch state | `DEFER` only |
| `terminal_correctness` | exact finding, pending/latch/acknowledgement frames, target/actuator proof and explicit remainder; oracle complete with the same finding/code | `REJECT_EXACT_PIN` only if the entire proof remains valid and retained, otherwise `DEFER` |
| `campaign_complete` | every row reconciled; no omitted population; oracle complete | `PRESCREEN_PASS`, `PRESCREEN_NO_GO`, or `DEFER` under the existing precedence; mechanism complete is mandatory for pass/no-go |

Cleanup modifies this matrix rather than creating another result shape. Zero transient databases is
valid only with a header-only cleanup journal and zero-record cleanup report. Any `failed` cleanup
record forces `DEFER`
except that a complete retained terminal proof may preserve `REJECT_EXACT_PIN`. Missing or malformed
cleanup journal, cleanup report or final index makes the closure unfinalizable and is not a cleanup-
failure result. Loss or mutation after the evidence-close manifest of an object it calls retained, or
loss of a spine object before payload freeze, means no valid outcome. Loss after payload freeze leaves
the content outcome unchanged but blocks final sealing. `DEFER` is therefore a valid closed account
of an incomplete experiment, never an invalid closure describing itself.

#### Exact Docker smoke matrix

Docker reuses the run-start/raw-manifest binding pattern but has a separate tagged body. It has no
native campaign-control, mechanism report, terminal finding, terminal state or native derived
outcome. Each mandatory case uses a fresh byte-identical Docker-volume copy of the approved 64-MiB
base. The fixed-locator source remains read-only in the verified immutable approval-input version;
its hash and every case-copy hash are checked before candidate open, and the canonical logical digest
is independently scanned. Only case copies enter the transient cleanup set; the immutable source
never does.

The fixed seed is `2026072301` and the exact decision ledger is:

| Order | Exact slot ID |
|---:|---|
| 0 | `docker/s2026072301/txn/i1` |
| 1 | `docker/s2026072301/txn/i2` |
| 2 | `docker/s2026072301/txn/qr` |
| 3 | `docker/s2026072301/hold/i1` |
| 4 | `docker/s2026072301/atomic/i1/t1` |
| 5 | `docker/s2026072301/atomic/i2/t2` |
| 6 | `docker/s2026072301/atomic/qr/t3` |
| 7 | `docker/s2026072301/atomic/i1/t4` |
| 8 | `docker/s2026072301/atomic/i2/t5` |
| 9 | `docker/s2026072301/atomic/qr/t6` |

Transaction and atomic cases use the frozen 128-mutation recipe; HOLD uses the frozen two-
transaction I1 barrier recipe. Every row is exactly `complete`, `incomplete` or `not_attempted`.
`complete` means the required observation population is complete, not that it passed; it may carry
an oracle violation. Checked arithmetic requires
`10 = complete_count + incomplete_count + not_attempted_count` and
`attempted_count = complete_count + incomplete_count`. `case_attempt_ordinal` values are exactly zero
through `attempted_count - 1`, attempted rows form a prefix, at most the final attempted row is
incomplete, and there is no reuse or rerun. The cut is a deterministic count projection:
`candidate_not_started` if and only if counts are `0/0/10`;
`smoke_complete` if and only if counts are `10/0/0`, regardless of pass or oracle verdict; and
`partial_incomplete` for every other stage-one-admitted stopped prefix.

`result/docker-smoke-control.json` is mandatory content with identity
`state-backend-redb-prescreen-docker-control/v1`. It binds the exact run-start and launch-ledger
descriptor/final hash, plus the checked ten-row final case-ledger projection, process identities, last
attempted/completed ordinal, RAW cut offset, closed cause and exact not-attempted suffix. Its cut is
exactly `candidate_not_started`, `partial_incomplete` or
`smoke_complete`. Cause is exactly `preflight_failed`, `preflight_incomplete`,
`child_dispatch_authorization_unavailable`, `candidate_bootstrap_failed`,
`pre_candidate_safety_bound`, `case_incomplete`, `oracle_violation`, `harness_invalid`,
`safety_bound`, or `mandatory_population_closed`, with the following exact legal pairs:
`candidate_not_started` permits the first five causes or pre-launch `harness_invalid`;
`partial_incomplete` permits `case_incomplete`, `candidate_bootstrap_failed`, `oracle_violation`,
`harness_invalid` or `safety_bound`; and `smoke_complete` permits only
`mandatory_population_closed`. It grants no runtime authority. Candidate error/non-return makes the
launched row incomplete; a fully observed oracle violation leaves it complete. The first mandatory
case, harness, oracle or safety failure closes the decision ledger and leaves the exact suffix not
attempted. A violation in the final row has no suffix, so it closes as `smoke_complete` with
`mandatory_population_closed` while the oracle envelope derives `oracle_not_passed`.

Every Docker case uses the same outer launch-broker dormant/armed/release barrier. The launch ledger
owns its process/release/exit receipts, and the control object is a checked projection that binds
their final root. Each released process gets a contiguous
`process_release_ordinal`; the case's first database-opening release also assigns its
`case_attempt_ordinal`, reused by associated process receipts. A case is attempted at that durable
release intent; an ambiguous release is incomplete. `candidate_bootstrap_failed` is required if and
only if an inert bootstrap fails before release, using `candidate_not_started` when
`attempted_count = 0` and `partial_incomplete` when `attempted_count > 0`, including an associated-
process failure in the current attempted case. The `harness_invalid` control cause excludes bootstrap
failures; the separately derived `harness_invalid` result reason still includes them. Failure to form
the control object after supervisor or outer-runner loss is no result, not a self-described
incomplete result.

The Docker closure is exactly:

```text
run_start -> docker_launch_ledger + raw case/copy evidence + docker_control -> raw_manifest
  -> validator envelope + oracle envelope
  -> evidence_close -> cleanup_journal -> cleanup_report -> final_index
  -> Docker result payload -> post-run receipt
```

The report-envelope, expected-leaf reconciliation, retained-index, 2-GiB cap and cleanup invariants
above apply through Docker-tagged schemas. The Docker expected set derives from its exact run-start,
Docker launch ledger, Docker control and raw manifest rather than native campaign-control. Docker
launch ledger and control are mandatory spine objects but, like native campaign-control, are excluded
from leaf reconciliation. Docker expected leaves contain only its static/case/copy leaves; native
mechanism, campaign-control and terminal roles are inapplicable, never missing.
Cleanup covers transient case copies only. Zero copies still requires a header-only cleanup journal
and zero-row cleanup report. Missing or malformed run-start, Docker launch ledger, Docker control,
raw manifest, validator/oracle envelope, evidence-close manifest, cleanup journal/report or final
index means no Docker result. A valid cleanup failure instead derives an incomplete smoke outcome.

`DOCKER_SMOKE_PASS` requires all ten rows complete, preflight passed, valid layout/goldens and
harness, completed validator, oracle `no_violation`, no retained-invalid/unavailable expected leaf,
no safety bound and complete cleanup, with `incomplete_reasons=[]`.
`DOCKER_SMOKE_INCOMPLETE` requires a valid closure and one through seven unique reasons in this exact
declaration/rank order:

1. `preflight_not_passed`;
2. `required_population_incomplete`;
3. `oracle_not_passed`;
4. `harness_invalid`;
5. `evidence_incomplete`;
6. `safety_bound_reached`; and
7. `cleanup_incomplete`.

All independently applicable reasons are emitted. Failed/incomplete preflight adds
`preflight_not_passed`. Any incomplete or not-attempted mandatory row adds
`required_population_incomplete`. A complete observed violation, or an oracle status other than
complete/`no_violation` after at least one case was attempted, adds `oracle_not_passed`; oracle
`not_run` solely because no candidate case started does not. A harness-invalid cause or closed
harness-domain validator error or `candidate_bootstrap_failed` adds `harness_invalid`.
Retained-invalid/unavailable leaves add
`evidence_incomplete`. A reached run safety bound adds
`safety_bound_reached`, and any failed cleanup row adds `cleanup_incomplete`. Detail stays in the
ledger and envelopes; a candidate error can never reject, select or no-go a backend.

Without verified first-stage Docker admission there is no run-start, manifest or result. A valid
failed/incomplete preflight or unavailable second-stage child capability may close as
`candidate_not_started` and an incomplete smoke result. Missing later live review, storage or run
provenance does not add a reason or change a frozen smoke outcome; it prevents
`DOCKER_SMOKE_PREREQUISITE_VERIFIED_NO_DECISION`. Two- and five-second bursts are removed from the
formal campaign; any future burst needs a separate `docker_diagnostic_no_result` identity incapable
of producing a smoke outcome or prerequisite capability.

#### Cycle 25 implementation blockers

At Cycle 25 close no further schema or validator implementation was safe. The blockers were the
exact raw role/locator/media/cardinality and
per-role cap registry; additive 29-row approval schema and independently constructed 64-MiB fixture
descriptor/digests; actual-target and preflight-cut schemas; proven run-start/raw-manifest byte,
depth, node and descriptor caps; exact duplicate-key/number/parser and deterministic terminal-
serializer contracts; redb-specific binary domains, headers and literal goldens; two-stage dispatcher
mint/handoff/freshness/replay rules; root/control lease and launch-broker/dormant-release contracts;
process identity and dynamic-ledger encodings; campaign-control frame, sync, parent-durability,
torn-tail and no-resume recovery rules; pre-authorized handle lifetime;
attempt-close, crash/scan/oracle/panic witness schemas and panic-origin proof; report-envelope and
expected-leaf/reconciliation wires; Docker launch-ledger/control/case roles; evidence-close
durability; and
complete positive/negative fixtures. Cycle 26 below resolves only the structural 29-row payload and
matching receipt schemas. The independently constructed 64-MiB descriptor/digests and every other
listed item remain blocked.

Candidate caps of 64 KiB for run-start, 256 KiB for raw manifest, 128 raw descriptors and 256 MiB per
raw object remain non-normative until exact fixtures prove them. At Cycle 25 close no schema
recognized the formal 29-row packet or any reserved Cycle 25 identity, and no code could construct a
child-dispatch capability, control journal, terminal latch, smoke/native outcome or trusted state.

### Cycle 26 formal-input content freeze

Cycle 26 is one bounded, redb-free conformance slice. It freezes and may validate only the copied
pre-run JSON content needed to describe the common 29-object approval input set. It does not open a
descriptor target, construct the 64-MiB fixture, authenticate a provider or storage version, collect
target/preflight evidence, construct a run-start, dispatch a process, open redb, classify a result or
seal evidence. Success remains `authorization_unverified`, and both execution and result-sealing
accessors remain unconditionally false.

The two additive schema identities and files are:

| Object | Exact `schema_version` | Exact schema file and `$id` |
|---|---|---|
| formal approval payload | `state-backend-redb-prescreen-approval-payload/v2` | `redb-prescreen-approval-payload-v2.schema.json`; `https://laminardb.dev/schemas/state-backend-redb-prescreen-approval-payload-v2.json` |
| copied protected-review receipt | `state-backend-redb-prescreen-protected-review-receipt/v2` | `redb-prescreen-protected-review-receipt-v2.schema.json`; `https://laminardb.dev/schemas/state-backend-redb-prescreen-protected-review-receipt-v2.json` |

The `/v2` payload has exactly the field set and values specified for the Cycle 22 payload except for
its schema identity and the additive artifact array. That array has exactly 29 rows: rows 1--25 of
the legacy registry without reinterpretation; row 26
`(redb-prescreen-base-64m,fixtures/base-64m.redb,application/octet-stream)`; then the legacy 256-MiB,
1-GiB and 4-GiB rows as rows 27--29. Docker and native payloads must carry byte-for-byte equal 29-row
descriptor arrays; this slice validates each array against the same registry, while the later native
prerequisite verifier must compare the two reviewed payloads. Docker requires
`prior_smoke_result = null`; native requires the separately fixed reviewed-smoke descriptor. The redb
archive remains row 15 with exact length `188200` and SHA-256
`8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`; row 14 binds the exact
supplied policy bytes.

The formal payload cap remains 64 KiB, root depth remains one with maximum depth 16, and the decoded
node cap remains 4,096. Non-fixture roles remain capped at 256 MiB; the 64-MiB and 256-MiB fixture
roles are each capped at 1 GiB, the 1-GiB role at 4 GiB and the 4-GiB role at 12 GiB. Checked
addition of all 29 declared lengths and the native prior-smoke length, when present, must not exceed
20 GiB. The descriptor, identifier, locator, digest, media-type, unknown-field, duplicate-key,
placeholder and unsigned-JSON-number rules remain exactly those in Cycle 22.

The two JSON Schema files enforce bounded closed structure, not complete conformance by themselves.
The redb-free semantic validator must additionally enforce every exact tuple/order/per-role and
aggregate cap, policy/crate byte binding, run-class branch and payload-selected version pairing before
it can return the still-ineligible content-valid summary. Schema validity alone cannot produce that
summary or any authority.

The `/v2` receipt has exactly the closed receipt field layout from Cycle 22 except for its schema
identity. Its `post_run` branch is a reserved structural branch outside this Cycle 26 conformance
slice. This slice accepts only `stage = pre_run`, `retained_evidence = null`, and the exact supplied
`/v2` approval-payload length and digest. The protected-review policy remains `/v1`; therefore the
decision literal deliberately remains `APPROVE_REDB_PRESCREEN_EXECUTION_V1`. Introducing a `_V2`
literal would require a separately reviewed policy successor and is rejected here. Semantic
validation selects the receipt schema from the already decoded payload version and rejects every
mixed `/v1` payload--`/v2` receipt or `/v2` payload--`/v1` receipt pair. A receipt cannot nominate its
own schema branch.

Literal fixtures for this slice are hand-authored, LF-pinned, synthetic copied-content examples.
They may prove only stable byte parsing and exact length/SHA bindings; they are not the future
`contract/literal-goldens.tar.zst`, do not contain an independently constructed 64-MiB database
digest, and cannot satisfy live provider or storage verification. The legacy 28-row schemas,
fixtures, API output and regression behavior remain unchanged.

The approval-input storage-version binding is now placed in the DAG but not given an implementing
schema. It will be strict nested copied content inside the future run-start, never a standalone role,
locator or retained/indexed leaf. It will bind the exact `/v2` approval-payload descriptor, the exact
ordered 29 descriptors and provider/store/version equality identifiers. It must contain no root
selector, URL, handle, credential, self-hash, future-object reference, retention or immutability
claim, verification state, authorization or capability. The independently held opaque storage
capability binds the hidden opened root/version and supplies all authority. Exact provider fields,
identifier domains, retention proof, freshness and TOCTOU rules remain blockers, so this cycle adds
no storage-version schema, fixture, public type or validator.

Actual-target, preflight-cut, run-start, raw-manifest, campaign-control, Docker launch-ledger/control,
report and evidence-close schemas remain blocked. In particular, the 64-KiB run-start, 256-KiB raw-
manifest,
128-descriptor and 4,096-node suggestions remain non-normative until the native 105-row and Docker
ten-row role/process/cardinality registries prove their maxima. Before any Docker runner exists, a
Docker-tagged durable launch ledger or equivalently retained broker-receipt chain must also own its
start/armed/release ambiguity; a final JSON control object alone cannot prove the attempt cut.

### Cycle 27 tagged target identity, DAG and initial-check registry freeze

Cycle 27 freezes identities, dependency direction, minimum fact records and check/status taxonomy; it
does not yet freeze every predicate, authoritative source, sampling window or wire field. It adds no
schema, fixture, validator, collector, provider client, launch broker, process runner, redb dependency
or candidate command. In particular, copied content cannot prove that an observation came from the
protected run or that a policy predicate is true. A later bytes-only validator may report only
content conformance and unverified authority; only the future live dispatcher may reobserve the
target and consider child dispatch.

#### Identities, roles and acyclic bindings

The four additive, run-class-disjoint identities are:

| Run class | Object identity | Descriptor role | Fixed locator |
|---|---|---|---|
| `native_prescreen_decision` | `state-backend-redb-prescreen-native-actual-target/v1` | `redb-prescreen-native-actual-target` | `evidence/actual-target.json` |
| `native_prescreen_decision` | `state-backend-redb-prescreen-native-preflight-cut/v1` | `redb-prescreen-native-preflight-cut` | `evidence/preflight-cut.json` |
| `docker_smoke_no_decision` | `state-backend-redb-prescreen-docker-actual-target/v1` | `redb-prescreen-docker-actual-target` | `evidence/actual-target.json` |
| `docker_smoke_no_decision` | `state-backend-redb-prescreen-docker-preflight-cut/v1` | `redb-prescreen-docker-preflight-cut` | `evidence/preflight-cut.json` |

Each is a singleton `application/json` object in its run-class-specific result root. These roles are
additive; the similarly named roles in the legacy synthetic result schema are not aliases or
defaults. A native identity in a Docker run, a Docker identity in a native run, or both roles in one
result root is invalid.

Bindings use exact `application/json` tuples. The formal payload is
`(redb-prescreen-approval-payload,approval/payload.json)` and the newly reserved receipt role is
`(redb-prescreen-pre-run-protected-review-receipt,approval/protected-review.json)`. Neither role is
part of the approval payload's 29-object array; both live in the result root and are later bound by
run-start. Target/preflight role or media substitution is invalid.

The only legal construction direction is:

```text
exact /v2 approval payload + receipt + copied protected-attempt context + bound policies
  -> run-class actual target
  -> run-class initial preflight cut
  -> future run-start binding
```

The actual-target object binds no preflight or run-start object. The preflight object descriptor-
binds the exact actual-target bytes but no run-start or later object. The future run-start binds both
singletons. Neither singleton hashes itself. This removes a target/preflight/run-start cycle and
prevents a later result from choosing the target on which it claims to have started.

Both objects carry the exact protocol/run class/payload ID and the same dispatcher-generated
`collection_id` encoded as exactly 64 lowercase hexadecimal characters and not all zero. They carry
the same copied attempt context: provider contract, repository ID, change ID,
head revision, workflow file, job name, protected environment ID, workflow run ID and attempt,
workflow job ID, and stage-one dispatcher audit-event ID. They also carry the all-false evidence
scope. Content validation requires the receipt-derived context fields to equal the supplied `/v2`
payload/receipt and requires every copied field plus `collection_id` to be equal between the two
objects. The dispatcher audit-event ID and collection ID instead compare with live dispatcher input
because neither exists in the payload/receipt. Their generation/freshness, provider authenticity,
single use and protected-attempt membership remain live checks; well-formed copied context has no
authority.

The actual-target `bindings` field contains exact descriptors for the approval payload, pre-run
receipt, target-identity policy, preflight policy, clock/isolation policy, bounds and collector
executable (the fixed `redb-prescreen-supervisor` descriptor). The preflight `bindings` field
contains those same roots plus the exact actual-target descriptor. Descriptor equality is byte
length, SHA-256, role, locator and media type. A mismatch is
invalid content, not an environmental preflight failure that can close as `DEFER`.

Docker target and preflight also copy one exact `broker_topology` literal. Cycle 27 reserved
`same_container_pidfd` and `container_per_process`; the Cycle 29 clause below narrows v1 to
`container_per_process`. The actual-target observation, barrier-readiness probe, future run-start and
launch-ledger header must all equal that approved value. A missing, mixed or post-run-start chosen
topology is invalid content; there is no runtime fallback.

Both objects use one Linux boot-ID and `CLOCK_MONOTONIC_RAW` origin. The actual-target capture stores
the positive RAW resolution, start/end offsets and matching UTC audit bracket; the preflight cut
copies the clock identity and stores later start/end offsets. Checked ordering requires target start
at or after the origin, target end at or after target start, preflight start at or after target end,
and preflight end at or after preflight start. UTC is audit context only and never enters a duration.
Failure to establish this common clock header means no valid singleton, not a self-described
`incomplete` observation.

#### Actual-target fact registries

An actual-target body is discriminated by `collection_status`, but never discards successfully
captured facts. Its `facts` array contains every run-class fact ID below exactly once in registry
order. Each row is exactly
`{"fact_id":<id>,"status":"observed","value":<future-closed-value>,"cause":null}` or
`{"fact_id":<id>,"status":"unavailable","value":null,"cause":<closed-cause>}`. An observed value
will use the neutral minimum semantics below and describes what exists, including a policy mismatch;
its exact wire remains unfrozen. For example ext4, a virtual device, cgroup v1 or an unsupported
quota query is observed rather than unavailable. `complete` is the exact projection
that every fact row is observed; `incomplete` requires at least one unavailable row. Missing values
cannot be filled with zero, `unknown`, a placeholder or a claimed default. Causes are exactly
`source_unavailable`, `source_ambiguous`, `source_over_cap`, `identity_changed_during_capture`,
`counter_regressed_or_wrapped`, `integer_overflow` or `deadline_exceeded`, in that precedence order.
If multiple causes affect one fact, only the earliest is retained. A captured source identity
change or source observation over its cap may use its closed incomplete cause; a malformed,
unpublished, document-over-cap or file-identity-changing envelope is not a valid incomplete record.

The native fact registry and neutral observed values are:

| Exact fact ID | Neutral observed value |
|---|---|
| `collector_process_identity` | Linux boot ID, PID namespace inode, PID, `/proc` start ticks, collector-cgroup device/inode and exact supervisor digest, plus self mount/cgroup/user namespace inodes |
| `linux_uname_identity` | `uname` system/machine and kernel release plus OS ID/version; the Rust target triple remains a build binding |
| `libc_abi_identity` | explicitly sourced libc implementation/version and ABI observation; source selection remains blocked |
| `pid1_namespace_identity` | PID 1 PID/mount/cgroup/user namespace inodes |
| `cpu_sets` | normalized online, runner-affinity and campaign-parent-effective CPU/memory-node lists |
| `cpu_frequency` | exposure kind plus one bounded record per sorted cpufreq policy-directory identity with affected CPU set, scaling driver/governor and exact scaling minimum/maximum kHz; equal-setting directories are not merged, and unsupported exposure remains an observed kind |
| `monotonic_clock_source` | `CLOCK_MONOTONIC_RAW` resolution and current kernel clocksource identity |
| `workspace_statx_identity` | workspace device/inode/mount ID, project ID and inherit flag |
| `workspace_mount_record` | selected bounded mountinfo record, filesystem kind, block size, mount root/point/source and normalized options; an optional XFS body contains UUID and quota-option observations only when the kind is XFS |
| `block_device_chain` | bounded neutral leaf-to-physical node kinds and identities; an optional NVMe body contains NSID, UUID/NGUID/EUI64 under a closed availability rule, controller serial/model/firmware, geometry, rotational flag and scheduler only when terminal kind is NVMe |
| `campaign_parent_cgroup` | observed cgroup version and already-open parent identity; an optional v2 body contains selected cgroup2 mount identity/options, ordered opened ancestor identities/local CPU/memory/swap/PID limit tuples, parent controllers/subtree-control/type, full `cgroup.events`, effective CPU/memory-node sets, empty `cgroup.procs` observation and bounded child-directory enumeration only when version is v2 |
| `project_quota_query` | query outcome `supported` or `unsupported`; only `supported` carries exact accounting/enforcement facts and checked 512-byte conversions of `d_bcount`, `d_blk_softlimit` and `d_blk_hardlimit`, while operational/query ambiguity is unavailable rather than an observed error default |
| `nofile_limit` | soft and hard `RLIMIT_NOFILE` |
| `host_swap` | host swap configuration plus campaign-parent swap observation when applicable |
| `filesystem_free_space` | checked `f_bavail`, fragment size and resulting unprivileged available bytes |
| `marker_source` | fresh non-executable fixed-size, sealed `memfd` and frozen `fstat`/seal identity; owner/helper/probe-parent/leaf and mapping/holder provenance; both observed initial sequence/publication-payload pairs |
| `device_ownership_source` | dedicated-device ownership/lease source identity and observed exclusivity value |
| `device_write_source` | target-device attribution source identity and initial counter/in-flight observations; no missing `io.stat` row is encoded as zero |
| `cgroup_cpu_throttle_source` | campaign-parent CPU throttle counter identity and initial values |
| `cgroup_memory_events_source` | campaign-parent memory-event counter identity and initial values |
| `thermal_source` | bounded thermal source identities and initial counters |
| `kernel_block_error_source` | kernel/block-error source identities, cursors and initial counters |
| `broker_barrier_source` | one-shot anonymous sequenced-packet endpoint/open-holder and retained collector/outer-process identities; dedicated probe-parent identity/policy/expected child set, absent deterministic N28 child locator and descendant/dying-descendant baseline; common RAW origin, policy-bound start/deadline operands and initial idle sequence-zero observation |

The collector's cgroup is only in `collector_process_identity`; `campaign_parent_cgroup` is the pre-
created parent the runner owns; later per-process children exist only in campaign-control evidence.
Maximum CPU, frequency-group, device-chain, option, controller and source cardinalities remain
unfrozen until maximum-width fixtures and a supported-host inventory prove them.

The Docker registry is deliberately different and cannot carry native XFS/NVMe facts:

| Exact fact ID | Neutral observed value |
|---|---|
| `collector_process_identity` | Linux boot ID, full PID/start-ticks/cgroup/executable identity and self namespace inodes |
| `windows_wsl_identity` | Windows build, WSL version and Docker Desktop WSL2 backend identity, without credentials or raw host paths |
| `linux_vm_identity` | Linux boot ID, architecture and kernel release |
| `docker_engine_identity` | Desktop/Engine/API versions, Engine ID, Linux-container mode, storage driver and rootless/live-restore settings |
| `broker_connection_receipts` | broker connection ID, contiguous broker receipt sequence and observed Docker event identity/time; Docker supplies no durable event cursor |
| `broker_container_identity` | immutable control-only broker container/image/config and full init-process, namespace and cgroup identity; protocol forbids a candidate database mount, but Engine authority enforcement remains a blocker |
| `image_executable_bindings` | exact approved image/config, supervisor, child, actuator, oracle and verifier digests |
| `probe_volume_mount` | neutral Docker mount type/driver/scope/options and mountinfo/filesystem/device/workspace identity for the disposable probe; expected local-volume properties are predicates, not structural constants |
| `container_root_mount` | separate container-root mount identity and filesystem kind |
| `resource_limits` | cgroup version and CPU/memory/swap/PID/file-descriptor limits plus probe-volume free bytes |
| `volume_freshness_source` | disposable volume/copy freshness and exclusivity source identity/observation |
| `process_reconciliation_source` | broker process/container enumeration source identity/observation |
| `cleanup_root_source` | bounded cleanup-root identity and capacity observation |
| `broker_barrier_source` | inert broker/barrier probe identity and observation |
| `fixture_source_identity` | read-only 64-MiB fixture descriptor and opened-source identity/digest observation |

Candidate child, actuator and scanner identities do not exist here; they first appear after run-start
in the launch ledger. These facts describe a smoke target, not native storage: even a complete Docker body cannot
claim XFS project quota, dedicated NVMe,
device-write attribution, native latency, physical amplification, power-loss or endurance evidence.
Engine ID alone is not a daemon-restart epoch; the trustworthy broker-connection/event-discontinuity
rule remains a schema blocker.

#### Initial-preflight status and checks

The preflight cut is initial-only. It contains the exact common roots above, `status`, one fixed
ordered `checks` array, declaration-ordered `failed_check_ids` and `incomplete_check_ids`, a bounded
class-specific observation body, and the all-false evidence scope. Every check occurs exactly once
and is exactly `passed`, `failed` or `incomplete`. A passed or failed row has
`incomplete_cause = null`; an incomplete row has one source cause above or
`prerequisite_incomplete`. The latter is legal only when a required actual-target fact is
unavailable. When multiple unavailable preflight sources feed one check, its scalar cause is selected
first by the seven-value cause precedence and then by the Cycle 29 source-ID derivation. The exact
capture recipes and blocked authorities, rather than ordering, remain schema blockers; arbitrary
collector discovery order is never accepted.

`failed` means a complete, attributable observation proved the policy predicate false. `incomplete`
means the required observation could not be established; it must never encode an adverse fact as
missing. The two ID arrays are exact projections of non-passing rows in registry order and are not
caller-selected reasons. Overall status is `incomplete` if any row is incomplete, otherwise `failed`
if any row failed, otherwise `passed`. Thus `passed` requires every row passed and both arrays empty.
Native and Docker control causes preserve the matching `preflight_incomplete` or `preflight_failed`;
only the later derived Docker result folds either into `preflight_not_passed`. Only a live recheck of
a content-valid `passed` cut may be considered for the separate child-dispatch capability.

`actual_target_complete` is passed if and only if the bound target has
`collection_status = complete`; an incomplete target makes that row incomplete and can never be
relabeled failed; its `incomplete_cause` copies the cause from the first unavailable fact row in
registry order. Every other
check that depends on an unavailable target fact is incomplete with `prerequisite_incomplete`; it
cannot pass or fail using another group's value. The exact live comparison behind every row remains
a run-class-specific schema blocker.

The exact native initial-check order is:

1. `actual_target_complete`;
2. `boot_id_stable`;
3. `native_linux_host_namespace`;
4. `kernel_identity_match`;
5. `cpu_affinity_match`;
6. `cpu_frequency_policy_match`;
7. `monotonic_raw_clock_match`;
8. `marker_sequence_monotonic`;
9. `workspace_mount_identity_match`;
10. `xfs_mount_policy_match`;
11. `nonvirtual_storage_path`;
12. `dedicated_nvme_identity_match`;
13. `device_exclusive`;
14. `device_write_attribution_available`;
15. `xfs_project_quota_enforced`;
16. `workspace_project_quota_match`;
17. `cgroup_v2_identity_match`;
18. `cgroup_limits_match`;
19. `swap_disabled`;
20. `nofile_limit_match`;
21. `free_space_headroom`;
22. `cpu_throttle_quiet`;
23. `memory_events_quiet`;
24. `thermal_quiet`;
25. `kernel_block_errors_quiet`;
26. `preflight_device_io_quiet`; and
27. `broker_barrier_readiness`.

The native absent-fact dependency projection is exact. Check 1 depends on every fact row. Check 2
uses only the mandatory target/preflight clock headers. Checks 3--27 respectively depend on:

| Check | Required native fact IDs |
|---:|---|
| 3 | `collector_process_identity`, `pid1_namespace_identity` |
| 4 | `linux_uname_identity`, `libc_abi_identity` |
| 5 | `cpu_sets`, `campaign_parent_cgroup` |
| 6 | `cpu_frequency` |
| 7 | `monotonic_clock_source` |
| 8 | `marker_source` |
| 9 | `workspace_statx_identity`, `workspace_mount_record` |
| 10 | `workspace_mount_record` |
| 11 | `workspace_mount_record`, `block_device_chain` |
| 12 | `block_device_chain` |
| 13 | `device_ownership_source` |
| 14 | `device_write_source`, `block_device_chain`, `campaign_parent_cgroup` |
| 15 | `workspace_mount_record`, `project_quota_query` |
| 16 | `workspace_statx_identity`, `project_quota_query` |
| 17--18 | `campaign_parent_cgroup` |
| 19 | `host_swap`, `campaign_parent_cgroup` |
| 20 | `nofile_limit` |
| 21 | `filesystem_free_space`, `project_quota_query` |
| 22 | `cgroup_cpu_throttle_source`, `campaign_parent_cgroup` |
| 23 | `cgroup_memory_events_source`, `campaign_parent_cgroup` |
| 24 | `thermal_source` |
| 25 | `kernel_block_error_source`, `block_device_chain` |
| 26 | `device_write_source`, `block_device_chain` |
| 27 | `broker_barrier_source`, `collector_process_identity` |

Any unavailable listed fact forces that row to `prerequisite_incomplete` even when another available
input would have proved a mismatch.

The native observation body must retain the end boot ID/clock identity; the complete marker
transcript needed to recompute both initial observations, helper CAS prior/result, fenced live and
post-exit sequence/payload, exact frozen `fstat`/seal/owner/helper/leaf identities, holder/mapping
populations, metadata/reserved-byte stability, N19-owned-resource closure, empty bootstrap leaf and
RAW bracket; policy-comparison facts; free-space and quota operands; selected cgroup2 mount options, ordered
ancestor identity/local-limit tuples, parent `cgroup.events`, `cgroup.procs` and child-directory
observations; cgroup throttle/memory start/end counters; swap and `RLIMIT_NOFILE` observations;
thermal source/counter pairs; kernel/block-error cursor and delta; initial target-device in-flight/
write observations; and the complete broker-barrier transcript needed to recompute target-idle
endpoint/owner/probe-parent binding, frozen RAW start/deadline equality, request, opened-leaf/helper
identities, credentials, armed-without-release cut, planned kill/wait, stop receipt, collector guard-
release acknowledgement, task/socket/pidfd/cgroup-handle closure, leaf removal, parent descendant/
dying-descendant reconciliation and RAW bracket. Exact
counter sources, cursors, privileges, sampling duration and maximum-width encodings remain freeze
blockers. `preflight_device_io_quiet` is only an initial idle observation and never evaluates the
future one-percent-of-candidate-writes rule. A fresh campaign parent may have no target-device
`io.stat` row before I/O; absence is incomplete unless a separately approved bounded redb-free
attribution probe establishes the row, and is never submitted as a zero counter.

The exact Docker initial-check order is:

1. `actual_target_complete`;
2. `windows_wsl2_backend`;
3. `linux_image_and_executables`;
4. `engine_epoch_and_event_stream`;
5. `linux_clock_and_process_identity`;
6. `namespace_and_cgroup_v2`;
7. `resource_limits`;
8. `managed_volume_storage_class`;
9. `volume_freshness_and_exclusivity`;
10. `fixture_source_and_probe_copy`;
11. `broker_barrier_readiness`; and
12. `workspace_evidence_and_cleanup_bounds`.

The Docker absent-fact dependency projection is exact. Check 1 depends on all rows; checks 2--12
respectively require: `(windows_wsl_identity, linux_vm_identity)`,
`(broker_container_identity, image_executable_bindings)`,
`(docker_engine_identity, broker_connection_receipts)`,
`(collector_process_identity, linux_vm_identity, broker_container_identity)`,
`(collector_process_identity, broker_container_identity, resource_limits)`, `resource_limits`,
`(probe_volume_mount, container_root_mount)`,
`(probe_volume_mount, volume_freshness_source)`,
`(probe_volume_mount, fixture_source_identity)`,
`(collector_process_identity, docker_engine_identity, broker_connection_receipts,
broker_container_identity, process_reconciliation_source, broker_barrier_source)`, and
`(probe_volume_mount, resource_limits, cleanup_root_source)`. Any unavailable listed fact has the
same prerequisite rule.

The Docker observation body retains the exact comparisons, broker receipt-sequence/connection and
observed event identity/time, clock/process identity, cgroup limits, named-volume identity/freshness,
read-only fixture-source and disposable probe-copy digests, broker barrier probe, free-space/resource
operands and cleanup-root bounds. The probe volume/copy is destroyed before run-start, cannot enter
the ten-case population and grants no later copy or release authority. The preflight observation
retains its removal/absence receipt; successful removal is a preflight-envelope publication
prerequisite, so removal failure produces no valid preflight object rather than entering the later
transient cleanup registry. It contains no native quiet-target, XFS, NVMe, quota or latency check.

The initial cut cannot predict or satisfy release-lateness p99/maximum, per-attempt cgroup/thermal/
kernel deltas, non-candidate writes relative to candidate writes, boundary quiescence or cache-reset
receipts. It also cannot satisfy a real case-copy digest/scan or any process START/ARMED/RELEASE
receipt. The ten formal case volumes/copies are created and descriptor-bound after run-start; each
copy digest is durably recorded before its first process release. Those are later repeatable raw/
control evidence tied to the run-start and attempt bracket. Moving them into preflight would create
a temporal impossibility and allow a stale pass to replace ongoing target validity.

#### Docker launch ledger boundary

Docker requires the mandatory `application/octet-stream` role
`redb-prescreen-docker-launch-ledger` at
`result/docker-launch-ledger.bin`, with reserved identity
`state-backend-redb-prescreen-docker-launch-ledger/v1`. It is created only after the run-start bytes
exist, binds their exact SHA-256 in every frame, and is excluded from the preflight cut. The final
Docker control object binds its exact descriptor and checked final root; it cannot replace the
ledger.

The protected outer broker is the exact Linux process identified as PID 1 by
`broker_container_identity`. It is the sole ledger writer and owns the RAW-clock domain, Engine/event
connections, control/evidence-root handles, leases and recovery authority. It never executes a
candidate, and candidate containers never receive the Engine endpoint or broker control/evidence
mounts. Broker loss is unfinalizable; ownership cannot move to a replacement process.

After stage-one admission that broker holds a non-serializable
`DOCKER_SMOKE_ROOT_LEASE` over the exact attempt context, control-only broker container, Docker
connection, evidence/control-root handles and the disposable probe only until its verified preflight
removal.
The removal/absence receipt is retained, the probe is removed from the lease, and only then may
preflight and run-start be published. The root lease grants no protocol authority to launch or
release candidate code; because raw Docker access is physically stronger than this logical type, a
dedicated Engine/VM and exclusive Engine-client lease is a separate mandatory enforcement blocker.
After exact run-start publication and create-new plus reopen verification of the ledger, the broker
irreversibly narrows the lease to
`DOCKER_LAUNCH_LEDGER_LEASE`, binding that file identity and run-start. Neither lease replaces the
single-use child-dispatch capability. Parsed bytes, labels and copied IDs cannot recreate either
lease.

The future ledger is broker-owned, append-only, single-writer, contiguous-sequence and SHA-256-chain
protected. Create-new file and parent-directory durability plus reopen verification precede
`LEDGER_OPENED`. Each later authority-changing frame is file-durable before broker acknowledgement;
it does not resync the unchanged parent directory. Every frame binds campaign ID, run-start SHA-256,
prior-frame hash and monotonic-RAW offset. Its legal top-level grammar allows either zero launch or
repeated cases:

```text
LEDGER_OPENED -> STOP_DECIDED -> EVIDENCE_CUT

LEDGER_OPENED -> (CASE_OPENED -> process-subgraph+ -> CASE_CLOSED)+
  -> STOP_DECIDED -> EVIDENCE_CUT
```

`CASE_OPENED` first binds the exact source descriptor and approved logical-verification receipt,
fresh volume/copy stable identity, byte-copy length/SHA-256 receipt and cleanup identity. It is durable
before any process intent, so later evidence cannot retrospectively choose which copy was opened.

Each process subgraph begins with a common deterministic, never-reused `process_intent_id` binding
run-start hash, campaign ID, case ID/ordinal, process role and intent ordinal. The ledger header is
exactly `broker_topology = container_per_process`. `CONTAINER_CREATE_INTENT` additionally binds the
deterministic Docker create key and labels. It is followed by definite `CONTAINER_CREATE_FAILED` or
`CONTAINER_CREATED`; the latter is followed by `PROCESS_START_INTENT`, then definite
`PROCESS_START_FAILED` or `PROCESS_STARTED` with the container and in-container process identity.
A lost or ambiguous create, start, wait, kill/stop, inspect or remove response is reconciled against
the same intent and exact container identity before a later authority-changing frame. Inability to
establish one outcome is unfinalizable; no retry can become a fresh attempt.

A definite container-create or process-start failure before a bootstrap started maps to pre-launch
`harness_invalid` unless a separately bound safety cause controls. After
`PROCESS_STARTED`, an exit before arming records `PROCESS_EXIT_OBSERVED` and reconciliation and is
`candidate_bootstrap_failed`. Otherwise `PROCESS_ARMED` is followed either by the unreleased stop
path `PROCESS_STOP_INTENT -> PROCESS_STOP_OBSERVED -> PROCESS_RECONCILED`, or by
`PROCESS_RELEASE_INTENT` and exactly one of:

- the recovery-stop path above before a broker release commit; or
- `BROKER_RELEASE_COMMITTED`, then an acknowledgement if observed, then an exit if observed, then
  any required stop receipts and `PROCESS_RECONCILED`.

COMMITTED is required before acknowledgement or release delivery. Exit without a retained
acknowledgement is legal only after COMMITTED; acknowledgement without COMMITTED is invalid. A
retained later receipt may not be omitted merely to manufacture an earlier prefix. `CASE_CLOSED`
requires every subgraph reconciled; no next case may open earlier. `EVIDENCE_CUT` requires no live or
unaccounted candidate-affecting process/container, no unresolved create key, and an empty selected
candidate child-cgroup or per-case-container domain. The control-only broker and closure-only processes are
excluded from that domain and remain bound by the root/ledger lease.

Process identity is never PID or container ID alone. Every process binds Linux boot ID, PID
namespace, PID, `/proc` start ticks, cgroup identity, exact executable digest and its fresh container
ID.

Durable `PROCESS_RELEASE_INTENT` for the case's first database-opening process is the conservative
case-attempt cut. A START/ARMED-only process is unreleased; the case is unattempted only when no first
database-opening release intent exists. Failure of a later scanner, actuator or control bootstrap
preserves the existing case-attempt ordinal and closes that attempted case incomplete. Release intent
without commit/ack, committed release without acknowledgement, or missing exit is likewise attempted
and incomplete. No ambiguous or failed process may be silently retried.

After supervisor-worker loss, closure-only recovery is possible only while
`DOCKER_LAUNCH_LEDGER_LEASE` survives: it stops the exact registered set, records
`PROCESS_STOP_INTENT`/`PROCESS_STOP_OBSERVED`, proves the per-case-container domain empty, and appends
only reconciliation, stop and cut frames. The separately lease-
bound control-only broker/closure processes remain outside that emptiness predicate. Lease loss, Docker API/
event-stream disconnect, uncertain daemon epoch or an unaccounted candidate-affecting process makes
the closure unfinalizable and produces no result.

Cycle 29 selects the Docker-API container-per-process topology using exact create, inspect, start,
wait, kill and reconciliation receipts. Cycle 27's `same_container_pidfd` alternative is superseded
decision history: no v1 policy, ledger or parser accepts its tag or local-spawn frames. Daemon-epoch
continuity, independent container/cgroup-empty proof, the exact ten-case and per-case
process/copy/scan/helper registries, binary frame layout, frame/byte caps, torn-tail recovery and
literal goldens remain blocked. No 26-process or other total cap is inferred from the ten case rows.

#### Schema and validation boundary

Proposed bounds of 64 KiB per JSON object, depth 16 and 4,096 decoded nodes remain non-normative.
Likewise, 256 expanded CPUs, 16 frequency groups, eight device-chain nodes, 32 mount options and 16
cgroup controllers are candidate caps only. Before a schema or validator is added, hand-authored
minimum- and maximum-width native/Docker fixtures must prove every string/list/node/byte maximum and
cap-plus-one rejection. Positive fixtures must cover complete/passed, observed mismatch/failed and
source-loss/incomplete for each class. Hostile fixtures must cover duplicate/unknown keys, trailing
or bad UTF-8, non-`u64` numbers, digest/locator drift, class substitution, policy/target mismatch,
self/future edges, wrong check order/count, false status projections, adverse-as-incomplete,
unavailable-as-failed, dependency-projection drift, native claims in Docker, nonempty local-driver
options, bind/tmpfs/overlay substitution, event reconnect treated as continuity, candidate identity
before run-start, and malformed bytes pretending to be incomplete. Ledger goldens additionally cover
torn header/tail, frame over-cap, sequence/hash/run-start drift, illegal transition, lost-create-
response and create/start/wait/kill/inspect/remove ambiguity, bootstrap exit before ARMED, release
without ARM, acknowledgement without COMMITTED, wrong-topology/local-spawn frame, duplicate/gapped
case or release ordinal, acknowledgement/exit identity mismatch, next-case start before
reconciliation, final cut with a live process, daemon disconnect, candidate Engine-socket/control-
root mount, missing exclusive Engine authority and an unaccounted container/cgroup.

The remaining schema blockers are the exact approved native target scalar/identifier values,
authoritative libc/ABI source and freshness window; native-host authority and campaign-parent cgroup
ownership and hierarchical-limit rules; authoritative device exclusivity/write-attribution plus an
approved bounded attribution-probe executable/recipe; thermal and kernel-error sources; XFS
accounting/enforcement flags, query behavior,
project-ID allocation and `pquota`/`prjquota` alias normalization; the checked use of `f_bavail`
rather than privileged `f_bfree` and the meaning of 64 GiB free beyond workspace quota; cpufreq
behavior on unsupported hosts; Docker daemon-epoch continuity, exclusive Engine authority, exact
container security profile and feasible container/cgroup-empty proof; broker-barrier probe
executable/recipe; raw-source retention needed for
independent verification; live stage-two expiry/drift, provider/storage/capability TOCTOU; and maximum
fixtures. Run-start/raw-manifest caps additionally
remain blocked on literal 105/10 schedule goldens and complete per-row process/raw-role cardinalities.
None of these contracts authorizes a backend, candidate run, cluster state, exactly-once claim,
source/sink delivery claim, production use or soak claim.

### Cycle 29 candidate-neutral source, scalar and predicate freeze

Cycle 29 narrows the remaining target/preflight work without claiming a final wire contract. It
freezes an additive semantic skeleton, ordered source-domain requirements, portable scalar
semantics, source-failure classification, minimum check predicates and the sole Docker v1 broker
topology. It does not freeze complete JSON pointer/value layouts, machine-specific expected values,
source or document caps, schemas, collectors, raw-source storage, an external authority term and
provider lifecycle, or an executable probe. A blocked source ID reserves an ordered requirement but
does not claim its authority or capture recipe exists. No source ID is an extension point or backend
adapter.

The ordered Cycle 27 fact/check registries, fact dependencies and status projections remain exact.
This cycle freezes one additional skeleton field: both preflight bodies contain an `observations`
array with the same fact IDs and order as their run class's actual target. A row uses the same
observed-value/unavailable-cause union, but its future closed value is phase-specific: stable
identities are re-read, quiet sources carry begin/end samples, and marker, attribution and barrier
sources carry bounded probe receipts. Check rows remain exactly `check_id`, `status` and
`incomplete_cause`. Every predicate operand must be recoverable from the exact bound policy, ordered
observations and any explicitly listed live authority source; it may not be replaced by a
caller-supplied `matched=true`. Complete pointer and observation-value shapes remain blocked with the
schemas.

#### Scalar algebra

Every future target/preflight schema and fixture uses these rules:

- A JSON integer has lexical form `0|[1-9][0-9]{0,19}`, parses at most `u64::MAX`, and is never
  accepted through float coercion. Negative zero, signs, fractions and exponents are invalid.
  Units appear in field names, including `_ns`, `_bytes`, `_khz` and `_512b_blocks`; conversions,
  sums, products and differences use checked arithmetic.
- A Linux path or other OS byte string is represented losslessly as an even-length lowercase hex
  string plus its decoded `byte_length`; it is never normalized as Unicode. Protocol IDs, source
  IDs, field keys and closed enums are ASCII literals. SHA-256 values are 64 lowercase hexadecimal
  characters; only an existing descriptor type with an explicit nonzero rule rejects the valid
  all-zero bit pattern. Linux boot IDs are canonical lowercase UUID text.
- A device identity is `{major:u64,minor:u64}`. CPU and memory-node sets are arrays of
  sorted, nonoverlapping and nonadjacent inclusive `{first,last}` ranges. Their expanded count is
  checked without allocating from the claimed count. An empty observed set is representable and
  fails a predicate that requires CPUs or nodes; it is not encoded as unavailable.
- A kernel limit is exactly `{"kind":"max"}` or
  `{"kind":"value","value":<u64>}`. Zero, `u64::MAX`, `-1`, `null`, `unknown` and empty text are
  never absence or unlimited sentinels. A source-defined value wider than `u64`, if one is later
  approved, uses a fixed-width lowercase big-endian hex type rather than a JSON number.
- Keyed kernel files are parsed by key, not line order, and represented as key-sorted unique arrays.
  CPU/memory range strings and cgroup `io.stat` rows are normalized before comparison; `io.stat`
  rows are keyed by device major/minor. Unknown source keys are retained within the future source
  cap rather than silently discarded. Mount options are sorted unique `(ASCII name, raw-byte value
  or null)` pairs. XFS `pquota` and `prjquota` normalize to `prjquota`; observing both aliases after
  normalization is ambiguous rather than two independent facts.
- A counter observation binds its source identity and begin/end unsigned values. It cannot contain
  only a delta. UTC and Docker event timestamps are audit fields, never monotonic ordering evidence;
  native durations and deadlines use nonnegative `CLOCK_MONOTONIC_RAW` nanosecond offsets.
- Every compound predicate has a fixed clause order. Evidence retains the observed operands and
  binds the exact expected policy; it cannot retain only comparison booleans. Arrays are ordered
  where this protocol declares an order. JSON object member order is not semantic, and accepted
  bytes need not use JCS; literal goldens use compact deterministic property order while the
  separate document-byte cap bounds whitespace.

The existing exact descriptor, protocol-ID, provider-ID, locator and canonical UTC definitions are
imported unchanged where those types occur. Bounds not already exact remain intentionally absent.

#### Source-failure classification

An implementation may acquire independent sources concurrently, but classification order is the
registry order below, never task-completion or filesystem-enumeration order. Within a fact or check,
the seven Cycle 27 causes win first by their frozen cause precedence and then by source ID. The
classification is exact:

| Cause | Meaning |
|---|---|
| `source_unavailable` | The selected required interface, open, syscall, privilege or required keyed row is definitively absent and no approved alternative exists. A closed policy-relevant `unsupported` value, such as unsupported cpufreq or quota capability, is observed and can fail instead. |
| `source_ambiguous` | A complete bounded raw source is malformed, has an out-of-domain or duplicate normalized record, finds more than one match, or contains disagreement that prevents one authoritative interpretation. A missing endpoint or required keyed row is `source_unavailable`. |
| `source_over_cap` | A valid collector reaches a bound source byte/list/population cap before complete capture. It emits no truncated observed value. |
| `identity_changed_during_capture` | Begin/end identity reads differ or a source field declared stable for that capture changes; expected counter movement is classified by its predicate instead. |
| `counter_regressed_or_wrapped` | A selected externally monotonic counter has `end < begin`, a proved wrap, or saturation that prevents a safe comparison. A stable positive marker jump is an observed predicate failure, not this cause. |
| `integer_overflow` | Lexical parsing, unit conversion or checked arithmetic cannot be represented by its frozen output type. |
| `deadline_exceeded` | The bound RAW-clock source/probe deadline expires before complete capture. |

Malformed JSON, duplicate/unknown envelope keys, bad UTF-8, an invalid mandatory header, broker
receipt-chain corruption, a submitted document above its global cap or a file identity change while
reading the envelope is invalid content and produces no singleton. It is not a fact-level
`incomplete` result. A stable actual-target observation A and stable preflight observation B with
`A != B` fails the applicable comparison; instability inside A or B is incomplete with
`identity_changed_during_capture`. A positive quiet-counter delta fails; a regression is incomplete.
Docker currently has no approved external monotonic counter for which
`counter_regressed_or_wrapped` is legal: its broker sequence is envelope integrity and event
`timeNano` is wall time.

For a fact, source order is its projection row below. For a check-local capture, source order is the
deduplicated union of its required facts' projected sources in ascending run-class source-ID order;
the boot/clock header check uses native `N01,N02`, native-host check 3 appends check-local `N29`, and
`actual_target_complete` uses fact-registry order rather than a source registry. An unavailable
actual-target dependency still wins as `prerequisite_incomplete`. Otherwise a preflight source
failure uses the cause/source precedence above. This derivation is closed; a collector cannot insert
a discovered source into the order.

#### Native source requirements and fact projection

The ordered native source-requirement registry is:

| ID | Reserved source requirement |
|---|---|
| `N01` | opened `/proc/sys/kernel/random/boot_id` |
| `N02` | `clock_gettime` and `clock_getres` for `CLOCK_MONOTONIC_RAW` |
| `N03` | `getpid`, `/proc/self/stat` field 22, opened self namespace/cgroup/executable identities and executable SHA-256 |
| `N04` | `uname(2)` plus opened `/etc/os-release`, falling back only when absent to opened `/usr/lib/os-release` |
| `N05` | selected libc/ELF ABI observation; exact source remains blocked |
| `N06` | opened PID 1 namespace identities |
| `N07` | CPU online sysfs and `sched_getaffinity` |
| `N08` | every sorted cpufreq `policy*` directory identity, affected CPU set, scaling driver/governor and exact `scaling_min_freq`/`scaling_max_freq` files; equal settings are not merged |
| `N09` | opened kernel current-clocksource sysfs |
| `N10` | preopened workspace `fstat`/`statx`/`fstatfs`, `FS_IOC_FSGETXATTR`, and conditional `XFS_IOC_FSGEOMETRY` block-size/UUID query |
| `N11` | bounded opened `/proc/self/mountinfo` capture |
| `N12` | bounded block sysfs holder/slave/partition topology only |
| `N13` | NVMe namespace/controller identity interfaces |
| `N14` | preopened campaign-parent cgroup directory, selected cgroup2 mount record, verified opened ancestor chain carrying relevant keyed cgroup v2 configuration/effective files, and bounded parent process/child-directory emptiness capture |
| `N15` | XFS project-quota query and accounting/enforcement state |
| `N16` | `getrlimit(RLIMIT_NOFILE)` |
| `N17` | bounded opened `/proc/swaps` capture |
| `N18` | `fstatvfs` on the preopened workspace |
| `N19` | selected fresh `memfd`/`MAP_SHARED` marker recipe using a separately assembled Linux/x86-64 publication/observation shim; owner target-ABI acceptance, exact build, independent cross-process corroboration and implementation remain blocked |
| `N20` | authoritative operations/provider device lease; provider remains blocked |
| `N21` | campaign-parent cgroup `io.stat` plus a selected attribution probe; recipe remains blocked |
| `N22` | selected whole-device `/sys/block/*/stat` row |
| `N23` | campaign-parent `cpu.stat` |
| `N24` | campaign-parent hierarchical `memory.events` |
| `N25` | approved CPU thermal-throttle counters; supported-host source set remains blocked |
| `N26` | approved bounded kernel/block-error cursor; source remains blocked |
| `N27` | approved NVMe health/error observations; source and privilege remain blocked |
| `N28` | selected one-shot anonymous `AF_UNIX`/`SOCK_SEQPACKET` inert broker/barrier recipe; host capability proof and implementation remain blocked |
| `N29` | live protected-dispatcher/provider native-host attestation bound to the collection and runner identity; exact authority remains blocked |

The exact source projection is:

| Fact ID | Ordered source IDs |
|---|---|
| `collector_process_identity` | `N01,N03` |
| `linux_uname_identity` | `N04` |
| `libc_abi_identity` | `N05` |
| `pid1_namespace_identity` | `N06` |
| `cpu_sets` | `N07,N14` |
| `cpu_frequency` | `N08` |
| `monotonic_clock_source` | `N02,N09` |
| `workspace_statx_identity` | `N10` |
| `workspace_mount_record` | `N10,N11` |
| `block_device_chain` | `N11,N12,N13` |
| `campaign_parent_cgroup` | `N14` |
| `project_quota_query` | `N10,N11,N15` |
| `nofile_limit` | `N16` |
| `host_swap` | `N17,N14` |
| `filesystem_free_space` | `N18` |
| `marker_source` | `N19` |
| `device_ownership_source` | `N20,N11,N12` |
| `device_write_source` | `N12,N14,N21,N22` |
| `cgroup_cpu_throttle_source` | `N23` |
| `cgroup_memory_events_source` | `N24` |
| `thermal_source` | `N25` |
| `kernel_block_error_source` | `N26,N27` |
| `broker_barrier_source` | `N28,N03` |

Unsupported cpufreq exposure, a non-XFS filesystem, a virtual device, cgroup v1 or an unsupported
quota query is a complete observed tagged kind and can fail policy. Operational ambiguity or loss is
unavailable. Mount IDs can be reused and therefore never identify a mount without the opened
workspace and selected mount record. Kernel keyed-file line order is never identity. These choices
follow the Linux kernel's [cgroup v2](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html),
[block-statistics](https://www.kernel.org/doc/html/latest/block/stat.html) and
[mountinfo](https://www.kernel.org/doc/html/v6.8/filesystems/proc.html#proc-pid-mountinfo-information-about-mounts)
semantics, plus the XFS userspace API's
[filesystem-geometry query](https://man7.org/linux/man-pages/man2/ioctl_xfs_fsgeometry.2.html).

#### Native minimum predicates

Machine-specific identity operands come from the reviewed target/preflight policies; they are not
copied from the ineligible Fjall/RocksDB profile or inferred from this workstation. Subject to every
required fact being observed, the 27 checks have these minimum predicates:

| Check(s) | Frozen minimum predicate |
|---|---|
| 1 | Every target fact is observed. |
| 2 | Boot ID and RAW-clock origin/resolution are stable and all target/preflight offsets are checked and ordered. |
| 3 | Collector and PID 1 namespace clauses match and check-local `N29` attests the same collection/runner as a protected native host. Namespace equality alone can also occur in a container and is insufficient. |
| 4 | Stable uname/libc/ELF observations equal the reviewed policy and build ABI. |
| 5 | Online CPUs contain the policy affinity; scheduler affinity and campaign-parent effective CPU/memory-node sets equal policy. |
| 6 | Exposure is complete and every normalized policy-directory/CPU-set/driver/governor/minimum/maximum record equals policy; unsupported exposure fails. |
| 7 | RAW resolution and current clocksource are stable and equal policy. |
| 8 | Both shim-observed initial pairs equal sequence/payload zero; the sole writer's audited shim publishes payload one before one locked compare-and-exchange changes sequence zero to one; the audited read-only shim sees both ones while the helper is parked and after clean exit, reserved bytes and the frozen `fstat`/seal subset remain stable, every N19-owned resource reconciles and the bootstrap leaf is stably empty. |
| 9 | Opened workspace device/inode/mount/project/inherit identity and selected mount record remain equal. |
| 10 | Filesystem is XFS, normalized `prjquota` is enabled, and no disabling quota option is present. |
| 11 | The linear path is an NVMe namespace or partition-to-NVMe namespace, with no holder, branch or virtual layer. |
| 12 | Namespace/controller identifiers, firmware, geometry, nonrotational flag and scheduler equal policy. |
| 13 | The authoritative lease is current, held and exclusive for that exact device chain. |
| 14 | An approved bounded redb-free synced write probe produces exactly one allowed-chain `io.stat` row with both `wbytes` and `wios` increasing and freezes that major/minor as the attribution source; every other row's write fields remain unchanged. Read/discard fields are retained but neither establish nor change write attribution. Zero matches are unavailable; multiple matches are ambiguous. |
| 15 | XFS project accounting and enforcement are enabled and the exact project query is supported. |
| 16 | Workspace project ID is nonzero, inherit is set, query and inode project IDs match, and the hard quota is exactly 64 GiB after checked 512-byte conversion. |
| 17 | The preopened parent and ancestor chain are stable cgroup v2; parent `cgroup.events populated=0`, `cgroup.procs` is empty, bounded enumeration finds no child cgroup directory, the parent is domain type, the cgroup2 mount does not enable `memory_localevents`, and policy-required CPU/cpuset/I/O/memory/PID controllers and subtree state match. |
| 18 | Every bound ancestor/local limit tuple and the kernel-reported effective CPU/memory-node sets equal policy; existing fixed parent operands include `memory.max = 16 GiB` and `memory.swap.max = 0`. Ancestor CPU constraints and remaining CPU/PID values are policy inputs, not inferred effective scalars. |
| 19 | `/proc/swaps` has no active rows and the effective campaign-parent swap limit is zero. |
| 20 | Soft and hard `RLIMIT_NOFILE` equal 512. |
| 21 | Quota use does not exceed its hard limit and checked `f_bavail * f_frsize >= (quota_hard_bytes - quota_used_bytes) + 64 GiB`. |
| 22 | `nr_throttled` and `throttled_usec` do not increase across the policy quiet window. |
| 23 | Every bounded hierarchical `memory.events` key/value is unchanged; a key-set change inside the bracket is source-identity change. |
| 24 | Every approved thermal-throttle counter is unchanged. |
| 25 | The complete approved kernel/block-error cursor reports zero new events; every approved NVMe monotonic error counter is unchanged and every approved critical-error bit is clear at both cuts. The exact counter/bit registry remains blocked with `N26,N27`. |
| 26 | Across the policy quiet window, whole-device block read/write/discard/flush completion counters are unchanged and in-flight is zero at both ends. Sectors use 512-byte units; the whole NVMe is sampled because partition rows omit flushes. |
| 27 | The one-shot inert broker returns the exact collection nonce and sequence zero, authenticated process/peer identities, armed-without-release state and planned-kill receipt; the target probe-parent child set/`nr_descendants` agree and `nr_dying_descendants` is zero; helper tasks, pidfds, leaf handles, leaf and both probe transports reconcile back to those parent baselines while required broker/collector identities remain stable, all within the RAW deadline. |

Check 14's probe can establish attribution only after its exact executable, cgroup, byte count,
sync, cleanup and maximum-write recipe is reviewed. A missing `io.stat` row is never zero. Any check
with a blocked authority, source, policy operand or probe recipe cannot pass until that dependency
exists.

#### Docker v1 source requirements and topology decision

Docker v1 uses only `broker_topology = container_per_process`. The Linux broker owns the durable
ledger, RAW clock, version-pinned Engine/event connections and recovery state; it creates one
candidate-affecting container per process using deterministic create keys and exact campaign labels.
Container startup does not enter backend-latency evidence because this lane is functional smoke, but
it remains subject to harness deadlines and reliability classification. `same_container_pidfd` is
rejected for v1 because writable child-cgroup delegation and local PID lifecycle reconciliation
after supervisor-worker loss are unproved; the selected path instead retains Engine-owned container
identity/state while the protected broker remains live. Broker loss is unfinalizable in either
design. There is no fallback or mixed run.

Every candidate container uses `AutoRemove=false`, restart policy `no`, an exact platform image and
config digest with no tag resolution or pull, and an exact reviewed resource/mount/security profile.
The profile forbids privileged mode, host PID/IPC namespaces, unreviewed devices or mounts, the
Engine endpoint, and broker control/evidence roots. The exact network/user/capability/seccomp and
case-volume clauses remain policy blockers. Engine access is root-equivalent authority, so a passed
cut additionally requires a dedicated Engine/VM with a protected exclusive-client lease. Labels and
reviewed broker code alone do not enforce that exclusivity.

These are post-run-start launch conditions, not predictions made by initial preflight. After each
create and before start, the broker durably records an inspect receipt and proves the exact profile;
a mismatch is a definite pre-launch harness failure and the container is reconciled without start.

The ordered Docker source-requirement registry is:

| ID | Reserved source requirement |
|---|---|
| `D01` | Linux boot ID and `CLOCK_MONOTONIC_RAW` identity |
| `D02` | collector/broker `/proc` start ticks, opened namespaces/cgroup/executable and executable SHA-256 |
| `D03` | locale-independent Windows/WSL/Desktop backend identity; authoritative source remains blocked |
| `D04` | Linux VM `uname(2)` identity |
| `D05` | raw exact-version Engine `/version` and `/info` request/response captures with API negotiation disabled |
| `D06` | one uninterrupted, no-reconnect raw Engine `/events` observation stream and event tuple digests; advisory, never a completeness cursor |
| `D07` | broker receipt chain binding connection/peer identity, method, versioned path/canonical query, request length/SHA-256, status, response length/SHA-256 and RAW send/receive bracket |
| `D08` | raw container inspect plus in-container process/namespace/cgroup observations |
| `D09` | platform-specific image manifest and config digests |
| `D10` | opened supervisor/child/actuator/oracle/verifier executable SHA-256 values |
| `D11` | raw volume create/list/inspect/remove API responses |
| `D12` | probe-container Mounts inspect plus opened mountinfo/statx/statvfs observations |
| `D13` | separate opened container-root mountinfo/statx observation |
| `D14` | opened cgroup v2 CPU/cpuset/memory/swap/PID files, `prlimit`, and probe `statvfs` |
| `D15` | create-new label/name bracket, opened empty volume root, all-container volume-filter/list/inspect and foreign-reference reconciliation |
| `D16` | exact-intent container list/inspect/wait reconciliation plus independent host-cgroup enumeration |
| `D17` | preopened no-follow cleanup-root statx/statvfs and bounded directory enumeration |
| `D18` | `container_per_process` inert barrier-probe create/start/arm/stop/reconcile proof; recipe remains blocked |
| `D19` | opened read-only fixture source and probe-copy length/SHA-256/logical digest observations |
| `D20` | protected Engine endpoint/proxy/backend/runtime epoch chain with retained process handles; Docker Desktop mechanism remains blocked |
| `D21` | attempt-exclusive VM, external evidence-authority term, provider lifecycle, and protected exclusive Engine-client capability; all remain blocked |

The fact projection is:

| Fact ID | Ordered source IDs |
|---|---|
| `collector_process_identity` | `D01,D02` |
| `windows_wsl_identity` | `D03` |
| `linux_vm_identity` | `D01,D04` |
| `docker_engine_identity` | `D05,D20` |
| `broker_connection_receipts` | `D06,D07,D20,D21` |
| `broker_container_identity` | `D08,D02` |
| `image_executable_bindings` | `D09,D10` |
| `probe_volume_mount` | `D11,D12` |
| `container_root_mount` | `D13` |
| `resource_limits` | `D14` |
| `volume_freshness_source` | `D11,D15,D21` |
| `process_reconciliation_source` | `D06,D16,D20,D21` |
| `cleanup_root_source` | `D17` |
| `broker_barrier_source` | `D05,D06,D07,D08,D16,D18,D20,D21` |
| `fixture_source_identity` | `D19` |

The broker disables redirects, API negotiation, automatic retries and reconnects. A future daemon-
epoch witness must bind the Engine endpoint and complete proxy/backend/runtime server chain to Linux
boot ID, PID namespace, PID, start ticks and executable digest, retain the necessary process handles
through evidence cut, and require `live_restore=false`. Raw exact-version `/version` and `/info`
responses are retained at collection, run-start and evidence-cut, but only a future closed stable
identity projection is compared; volatile fields such as server time, container counts and runtime
telemetry are explicitly excluded from equality and may not become identity. If Docker Desktop's
backend chain cannot be bound through its stable proxy, Docker v1 remains unsupported. An ordinary
container's self namespace and socket-peer observation is insufficient; `D20` must bind the
authorized host helper, its visibility/privileges and every process hop it relies on.

One no-reconnect `/events` connection remains open across the same interval. EOF or reconnect is
terminal, but uninterrupted transport is not a completeness or ordering witness: Docker documents
timestamped event tuples, not a durable cursor, unique event ID or gap detector. Boundary marker
events prove only that those markers were received. Durable intent plus exact request/response,
wait/inspect/list and independent cgroup reconciliation are lifecycle authority; the contiguous
broker sequence orders only receipts the broker actually obtained. Engine ID, volume `CreatedAt`,
event timestamps and a `(Type,Action,Actor.ID,sorted attributes,scope,time,timeNano)` digest remain
audit observations, not daemon epochs or cursors. Docker documents both the explicitly versioned
[Engine API](https://docs.docker.com/reference/api/engine/) and timestamp-streamed
[event endpoint](https://docs.docker.com/reference/api/engine/version/v1.45/). Its
[security guidance](https://docs.docker.com/engine/security/) is why Engine access remains protected
authority rather than a capability supplied to candidate code.

#### Docker minimum predicates

| Check | Frozen minimum predicate |
|---:|---|
| 1 | Every Docker target fact is observed. |
| 2 | Exact reviewed Windows, WSL2, Docker Desktop backend and Linux-VM linkage operands match. |
| 3 | Platform is the reviewed Linux architecture; platform manifest, image config and every opened executable digest match policy. A tag or multi-platform index alone is insufficient. |
| 4 | The closed stable `/version`/`/info` projection and complete `D20` backend epoch chain match between target and initial-preflight cuts, and the advisory event transport has remained connected without reconnect so far. Later cuts must recheck continuity separately. |
| 5 | Boot, process, namespace, cgroup, executable and RAW-clock identities remain stable and equal policy. |
| 6 | Collector, control broker and inert probe use the reviewed unified cgroup v2 placement, namespace relations, controllers and preflight security configuration. Candidate-container configuration is checked after create and before start, not predicted here. |
| 7 | CPU/cpuset/memory/swap/PID/nofile operands equal policy and checked unprivileged probe bytes meet its bound. Exact numeric smoke values remain blocked policy inputs. |
| 8 | The probe is a Docker-managed named `volume` using local scope/driver with empty driver options, distinct from the container-root overlay and on the approved smoke filesystem class. Bind, tmpfs and overlay substitution fail. |
| 9 | Under live `D21` exclusive Engine authority: exact-name pre-create absence, create receipt inside the RAW bracket, exact labels, opened empty root, and `all=true` volume-filter/list plus inspect find zero foreign container/mount references at the cut. Without `D21`, labels and enumeration cannot prove exclusivity. `CreatedAt` alone is audit context. |
| 10 | Opened source is read-only and source/copy byte lengths, SHA-256 values and logical digests match. |
| 11 | Under `D20,D21`, the exact `container_per_process` inert probe reaches create/start/armed, records release commitment before acknowledgement where release is tested, exits/reconciles, and leaves its selected container/cgroup domain empty. Exact recipe and independent emptiness proof remain blockers. |
| 12 | Opened evidence/cleanup-root identities and capacity/entry operands satisfy policy, and probe removal plus post-removal absence is retained. |

Raw Engine API responses, not localized CLI rendering, are the source. Only a complete stable
semantic projection that differs from policy is failed. A malformed, truncated, contradictory,
transport-ambiguous or unavailable source is incomplete under the closed source causes; malformed
submitted evidence remains invalid. Events never replace list/inspect and independent cgroup
reconciliation.

#### Maximum-width evidence and remaining blockers

Final caps are derived from evidence, not guessed from this workstation. Before adding any of the
four schemas or validators:

1. Approve a redb-free supported-host/source inventory, exact machine policy values, per-source
   byte/list/population caps, sampling windows and deadlines.
2. Hand-author, per run class, minimum complete/pass, simultaneous eligible maximum/pass, maximum
   neutral mismatch/failed, every-fact unavailable/incomplete, maximum incomplete, and mixed
   failed-plus-incomplete fixtures. Add one fixture for each legal cause/site and each mutually
   exclusive maximum union arm.
3. Saturate every compatible string, raw-byte, range, keyed-row, list, numeric and tagged-limit
   bound. Independently recompute compact byte length/SHA-256, decoded node/depth counts and expanded
   cardinalities without trusting declared counts.
4. Test exact-at-cap acceptance and cap-plus-one rejection for every dimension. A collector source
   cap-plus-one becomes `source_over_cap` without truncation; a submitted envelope, depth or node
   cap-plus-one is invalid. Cover integer maxima, unsafe conversions, status projections, stable
   same/stable mismatch/intra-capture change and no-delta/positive-delta/regression triples.
5. Only then freeze JSON property layouts, semantic validators and global byte/node/depth caps. The
   current 64-KiB/4,096-node/depth-16 and list suggestions remain non-normative; run-start/raw-
   manifest caps remain separately blocked on literal native 105-row and Docker ten-row goldens.

Native blockers are `N29` protected native-host authority; exact libc/ABI observation; provider
device lease; attribution-probe executable/recipe; complete thermal/kernel/block/NVMe error source
set and exact error-bit/counter registry; marker/barrier transports; raw-source retention; machine
policy bytes and supported-host inventory. Docker blockers are the locale-independent
Windows/Desktop identity source; `D20` complete backend-epoch chain and stable Engine projection;
`D21` dedicated/exclusive Engine authority; exact container security/resource profile; barrier and
independent container/cgroup-empty recipe; numeric resource policy; approved image/executable
values; fixture storage-version authority; raw-source retention; and volume-exclusivity TOCTOU
proof. These blockers prevent a valid passed cut, schema or collector; they do not invite a generic
plugin framework, errno taxonomy, arbitrary sysfs snapshot or guessed cap.

The local Windows/WSL/Docker installation may be used later only for non-gating functional smoke.
It cannot establish native XFS/project-quota/dedicated-NVMe identity, device attribution, production
latency, fault endurance or independent soak. This cycle authorizes no container, candidate or
backend execution.

### Cycle 30 host-class and authority decision

Cycle 30 closes the host-class choice without manufacturing an executable target. Docker
Desktop/WSL is rejected as a formal producer and a fresh, attempt-exclusive native-Linux VM is
selected as the successor host-class direction, while its Engine/authority mechanism and provider
remain unproved. “Exclusive” here does not require physical sole tenancy for the dummy probe.
The native prescreen freezes provider-acceptance requirements but selects no provider. These prose
decisions are not wire values or dispositions; they neither authorize a workflow nor satisfy a
prior-smoke prerequisite.

| Path | Cycle 30 status | Formal result or authority available now |
|---|---|---|
| local Docker Desktop/WSL | development-only; the formal path is rejected | none; it cannot emit `DOCKER_SMOKE_PASS`, `DOCKER_SMOKE_INCOMPLETE` or a native prerequisite |
| frozen Cycle 25/29 Desktop contracts | superseded, unimplemented design history/reference | none; no target/preflight/result schema, literal fixture, protected dispatcher or eligible producer exists |
| attempt-exclusive native-Linux VM and protected Engine endpoint | selected successor host-class direction; mechanism unproved | none; provider, successor wire and `D20,D21` proofs remain absent |
| GitHub-hosted standard `ubuntu-24.04` VM | superseded Cycle 30 feasibility/inventory idea; development-only | none; it lacks the selected external-term, provider-identity and final-absence path |
| native XFS/dedicated-NVMe prescreen host | acceptance contract frozen, provider unselected | no `N20` lease, `N29` attestation, target schema or dispatch authority |
| AWS I4i Dedicated Host | plausible native inventory subject only | no selected account/allocation, image, package, guest inventory, device lease or runner attestation |

#### Why Desktop/WSL cannot carry a gate

The current installation can expose useful development facts, but it cannot satisfy its own frozen
authority predicates:

- `D03` has no selected supported interface that binds the observed Windows, WSL, Desktop and
  `docker-desktop` VM identities to the exact live Engine instance. Docker documents that Desktop
  runs inside its own WSL distribution and makes its CLI available to the Windows user and enabled
  WSL distributions; status/version observations do not mint that binding
  ([WSL backend](https://docs.docker.com/desktop/features/wsl/),
  [Desktop CLI](https://docs.docker.com/desktop/features/desktop-cli/)).
- `D20` cannot retain a complete endpoint/proxy/backend/VM/`dockerd`/`containerd` epoch chain across
  the host/managed-VM boundary. Docker documents the Engine inside a Linux VM and the Windows
  `com.docker.backend` proxy/control plane, but no documented Desktop interface supplies a durable
  daemon epoch or retained process chain
  ([Desktop networking](https://docs.docker.com/desktop/features/networking/)). Engine ID is not
  specified as an epoch. Historical event queries are capped at the last 256 events, while the live
  event stream has no documented durable cursor, unique event ID or gap detector
  ([Engine events](https://docs.docker.com/reference/cli/docker/system/events/)).
- `D21` has no protected exclusive-client capability barrier. Desktop deliberately exposes Engine
  control to the launching user and selected WSL integrations
  ([Windows permission requirements](https://docs.docker.com/desktop/setup/install/windows-permission-requirements/)).
  Docker documents why only trusted users may control the root-equivalent daemon and recommends
  dedicating a Docker server to Docker-managed workloads
  ([Engine security](https://docs.docker.com/engine/security/)). Neither statement supplies a
   sole-client authority. Labels, an uninterrupted event connection and an otherwise quiet `docker ps`
  cannot prove that another same-user client was absent.

The last two conclusions are conservative inferences from the documented interfaces, not claims
that an undocumented Desktop implementation detail can never be inspected. Undocumented internals
are not a production evidence contract. A local developer may later exercise parser, broker,
container-profile and lifecycle mechanics only under a separately reviewed, versioned
development-only identity; none exists now. Such a run must not use either formal smoke literal or
satisfy `state-backend-redb-prescreen-approval-payload/v2`'s `prior_smoke_result`. Current authority
still forbids running the candidate at all.

#### Docker successor invariants and feasibility hypothesis

The selected native-Linux host-class direction uses a protected host-native broker/supervisor and
retains `container_per_process` only for candidate-bearing processes; it has no Desktop proxy or
broker control container. That ownership change is transitive. A successor must version or
explicitly supersede every affected Docker actual-target, preflight, run-start, launch-ledger,
control, raw-manifest, report/evidence-close, result/post-run receipt and run-provenance identity,
plus the prior-smoke capability/consumer binding. It must replace `windows_wsl_identity`/`D03`,
`broker_container_identity`, the Docker check/dependency registry and role/cardinality goldens.
Specifically, it cannot accept `state-backend-redb-prescreen-docker-launch-ledger/v1`,
`state-backend-redb-prescreen-docker-control/v1` or old Desktop target/preflight bytes. Existing
outer-validator fixtures remain regression-only; the missing target/preflight/result fixtures
cannot be implied. `state-backend-redb-prescreen-approval-payload/v2` needs an explicit compatibility
decision and is versioned only if its wire or prior-smoke semantics change. Until the successor and
consumer verifier exist, its exact prior-smoke condition is unsatisfiable.

The successor invariants are one review-bound Engine/runtime epoch, one protected endpoint, a closed
authorized Engine-client set, candidate denial of that capability, no unaccounted daemon/runtime or
client, and fail-closed continuity through final reconciliation. Stable evidence must bind the host
boot and namespaces; broker/Engine/runtime process identities; executable/configuration identities;
the endpoint; exact Engine connections; the external evidence-authority term; and provider
allocation, deletion, and final absence. EOF, process exit,
exec/reload/replacement, endpoint/configuration drift, reconnect, an unexpected process/client or a
reconciliation gap makes the result unfinalizable only when it loses a required connection or the
protected broker, Engine or base runtime, or prematurely loses a later process that must remain live
through its applicable cut. A planned candidate/shim exit followed by exact lifecycle reconciliation
remains legal. Engine events remain advisory.

A private-daemon takeover was the Cycle 30 GitHub feasibility hypothesis. The later attempt-exclusive-VM,
external-supervisor direction below supersedes it without yet becoming an eligible mechanism.
A redb-free probe would inventory and isolate or stop provider-preinstalled Docker services, then
supervise reviewed `dockerd` and `containerd` binaries with private roots and Unix endpoints. It must
demonstrate—not assume—that the opened endpoint has the expected inode without symlink/path
substitution; broker connections reach the retained `dockerd`; `dockerd`'s actual connection reaches
the retained `containerd`; and the containerd namespace/configuration/root/state plus every spawned
shim/runtime identity and executable digest stay bound. The initial cut can bind only the already
existing broker, Engine and runtime processes. Each later shim/runtime is bound after its container
is created and before that process can be released, then retained through its reconciliation and
the applicable evidence cut. Exact process population and the loss-detecting peer-binding mechanism
remain blockers, with restart, replacement, socket-substitution and reconnect hostile fixtures.

`D21` separately requires an attempt-exclusive VM joined to an external evidence-authority term,
provider deletion/final-absence proof, and a proved sole-client capability barrier. One feasibility
construction is a protected direct pathname socket, exact preopened bounded
connection pool, listener sealing/unlinking or an equivalent new-client barrier, a dedicated broker
UID, candidate denial, no other privileged actor during the live evidence-authority term and no
reconnect; another is
an approved loss-detecting connection observer. Either path also needs independent bracketed
peer/socket/process reconciliation. Exact pool cardinality, listener behavior, privileged-process
closure and hostile foreign-client fixtures remain blockers. `SO_PEERCRED` can identify the server
peer of a broker connection; it cannot prove that foreign clients were absent. Unix permissions,
provider authority and the protected dispatcher each cover different parts of the proof.

Cycle 30 considered the standard GitHub-hosted `ubuntu-24.04` runner as a first inventory subject;
Cycle 33 supersedes it as a formal mechanism target. GitHub documents a new VM per hosted job and
passwordless `sudo` on Linux, which remains useful only for non-evidentiary development inventory
([hosted runner lifecycle](https://docs.github.com/en/actions/how-tos/manage-runners/github-hosted-runners/use-github-hosted-runners),
[hosted runner specification](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)).
Those facts and the setup-log ImageVersion are copied lifecycle/inventory observations, not an
attempt-exclusive VM authority, provider-authenticated VM identity or attestation. Public and
private standard runners have different CPU/RAM classes and both publish only 14 GB SSD, so the
exact resource class must be bound. The image also changes over time and its exact software version
is learned from the job setup log
([runner-images versioning](https://github.com/actions/runner-images#readme)).
Formal eligibility instead waits for the protected workflow to dispatch an independently identified
GCP/AWS prescreen target, plus the provider/run/VM identity receipts, proved Engine/runtime and
sole-client mechanism, source inventory, raw retention and complete `D20,D21` hostile fixtures below.
The workflow-runner orchestration direction remains approved; using GitHub's standard hosted VM as
the evidence target and candidate execution remain unapproved.

#### Native provider and source-authority acceptance

Cycle 30 selects no native provider or host. The checked-in `linux-nvme-v3` object is explicitly
`candidate_unapproved`, `qualification_eligible=false`, has null image/package identities, and is a
Fjall/RocksDB numerical qualification proposal. Its `aws`/`i4i.2xlarge` text and numerical operands
are neither a redb target policy nor observed supported-host inventory. The local Windows/WSL
inventory is also ineligible.

Current AWS documentation makes I4i a plausible later inventory subject because the family offers
local NVMe instance storage and supports Dedicated Hosts
([storage-optimized specifications](https://docs.aws.amazon.com/ec2/latest/instancetypes/so.html)).
AWS defines a Dedicated Host as a physical server dedicated to the customer's use with placement
and affinity controls, but also permits optional cross-account capacity sharing
([Dedicated Hosts](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/dedicated-hosts-overview.html)).
Consequently a product page, SKU, tag, copied API response or guest observation proves neither a
selected unshared allocation nor a live campaign/device lease. No account, Region, Availability
Zone, host, tenancy, instance, image, package or device value is frozen here.

Before a provider can be selected, a dated operations-owned decision must bind all of:

1. the authenticated allocation/placement API and reviewed official contract revision, plus one
   physically dedicated and unshared host allocation with no unaccounted instance during a campaign;
2. immutable image, package and build-ABI identities and the provider-to-guest boot, process, mount
   and NVMe identity chain;
3. a linearizable operations/device lease acquired before target capture, held through evidence cut
   and cleanup, and carrying exact acquisition, renewal, loss, fencing, replacement and release
   behavior;
4. live provider/dispatcher attestation binding that allocation and lease to the `collection_id`,
   outer runner, boot and collector process, with freshness and TOCTOU checks; and
5. a complete redb-free supported-host/source inventory covering every `N01`--`N29` dependency:
   observed availability and bounded capture for selected host interfaces; exact
   `N05,N25,N26,N27` sources and privileges; reviewed `N19,N21,N28` recipes; and separately live
   `N20,N29` authorities.

The decision feeds the existing `contract/target-identity.json` role; it is not a thirtieth packet
row, a serializable bearer capability or a generic provider adapter. Copied content, repository
review, an instance identity document, tags, sysfs, file locks or namespace equality alone cannot
mint `N20` or `N29`. A selected provider would authorize only provider-specific policy/source
contract work, not a schema, collector, dispatch or redb run.

For `N29`, the protected dispatcher reobserves the allocation, lease and runner facts live, then
holds a non-serializable, freshness-bounded, single-use capability scoped to the collection and run.
The capability cannot be reconstructed from the dated selection decision, an identity document or
copied provider response. No child dispatch occurs while authority is unavailable or stale. Loss,
replacement or drift after authority-dependent execution begins makes the run unfinalizable and
prevents a pass or sealed result; it is never reclassified as a candidate failure.

The additional authority/recipe blockers requiring explicit closure are:

| Class | Source IDs | Missing proof |
|---|---|---|
| source, authority or supported-host blocked | `N05,N20,N25,N26,N27,N29` | libc/ELF identity; live device lease; thermal, loss-detecting kernel/block and NVMe health sources; live native-host attestation |
| recipe selected, platform/build/wire blocked | `N19,N28` | owner choice between the recommended N19 sequenced-packet redesign and the Cycle 32 assembly direction; qualification of the selected mechanism/helper build; sequenced-packet/credential/pidfd/cgroup support; protected FD installation; deadlines, caps, raw retention, schemas and hostile fixtures |
| recipe blocked | `N21` | bounded redb-free attributed write/sync probe |
| endpoint chosen, inventory/caps/raw retention blocked | `N22,N23,N24` | whole-device block-stat registry; cgroup `cpu.stat`; hierarchical `memory.events` capture |

Cycles 31--33 freeze the provider-neutral `N19,N28` semantic recipes and eligible implementation
directions below, then record the Cycle 33 engineering recommendation to replace the `N19`
assembly direction subject to owner acceptance. They do not make either
source executable or pass-capable. `N21` and final `N22`--`N24` contracts wait for the selected
host's block/filesystem/kernel inventory. This table is not the entire
inventory: all `N01`--`N29` remain subject to supported-host availability/cardinality, complete raw
capture, caps, retention and machine policy, and `N13` specifically needs proof of the selected
host's NVMe namespace/controller interfaces. An absent counter is never encoded as zero, a
timestamp/regex journal scan is not a loss-detecting kernel cursor, and guest-local state cannot
replace provider authority.

#### Repository inventory no-import decision

The repository audit found no supported target or cap authority:

| Existing material | Eligible reuse | Prohibited inference |
|---|---|---|
| `linux-nvme-v1/v2` | immutable validator/model regressions | target values, policy or cap authority |
| unapproved v3 and unmaterialized v4 direction | candidate-neutral workload/product portions after separate review | Fjall/RocksDB comparison, store or maintenance fields; redb machine values, host selection, source maxima or execution authority |
| dated WSL/Docker capability report | negative boundary and read-only inventory-command patterns | current identity, native evidence or formal `D03,D20,D21` proof |
| existing redb content validators/fixtures | strict-JSON, descriptor, digest, locator and cap/cap-plus-one test patterns | target/preflight layouts or status projections, per-source caps or maximum bodies |
| retained-evidence closure algebra and 2-GiB outer bound | final set/count/checked-sum reasoning | raw-role cardinality or any individual raw/document cap |

Caps follow evidence rather than preceding it. A provider-backed native and formal-Docker source
inventory must first fix complete source cardinalities, privilege and worst-case valid bytes; raw
retention must then fix exact-byte versus descriptor treatment, roles, locators, media types,
secret/privacy exclusions, durability, immutable-store provider/version, atomic publication,
retention and freshness/TOCTOU. Only then can hand-authored minimum, simultaneous-maximum,
mismatch, every-cause/site and cap-plus-one fixtures derive source/list/document limits. No existing
constant is a shortcut.

### Cycle 31--33 probe recipes and Docker-successor compatibility

Cycle 31 froze provider-neutral semantic mechanisms. Cycle 32 narrows their eligible implementation
and successor-placement directions. Cycle 33 recommends replacing the `N19` assembly direction and
source-audits a narrow Docker successor, but neither change has the outstanding owner or mechanism
proof needed for selection. These cycles add no executable or numeric policy authority. State names below are
explanatory, not a serializable vocabulary. Both probes are redb-free, execute only during
first-stage collection after protected admission, and mint no candidate, run or result authority.
Their implementations, exact wire encodings, caps, deadlines, fixtures and supported-host proofs
remain absent, so neither source can currently be observed or pass.

#### `N19` representative shared-marker recipe (current blocked v1 direction)

Under the current Cycle 32 v1 direction, `N19` remains shared memory and is blocked on owner
acceptance and qualification. If both owners select the Cycle 33 sequenced-packet recommendation,
this subsection becomes superseded design history and no v1 assembly evidence is accepted by the
successor. A sole `eventfd` replacement is rejected for this gate because it would
test a kernel counter rather than the shared publication visibility used by the crash frame; `N28`
separately tests IPC and barrier liveness. This is not a general rejection of `eventfd`. Linux
documents that `MAP_SHARED` updates are visible to other processes and that a `memfd` can be sealed
and transferred between processes
([`mmap(2)`](https://man7.org/linux/man-pages/man2/mmap.2.html),
[`memfd_create(2)`](https://man7.org/linux/man-pages/man2/memfd_create.2.html)). Rust 1.95 specifies
atomic ordering for threads in one program; neither its atomic API nor rustc's lowering through the
pinned LLVM revision supplies a normative happens-before relation between independently executed
processes
([Rust 1.95 atomic source](https://github.com/rust-lang/rust/blob/1.95.0/library/core/src/sync/atomic.rs),
[rustc LLVM atomic lowering](https://github.com/rust-lang/rust/blob/1.95.0/compiler/rustc_codegen_llvm/src/builder.rs),
[LLVM atomic memory model](https://github.com/rust-lang/llvm-project/blob/1cb4e3833c1919c2e6fb579a23ac0e2b22587b7e/llvm/docs/Atomics.rst)).
Consequently, direct Rust `AtomicU64` plus ordinary Rust payload access is rejected, even when the
generated instructions happen to look suitable. C++ recommends address-free lock-free atomics for
communication through shared memory between processes, but that recommendation does not
normatively establish this complete atomic-plus-ordinary-payload publication chain; a thin C/C++
wrapper therefore does not close the gap
([C++20 lock-free atomics](https://timsong-cpp.github.io/cppwp/n4868/atomics.lockfree)).

The selected direction is an explicitly target-qualified Linux/x86-64 System V ABI, not a portable
Rust guarantee. A tiny separately assembled, non-inline shim owns each complete publication or
observation transaction. The helper and collector pass only local scalar inputs and receive copied
local scalar outputs across opaque external calls; Rust creates no reference, atomic object or plain
load/store to either shared slot. The exact assembly source, assembler and linker identities, flags,
object bytes, final ELF function bytes, call sites, optimized LLVM IR around those calls and final
disassembly must be bound and independently reviewed. LTO, whole-program rewriting, identical-code
folding, an out-of-line atomic fallback and any unreviewed shared-memory access are forbidden. The
target policy must bind CPU vendor/family/model/stepping, microcode, kernel, ABI, toolchain,
virtualization/hypervisor identity and the matching live `N29` provider/host attestation and may
admit only combinations independently qualified for the exact bytes. Intel and AMD architecture
manuals, Linux mapping semantics and the closed single-writer lifecycle form the target argument;
they do not create a language-portable result
([Intel 64 and IA-32 manuals](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html),
[AMD64 architecture manuals](https://docs.amd.com/api/khub/documents/sfvvekC9mDflu6vd3R0NXA/content)).

The protected first-stage collector named by `collector_process_identity` is the sole object owner
and observer. It is neither the protected outer runner nor the later restartable campaign worker.
Before spawning that collector, the outer runner starts a separate bounded first-stage-bootstrap
lifecycle, creates one collection-bound preflight-probe leaf under its already-open dedicated probe
parent, verifies its collection binding, parent, domain policy, `pids.max=1` and emptiness, retains
independently opened parent and leaf handles, and only then atomically installs duplicates plus its
own pidfd as fixed collector bootstrap descriptors. That lifecycle covers leaf creation, collector
spawn, failed-spawn cleanup and, after the collector exits, final empty-leaf removal. The collector
joins the pidfd to the outer runner's full process tuple before N19. No N19 acknowledgement or
otherwise undeclared outer-runner/collector channel exists. The outer runner also retains the
collector pidfd so it can perform safety cleanup if the collector dies; it receives no marker
mapping. The leaf is a first-stage bootstrap resource, not an N19-owned object; at most one reviewed
inert preflight helper may occupy it, and it is proven empty between helpers.

The selected lifecycle is exact at the semantic level:

1. Before the native actual-target cut, the collector creates one fresh anonymous `memfd` with
   exactly `MFD_CLOEXEC|MFD_NOEXEC_SEAL`; the latter implies sealing and establishes the exact initial
   seal set `{F_SEAL_EXEC}`. This avoids dependence on the PID-namespace `vm.memfd_noexec` default
   ([non-executable memfd](https://docs.kernel.org/6.18/userspace-api/mfd_noexec.html)). Absence of
   `MFD_HUGETLB` and `MAP_HUGETLB` is required; the recipe makes no physical-page/THP claim. The
   collector obtains the positive checked `sysconf(_SC_PAGESIZE)` result, requires it large enough
   for the frozen layout, and performs one checked `ftruncate` from zero to exactly that length. The
   zero-filled extension supplies initial bytes; no userspace code writes bytes 0--15 during
   initialization ([`truncate(2)`](https://man7.org/linux/man-pages/man2/truncate.2.html)).
2. Offset zero is one naturally aligned raw 64-bit sequence slot. Offset eight is one naturally
   aligned raw 64-bit payload slot. They are not Rust `AtomicU64` or `u64` objects. Both begin at
   zero; every later byte is reserved zero. The collector creates only a read-only, non-executable
   `MAP_SHARED` observer mapping. Every live or post-exit read
   of either slot calls the observer shim with an expected sequence. The exact shim performs one
   aligned sequence `movq`, returns mismatch without touching the payload, or, on equality, executes
   `lfence` and one aligned payload `movq`, then copies both values out to caller-owned memory. The
   initial cut calls it with expected zero and the published cuts with expected one. Every write to
   either slot occurs inside the publisher shim. Rust may hash reserved bytes outside the first 16
   bytes but may never construct a typed or bytewise view that reads or writes either slot. The
   actual-target fact binds the helper's and collector's matching shim observations after final
   sealing and before advance. Mixed-width, overlapping, reset, wrap, byte-copy and object-reuse
   accesses are forbidden. Before transfer the collector adds `F_SEAL_GROW|F_SEAL_SHRINK` and
   verifies exact size and seals.
3. The collector opens and hashes the bound `redb-prescreen-supervisor` executable and creates one
   private `AF_UNIX` `SOCK_SEQPACKET|SOCK_CLOEXEC|SOCK_NONBLOCK` pair. It directly spawns one reviewed
   inert marker-helper mode into the precreated preflight-only cgroup outside the campaign parent.
   Before helper spawn, the collector freshly verifies the installed leaf handle's collection-bound
   identity, parent, policy and emptiness and joins that cut to the outer runner's retained pre-
   collector-spawn verification. Only then may the collector use
   `clone3(CLONE_PIDFD|CLONE_INTO_CGROUP)` with `clone_args.exit_signal=SIGCHLD`. The collector
   is single-threaded from clone through reap, binds the clone-caller TID, keeps SIGCHLD waitable and
   has no competing reaper. The leaf is domain type with `pids.max=1`;
   its exact `cgroup.procs`, `cgroup.threads`, `pids.current` and descendant-directory population is
   checked at every live cut. The async-signal-safe stub range-closes every
   unintended descriptor, installs only the control endpoint, sets and race-checks
   [`PR_SET_PDEATHSIG=SIGKILL`](https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html),
   and executes the retained handle without a path reopen or post-spawn
   cgroup migration. The non-set-ID/non-file-capability helper permits no credential transition and
   post-exec rechecks `PR_GET_PDEATHSIG==SIGKILL` plus the exact parent PID/TID relationship. The
   helper receives no database, workspace, device, provider, Engine, candidate or release authority.
4. After joining the helper's boot/PID-namespace/PID/start-ticks/executable/cgroup tuple to its
   retained pidfd, the collector transfers the `memfd` exactly once with `SCM_RIGHTS`. The helper
   receives it with `MSG_CMSG_CLOEXEC`, verifies identity/size/seals/layout, creates the only shared
   read/write/non-executable mapping, closes the descriptor, observes and reports the sequence and
   payload through the exact observer shim, then parks. The collector verifies helper mappings/
   descriptors and adds
   `F_SEAL_FUTURE_WRITE` followed by `F_SEAL_SEAL`. The final exact set is
   `{F_SEAL_EXEC,F_SEAL_GROW,F_SEAL_SHRINK,F_SEAL_FUTURE_WRITE,F_SEAL_SEAL}`. The kernel rejects later
   writable mappings and descriptor writes while preserving the helper's existing mapping
   ([file seals](https://man7.org/linux/man-pages/man2/F_GET_SEALS.2const.html)).
5. Seals alone do not prove sole-writer authority. The helper's single-thread/no-fork/no-exec/no-
   migration profile, closed process/privilege and mapping/holder populations, and denial of ptrace,
   `/proc/*/mem` and `userfaultfd` mutation are conjunctive eligibility requirements. The actual-
   target fact then freezes collection/layout identity; the outer/collector opened probe-parent and
   leaf identities plus holder provenance; retained object identity; the frozen `fstat`/seal subset,
   exactly `(st_mode & S_IFMT) == S_IFREG`, no `S_IXUSR|S_IXGRP|S_IXOTH` bit, the observed remaining
   permission-bit value, `st_dev`, `st_ino`, `st_size`, `st_uid`, `st_gid` and the complete seal set;
   owner/helper
   tuples and holder/mapping sets; helper-reported and collector-observed initial sequence and
   payload; reserved-byte digest; and RAW offset. Only that subset is compared later. `st_atime`,
   `st_mtime`, `st_ctime`, allocated-block counts, link count, names, descriptor numbers
   and `/proc` display text are audit-only. Initial nonzero or disagreeing values remain complete
   observations and do not make the fact unavailable.
6. If either observer reports a nonzero initial value or their pairs disagree, preflight sends no
   advance, retains the adverse operands, retires/reconciles the helper, and check 8 fails. Otherwise
   it sends exactly one advance carrying
   the same decoded 32-byte `collection_id` and probe sequence zero. In one opaque call, the publisher
   shim writes payload one with an aligned `movq`, loads expected sequence zero into the compare
   accumulator, and executes exactly one `lock cmpxchgq` that attempts to change sequence zero to
   one. It returns the prior sequence and success bit through caller-owned memory. Any mismatch is
   adverse; there is no retry or second write. The helper parks without another slot access. The
   collector's observer shim requires sequence one and payload one; stable frozen `fstat`/seal subset
   and unchanged reserved-byte digest are rechecked while the helper lives.
7. The collector sends one retire command, observes exact clean exit with deadline-aware pidfd
   polling followed by nonblocking `waitid(P_PIDFD,...,WEXITED|WNOHANG)`, and reaps exactly that
   `SIGCHLD` child. It repeats the shim sequence/payload observation and frozen `fstat`/seal-subset/
   reserved-byte-digest checks after exit, closes the control pair, unmaps the observer, closes the
   anonymous object, proves no unexpected holder/mapping/descriptor remains and reobserves the retained
   bootstrap leaf stably empty. Only then may preflight retain the observation. `N19` finishes before
   `N21` or an attribution/quiet bracket. The protected empty leaf remains under the separate first-
   stage-bootstrap lifecycle. There is no semantic resend/replay, reconnect, replacement, same-
   collection restart, marker-object reuse or helper reuse.

Control packets carry collection, probe sequence and expected transition and use nonblocking
`sendmsg`/`recvmsg`. Receivers enable `SO_PASSCRED`; they require exactly one kernel-validated
`SCM_CREDENTIALS`, while reviewed sender behavior proves that no explicit credential body was
supplied. Only the one declared `SCM_RIGHTS` transfer is legal. Wrong credentials, unexpected
ancillary data, missing `MSG_CMSG_CLOEXEC`, truncation, duplicate/out-of-order/trailing packet or EOF
is adverse. Credentials corroborate endpoint provenance and retained pidfd/process identity; they do
not replace either. Exact packet bytes and bounded continuation after EINTR/EAGAIN only while no
packet has committed remain blocked; it cannot authorize a semantic resend.

One RAW total deadline starts immediately before `memfd_create` and ends only after every N19-owned
resource is closed and the bootstrap leaf is reobserved empty. The enclosing first-stage-bootstrap
deadline starts before leaf creation and covers collector-spawn failure, collector loss and final
leaf removal; its numeric operand and exact bootstrap encoding remain blocked. Every setup,
target-held, advance, observation, retire and reconciliation transition also
uses one stage deadline:
`state_deadline = min(state_entry_raw + n19_stage_deadline_ns,
n19_start_raw + n19_total_deadline_ns)`, with checked addition and inclusive equality. No wait syscall
may block past the remaining bound. Numeric operands are not guessed before supported-host evidence.
Expiry is `deadline_exceeded`; pidfd/cgroup safety stop and reconciliation continue under a separate
bounded cleanup deadline but cannot rescue a pass.

On collector loss, the helper's parent-death signal and the outer runner's retained-leaf
`cgroup.kill=1` plus reconciliation are conjunctive safety paths; on outer-runner loss, the collector
stops and reconciles its helper. Either
authority loss prevents a preflight singleton. An authenticated live collector uses pidfd SIGKILL
and reaping on an adverse helper path; a bare PID is never used. Failure to prove process/cgroup,
mapping, descriptor and socket closure is unfinalizable. A stable attributable predicate mismatch is
`failed`; the existing closed causes cover unavailable, ambiguous, over-cap, identity drift,
regression/wrap, overflow and deadline evidence. Malformed submitted content is invalid. No path is
a candidate failure.

Required hostile fixtures include nonzero initial sequence or payload; unchanged/multi-step/
regressed sequence; missing, late or wrong payload; publication after CAS; second, unlocked or
retried operation; wrong width/alignment/instruction order; direct Rust/C/C++ shared access;
missing/extra shim call; private/executable/extra writable
mapping; missing/extra/floating seals, wrong `MFD_NOEXEC_SEAL`, resize or reserved-byte mutation;
object/helper reuse, fork, thread, exec or migration; clone-caller parent-thread exit; cleared or
mismatched parent-death signal after exec or a forbidden credential transition; wrong
process/cgroup/collection/credentials;
packet loss, duplication, reorder, truncation and extra descriptors; every stage/total/cleanup
deadline boundary and overflow; collector/runner/helper loss; early exit; and leaked object, mapping,
socket, process or cgroup population/descendant. Build fixtures reject extra or missing symbol calls,
text or call-target relocations affecting the shim/admitted entry points, alternate entry points,
shared-slot references outside the shim, wrong final bytes,
writer payload-after-CAS, missing reader fence, wrong instruction width or order, and any linker/
runtime fallback. The exact `1.95.0-x86_64-unknown-linux-gnu` Rust build and separately pinned
assembler/linker must reproduce the approved final ELF and disassembly; source or object inspection
alone is insufficient.

Before `N19` can become available, an independent validation team must run a redb-free,
cross-process publication campaign against those exact bytes on every admitted CPU/kernel/microcode
class. Each iteration creates one fresh `memfd` and never resets or reuses its slots. Independent
`exec` processes map it at independently selected virtual addresses, use read/write publisher and
read-only observer mappings, exercise all exposed/admitted core placements, including cross-NUMA or
cross-socket placement when present, plus scheduler load, preemption and migration, and retain every
iteration and environment identity under derived time/resource bounds. Observing sequence one with
payload other than one even once rejects that target tuple. Reversed-publication, `MAP_PRIVATE`, different-object
and synthetic-forbidden-observation fixtures validate harness sensitivity but cannot promote the
ABI merely because a deliberately weak variant happened not to fail. Miri, sanitizers, same-process
threads and zero forbidden outcomes are not substitutes for the target argument or independent
campaign. This is a required mechanism qualification, not the later independent backend soak and
not evidence about redb.

Owners must explicitly accept or reject this platform ABI before implementation. If they reject it,
if another architecture is required, or if any admitted build cannot preserve the exact boundary,
`N19` and every later crash marker require one versioned redesign using normative process-shared
POSIX synchronization or kernel IPC; there is no silent runtime fallback. POSIX explicitly defines
memory-synchronizing operations across threads or processes, but a process-shared mutex/semaphore or
message-passing design changes mapping authority, crash recovery and latency and therefore cannot be
substituted under these bytes
([POSIX Issue 8 memory synchronization](https://pubs.opengroup.org/onlinepubs/9799919799/basedefs/V1_chap04.html)).
Replacing only `N19` with `eventfd` is forbidden because it would leave the crash-marker contract
unproved.

##### Cycle 33--34 N19 and crash-coverage owner decision packets

The engineering recommendation is **reject the assembly ABI as the durable default and select a
versioned Linux `AF_UNIX`/`SOCK_SEQPACKET` redesign**. This is a recommendation, not the missing
workload-owner and operations-owner acceptance. The Cycle 32 assembly remains a technically
credible direction eligible for target-specific qualification; it is not qualified today or
selected merely because its instructions are fast.
N19 runs once during preflight and the crash markers run only in qualification trials. Neither
mechanism is on LaminarDB's record, timer, join, checkpoint or state-access hot path, so its
specialist CPU/microcode/hypervisor/toolchain burden is not justified by lower marker overhead whose
magnitude is unmeasured.

| Dimension | Cycle 32 assembly ABI | Recommended kernel-IPC redesign |
|---|---|---|
| correctness basis | exact x86-64 instructions plus CPU coherence and target/build audit | kernel-mediated, ordered, record-preserving packet delivery |
| language boundary | correctness depends on audited assembly owning every shared-slot access | no shared marker memory or cross-process language-memory claim |
| failure surface | a bad visibility result can silently corrupt evidence | pressure, interruption, truncation, order, EOF and peer failures are explicit |
| post-crash evidence | supervisor rereads a retained memfd | supervisor drains already queued packets through EOF after pidfd-confirmed exit |
| target envelope | every CPU/microcode/hypervisor/toolchain tuple separately qualified | initially the same Linux/x86-64 GNU envelope, without ISA-memory-model dependence |
| maintenance | source/object/IR/ELF/disassembly and ISA review | bounded frame/state-machine, syscall and kernel review |
| perturbation | smallest marker cost | one bounded send and possible scheduling point per transition; dummy-probe measurement required |
| product hot path | none | none |
| default | only if owners deliberately accept permanent narrow-platform support | **recommended** |

The redesign reuses N28's already selected transport mechanism, not its socket or predicate. Each
helper or crash trial gets one precreated, nonblocking `SOCK_SEQPACKET` pair. The supervisor owns the
complete immutable intent and every timestamp. A child first verifies the intent digest, then sends
only collection/trial identity, intent digest, stage and a monotonic sequence. Successful enqueue of
one complete packet by the child's send is the publication cut. That cut becomes admissible evidence
only when `SCM_CREDENTIALS.pid` equals the expected child TGID after translation into the receiver's
frozen PID namespace and its UID/GID equal the expected child real UID/GID after translation into
the receiver's frozen user namespace. The retained pidfd separately joins that child to boot ID,
PID-namespace inode, start ticks, executable, cgroup, collection and role. The unprivileged child has
no credential-changing capability or transition, and global descriptor/OFD reconciliation proves it
is the sole sender-end holder while the supervisor is the sole receiver-end holder. Its syscall
profile admits only exact connected-endpoint
`sendto(fd,frame,frame_len,MSG_DONTWAIT|MSG_NOSIGNAL,NULL,0)` and closes `sendmsg`, `sendmmsg`,
io_uring send/register routes and every other ancillary-injection route. The receiver enables
`SO_PASSCRED` and accepts exactly one kernel-generated `SCM_CREDENTIALS` record; missing or
additional ancillary data is adverse. The supervisor timestamps
receipt, so trigger delay is measured from receipt rather than retroactively pretending to timestamp
enqueue. A packet is never reconnected, replayed, shared between trials, duplicated or passed onward.
Whether a pre-enqueue `EINTR` is retried must be one frozen bounded rule, not an implementation
choice. After pidfd-confirmed child exit the supervisor drains queued complete packets through
expected EOF before classification. `EAGAIN`,
`EPIPE`, continuation after an unapproved interruption, partial/mismatched length,
`MSG_TRUNC`/`MSG_CTRUNC`, wrong credentials, duplicate/reordered stage, trailing packet or early EOF
is adverse. If selected, N19 would become the one-way live/post-exit publication predicate; N28
would remain the separate bidirectional barrier/liveness predicate
([`unix(7)`](https://man7.org/linux/man-pages/man7/unix.7.html),
[`send(2)`](https://man7.org/linux/man-pages/man2/send.2.html)).

A process-shared semaphore or robust mutex is not preferred: both retain writable shared
synchronization state and add recovery/mutation semantics to the post-exit proof. One `eventfd` is
also insufficient because its counter has no stage identity; multiple eventfds would recreate a
second framing protocol
([POSIX process-shared semaphore](https://pubs.opengroup.org/onlinepubs/9799919799.2024edition/functions/sem_init.html),
[`pthread_mutexattr_setrobust(3)`](https://man7.org/linux/man-pages/man3/pthread_mutexattr_setrobust.3.html),
[`eventfd(2)`](https://man7.org/linux/man-pages/man2/eventfd.2.html)).

The redesign must also stop calling a pre-call witness `commit_entered`. No external marker proves
entry into redb internals. Cycle 34 recommends **an in-adapter `adapter_commit_entered` direction**
over a caller-side `commit_call_imminent` direction, subject to the same two-owner decision as the
transport. The two choices have the same qualification-only packet count and no product hot-path
cost, but only the adapter-entry marker eliminates the unobserved scheduling gap before the adapter
is invoked:

| Property | Recommended adapter-entry direction | Caller-side alternative |
|---|---|---|
| proven cut | approved adapter method entered | child intends to invoke adapter |
| redb-internal progress | none | none |
| coverage name | `adapter_entry_observed && return_not_observed` | `call_imminent_observed && return_not_observed` |
| no-return oracle | exactly old or exactly complete-new | exactly old or exactly complete-new |
| current confirmed-in-commit gate | must be renamed and versioned | unsatisfied |
| current decision-bearing large-recovery campaign | blocked pending a separate internal-progress decision | cannot complete |

In the recommended direction, the exact approved non-inlined adapter method emits
`adapter_commit_entered` as its first externally observable action. Function-prologue mechanics are
allowed before it; allocation, logging, clock reads, candidate/file access and other application
effects are not. Successful complete-packet enqueue is the entry cut. Marker failure is adverse and
the adapter must not invoke redb. After the send returns, the next application operation is the exact
candidate commit call. Immediately after that call returns, the adapter's first observable action
emits `adapter_commit_returned_ok` or a separately frozen error stage and then parks without drop.
After final EOF, absence of a valid return packet means only **return not observed**; it never proves
that redb did not return. The Cycle 33 credentials, pidfd, sole-endpoint-holder, frame, ordering,
receipt-timestamp, EOF and failure predicates apply unchanged.

The successor oracle requires exactly old state for a deliberate pre-entry control, exactly old or
complete-new state after valid adapter entry with no valid return packet, and exactly complete-new
state after a valid successful-return packet. Torn, mixed, corrupt and out-of-domain states follow
the existing terminal proof. Return-error, missing/ambiguous evidence, marker failure or identity/
state-machine failure requires a separately frozen fail-closed classification and cannot be silently
treated as an atomicity outcome. Supervisor acknowledgement strengthens observation ordering only;
it does not strengthen durability.

Every current `commit_entered && !candidate_returned` claim must become
`adapter_entry_observed && return_not_observed` under successor bytes. Assembly-v1 populations,
seeds, adaptive retries, trigger schedules and prior-smoke evidence do not transfer automatically.
The resulting cut supports only a separately versioned diagnostic such as "reopen after kill timed
from adapter entry." Neither branch proves redb-internal progress, so the current nine-trial
decision-bearing recovery comparison and its two/five-second gates remain blocked until owners
choose candidate-specific internal instrumentation or explicitly redefine and revalidate the
scientific question and thresholds.

No compatibility is claimed before the owners choose a mechanism and the crash-classifier branch is
closed. If they select the sequenced-packet direction, the fail-closed minimum identity effects are:

| Surface | Assembly-v1 disposition | Minimum sequenced-packet successor |
|---|---|---|
| decision-class protocol | incompatible because N19 and crash-marker cuts are normative | `state-backend-redb-prescreen/v2` |
| protected-review policy | current instance/literals name the v1 decision contract | successor policy `/v2` |
| approval payload and pre-run receipt | payload `/v2` requires protocol v1; receipt `/v2` binds that payload | payload `/v3` and receipt `/v3` |
| target identity and N19 preflight/marker evidence | shared-memory mechanism and observation fields are normative | successor target identity and N19 evidence `/v2` |
| crash control/witness, run-start/manifest/campaign, classifier/oracle, evidence-close/result and prior smoke | `commit_entered` and its coverage/oracle meaning are normative | separately frozen successor identities, no lower than `/v2` |
| source/build artifacts | roles can remain only if IPC code is embedded in already bound binaries | new bytes, digests, SBOM and build evidence under the successor payload |

Assembly-v1 mechanism or campaign evidence can never satisfy an IPC-v2 predicate. Keeping the IPC
logic inside already approved binary roles avoids a new artifact row, but does not avoid semantic
versioning. A standalone IPC helper/trust object must be an explicit successor approval input; it
cannot be smuggled into the current 29-row payload.

Both owners must eventually accept one identical canonical transport object. If they select the
recommended sequenced-packet branch, they must also accept one identical compatible crash-coverage
object. Every object is canonical UTF-8 with keys in the shown order, no whitespace outside strings,
no BOM and no trailing newline. The recommended transport bytes are:

`{"schema_version":"state-backend-redb-n19-mechanism-decision/v1","notice":"DESIGN_ONLY_NO_IMPLEMENTATION_OR_EXECUTION_AUTHORITY","scope":"N19_AND_CRASH_MARKER_TRANSPORT","decision":"REJECT_N19_X86_64_ASSEMBLY_ABI_V1_AND_SELECT_SEQPACKET_REDESIGN_V2"}`

The transport alternative changes only `decision` to
`ACCEPT_N19_LINUX_X86_64_SYSV_ASSEMBLY_ABI_V1_DIRECTION_ONLY`. It cannot be paired with the
sequenced-packet-scoped crash object below. Selecting that alternative leaves implementation
blocked until owners separately freeze and review an assembly-compatible crash-coverage object;
neither this packet nor earlier assembly campaign semantics supplies one implicitly. The
recommended sequenced-packet crash-coverage bytes are:

`{"schema_version":"state-backend-redb-crash-coverage-decision/v1","notice":"DESIGN_ONLY_NO_IMPLEMENTATION_OR_EXECUTION_AUTHORITY","scope":"SEQPACKET_CRASH_COVERAGE_BOUNDARY","decision":"SELECT_ADAPTER_COMMIT_ENTERED_V2_DIRECTION_ONLY","coverage_claim":"ADAPTER_ENTRY_OBSERVED_AND_RETURN_NOT_OBSERVED","redb_internal_progress_claim":"NONE","large_recovery_authority":"BLOCKED_PENDING_SEPARATE_INTERNAL_PROGRESS_DECISION"}`

The sequenced-packet crash-coverage alternative changes only `decision` to
`SELECT_COMMIT_CALL_IMMINENT_V2_DIRECTION_ONLY` and `coverage_claim` to
`CALL_IMMINENT_OBSERVED_AND_RETURN_NOT_OBSERVED`. On the sequenced-packet branch, the selected
objects are embedded under the future
`contract/target-identity.json` fields `n19_mechanism_decision` and `crash_coverage_decision`, not
added as artifact rows. Workload-owner and operations-owner acceptance must be two distinct
protected review events over the same immutable repository head containing these exact protocol
bytes. That design review is not the later pre-run execution approval; the future `/v2` target
identity, approval payload `/v3` and two-role receipt `/v3` independently bind both selected objects
before any dispatch. A separate decision artifact or helper trust object must be an explicit row in
that successor payload rather than an implicit file.

Every transport or crash-boundary choice explicitly denies implementation, mechanism/candidate
execution, provider/container workflow, backend selection, cluster admission, production use and
soak authority. Implementation remains blocked until the owners accept one transport object and a
compatible separately frozen crash-coverage object. The IPC frame, queue bounds, deadlines,
exact retry/commit rules, credentials/pidfd/holder proof, exact adapter boundary/build audit,
entry/return/error frames, successor schemas/classifier/oracle/schedule, perturbation limits,
hostile fixtures and the separate large-recovery progress decision remain absent. A redesign probe
does not replace backend qualification or independent product soak.

#### `N28` one-shot broker/barrier recipe

`N28` selects anonymous socket pairs rather than a pathname or abstract listener. The topology has
one known outer runner, collector and inert helper; a listener would add bind/connect, pathname,
unlink, late-client and reconnect surfaces without a required dynamic client. Linux
`SOCK_SEQPACKET` preserves ordered message boundaries, and `SO_PASSCRED` supplies sender credentials
on each received message
([`unix(7)`](https://man7.org/linux/man-pages/man7/unix.7.html)). `SO_PEERCRED` is not the peer proof
because on a pre-fork socketpair it reports credentials from socketpair creation, not necessarily the
later endpoint holder. Even per-message credentials remain corroboration because a privileged sender
can nominate some credential values; endpoint provenance, pidfd/full process identity and live
`N29` privileged-population authority remain mandatory.

The semantic transcript is:

1. Before spawning the collector, the protected outer runner creates one anonymous
   `AF_UNIX`/`SOCK_SEQPACKET|SOCK_CLOEXEC|SOCK_NONBLOCK` pair and atomically installs only the intended
   endpoint in each process, along with immutable `n28_start_raw` and deadline operands, and proves
   every duplicate closed. The outer runner covers pair creation, collector-spawn failure and cleanup
   under that same total deadline. Each side independently retains the other's pidfd and full boot/
   PID-namespace/PID/start-ticks/executable/cgroup tuple. Both receivers enable `SO_PASSCRED`; each
   receive requires exactly one kernel-validated `SCM_CREDENTIALS`, while reviewed sender behavior
   proves that no explicit credential body was supplied. `MSG_TRUNC`, `MSG_CTRUNC`, `SCM_RIGHTS`,
   extra control records and extra packets are forbidden. The actual-target fact freezes endpoint/
   open-holder provenance, both retained process identities, the dedicated probe-parent identity,
   exact expected child set, absence of the policy-derived N28 relative leaf locator and `cgroup.stat`
   descendant/dying-descendant baseline, exact descriptor population, the common RAW origin,
   `n28_start_raw`, policy-bound total/stage operands and idle sequence-zero state before an N28
   request or helper exists. Those counts remain neutral observations, but check 27 requires
   `nr_descendants` to equal the enumerated child-set cardinality and `nr_dying_descendants=0`.
   Checked ordering requires
   `raw_origin <= n28_start_raw <= actual_target_start_raw`; the later transcript must equal every
   frozen value.
2. Once, after actual-target capture and before run-start, the collector sends a request binding the
   native class, collection ID, actual-target descriptor/digest, its `N03` identity, and expected
   outer-runner/helper builds, exact dedicated probe-parent identity, policy-derived relative leaf
   locator and create-new sibling-leaf policy; it cannot bind a not-yet-created leaf inode. The
   decoded `collection_id` is the challenge nonce and probe sequence is zero, disjoint from campaign
   ordinals. The request is non-authorizing data
   accepted only inside the runner's already-retained admitted collection context. That bounded
   context permits this one inert helper; no packet reconstructs admission, mints an operation or
   borrows a later campaign/child-dispatch lease. There is no semantic resend/replay or reconnect.
3. The outer runner is the launch broker; no additional broker process or listener exists. It creates
   one temporary domain leaf exclusively at the bound relative locator under the opened dedicated
   probe parent, sets `pids.max=1`, and returns its opened device/inode and effective policy. The
   collector independently opens it relative to its retained parent handle, verifies identity/policy/
   emptiness and the exact parent diff, and retains a cleanup handle before acknowledging preparation.
   No helper may be spawned before that acknowledgement. The broker then creates a second anonymous
   sequenced-packet pair under the same flags/credential/ancillary rules and uses
   `clone3(CLONE_PIDFD|CLONE_INTO_CGROUP)` with
   `clone_args.exit_signal=SIGCHLD` to place one reviewed `redb-prescreen-supervisor` inert barrier-
   helper mode directly in the leaf. The broker is single-threaded from clone through reap, binds the
   clone-caller TID, retains the helper pidfd, keeps SIGCHLD waitable and has no competing reaper. The
   async-signal-safe child stub installs only the barrier endpoint, sets and race-checks the same
   [`PR_SET_PDEATHSIG=SIGKILL`](https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html)
   parent-thread contract, and execs the retained executable handle. Fork-then-migrate and path reopen
   are forbidden. The non-set-ID/non-file-capability helper permits no credential
   transition and post-exec rechecks `PR_GET_PDEATHSIG==SIGKILL` plus exact parent PID/TID identity.
   No database/workspace, actuator, candidate payload, device, provider, Engine or release capability
   reaches the helper.
4. The broker reports the observed leaf and helper tuple. Before arm, the collector opens its own
   pidfd for that exact PID, rechecks PID reuse/start ticks/executable/cgroup/descriptor identity, and
   acknowledges that its independent cleanup guard is live. The broker then sends one arm challenge
   and accepts one matching armed echo after its credentials join the retained helper identity. The
   armed cut requires a live helper pidfd, domain leaf, `pids.max=1`, exactly the helper in
   `cgroup.procs` and `cgroup.threads`, `pids.current=1`, no descendant cgroup, the exact descriptor
   set, and no release intent/send/acknowledgement. The reviewed seccomp/permission profile denies
   thread/process creation, cgroup migration and cgroup-control access.
5. Without a release transition, the broker sends `SIGKILL` through its helper pidfd, uses deadline-
   aware pidfd polling and nonblocking `waitid(P_PIDFD,...,WEXITED|WNOHANG)`, and requires
   `CLD_KILLED/SIGKILL`. It observes helper-pair EOF and closes that pair. Both sides require
   `cgroup.events populated=0`, empty `cgroup.procs` and `cgroup.threads`, `pids.current=0`, no child
   cgroup and no live/unaccounted process. The broker sends one stop receipt containing the exact
   wait and empty-task observations. The collector independently observes its helper pidfd readable,
   joins the stopped identity and empty leaf, closes its helper pidfd plus every leaf/`cgroup.kill`
   handle, and sends one guard-release acknowledgement. Only after that acknowledgement may the
   broker close its helper pidfd and every leaf-specific handle, remove the leaf through its retained
   probe-parent handle, and require the exact frozen child set plus `nr_descendants` and
   `nr_dying_descendants` observations, with the former equal to that set's cardinality and the latter
   exactly zero. A voluntary/premature exit, leaked handle or lingering dying cgroup cannot satisfy
   the planned stop.
6. The broker then sends one final receipt carrying the same nonce/sequence, ordered RAW offsets,
   collector/broker/helper tuples and credentials, armed-without-release proof, exact wait result,
   stop/guard-release transcript and helper/task/helper-socket/pidfd/leaf-handle/cgroup closure, and
   closes its collector endpoint. The collector must receive exactly that receipt, then EOF, close
   its own endpoint, recheck the retained live broker identity and probe-parent baselines, and prove
   no duplicate endpoint. This post-receipt EOF/close is part of the observation and deadline; the
   receipt cannot claim its own transport was already closed. `N28` runs after the
   `N21` attribution probe but before the final CPU/memory/thermal/kernel/block/device quiet brackets
   and preflight end cut, so its helper activity cannot make those observations stale.
7. Collector loss is detected by the broker's retained pidfd and endpoint; it stops/reaps the helper,
   uses `cgroup.kill=1` as an adverse-path backstop, and reconciles without a pass. Broker loss is
   detected by the collector's retained pidfd and endpoint; the helper's parent-death signal plus the
   collector's helper pidfd and retained-leaf `cgroup.kill=1` are conjunctive safety paths. After a
   guard-release sequence begins, the collector retains its probe-parent handle and exact leaf
   locator/identity: on broker loss it either securely reopens and reconciles the still-present leaf
   or proves removal and the frozen parent baselines. If broker loss occurs before identity/handle
   handoff, the collector compares the bounded parent enumeration with the frozen set, opens the exact
   policy-derived new locator when present, applies `cgroup.kill=1`, proves it empty/descendant-free,
   closes every opened handle, removes it and restores the parent baselines. Any other diff is
   unfinalizable and every new child is safety-reconciled under the cleanup deadline. It can prove
   exit/empty task state but cannot
   manufacture the missing parent's wait receipt, so no preflight singleton results. Simultaneous
   authority loss is likewise unfinalizable. There is no replacement, adoption or same-collection
   resume.

One RAW total deadline begins immediately before creating the outer-runner/collector control pair and ends only
after the post-receipt EOF/close/recheck. Each setup, target-held, request, prepare, spawn, arm, stop,
receipt and closure transition uses
`state_deadline = min(state_entry_raw + n28_stage_deadline_ns,
n28_start_raw + n28_total_deadline_ns)` with checked addition and inclusive equality. No blocking syscall
may overrun the remaining bound. On expiry or loss, safety stop and task/cgroup/socket reconciliation
continue under a separate bounded cleanup deadline; cleanup cannot rescue a pass. Numeric operands
and the nonblocking EINTR/EAGAIN continuation rule while no packet has committed remain policy-
blocked; continuation cannot authorize a second semantic packet. A complete trustworthy negative is
a failed predicate; the existing seven
source causes and precedence cover incomplete collection. Unknown cleanup prevents any valid
singleton, and no N28-specific status or capability is introduced.

Hostile fixtures cover wrong/replayed nonce or sequence; duplicate/reordered/extra/short/oversize/
truncated packets; explicit, missing, duplicate or forged credentials; unexpected descriptors;
endpoint/holder substitution; PID reuse and broker/helper exec or cgroup drift; wrong leaf/policy/
locator/parent diff/task/fd population; broker loss before leaf handoff; helper spawn before the
collector guard; descendant/thread creation or migration; clone-caller parent-thread exit;
cleared or mismatched parent-death signal after exec or a forbidden credential transition; arm
before independent guard; missing/late/duplicate arm; any release transition; collector, broker or
helper loss at every state; parent-death race; deadline equality/plus-one/overflow; blocking-wait
overrun; signal/wait mismatch; missing/early/duplicate guard-release; leaked helper pidfd or leaf/
`cgroup.kill` handle; `cgroup.kill`/empty/removal failure; descendant/dying-descendant or parent drift;
N28 after a final quiet cut; target/preflight RAW-start/deadline mismatch; final receipt before helper
reconciliation; and pass before outer-pair EOF/close. Exact builds, protected FD install, kernel/seccomp/cgroup support,
privileges, security profile, deadlines, caps, wire/raw roles and live `N29` remain blockers.

#### Docker-successor compatibility map

No Desktop-era wire is promotable by changing its producer. In the table, **conditional** means the
existing schema or semantic may survive only after an explicit compatibility proof; **version**
means preserve purpose under a successor identity; **replace** means the old topology concept is
invalid; and **reject** means historical/regression input only. Reusing a role, locator, fact/check
label or outcome word never implies byte compatibility.

| Existing surface | Required successor action |
|---|---|
| `state-backend-redb-prescreen/v1` | Keep: the decision class and protocol ID do not encode the rejected Desktop topology. Topology-sensitive child objects are versioned below. |
| protected-review-policy `/v1` | Keep the schema; a new exact policy instance binds the selected protected workflow, GitHub review-export contract and environment. The compute/VM provider belongs to target policy and live authority, not this receipt's fixed provider kind. |
| approval payload `/v2` | Keep under the closed Cycle 32 placement rule: exactly the existing 29 rows, with no new standalone successor approval-input artifact, and only a reviewed successor smoke result satisfying `prior_smoke_result`. |
| protected-review receipt `/v2` | Keep the pre-run shape because its exact payload binding and review semantics do not change; the reserved post-run branch remains unimplemented and may not be inferred. |
| Docker actual-target and preflight-cut `/v1` | Version and reject all old Desktop bytes. |
| run-start binding and raw-run manifest `/v1` | Version: target/preflight, authority kinds, process/runtime tuples and raw-role population change. |
| Docker launch-ledger and control `/v1` | Replace the broker-container ownership model, version both purposes and reject `/v1` bytes. |
| reserved result-payload `/v1`, result/post-run receipt and evidence-close/report envelopes | Version or explicitly supersede; no existing result producer or complete schema can be inherited. |
| retained-evidence root and artifact-index namespaces | Conditional only after a successor registry/schema proves exact descriptor closure and cardinality. |
| legacy payload/receipt `/v1` and result `/v1` | Reject as synthetic regression input. |
| `DOCKER_SMOKE_PASS`, `DOCKER_SMOKE_INCOMPLETE`, `docker_smoke_no_decision` | May be explicitly readopted only inside a new result identity; no current producer may emit them. |
| dispatch and child-dispatch capabilities | Replace/rebind to the exact successor provider/VM, target, run-start and live `D20,D21` recheck. |
| root and launch-ledger leases | Replace control-container semantics with host-native broker/endpoint/runtime ownership. |
| Docker-prerequisite capability and consumer | Version or supersede; old Desktop evidence cannot satisfy it. |
| approval-input storage capability | Conditional if the approval-input set remains byte-compatible; it supplies no runtime authority. |
| Docker run-provenance authority | Missing: freeze an exact successor capability and live verifier rather than implying one. |

The existing subject/source/build/fixture role strings and locators may remain only where their
purpose, media type and validation semantics are unchanged; their exact current bytes remain bound
by length and SHA-256. Old Desktop bytes themselves remain ineligible. Topology-sensitive contracts,
plans, policies, configuration and goldens require new reviewed bytes. Cycle 32 places each
successor identity in an existing approval row without reinterpreting that row:

| Successor identity/input | Sole approval owner |
|---|---|
| host-native broker/collector/helper/gate modes and embedded assembly/BPF objects | formal source, Cargo lockfile, SBOM and build manifest describe them; the existing supervisor executable descriptor binds their final host and container-gate bytes |
| child, actuator, oracle and verifier | their existing executable descriptors and the same source/build closure |
| expected `dockerd`, `containerd`, shim, OCI-runtime, proxy and closed helper executable set, ELF interpreter/shared-object closure, package/image provenance, kernel/config/BTF/module and BPF-JIT policy, Linux VM image and boot chain | `contract/target-identity.json`; later target/preflight evidence rehashes and joins the exact live instances |
| exact static daemon/runtime configuration bytes and argv/environment, descriptor roles, private-root/socket/cgroup locator policies, creation order, observer schemas/predicates and broker-pool plan | `contract/execution-plan.json`; only genuinely static generated files may be canonical bytes derived solely from this object and rehashed live |
| OCI manifest/config/layer digests and preloaded content-store identity | expected values in `contract/target-identity.json`; the content store is a target input, never an unlisted approval artifact |
| container create/start request, mounts, namespaces, security/resource profile, inert entrypoint and later candidate arguments | `contract/candidate-configuration.json` |
| external evidence-authority controller and etcd cluster/term identities plus acquisition/renewal/loss receipts; provider allocation/delete authority and VM/boot attestation; loaded kernel/module/BTF and BPF translated/JIT identities; BPF program/map/link instances; runtime FD numbers, PIDs, mount/cgroup/socket identities, daemon/runtime processes, socket peers/holders and content-store observations | versioned successor actual-target, preflight, run-start, launch-ledger, control and result evidence; copied observations carry no authority, never serialize the live renewal stream/lease handle or provider mutation credential, and never imply a VM execution fence |

Per-run shim bootstrap messages, OCI bundles/configs, resolved descriptors, PIDs, inodes, mounts,
cgroups, sockets and runtime-created paths cannot be pre-run artifacts or be claimed as derived
solely from a static plan. Approval binds their exact generator/gate implementation, schemas and
closed validation predicates; the generated bytes and observed identities are committed in the
versioned successor launch ledger before candidate release.

An embedded object must be byte-contained in the bound supervisor executable as well as described by
source/build/SBOM records; merely mentioning an external file in a manifest is not enough. If the
implementation requires a new standalone approval-input broker, gate, BPF object, daemon package,
OCI archive or other object, the exact 29-row set no longer closes and approval payload `/v3` is mandatory.
The same is true if `prior_smoke_result` ceases to mean one reviewed formal successor smoke over the
identical common inputs. Subject to those fail-closed conditions, payload `/v2`, its pre-run receipt
`/v2`, protected-review-policy schema `/v1` and protocol ID `/v1` remain compatible. This is a
content-placement decision, not schema implementation or permission to source target binaries from
unreviewed locations.

Every raw binary/JSON header and role/cardinality golden must bind the versioned run-start. The
current Docker fact and ordered-check registries are rejected wholesale: conceptually reusable facts
and predicates move into a new registry, `windows_wsl_identity`/`D03` and
`broker_container_identity` are replaced, and `D02,D07,D08,D20,D21` are re-specified. Candidate
topology remains `container_per_process`; `same_container_pidfd` and control/job-container ownership
remain rejected.

#### Redb-free `D20,D21` feasibility decision matrix

Cycle 31 posed these mandatory questions. Cycles 32--33 select and refine the narrowest mechanism
direction that could answer them; every row still needs a later redb-free probe and no row is
eligible evidence today:

| Gate | Required question | Minimum sufficient proof | Status |
|---|---|---|---|
| `D20.1` takeover | Can the VM prove every preexisting Engine/containerd/shim/OCI-runtime/proxy/network or other configured helper process, endpoint, container, private-root/state object and relevant cgroup absent or isolated before target capture? | Closed process/socket/service/container/cgroup/root-state inventory tied to provider attempt, VM and boot, plus configuration-derived proof that no omitted helper kind can exist. | dedicated fresh-VM/private-root direction selected; provider proof absent |
| `D20.2` broker to Engine | Can every preopened broker connection be bound through the exact protected socket object to the retained reviewed `dockerd` without path/symlink substitution? | Endpoint/open-connection/server-process identity plus retained process handle across the cut; `SO_PEERCRED` alone is not exclusivity. | dockerd-created private listener and fixed broker-pool direction selected; probe absent |
| `D20.3` Engine to containerd | Can the complete actual live dockerd/containerd/shim control-connection population, not merely configuration text or one sampled connection, be bound to retained reviewed processes? | Closed socket/peer/process graph plus namespace, executable, configuration and private root/state identities, including allowed lazy connections and fail-on-reconnect rules. | Cycle 34 classic-`vfs` graph and exact plugin-ID hypothesis recorded; default snapshotter path rejected; physical inventory/population and probe absent |
| `D20.4` later runtime set | Can approval bind every executable/configuration/private-root input before spawn; loss-detecting supervision capture every later short- or long-lived shim, OCI runtime, proxy, network helper, daemon helper and re-exec from creation/exec; and the candidate remain inert until every live chain is joined and every already-exited helper is historically reconciled? | Reviewed and attributed setup effects; accepted transcript with zero observer loss from private-daemon start through final cut; pre-spawn executable/config/root bindings; inert-until-joined candidate bootstrap; container/cgroup linkage and legal planned-exit handling. A post-create snapshot is insufficient. | BPF history plus source-derived runc/BuildKit/AppArmor/shim population recorded; exact reachability/order, target security state and probe absent |
| `D20.5` continuity | Conjunctively with `D20.1`--`D20.4`, can restart, replacement, exec/reload, socket substitution, reconnect, required-process loss and every observer gap fail closed? | Redb-free hostile fixtures for each transition and observation-loss point; Engine events remain advisory. | fail-on-transition direction selected; fixtures absent |
| `D21.1` VM authority | Can a single-writer external term be joined live to the exact workflow attempt and provider-authenticated VM/boot identity, with freshness, loss, fail-closed guest stop and provider deletion? | Live external evidence-authority lease plus provider attestation and bounded delete/final-absence proof; a copied term, setup-log image version or “new VM per job” is insufficient, and the lease does not fence VM execution. | GCP plus external etcd term conditionally recommended for owner review; fixed-deadline GCP is a weaker alternative; exact pins, timing and probe absent |
| `D21.2` client-capability barrier | Can the Engine listener be closed to new clients and every existing connection/open-file-description holder be confined to one exact bounded broker set? | Proved atomic connect-admission seal plus unlink barrier, exact pool and holder population, no reconnect, and denial/detection of duplicate, fork inheritance and `SCM_RIGHTS` passing. | exact three-client 22/23-request schedule plus precreate/TSYNC/BPF-seal/reconcile/unlink direction recorded; keepalive and mechanism probe absent |
| `D21.3` sole-client observation | Can every foreign Engine connection or connection-capability holder be detected without gaps throughout authority? | Bracketed peer/process/holder reconciliation is sufficient only conjunctively with the proved `D21.2` no-new-client/no-new-holder barrier; continuous process/socket observation remains required for barrier integrity. | `UNIX_DIAG` plus closed-holder and lifecycle-history direction selected; probe absent |
| `D21.4` privilege closure | Can every actor able to reach/recreate the endpoint or duplicate/pass a client capability be kept in a closed trusted set while candidates are denied it? | Broker identity, post-barrier capability confinement, external evidence-authority term, provider allocation/delete authority, and continuously enforced or loss-detected privileged-process/holder closure; permissions and bounded snapshots alone are insufficient. | confinement direction selected; provider/privilege proof absent |
| `D21.5` hostile proof | Do same-user/root foreign connection, inherited/duplicated/`SCM_RIGHTS`-passed client, endpoint replacement and reconciliation-gap attempts prevent finalization? | Exact redb-free hostile fixtures with fail-closed expected outcomes. | absent |

#### Selected attempt-exclusive-VM supervision and Engine-client direction

This direction is a conditional GO only for a later redb-free, dummy-only mechanism probe. It is a
NO-GO for Docker smoke or candidate execution. It trusts the selected kernel/hypervisor and a closed
set of reviewed privileged actors; an actively malicious host root or hypervisor is outside this
contract and would require a separate confidential-VM or external-hypervisor trust design. A same-
user/root hostile fixture tests whether an *unexpected* actor is detected and poisons the attempt; it
does not claim to withstand a trusted root that deliberately removes the observer.

Across the three examined providers--GCP, AWS and Azure--Cycle 33 finds no single primitive that
atomically supplies the whole required chain:

`protected workflow attempt -> renewable evidence-authority term -> VM instance -> guest boot/image -> stop/delete and final-absence proof`

Provider identity/attestation, a coordination lease and a delete API are separate authorities. A
database/blob lock does not stop a VM, an attestation token does not renew a lease, and an
asynchronous delete request is not final absence. The provider shortlist is therefore frozen only
for a later provider-API prescreen; no provider, account, project, Region, zone, SKU, image, host,
lease service or controller is selected.

| Provider direction | Useful provider primitive | Unclosed external-term/execution-cessation gap | Current disposition |
|---|---|---|---|
| GCP AMD-SEV Confidential VM; sole-tenant only if later policy requires physical-host identity | nonce-bound managed attestation; optional sole-tenant `serverId` and instance inventory; absolute provider-scheduled `terminationTime` with `DELETE` | deletion may begin up to 30 seconds after the absolute trigger; no cessation/final-absence bound is documented, and the trigger is not atomically coupled to an external term | **conditional shortlist; strongest absolute deletion-trigger backstop among those examined** |
| AWS NitroTPM/Attestable AMI; Dedicated Host only if later target policy requires it | nonce-bearing measured-boot attestation, signed instance identity plus explicit host placement/inventory and controls that disable host recovery/maintenance | no provider absolute deletion trigger or bounded cessation/final absence has been identified; attestation, instance/AMI/host identity, external term and termination must be joined | **conditional shortlist; strongest image/no-replacement direction among those examined** |
| Azure Trusted Launch/Dedicated Host | signed nonce-bearing IMDS attestation, host/VM inventory and optional finite Blob lease | Blob lease fences blob mutation rather than VM execution; host and exact custom-image measurement are not one attested claim, and Guest Attestation adds a privileged service/extension | hold |

The comparison is grounded in current provider interfaces
([GCP Confidential VM attestation](https://cloud.google.com/confidential-computing/confidential-vm/docs/attestation),
[GCP token claims](https://cloud.google.com/confidential-computing/confidential-vm/docs/token-claims),
[GCP runtime limit](https://cloud.google.com/compute/docs/instances/limit-vm-runtime),
[GCP sole-tenant inventory](https://cloud.google.com/compute/docs/reference/rest/v1/nodeGroups/listNodes),
[AWS NitroTPM attestation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nitrotpm-attestation.html),
[AWS Attestable AMI](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/attestable-ami.html),
[AWS Dedicated Hosts](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/dedicated-hosts-understanding.html),
[AWS signed instance identity](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/verify-iid.html),
[AWS Dedicated Host maintenance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/dedicated-hosts-maintenance-configuring.html),
[AWS Dedicated Host recovery](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/dedicated-hosts-recovery-enable.html),
[Azure IMDS attestation](https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service),
[Azure Dedicated Hosts](https://learn.microsoft.com/en-us/azure/virtual-machines/dedicated-hosts),
[Azure Dedicated Host inventory](https://learn.microsoft.com/en-us/rest/api/compute/dedicated-hosts/get?view=rest-compute-2025-04-01),
[Azure Guest Attestation](https://learn.microsoft.com/en-us/azure/virtual-machines/boot-integrity-monitoring-overview),
[Azure extension security](https://learn.microsoft.com/en-us/azure/virtual-machines/extensions/features-linux),
[Azure Blob lease](https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob)).

Cycle 34's focused
[external execution-term options](../reports/redb-prescreen-external-execution-term-2026-07-25.md)
recommend, for owner review only, composing GCP's absolute deletion trigger with an external etcd
lease plus CAS acquisition-transaction-revision term. A fixed, nonrenewable GCP deadline is the
simpler alternative but does not satisfy the current renewable-term requirement. No etcd
deployment/version/client, controller, provider tuple, numeric deadline or mechanism is selected or
executable.
The recommended minimum is one unleased durable gate plus one lease-attached live key; its term is
`(cluster_id, acquisition_txn_revision, lease_id, attempt_nonce)`. Lease expiry removes only the live
key, leaving a non-closed gate that blocks a successor until provider absence is independently
reconciled. Etcd invalidates evidence authority and drives cooperative stop/delete; it does not fence
GCP VM execution directly.

The later prescreen must bind a fresh challenge to workflow run/attempt, dispatcher identity,
provider instance, guest boot, immutable image/PCR policy and the current term; make uncertainty,
expiry or revocation stop guest work and issue the provider DELETE request within the controller's
stated bound; retain an absolute provider deletion-trigger backstop; and reconcile the delete
operation to terminal state plus live instance/inventory absence. Neither issue-DELETE bound nor
provider trigger is a proved execution-cessation or final-absence bound. Watch is absent from the
safety path and transport keepalive alone is advisory;
live authority requires bounded linearizable reads/CAS. A provider without even the absolute
trigger, including the current AWS direction, remains more weakly conditional. Every examined
provider remains blocked until the accepted contract and a mechanism probe establish cessation/
final-absence treatment. Maintenance, emergency failure, preemption, replacement and restart must
never silently create a successor epoch. IAM principals, guest agents, login/serial/repair paths,
autoscaling, sharing and alternate start/delete authority must form one reviewed closed set.
Provider audit logs are retained retrospective evidence, not live authority or an execution fence.

This D20/D21 dummy-mechanism VM is deliberately distinct from the eventual N20/N29 native storage
qualification host. Because this threat model trusts the selected hypervisor, a physically
dedicated host and dedicated NVMe are not requirements for a redb-free Engine-authority probe.
They may remain mandatory for later device/latency qualification. Conversely, passing a sole-tenant
provider inventory says nothing about NVMe attribution, backend tails or production soak. GitHub's
protected-review/workflow identity also stays separate from the external authority-controller term
and provider allocation identity; they are joined by fresh run-attempt challenge equality, never
treated as one principal.

Provider policy belongs in `contract/target-identity.json`; static acquisition, renewal,
fail-closed guest-stop and provider-deletion predicates belong in `contract/execution-plan.json`;
signed attestations, live
instance/host inventory, boot ID, term receipts, notices, termination operation and final absence
belong in versioned live evidence. The controller's live renewal stream/lease handle and provider
mutation credentials remain nonserializable and do not become a VM execution fence. The
recommended etcd composition necessarily adds approval-input trust,
configuration, role and controller identities, so current approval payload `/v2` cannot authorize
it. If the N19 sequenced-packet direction is selected, those inputs must be co-frozen in its already
required payload/receipt `/v3`; otherwise a separately reviewed successor identity is still
required.

`D20` uses this closed sequence:

1. The dispatcher establishes the external evidence-authority term, and the provider supplies a fresh,
   attempt-exclusive native-Linux VM whose authenticated instance, image and boot identity bind the
   protected workflow attempt and current term. System Docker/containerd services are absent or
   masked, no foreign login or management agent has Engine authority, and fresh mutable daemon/exec roots,
   sockets, cgroups and result state begin absent under retained parent handles. A separately
   approved read-only preloaded image/content source may exist only at its exact target-policy
   identity; it is never conflated with those fresh mutable roots. Docker Desktop and WSL
   remain useful only for non-evidentiary syntax/development checks: their hidden VM/service/client
   population, virtual storage and missing dedicated XFS/NVMe authority cannot satisfy `D20,D21` or
   the native target.
2. Before any daemon starts, the reviewed supervisor installs the exact pinned BPF LSM and lifecycle
   programs, preallocated maps, one global ring buffer, a sticky poison bit and a checked loss
   counter whose saturation or overflow also poisons the attempt.
   The closed event set includes process creation before child wake, executable admission, successful
   exec/exit and the Unix-socket/capability transitions required by `D21`. The exec hook admits only
   approval-bound executable identities and denies an exec when its record cannot be committed;
   creation-history loss that cannot be denied sets poison before candidate release. Program, map
   and link identities remain retained and reverified. Ring-buffer reservation can fail and its
   producer/consumer queries are only transient diagnostics, so zero explicit loss, an ordered
   supervisor challenge record and reconciled process/cgroup snapshots are all required
   ([BPF LSM](https://docs.kernel.org/bpf/prog_lsm.html),
   [BPF ring buffer](https://docs.kernel.org/6.6/bpf/ringbuf.html),
   [Linux fork path](https://github.com/torvalds/linux/blob/master/kernel/fork.c)).
3. The supervisor starts exactly one private `containerd`, retains its pidfd and complete process/
   namespace/cgroup/executable/configuration/root/socket identity, then starts exactly one private
   `dockerd` against that explicit containerd socket. `dockerd` creates the sole filesystem
   `AF_UNIX` Engine listener in a retained private directory; socket activation is rejected because
   peer credentials/PIDFD can identify the listener creator rather than prove that dockerd created
   it ([Linux `SO_PEERPIDFD`](https://github.com/torvalds/linux/commit/7b26952a91cf65ff1cc867a2382a8964d8c0ee7d)).
   No TCP or abstract Engine endpoint exists. The closed broker API contains no build, pull, plugin,
   swarm or network-management endpoint; daemon egress is denied, the candidate uses no container
   network or published port, CDI is disabled with an empty closed source set, and live restore,
   configuration reload/SIGHUP, automatic restart and unplanned runtime discovery are denied or
   loss-detected. Exact Engine API, namespace/egress/filter, signal, image-store, root and runtime-
   path controls are approval inputs. Docker
   documents both explicit external-containerd and isolated-root/no-network daemon configuration,
   but also calls multiple daemons experimental, so no system daemon may coexist
   ([`dockerd` reference](https://docs.docker.com/reference/cli/dockerd/)).
4. Configuration names every admissible shim and OCI-runtime path. The supervisor executable's
   reviewed multi-call shim-gate and runc-gate modes validate and durably commit the exact bootstrap
   message, argv/environment, working directory, inherited descriptors, OCI bundle/config, requested
   rootfs/mount inputs and real approved executable before preserving the pinned protocol
   and execing the official binary. A pre-hash is insufficient: committed configuration bytes must
   be immutable through the official consumer's open, requested mount objects must join the later
   observed mount results, and any stable-handle/namespace/write-denial gap makes the gate
   infeasible. The gate must preserve every version-specific stdin/stdout, descriptor, signal,
   subcommand and exit semantic. For the Cycle 33 proposed containerd 2.2.6 tuple, shim-start input
   remains the exact legacy argv/environment plus runtime-options-on-stdin channel, while the
   official runc-v2 start helper returns JSON `BootstrapParams` version 3 with `ttrpc`. Only that
   pairing is admissible; the still-accepted raw-address output fallback is poison. Containerd 2.3's
   protobuf bootstrap *input* is a different contract and is not silently substituted. No gate or
   evidence transfers across a containerd pin
   ([containerd 2.2.6 shim manager](https://github.com/containerd/containerd/blob/v2.2.6/cmd/containerd-shim-runc-v2/manager/manager_linux.go),
   [containerd 2.2.6 fallback parser](https://github.com/containerd/containerd/blob/v2.2.6/core/runtime/v2/shim.go),
   [containerd Runtime v2](https://github.com/containerd/containerd/blob/v2.2.6/core/runtime/v2/README.md)).
   Global BPF history remains mandatory because a path gate alone cannot prove that every process
   creation route was covered.
5. The created container initially runs only the exact static inert gate mode of the bound
   supervisor binary as PID 1. It reports one nonce-bound armed state and cannot open a database or
   exec the candidate. Before release, the host drains history through the ordered challenge,
   reconciles every already-exited helper and joins every live daemon/shim/runtime/gate to its
   approved binary, input, pidfd, cgroup and container. A durable release commitment precedes one
   capability consumption; only then may the gate close its channel and exec the exact candidate
   child. Docker Engine events are retained corroboration, never process authority.
6. The lifecycle observer, poison state, daemon/runtime pidfds, cgroups, Engine/containerd peers and
   gate history remain live through final reconciliation. Reload, re-exec, replacement, reconnect,
   observer detach/loss/overflow, unexpected executable or process, required-process loss, endpoint
   substitution or unknown planned-exit state makes the attempt unfinalizable and triggers bounded
   safety stop. There is no resume or replacement. These controls run on process/control events,
   not LaminarDB's per-record path; their startup/recovery and steady resource cost still require
   measurement before the smoke design can be accepted.

`D21` adds one capability barrier to that process authority:

1. A separate broker process mode of the exact supervisor binary precreates the complete bounded
   set of unconnected raw HTTP/1.1 Engine client sockets before the barrier. Pool cardinality and
   endpoint-to-connection assignment derive from the later closed API plan; there is no general
   Docker client library, lazy socket or connection creation. Before any connect, the broker proves
   its closed thread/descriptor population, applies `no_new_privs` and one reviewed TSYNC seccomp
   filter to every thread, then rechecks both. The filter denies process/thread creation, exec, new
   sockets/socketpairs, descriptor duplication, `sendmsg`/`sendmmsg` and `recvmsg`/`recvmmsg`
   outright, `pidfd_getfd`, BPF and io_uring setup/registration, descriptor-table unsharing and every
   other source-derived holder-creation route while allowing only the bounded initial connects and
   non-ancillary raw-HTTP reads/writes. There is no pre-filter connected-socket window
   ([seccomp filter](https://docs.kernel.org/userspace-api/seccomp_filter.html)).
2. The already-retained BPF Unix-stream connect state begins with the exact collection/broker
   generation, broker process identity, Engine listener identity and exactly the plan-derived number
   of unspent admission tokens. It denies and poisons every other actor, listener or excess attempt.
   Each broker connect atomically consumes one token; the last allowed attempt changes the map to
   sealed before returning from the kernel hook. A failed attempt, count disagreement or inability
   to prove that atomic transition invalidates the VM--there is no retry. Each successful connection
   is joined to the listener object, retained dockerd pidfd and direct `SO_PEERCRED`/
   `SO_PEERPIDFD`; each newly returned peer pidfd is joined and closed, while the supervisor's
   independently retained dockerd pidfd remains. A stacked TSYNC filter then denies `connect` too.
   The exact kernel hook/map operation and atomicity are mandatory probe subjects, not assumptions.
3. Only after the BPF state is sealed does the supervisor close every creator/diagnostic duplicate.
   Under the provider-backed closed privileged-process set, it requests
   `UDIAG_SHOW_NAME|UDIAG_SHOW_VFS|UDIAG_SHOW_PEER|UDIAG_SHOW_ICONS|UDIAG_SHOW_RQLEN` for the exact
   listener, validates the corresponding `UNIX_DIAG_*` response attributes, requires zero pending
   connections and an empty icon set, and joins every established peer socket to the fixed broker/
   dockerd descriptor populations. It then unlinks the pathname relative to its retained directory
   handle and rechecks pathname absence, socket cookies/inodes, exact open-file-description holders
   and peer pairs. Linux reports both pending listener connections and their socket inodes through
   `sock_diag`; unlink removes the name while existing open sockets continue
   ([`sock_diag(7)`](https://man7.org/linux/man-pages/man7/sock_diag.7.html),
   [`unlink(2)`](https://man7.org/linux/man-pages/man2/unlink.2.html)). The atomic BPF seal, not a
   racy `UNIX_DIAG`/unlink pair, is the no-new-client authority. Unlink alone says nothing about
   inherited or transferred descriptors.
4. `D20` separately tracks the complete source-derived dockerd/containerd/shim control-connection
   graph, including every planned lazy connection; no singular sampled gRPC socket stands for that
   population. Before the seal, a privileged verifier may use `pidfd_getfd` only under one nonce-
   bound BPF `file_receive` exception naming the exact verifier, source process and protected file.
   The hook atomically consumes the exception, the verifier joins and closes the duplicate, and the
   supervisor proves revocation before proceeding. Every other protected-file receipt is denied and
   poisoned; future planned lazy runtime connections use the qualified connect/peer history rather
   than another exception. Configuration text alone never satisfies the graph
   ([`pidfd_getfd(2)`](https://man7.org/linux/man-pages/man2/pidfd_getfd.2.html)). The candidate
   receives neither an Engine descriptor nor its host socket namespace. Direct filters are selected
   over seccomp user notification, whose documented TOCTOU limitations make it unsuitable as the
   security policy ([`seccomp_unotify(2)`](https://man7.org/linux/man-pages/man2/seccomp_unotify.2.html)).
5. `/proc` descriptor enumeration, `KCMP_FILE`, `UNIX_DIAG`, BPF history and the closed live process
   set are conjunctive: none alone proves ownership. Any broker connection close, Engine/containerd
   reconnect, new peer/holder, listener recreation, capability duplicate/transfer, observation gap,
   provider-lease loss or privileged-population drift poisons the attempt. Reconnect is forbidden;
   if the exact Docker pin cannot complete the closed endpoint plan over the preconnected pool, this
   mechanism is infeasible rather than eligible for a transparent fallback
   ([`kcmp(2)`](https://man7.org/linux/man-pages/man2/kcmp.2.html)).

#### Cycle 34 Docker/runtime source-closure proposal

The focused
[Docker/runtime source-closure report](../reports/redb-prescreen-docker-runtime-source-closure-2026-07-25.md)
supersedes the stale one-client summary previously carried here. The tuple remains rootful
Linux/amd64 Moby 29.6.2, containerd 2.2.6, its matching `containerd-shim-runc-v2`, runc 1.3.6, and
Engine API v1.55. Tagged source and release identities are research inputs, not executable, image,
configuration, or target identities; nothing was downloaded or executed.

Docker 29's default containerd image-store branch is rejected for this mechanism hypothesis because
unconditional BuildKit construction creates a second external containerd client. The only narrower
direction carried forward explicitly disables `containerd-snapshotter`, `containerd-migration`,
CDI, NRI, daemon/container networking, telemetry export, live restore, and builder GC; it selects
the classic `vfs` graphdriver, a private externally supervised containerd, a sealed executable path,
and a host with AppArmor and `binfmt_misc` absent/empty. `vfs` is chosen only to minimize one inert
dummy probe's dependency surface; it is not a LaminarDB state-backend recommendation.

The expected containerd population is now an exact-ID hypothesis covering content, events,
metadata, GC, leases, mount manager, warnings, shim/runtime-v2 task management, the local
containers/content/namespaces/tasks services, and only the containers/content/events/leases/
namespaces/tasks/version/health gRPC facades. Moby's first container-create path requires a
namespace-label read before its explicit runtime option is applied. The corrected mount-manager ID is
`io.containerd.mount-manager.v1.bolt`. Snapshotter, image, snapshot, diff, transfer, CRI, NRI,
sandbox, restart, streaming, introspection, network, task-monitor, tracing, and mounts-
facade plugins remain excluded. Containerd has no allowlist mode, so tagged source alone cannot
prove this population; any later probe must compare the initialized inventory exactly and reject an
extra component.

Exactly three already-connected Engine sockets remain proposed. `E1` owns one type-and-unique-label
filtered container-event stream, `E2` owns one outstanding wait, and `E3` serializes every finite
control request. The corrected normal order is events; initial version/info and absence checks;
volume and container create; created inspect; start; authenticated non-Engine armed report; running
inspect; wait; post-start version/info; non-Engine release; wait/event; exited inspect; container and
volume cleanup/absence; final version/info. That is exactly 22 requests (`E1=1,E2=1,E3=20`). The
only hostile variant adds one `SIGKILL` request after the post-start cut without releasing the gate,
for 23 (`E3=21`). A reconnect, retry, additional request/action/client, early close, or ambiguous
response poisons the VM.

The source-derived helper set is larger than Cycle 33 recorded: two startup `runc features`
executions; six `runc --version`, six `docker-init --version`, and six containerd Version RPCs across
the three info/version cuts; one native amd64 plus ten non-amd64 BuildKit architecture-check payload
attempts; possible
`apparmor_parser` when AppArmor is active; the transient shim launcher, persistent shim, exact runc
lifecycle children and reverse shim-event publisher connection. The narrow target chooses AppArmor
and `binfmt_misc` absence, but the native payload executes to report the amd64 ISA level and the
`386` probe can still execute through native compatibility. Every attempt/child, lazy connection,
async containerd GC effect, the scheduled BuildKit no-policy GC callback, descriptor, mount,
cgroup, and planned exit remains a runtime-ledger obligation.

This proposal is still **not approval-ready**. Exact executable/ELF/DSO and embedded-probe bytes,
OCI image/PID1, daemon/containerd configuration and parser result, initialized plugins, physical
gRPC/ttrpc graph, request/response goldens, deadlines, keepalive behavior, helper order, security
state, provider/kernel/external-term identity, and a separately approved redb-free hostile dummy
probe remain absent. If the tuple cannot complete the exact schedule using this closed population
and the sealed socket pool, `D20,D21` is infeasible; there is no automatic plugin, helper,
connection, or retry fallback.

#### Cycle 33 kernel-ledger and gate predicate proposal

This freezes the questions and fail-closed outcomes for review, not a kernel implementation or
feasibility verdict. A later mechanism probe must use the following source-derived predicates. Names
identify kernel hooks/cuts, not a promise that an untested BPF program will load on an unselected kernel. Exact
kernel tag/config/BTF, verifier-accepted bytecode, translated/JIT bytes, program/map/link IDs and
call sites remain target identity and probe evidence.

| Cut | Required authority | Explicit limitation and fail-closed rule |
|---|---|---|
| setup | all LSM/trace programs, fixed-capacity maps and one global ring buffer load and attach before either daemon; policy maps are created with target-proven `BPF_F_RDONLY_PROG`, then base collection/executable/actor policy is populated and `BPF_MAP_FREEZE`d; while pre-resolution Engine connects remain denied, the verifier populates the listener-specific policy exactly once after dockerd binds and freezes it before arm; mutable token/poison state is BPF-owned after arm | map freeze blocks later syscall writes but not program writes, so the pinned kernel must separately accept and prove the program-read-only map flag; any unsupported hook/helper/map/flag/atomic operation, attach gap, replacement, mutation, verifier drift or JIT drift is ineligible |
| creation attempt | `task_alloc` checks current parent/phase/clone flags and may deny an inadmissible attempt or an attempt whose record cannot be reserved | this hook precedes PID allocation and cannot identify a committed child; an admission event is not a fork-success event |
| committed child | `sched_process_fork` records parent/child task, PID namespace, PID/TGID, start-boottime, cgroup and role-candidate before `wake_up_new_task` | a tracepoint cannot abort the already committed fork; map/ring failure sets sticky poison synchronously, and the inert gate prevents candidate release |
| exec | `bprm_check_security` admits only a prebound executable object/role, lineage, cgroup and exact interpreter/DSO policy and denies when evidence cannot commit; `sched_process_exec` separately records success | the hook does not hash an ELF; scripts, unknown interpreters/DSOs, fallback path opens, mismatched prebound file identity/gate lineage or missing success record poison; an admission attempt is not exec success |
| lifecycle/cgroup | `sched_process_exit`, `task_free`, `cgroup_attach_task`, `cgroup_transfer_tasks`, and the pinned kernel's `cgroup_mkdir`, `cgroup_rmdir`, `cgroup_release`, `cgroup_rename`, `cgroup_freeze`, `cgroup_unfreeze` and populated/frozen notifications close the history against pidfd and cgroup snapshots | non-deniable event loss poisons; trace history does not replace retained pidfds, `/proc` identity or final cgroup emptiness |
| Engine pathname | `socket_bind`, pre-resolution `socket_connect` and the pinned path/inode/mount mutation hooks close the private directory and ancestors, including mknod, symlink, link, rename, unlink, permission/owner and mount-object changes: dockerd gets one bind; the supervisor gets one post-seal unlink; other mutation/connection attempts deny and poison | a pathname hook does not identify the resolved peer and is never the connect-seal authority; incomplete target-kernel call-path coverage is ineligible |
| resolved Engine connect | `unix_stream_connect` receives the already resolved peer `other`; it must match the approved listener object/net namespace plus exact broker identity and generation | exact listener-cookie/inode fields must be derivable and stable on the pinned BTF/kernel or the design is infeasible |
| Engine acceptance/close | the connect hook records both client and queued server-side socket identities; accept attempts, protected-file final release, `UNIX_DIAG`, broker EOF/poll state and exact descriptor/OFD reconciliation close acceptance and loss | `socket_accept` alone is pre-accept authorization, not proof of the accepted peer; no single close or diagnostic hook proves holder closure |
| capability receipt | `file_receive` denies/poisons every protected Engine/listener file receipt except one pre-seal verifier token | the hook exposes the receiving task and file, not the sender/source process; source binding must be supplied conjunctively as described below |
| BPF authority | after arm, BPF syscall policy admits only exact supervisor read/challenge operations on retained objects and denies new program/map/link creation, attachment or protected map mutation | an actor in the explicitly trusted root/hypervisor set remains trusted; killing the supervisor invalidates the VM rather than claiming hostile-root resistance |

Linux's fork source places `security_task_alloc` before `alloc_pid`, and emits
`sched_process_fork` after `copy_process` but before waking the child. Its AF_UNIX source calls
`security_unix_stream_connect` only after resolving and locking the listener and immediately before
installing the connected state. The LSM `file_receive` signature contains only the received file;
`pidfd_getfd` ultimately installs that file through the receive path. These boundaries are why the
table uses conjunctive hooks rather than treating a convenient observation as stronger than its
arguments
([Linux 6.18 fork source](https://github.com/torvalds/linux/blob/v6.18/kernel/fork.c),
[Linux 6.18 AF_UNIX source](https://github.com/torvalds/linux/blob/v6.18/net/unix/af_unix.c),
[Linux 6.18 LSM hooks](https://github.com/torvalds/linux/blob/v6.18/security/security.c),
[Linux 6.18 pidfd_getfd source](https://github.com/torvalds/linux/blob/v6.18/kernel/pid.c),
[Linux 6.18 cgroup tracepoints](https://github.com/torvalds/linux/blob/v6.18/include/trace/events/cgroup.h),
[Linux 6.18 BPF UAPI](https://github.com/torvalds/linux/blob/v6.18/include/uapi/linux/bpf.h),
[eBPF map freeze](https://docs.kernel.org/6.8/userspace-api/ebpf/syscall.html),
[LSM development](https://docs.kernel.org/security/lsm-development.html)).

The kernel state uses a preallocated control value with a BPF spin lock, pre-daemon immutable
executable/actor policy plus the separately frozen listener policy described above, fixed-capacity
process and connection tables, one one-shot verifier slot, one sticky poison bit, a saturating loss
counter and one global ring buffer. Every record carries the
collection/generation, event kind, kernel monotonic timestamp, current/subject task identity,
relevant cgroup/socket/file identity and allow/deny outcome. Enforcement hooks must reserve their
record before allowing the effect. Reservation failure sets poison and denies. Non-enforcement
tracepoint failure sets poison before release can occur. Poison has no clear operation. Capacity
plus one, counter saturation and a supervisor challenge whose globally ordered record cannot be
drained all reject the VM; ring producer/consumer counters are diagnostics, not zero-loss proof.

For Engine admission, the broker begins with exactly three unconnected sockets and the immutable
listener-token policy is armed only after the listener is identified. From initial attach, the
resolved `unix_stream_connect` program default-denies every actor outside the frozen setup/runtime
set, and the broker remains denied until its listener-specific policy is frozen. It then
successfully reserves its record and, under the control-value spin lock, checks generation, broker,
listener, phase and remaining token count; it consumes exactly one token and changes the state to
sealed on token three before the hook returns. Any wrong actor/listener/phase, fourth attempt,
failed connect, count mismatch or inability to prove the locked transition poisons and denies. The
pinned AF_UNIX call path must still be audited to confirm that no fallible post-hook operation can
turn that consumption into an unobserved failed connect. The pre-resolution hook blocks every
Engine-path attempt before arm and observes attempts after unlink, but the resolved hook is the
authority. Only after the sealed state and challenge drain may descriptor/diag reconciliation and
the one allowed unlink proceed.

The listener-registration window also closes mount aliases. Before daemon start, the supervisor
freezes the host mount-namespace actor set and attaches the target-kernel path/mount policy. The
policy must deny or poison any unapproved `mount`, `umount2`, `open_tree`, `move_mount`, `fsopen`,
`fsconfig`, `fsmount`, `fspick`, `mount_setattr`, mount-namespace `clone`/`unshare` or `setns`, and
any bind/open-tree/move-mount alias of the protected Engine directory or listener. Runtime mount
operations become eligible only after listener-policy freeze, only for exact reviewed actors and
roots, and never for that protected object. Pre-arm and final mountinfo/object inventories must show
no alias. If the pinned LSM/syscall call paths cannot enforce and observe that split, the mechanism
is infeasible; pathname matching is not a fallback.

The one verifier exception is narrower than the Cycle 32 shorthand implied. Before applying its own
TSYNC filter, a single-thread verifier receives the retained dockerd pidfd at one fixed descriptor
and the exact target-fd integer operand in immutable input selected from closed `/proc`/`UNIX_DIAG`
inventory. Its filter permits only `pidfd_getfd(fixed_pidfd,fixed_targetfd,0)` for this operation and denies descriptor
substitution, fork/exec, socket and holder-creation routes. The BPF `file_receive` token names the
verifier, generation and expected protected-file properties and atomically consumes once. Because
`pidfd_getfd` installs the duplicate in its caller's FD table, that same verifier--not the
supervisor--must join the duplicate to the prebound dockerd/listener identity, check the local
`close` result, prove the installed FD absent from its ordered post-close FD table, and report only a
bounded result over one preinstalled non-Engine result pipe. The supervisor then confirms verifier
exit through its pidfd and reconciles the empty verifier cgroup. Closing this duplicate is not final
file release while dockerd retains the same open file description, so no such claim is made. Thus
source-process identity comes from retained pidfd + fixed seccomp operands + closed FD table; BPF
alone does not claim to see it. Wrong file, receiver, arguments, repeat, token non-consumption,
close/report mismatch or failure to prove verifier exit poisons
([Linux 6.18 `pidfd_getfd`](https://github.com/torvalds/linux/blob/v6.18/kernel/pid.c)).

The broker's first TSYNC filter must additionally deny `epoll_create*`/`epoll_ctl`, classic AIO
setup/submission, mount/mount-namespace operations and `close_range`, alongside the already closed
fork/clone/exec, new socket/socketpair, `dup*`, `fcntl(F_DUPFD*)`, ancillary send/receive,
`pidfd_getfd`, BPF and io_uring routes. The fixed broker
uses bounded `poll`/`ppoll` plus non-ancillary `read`/`write`; it owns no epoll, registered-file or
other holder object. After the third connect a stacked filter denies `connect`. Any filter install
result other than exact TSYNC success, compat syscall path, unexpected thread/FD, early close or
holder discovery invalidates the VM.

The shim gate consumes the exact bounded 2.2.6 legacy stdin object through EOF, validates it, then
replays the identical bytes from a rewound, write-sealed fixed-stdin object to the official shim
while preserving the pinned argv/environment/stdout/extra descriptors. The probe must show that the
official consumer accepts that file shape and reads exactly those bytes. The runc gate must present
the official binary with a private, stable, read-only bundle/config view, then join every requested
rootfs/mount handle to the observed mount result; a digest followed by a path reopen is insufficient.
The PID1 inert gate remains unable to open the database or exec the candidate until every transient
and live input/process/socket edge reconciles and one durable release capability is consumed. Any
official-consumer incompatibility, writable alias, namespace/mount substitution, gate bypass or
release replay makes the gate infeasible.

The redb-free hostile matrix is now closed semantically; exact bytes and implementation remain
blocked:

| Fixture family | Mandatory cases | Required outcome |
|---|---|---|
| ledger loss/capacity | ring and every fixed map at capacity plus one; saturating loss counter; consumer death/stall; program/link/map detach, replacement or mutation; missing challenge | enforcement event denies where possible, sticky poison always sets, candidate never releases, cleanup cannot promote |
| process lifecycle | unknown parent/exec, child between allocation and exec, script/interpreter substitution, cgroup move, planned helper omitted, unexpected/early helper exit, daemon reload/re-exec/restart | admission hook denies or non-deniable event poisons; every already-exited process remains in history; no replacement/resume |
| Engine endpoint | foreign connect before arm, each token boundary, fourth/post-seal connect, wrong listener/alias, failed final connect, nonempty accept queue, same-user and fixture-root unlink/rebind/substitute plus bind/open-tree/move-mount alias before/during/after listener registration and unlink | wrong connect/mutation/alias denies and poisons; failed admitted connect invalidates without token reset; only exact three-peer graph and alias-free mount inventory can finalize |
| holder creation | broker `fork`/`clone*`, inherited FD, `dup*`, `fcntl(F_DUPFD*)`, `SCM_RIGHTS`, `pidfd_getfd`, `close_range(...UNSHARE...)`, epoll or classic-AIO registration, io_uring registered files and BPF sock-map path | broker seccomp or BPF denies; any observed extra OFD/reference or ambiguous route poisons; early pool close has no reconnect |
| verifier token | wrong source-pidfd/target-fd operand, receiver, file, generation or nonce; duplicate call; token not consumed/revoked; protected file received by another actor | deny and poison; no retry or alternative diagnostic route |
| runtime graph | extra/plugin/health client, missing planned lazy shim publisher, dockerd-containerd or shim-containerd reconnect, raw-address shim fallback, grouped shim, unplanned runc/helper/socketpair | poison and no release; planned transient exits must reconcile exactly |
| gate/TOCTOU | omitted shim/runc/PID1 gate; malformed/truncated/trailing bootstrap; stdin/config/argv/environment/descriptor mutation; writable bundle alias; requested/actual mount mismatch; premature/replayed release | gate refuses official exec or candidate release and poisons; a pre-hash never rescues the case |
| provider/authority | stale/wrong attestation nonce, IAM/agent/login drift, renewal loss, scheduled-deletion trigger miss/late start, continued execution, preemption/maintenance/replacement, delete timeout, or final inventory still contains VM | poison, bounded safety stop and provider deletion; no copied receipt or successor VM resumes the attempt |
| evidence boundary | observation/release/stop/final-cut/cleanup interruption, event-stream EOF, broker response ambiguity, cleanup needing a new Engine socket | unfinalizable; provider teardown is cleanup, never a passing continuation |

Each destructive case uses a fresh VM and can produce only mechanism evidence. Exact kernel
program/helper viability, provider choice, binary/image hashes, physical runtime/socket population,
privileges, deadlines, caps, raw roles/cardinalities, schemas, byte goldens and run-provenance
authority remain blocked.

No successor target/preflight schema cut begins until the remaining external-term/provider-absence
contract, exact pins, mechanism probe, process/event populations, raw roles/cardinalities, derived
caps, result fixtures and run-provenance authority are independently reviewed. Cycle 33 adds no
implementation or executable authority.

## Isolation and clocks

An external Linux supervisor owns the database directory and starts at most one scheduled candidate-
owned process at a time; transaction children, reopen/scanners and clean controls are sequential. A
child owns exactly one `redb::Database`; lanes are OS threads in that child. The supervisor supplies
open-loop release times, process/cgroup sampling, intent/acknowledgement memory, `SIGKILL`, reopen,
and the independent expected-state oracle. The child cannot classify its own crash result.

The target is the runner's native Linux/XFS/dedicated-NVMe class with fixed CPU affinity, an
otherwise idle device, cgroup v2, synchronized monotonic clocks, and the target/preflight rules
frozen in this protocol and its bound policy files. Virtual disks, overlay filesystems, shared host
NVMe, missing project quota, or missing device-write attribution cannot produce a prescreen outcome.

Supervisor and child use `clock_gettime(CLOCK_MONOTONIC_RAW)` on the same host and record the Linux
boot ID, clock resolution, start/end raw values and matching UTC audit timestamps. UTC never enters a
duration. This is one host-wide clock domain; the protocol does not mislabel IPC/scheduling latency
as cross-clock uncertainty. Shared-memory marker sequence monotonicity is still a preflight gate.

For every steady/HOLD transaction the supervisor records scheduled release `S`, queue enqueue `E`,
lane dispatch `D`, timestamp immediately before calling `begin_write` `B`, writer acquired `A`,
commit entry `C`, optional candidate return `R`, and terminal result-ring observation `T`. Checked
integer formulas for a returned transaction are:

```text
scheduler_lateness = max(0, E - S)
queue_wait          = D - E
writer_acquisition  = A - B
candidate_service   = R - B
end_to_end          = T - S
```

Every returned transaction must satisfy `S <= E <= D <= B <= A <= C <= R <= T`. A planned
candidate-timeout record has a closed prefix through its last reached stage, no `R`, and supervisor
terminal time `T`; prefix fields remain ordered, service/end-to-end are right-censored at the
candidate liveness cutoff, and the record is a candidate soft failure rather than a passing latency
sample. Any other missing, reversed or overflowing timestamp invalidates the attempt. Crash-matrix
targets use the intent/marker/signal/exit frame below, not this steady/HOLD shape. Oracle, hashing and
resource sampling run outside `B..R`; their time is never subtracted from end-to-end latency. All event/result rings are
preallocated for the plan's exact population plus 10%, use monotonically increasing sequence
numbers, and may not overwrite. Loss, lag past the 10-second drain, or overflow invalidates the
attempt.

The fixed quiet-target rules are: swap disabled; no cgroup CPU throttling or memory event; no thermal
throttle-counter increase; no kernel/block I/O error; release-lateness p99 at most 500 microseconds
and maximum at most 5 milliseconds; non-candidate target-device writes no greater than both 64 MiB
and 1% of candidate-attributed writes in any attempt bracket; and at least 64 GiB free beyond the
campaign workspace quota at preflight and every boundary cut. CPU affinity, governor/frequency,
kernel, filesystem/mount, device, cgroup and project IDs are bound in target identity. A quiet-rule
miss is environmental `DEFER`, not a candidate pass or failure. All clocks and raw records use the
bounded binary schemas/goldens bound before construction.

The supervisor resolves the dedicated mount and block device from `/proc/self/mountinfo`, verifies
XFS project-quota accounting and enforcement, obtains the database directory's project ID with
`FS_IOC_FSGETXATTR`, and reads that project through `Q_XGETQUOTA`. XFS `d_bcount` is converted from
512-byte basic blocks with checked multiplication. Project-ID change, disabled enforcement,
unsupported query, counter regression/wrap or unit overflow invalidates the attempt. Allocation is
sampled once per second and immediately before/after open, measured traffic, commit, kill, reopen,
scan and cleanup. Cgroup v2 identity comes from the child PID and an already-open cgroup directory;
memory, CPU and I/O files are sampled from that directory. Target-device major/minor must match the
cgroup `io.stat` row and the bound block-trace filter; missing attribution invalidates evidence.

Cold reopen uses a dedicated host with all packet processes closed except the supervisor: `syncfs`
the mount, write exactly `3\n` to `/proc/sys/vm/drop_caches`, wait for two consecutive one-second
cuts with zero target-device I/O in flight, and record the cache-reset receipt before launching the
fresh opener. Failure, an unexpected open file, or background target-device traffic beyond the quiet
rule yields `DEFER`. This is a process-crash/cache-loss probe, never power-loss evidence.

## Steady-state matrix

The fixed seeds are `2026072301`, `2026072302`, and `2026072303`. Each mode runs all four probes once
per seed: 27 measured `W0`--`W2` attempts and nine short `HOLD` attempts. Candidate mode and probe
order rotate by a precommitted schedule; failures are retained and never silently rerun under the
same slot identity.

Each `W0`--`W2` attempt has 15 seconds warmup, 60 seconds measured open-loop traffic, 10 seconds
drain, and 15 seconds resource tail. Its hard wall-clock cap is 120 seconds. All key/value/mutation
frames are generated and oracle-checked before warmup; their resident bytes remain charged to the
cgroup but generation never enters candidate service timing.

| Probe | Offered traffic | Question |
|---|---|---|
| `W0` | one lane, 100 transactions/s, 128 mutations/transaction (128 KiB) | uncontended commit cost |
| `W1` | two disjoint-vnode lanes, 50 transactions/s each, 128 mutations/transaction | global-writer acquisition and fairness |
| `W2` | hot lane: 8 transactions/s at 4,096 mutations (4 MiB); victim lane: 100 transactions/s at 128 mutations | victim tail behind large commits |
| `HOLD` | holder acquires the writer for 250 ms; a victim starts 50 ms later | prove the sole-writer acquisition boundary |

`HOLD` uses a supervisor barrier after the holder has acquired its write transaction. The victim must
not report writer acquisition during the supervisor-controlled 250-ms live-transaction hold and
must acquire within 500 ms after the supervisor releases the holder to commit. A holder return
marker is not used as the unlock instant because its thread can be descheduled after releasing the
writer. The 500-ms bound is a prescreen soft limit, not a claim that redb provides cancellation or a
timeout API. `HOLD` has a ten-second external process deadline. In `W1` and `W2`, both lane
threads call `begin_write` directly; an adapter queue or dispatcher must not serialize them before
the engine acquisition point. `W2` is a proposed synthetic stress point that must be owner-approved
before a formal run; it combines the
profile's 4-MiB target batch with victim work; its mutation rate is not equated with a scenario's
source-row throughput gate.

The measured scheduler emits exactly 6,000 releases for `W0`, 3,000 per `W1` lane, and 480 hot plus
6,000 victim releases for `W2`. Across all modes and seeds this is exactly 166,320 measured
transactions; warmup emits exactly 41,580 more. Every scheduled release is emitted and dispatched
exactly once with no candidate or harness retry and must reach one successful terminal result by the
end of the ten-second drain. Scheduler omission, duplication, a wrong sequence or a harness-dropped
result invalidates the attempt. A candidate-attributable error or non-return is a retained soft
liveness failure. At drain expiry the supervisor atomically commits `candidate_timeout` for each
non-return, sends `SIGKILL`, and requires pidfd-confirmed exit plus complete prefix/actuation evidence
within five seconds. That planned cutoff is not the 120-second attempt safety cap; successful
actuation yields a valid candidate-failure population. Missing/ambiguous actuation or reaching the
attempt cap is `DEFER`. A repeated identical mode/probe/gate failure in all three valid seeds becomes
`PRESCREEN_NO_GO`, while one or two becomes `DEFER`. Achieved rate remains a diagnostic and is not a
95% escape hatch.

The following soft limits apply to every otherwise valid repetition. A miss is retained and cannot
be `REJECT_EXACT_PIN`; the final `DEFER` versus repeatable `PRESCREEN_NO_GO` rule is below:

- no child, adapter, oracle, timeout, result-ring, or sampling errors other than an exactly
  attributable candidate error/non-return classified above;
- `W0`/`W1`: service p99 at most 10 ms, end-to-end p99 at most 25 ms, and end-to-end maximum at most
  250 ms;
- `W2`: victim writer-acquisition p99 at most 25 ms and maximum at most 250 ms; victim end-to-end
  p99 at most 100 ms and maximum at most 500 ms; hot service p99 at most 250 ms and maximum at most
  one second; and
- cgroup memory peak at most 12 GiB, process file descriptors at most 256, and every bracketed XFS
  project-quota allocation no greater than `min(4 * logical_live_bytes, 12 GiB)`.

Latency populations contain every transaction released during the 60-second measured interval for
that lane; a completion during the ten-second drain stays in the population. Warmup is excluded, no
outlier is removed, and an unreturned measured release prevents pass. Raw integer nanoseconds are
sorted and p99 is nearest rank `ceil(990*N/1000)`; maximum is the exact largest sample. Achieved rate
uses measured releases and completions over the fixed 60-second horizon, not drain time.

The resource tail records basic cgroup memory/CPU/I/O, queue/writer/service timing, file allocated
bytes, process file descriptors, `/proc` dirty/writeback state, block-device writes, and any
source-proven optional redb cache counters. Physical/logical allocation and write ratios are retained
as diagnostics, not imported C2 gates. It does not recreate resource-v2 or invent LSM debt,
background compaction, write-stall, or pinned-snapshot values. Full-tree statistics may be collected
only after the attempt and are labelled offline diagnostics.

## Process-crash matrix

For each transaction mode and seed, six fresh atomicity trials run against a verified 256-MiB
logical base. The clean fixture is copied byte-for-byte without reflink into an independent file and
hash verified before use; copy method/time/bytes are recorded outside the attempt. Every
fixture includes deterministic insert, overwrite, and delete sentinels in all four tables, plus
bounded churn that creates allocated/free-page fragmentation. Its recipe and canonical digest are
recorded. Each trial applies one cross-table transaction and kills only the child at one of these
supervisor triggers:

1. after the child records the complete intent but before commit entry;
2. immediately after the commit-entry marker;
3. 250 microseconds after commit entry;
4. 2 milliseconds after commit entry;
5. 10 milliseconds after commit entry; or
6. after candidate return and observed supervisor acknowledgement.

After opening its private copy, the child first completes one returned priming transaction in the
trial's `I1`, `I2`, or `QR` mode and remains open. The post-prime state becomes that trial's old-state
oracle. This prevents the clean fixture's `Database::drop` quick-repair allocator state from making
an interrupted first `I1`/`I2` commit look artificially cheap. The target transaction then starts
without closing the database.

The supervisor independently derives the expected post-prime digest from the verified fixture and
fixed priming mutation, then constructs and durably retains the intent containing that digest,
complete intended mutation digest, transaction, mode, seed, trigger identity and sequence number
before child dispatch. The child receives and digest-checks that immutable intent through the
separately framed control channel, performs the priming transaction, verifies its actual primed
state against the expected digest, and only then publishes the scalar `intent` transition. No multi-
byte intent body is published through shared memory. A child-owned shared
object exposes only the scalar monotonic `intent`, `commit_entered`, and `candidate_returned`
transitions. Each transition has its own immutable,
naturally aligned raw sequence/payload slot and uses the same approved publisher-shim transaction as
`N19`; slots are never reset or reused. `acknowledged` is separate supervisor-owned evidence and is
never written through the read-only child-marker mapping. Cycle 32's exact target-qualified assembly
ABI, build audit and independent cross-process campaign are eligibility gates for both child and
supervisor binaries. The supervisor observes each expected transition only through the matching
observer-shim transaction; a sequence mismatch returns without payload inspection. Rust never
accesses a shared slot directly. `N19` corroborates the exact instruction and mapping shape but does
not prove the later multi-slot layout, process-crash classification or database semantics. The
supervisor records acknowledgement only
after observing candidate return, and trigger 6 waits until that field is recorded in supervisor
evidence state before kill. A pre-commit barrier lets the
supervisor arm its pidfd/timer before the child records `commit_entered` and immediately calls
commit. Trigger 1 kills before releasing that barrier. Triggers 2--5 release it and actuate at 0,
250 microseconds, 2 milliseconds, and 10 milliseconds respectively after the supervisor observes
`commit_entered`. After return the child records `candidate_returned` and parks in a non-dropping
state until killed, so scheduling cannot turn a return-boundary trial into clean `Database::drop`.
The bounded crash frame contains trial/trigger/sequence IDs, intent-published, barrier-released,
optional commit-entered, optional candidate-returned, optional acknowledgement, signal-requested,
signal-delivered, and exit-observed raw-clock offsets plus the final marker bitmap. Present offsets
are monotonically ordered; absent stages stay absent rather than being zero-duration returns. Trigger
1 requires no commit/return marker, triggers 2--5 require commit entry and classify by the final
return marker, and trigger 6 requires return plus acknowledgement. This frame is distinct from the
steady/HOLD latency wire. Every frame offset is a supervisor-owned control/observation timestamp;
neither offsets nor the bitmap are child-written shared payloads.
The supervisor records the child markers and its own acknowledgement state when it requests the
signal, then after `waitid`/pidfd exit rereads the child fields with the same exact observer-shim
transaction. Final combined fields classify the trial because
return may race signal delivery; a requested timed trigger is not silently called “in commit.”
Across triggers 2--5, each mode needs at least three finally confirmed
`commit_entered && !candidate_returned` kills. At most two extra, separately identified trials per
mode may vary only the delay to meet that coverage: extra 0 reuses seed `2026072301` at zero delay
and extra 1 reuses seed `2026072302` at 50 microseconds. All other bytes and ordering stay fixed.
They run in that order only until coverage is met; otherwise the outcome is `DEFER`.

Shared memory is protocol state, not proof of database durability. `SIGKILL` is delivered by
PID/pidfd and the supervisor records delivery and observed exit. The child must die without unwind
or `Database::drop`: redb's clean drop makes a quick-repair/shrink commit and can mask the crash path.
A normal child exit is invalid. Graceful drop is measured separately as a clean-close control;
container stop, machine reboot, and power loss are different fault classes.

After each kill, a fresh process opens the file and independently scans all four tables in canonical
order. Open and full scan each have a 60-second cap and their durations are never combined. An
attempt killed before `commit_entered` must contain exactly the old state. Once commit was entered
but before `candidate_returned`, either exactly old or exactly complete new is allowed. Once
`candidate_returned` is visible, complete new is required whether or not supervisor acknowledgement
was written; acknowledged is therefore also complete new.

For the eligible small-crash rows only, the exact Cycle 25 code/observation cross-product controls
terminal validation: a state observation outside that marker-derived allowed set, exact candidate
corruption error, or exact candidate reopen/scanner panic may construct the private terminal token
only when every target, actuator, origin and oracle prerequisite is valid. The observation itself is
not an outcome. Only the durable latch and retained closed proof can later derive
`REJECT_EXACT_PIN`. Timeout, actuator ambiguity, host noise, generic candidate error, harness/oracle
panic, or resource-observation failure is `DEFER`.

A separate large-recovery comparison uses one 4-GiB fragmented fixture per mode and seed: nine
trials total, each with a confirmed in-commit kill. Independent clean-control and crash copies start
from the same verified fixture. Both execute and retain the mode-specific priming commit while open;
the control then closes normally, while the crash copy uses the armed pre-commit barrier and is
killed 50 microseconds after observed commit entry without drop. It records clean-control reopen,
crash reopen, and full-scan duration separately. A return before signal or any unconfirmed
in-commit marker makes that slot and the campaign `DEFER`; large recovery has no adaptive retry.
Before each reopen, the dedicated host follows the same reviewed, recorded file/device quiescence
and page-cache-reset procedure; no comparison is accepted if either side's cold state cannot be
established. Docker Desktop/WSL results are never used here. These nine trials answer recovery cost;
the smaller 54-trial matrix supplies broad atomicity timing coverage. Large-recovery and clean-
control rows are not terminal-latch eligible in v1; any correctness anomaly in them forces `DEFER`
pending a separate proof contract.

For `QR`, crash-reopen median must be at most two seconds and each crash-reopen at most five seconds.
If the matching `I2` crash-reopen median exceeds two seconds, `QR` median must also be no more than
half that `I2` median. These are soft investment gates; the full scan remains correctness-bearing
and cannot be replaced by reopen success or redb's internal statistics. Median and ratio are
complete-population gates; a valid miss is `PRESCREEN_NO_GO`. One seed exceeding the five-second
maximum is `DEFER`; two or more are `PRESCREEN_NO_GO`.

## Bounds and disposition

The performance base is 1 GiB logical, the atomicity base is 256 MiB, and the large-recovery base is
4 GiB. Every database lives in its own XFS project with a 16-GiB hard quota; the whole transient
campaign workspace has a separate 64-GiB hard quota. `memory.max` is 16 GiB, swap is disabled, and
`RLIMIT_NOFILE` is 512. These protect the host and are not decision thresholds. The decision limits
remain strictly lower: 12-GiB cgroup memory peak, 256 FDs, and per-database allocated bytes no greater
than `min(4 * logical_live_bytes, 12 GiB)`. Checked multiplication overflow is invalid evidence.

The 2-GiB retained-result-evidence cap excludes the separately immutable approval-input object set,
transient working database copies and streamed scan bytes. Full scans feed the redb-free oracle
incrementally and are never materialized as exports.
Retained result evidence includes the exact approval/result payloads and receipts, run-start
(including its nested copied input-version binding) and manifests, raw timing/resource/marker frames,
target/preflight/noise records, state counts and digests, at most 1 MiB per mismatch excerpt,
process/kernel logs,
validator/oracle/mechanism reports, cleanup journal/report and final artifact index. It binds but
does not duplicate/index the separately retained approval-input plans, schedule, binaries or physical
bases. The Cycle 24 hierarchy defines the exact set relationship and retained-cap accounting.
Deletion begins only
after the close manifest containing the independent oracle and validator reports is durably written,
reopened and rehashed. Missing cleanup evidence is never a smaller claimed set. A cleanup incident
normally makes the classifier derive `DEFER`; after a valid terminal correctness stop it may preserve
`REJECT_EXACT_PIN` only when the complete proof for the original finding remains valid and retained.

No fixture is generated inside the decision campaign: the approved packet binds prebuilt canonical
bases. The five-hour watchdog begins before build/environment verification and ends only after
evidence close, crash-recoverable cleanup, final-index validation and local result-payload freeze.
Human owner review, immutable publication, live authority checks and registry finalization are
outside that native watchdog and cannot extend or rerun its campaign. Its worst-case allocation is
exact:

| Step | Count and ceiling | Budget |
|---|---|---:|
| Build/environment/fixture verification | one aggregate cap | 15 min |
| `W0`--`W2` | 27 at 120 s | 54 min |
| `HOLD` | 9 at 10 s | 1.5 min |
| Crash open-and-scan pairs | 54 baseline small + 6 reserved small + 9 large, each 120 s | 138 min |
| Large clean control close/reopen | 9 closes + 9 reopens, each 60 s | 18 min |
| Fixture copies and hash checks | aggregate | 25 min |
| Priming and trigger setup | aggregate | 10 min |
| Quiescence and cache resets | aggregate | 15 min |
| Evidence close, reopen and rehash | aggregate | 5 min |
| Journaled cleanup and absence checks | aggregate | 5 min |
| Cleanup report, final index, classification and local payload freeze | aggregate | 5 min |
| **Allocated maximum** | | **291.5 min** |
| **Watchdog slack** | | **8.5 min** |

The campaign also caps all retained decision-bearing transaction records at 250,000. A per-step,
aggregate, resource, sample, workspace or five-hour safety bound reached before a valid terminal
correctness finding is `DEFER`; no partial population passes. These ceilings remain materially
cheaper than the 45-hour C2 sketch. The cleanup allocation includes the required journal-header,
per-database intent/terminal durability and absence checks; exceeding it follows the same cleanup
classification rules. Separate governance-finalization deadlines remain part of the unfrozen
capability/TOCTOU contract and cannot produce `FINAL_PRESCREEN_RESULT_SEALED` after they expire.

After final-index validation, the redb-free classifier derives exactly one outcome in this
precedence order. Post-run owner review is deliberately not a classifier input:

1. Before a terminal correctness stop is latched, invalid bound approval or copied pre-run receipt
   content, target, preflight, actuator, clock, harness, oracle, schedule, artifact or evidence
   needed to judge that finding yields `DEFER`; an invalid finding attempt cannot prove a candidate
   defect. Failure to obtain live first-stage admission permits no formal run and no outcome instead;
   unavailable second-stage child authority after valid admission uses the no-candidate `DEFER` path.
2. One exact Cycle 25 invariant code in an eligible small-crash row, validated into the private token
   and durably committed through `TERMINAL_STOP_LATCHED`, latches
   `TERMINAL_CORRECTNESS_STOP_LATCHED` and stops all further candidate decision/diagnostic
   execution. The final classifier derives `REJECT_EXACT_PIN` only if the acknowledgement and
   complete attributable proof remain valid and retained; otherwise it derives `DEFER`.
3. Without such valid retained rejection proof, a safety bound, incomplete required population,
   cleanup failure, missing in-commit or
   cold-cache proof, or mechanism-probe status other than `complete` yields `DEFER`.
4. The QR complete-population median/ratio/maximum rules above apply first with checked integer
   arithmetic; their explicit `DEFER`/`PRESCREEN_NO_GO` thresholds override the general repetition
   rule below.
5. Excluding those QR-specific thresholds, the same candidate writer/latency/recovery/resource/
   liveness soft gate failing for the same mode/probe in all three valid seeds yields
   `PRESCREEN_NO_GO`. One or two failures yields `DEFER`. Harness/oracle/sampler/actuator/observer
   faults never become candidate failures by repetition.
6. `PRESCREEN_PASS` requires every baseline and activated replacement slot valid and passing, an
   exact matching prior Docker result whose content is non-synthetic `DOCKER_SMOKE_PASS`, a raw run
   manifest recording its dispatch binding, a complete mechanism probe and complete artifact
   reconciliation.

That Docker predicate is content-only and cannot authorize or retroactively legitimize a native
run. Final sealing separately requires `NATIVE_RUN_PROVENANCE_VERIFIED` to prove that the exact
`DOCKER_SMOKE_PREREQUISITE_VERIFIED_NO_DECISION` capability was consumed at dispatch.

Owners attest the frozen derived payload after classification; they cannot manually select, change
or weaken its outcome. Refusal or loss of current review prevents final sealing without changing the
derived outcome. A
mechanism-probe report is emitted even when preflight or an earlier terminal result prevents the
probe and carries `not_run` or `incomplete` plus the reason; only `complete` is pass-capable.

Every root/manifest uses `NOT QUALIFICATION EVIDENCE` and sets all qualification, selection,
production, soak and admission booleans false. A pass still leaves DKS-Q2-006's mechanism-map schema
and redb mapping, DKS-Q2-007 persistence/configuration work, the complete candidate adapter,
C1/C2/C3, physical fault/endurance qualification, and independent production soak open.

Before owners approve this prescreen, an exact-source mechanism note must inventory the sole-writer
wait, synchronous allocator reclamation, quick-repair allocator-state writes, clean-close
quick-repair/shrink commit, kernel writeback, and any thread/background activity. That proof—not the
absence of activity in a short run—decides which DKS-Q2-006 arms may be `not_applicable`. The probe
only corroborates the pinned source/configuration mapping. The separately bound execution,
preflight, and observation policies must freeze the XFS quota query, units, one-second/boundary cuts,
16-GiB hard-quota setup, and error/wrap behavior used by the allocation gate. The current profile v3
remains Fjall/RocksDB-specific, so even a pass needs an
additive redb profile/schema proposal rather than editing or reinterpreting `linux-nvme-v3`;
`linux-nvme-v2` remains an immutable regression fixture.

## Docker Desktop/WSL development subset (Cycle 30 supersession)

Docker Desktop on this Windows host is development-only. It may later exercise the pinned Linux
build, four-table layout, schema/golden/oracle agreement, one transaction in each mode, one `HOLD`
and one trial at each kill trigger against a 64-MiB Docker-volume copy, but only under a separately
reviewed and versioned development-only identity; none exists now. It may not emit `DOCKER_SMOKE_PASS`,
`DOCKER_SMOKE_INCOMPLETE`, the Docker-prerequisite capability or any native outcome. The frozen
Cycle 25 population/result semantics remain superseded design history/reference until the versioned
native-Linux successor exists; there are no literal target/preflight/result fixtures and they do not
authorize a Desktop producer. Current authority permits no such candidate run.

A named-volume database on this host uses Docker's managed ext4/VHDX/NTFS/shared-NVMe path while the
container root also uses overlayfs. It cannot validate XFS quota, direct device writes, physical
amplification, native-NVMe latency, power loss, endurance, C2/C3, the prescreen disposition or
independent soak. A future formal native-Linux smoke pass remains a content prerequisite for
spending target-host time only after its own protected live-review, provider/Engine authority and
immutable-storage verification; it still would not show that redb is suitable.

Cycle 16 implements a narrower construction lane, not this smoke subset. It has no crash actuator,
approval verifier, or result classifier. The canonical result and evidence boundary are recorded in
the [Cycle 16 carry-forward matrix](../reports/state-backend-carry-forward-matrix-2026-07-24.md#cycle-16-redb-construction-result).
This closes tool construction only; all writer-rate, crash, recovery, native-target, and disposition
questions above remain open.
