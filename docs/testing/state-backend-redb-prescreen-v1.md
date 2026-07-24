# redb 4.1.0 bounded state-backend prescreen v1

- **Identity:** `state-backend-redb-prescreen/v1`
- **Status:** Cycle 21 protocol freeze candidate; validation-only work is authorized, while the
  protected-review/semantic verifier, native supervisor/child/actuator/oracle, reviewed build,
  owner approvals, and protocol execution remain absent
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

The three physical bases cannot be created by the current instruction. A later explicit
`fixture-construction-no-decision` authorization must first approve the as-built generator, recipe,
source/build and target scratch location. That bounded stage may open redb only to create the three
fixed fixture files and their logical/file digests; it emits no smoke/native disposition and every
qualification/selection/production field remains false. Independent verification then supplies the
fixture descriptors for the two-owner pre-run payload. Fixture generation is therefore outside the
five-hour decision campaign without being an unapproved hidden pre-run. The Cycle 16 construction
lane does not inherit this authority.

### Fixed packet root and locators

Every formal descriptor is `(role, locator, byte_length, sha256, media_type)`. `locator` is a UTF-8,
forward-slash, packet-root-relative path: no empty component, `.`, `..`, drive/UNC prefix, backslash,
symlink, hard link, device, socket, FIFO or path escape is allowed. Each role appears exactly once at
the fixed locator below. The verifier opens relative to an already-open packet directory with
no-follow semantics, verifies regular-file identity before and after streaming, caps bytes before
allocation, and rejects aliases or extra decision-bearing files.

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
| 256-MiB physical base | `fixtures/base-256m.redb` | pre-run approval |
| 1-GiB physical base | `fixtures/base-1g.redb` | pre-run approval |
| 4-GiB physical base | `fixtures/base-4g.redb` | pre-run approval |
| approval payload | `approval/payload.json` | dispatch |
| pre-run protected-review receipt | `approval/protected-review.json` | dispatch |
| reviewed smoke result | `evidence/prior-smoke-result.json` | native dispatch only |
| raw run manifest | `result/raw-run-manifest.json` | result review |
| retained artifact index | `result/artifact-index.json` | result review |
| validator report | `result/validator-report.json` | result review |
| independent oracle report | `result/oracle-report.json` | result review |
| bounded mechanism-probe report | `result/mechanism-probe.json` | native result review |
| reviewed result payload | `result/payload.json` | result review |
| post-run protected-review receipt | `result/protected-review.json` | final disposition |

The approval payload binds every pre-run contract, subject, build, physical-fixture and target row,
but excludes `approval/payload.json`, its protected-review receipt, and every result row. The pre-run
receipt binds the approval payload's exact length and SHA-256. The raw run manifest binds the exact
approval payload/receipt pair and the authenticated dispatch context verified before child launch.
The result payload binds that complete approval packet plus every applicable run/result evidence row,
but excludes `result/payload.json` and its post-run receipt. The post-run receipt binds the result
payload's exact length and SHA-256. Neither payload binds itself or the receipt that is necessarily
created afterward. The redb archive is the fixed packet row `subject/redb-4.1.0.crate`, not an
unresolved external descriptor. A prior smoke result is accepted only when the verifier
proves that it is a non-synthetic `SMOKE_PASS` over the identical subject, source/build, schemas,
goldens and Docker-smoke plan; an opaque or synthetic descriptor never satisfies that prerequisite.

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
bases contain exactly 262,144 entries for 256 MiB, 1,048,576 entries for 1 GiB, and 4,194,304
entries for 4 GiB, divided equally across the four tables. Four same-ordinal table entries form one
logical entity. Fixture creation uses a fresh exclusive create-new regular file. The generator calls
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

Each base divides every table evenly across fixed vnodes 0 through 7, inclusive; all three
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

This proposal does not authorize protocol execution. Any command that claims the Docker smoke or
native prescreen identity requires an exact `state-backend-redb-prescreen-approval-payload/v1` and
one provider-authenticated `state-backend-redb-prescreen-protected-review-receipt/v1` over those
identical payload bytes. The unsigned payload contains all decision-bearing fields and descriptors,
plus the two required roles and exact required decision literal. It contains no concrete principal,
review-event ID, review timestamp, or receipt descriptor; those facts can exist only after review,
so the packet is non-circular.

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
- one immutable provider review-event ID, stable account ID, role, decision, and UTC time for each
  of `workload_owner` and `operations_owner`; and
- the authenticated dispatch workflow/environment/run identity and verification UTC time.

The two principals and review-event IDs are distinct. Both decisions are exact approval literals,
both events apply to the bound head and payload digest, and neither review is stale, dismissed,
superseded, self-approved, or outside its configured role. Unknown fields, a locally invented
identity, a provider mismatch, mutable branch-only identity, missing protected-environment context,
or a payload/head/configuration change fails closed. The receipt is produced only by the trusted
dispatcher after querying the configured protected-review provider; merely placing matching JSON in
a packet never authorizes execution.

Result review uses the same receipt schema with stage `post_run`, the exact result-payload length and
SHA-256, two distinct current role reviews, and the retained-evidence content root. The later result
contract must separately bind trusted immutable storage version and retention evidence. The result
payload contains the derived disposition plus the two required roles and exact acceptance literal,
but no concrete reviewer/event/time fields. Its later receipt supplies those facts and binds those
bytes; neither object hashes itself.

The redb-free semantic verifier validates schemas, exact bytes/locators, payload/receipt equality,
and cross-artifact invariants. Outside an authenticated protected workflow it can report only
content validity and `authorization_unverified`; it cannot turn a copied receipt into authority. The
trusted dispatcher independently validates the live provider context before it may invoke an
execution entry point, and the candidate child is opened only after both checks succeed. If the
repository cannot enforce and export two role-separated reviews, the protocol remains `DEFER` and
no command runs. Portable offline cryptographic attestations would require a separate security ADR,
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
| result payload | `state-backend-redb-prescreen-result-payload/v1` | `redb-prescreen-result-payload-v1.schema.json` | identity reserved; later validation slice |
| protected-review receipt | `state-backend-redb-prescreen-protected-review-receipt/v1` | `redb-prescreen-protected-review-receipt-v1.schema.json` | pre-run content validation; post-run shape reserved |

The exact `$id` values, in the same order, are
`https://laminardb.dev/schemas/state-backend-redb-prescreen-protected-review-policy-v1.json`,
`https://laminardb.dev/schemas/state-backend-redb-prescreen-approval-payload-v1.json`,
`https://laminardb.dev/schemas/state-backend-redb-prescreen-result-payload-v1.json`, and
`https://laminardb.dev/schemas/state-backend-redb-prescreen-protected-review-receipt-v1.json`.

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

The native-only prior-smoke descriptor is
`(redb-prescreen-reviewed-smoke-result, evidence/prior-smoke-result.json, length, sha256,
application/json)`. It is outside the fixed 28 because it does not exist before Docker smoke. The
native payload binds it separately, and the later semantic slice must prove its subject, build,
schemas, goldens and Docker plan equal the native payload before dispatch can be considered.

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
are at most 20 GiB. The final retained total is checked as `retained_artifact_bytes +
artifact_index.byte_length + result_payload.byte_length + post_run_receipt.byte_length` and must be
at most 2 GiB. Declared lengths never drive
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
prints usage. No output or public type contains `DEFER`, `SMOKE_PASS`, `PRESCREEN_PASS`,
`PRESCREEN_NO_GO`, `REJECT_EXACT_PIN`, provider-authenticated, executable or sealed state.

## Isolation and clocks

An external Linux supervisor owns the database directory and starts one child process. The child
owns exactly one `redb::Database`; lanes are OS threads in that child. The supervisor supplies
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

The intent contains the post-prime digest, complete intended mutation digest, transaction, mode,
seed, trigger identity, and sequence number. Shared memory exposes monotonic `intent`,
`commit_entered`, `candidate_returned`, and `acknowledged` transitions. Child marker stores use
release ordering. The supervisor writes acknowledgement only after observing candidate return, and
trigger 6 waits until acknowledgement is visible before kill. A pre-commit barrier lets the
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
steady/HOLD latency wire.
The supervisor records the markers observed when it requests the signal, then after `waitid`/pidfd
exit rereads their final values with acquire ordering. Final markers classify the trial because
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
was written; acknowledged is therefore also complete new. A torn/mixed transaction, extra key/value,
missing post-return mutation, checksum/corruption error, candidate/redb panic attributable to valid
input, or non-canonical duplicate is `REJECT_EXACT_PIN` when the target and actuator evidence are
valid. Timeout, actuator ambiguity, host noise, harness/oracle panic, or resource-observation failure
is `DEFER`.

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
the smaller 54-trial matrix supplies broad atomicity timing coverage.

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

The 2-GiB retained-evidence cap excludes verified transient database fixtures/copies and streamed
scan bytes. Full scans feed the redb-free oracle incrementally and are never materialized as exports.
Retained evidence includes the exact approval/result packets, plans, manifests, schedule, raw timing/
resource/marker frames, target/preflight/noise records, state counts and digests, at most 1 MiB per
mismatch excerpt, process/kernel logs, validator/oracle/mechanism reports, protected-review receipts,
and an artifact index. The index records every retained file's locator/length/hash except its own
bytes, `result/payload.json`, and `result/protected-review.json`; those three objects are necessarily
created after the indexed set closes. It also records every verified-then-destroyed database's
pre/post digests and destruction time. Its checked sum, excluding those three objects, must equal
`retained_artifact_bytes`. Deletion begins only after independent oracle and validator reports are
durably indexed; missing transient-destruction evidence is `DEFER`, not a smaller claimed total.
If a valid `REJECT_EXACT_PIN` was already sealed, later cleanup/destruction failure is retained as a
post-seal protocol incident but cannot downgrade that rejection.

No fixture is generated inside the decision campaign: the approved packet binds prebuilt canonical
bases. The five-hour watchdog begins before build/environment verification and ends only after
artifact finalization and cleanup. Its worst-case allocation is exact:

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
| Retained-evidence finalization | aggregate | 10 min |
| Final cleanup | aggregate | 1 min |
| **Allocated maximum** | | **287.5 min** |
| **Watchdog slack** | | **12.5 min** |

The campaign also caps all retained decision-bearing transaction records at 250,000. A per-step,
aggregate, resource, sample, workspace or five-hour safety bound reached before a valid terminal
correctness finding is `DEFER`; no partial population passes. These ceilings remain materially
cheaper than the 45-hour C2 sketch.

The redb-free classifier derives exactly one outcome in this precedence order:

1. Before a terminal finding is sealed, invalid approval/protected review, target, preflight,
   actuator, clock, harness, oracle, schedule, artifact or evidence needed to judge that finding
   yields `DEFER`; an
   invalid finding attempt cannot prove a candidate defect.
2. One fully attributable atomicity/durability/corruption invariant violation in a valid attempt
   seals `REJECT_EXACT_PIN` and stops all further candidate decision/diagnostic execution. Later
   artifact-finalization or cleanup faults are retained but cannot downgrade or erase the sealed
   rejection; loss of evidence required to validate the original finding means it was never sealed.
3. Without a sealed rejection, a safety bound, incomplete required population, missing in-commit or
   cold-cache proof, or mechanism-probe status other than `complete` yields `DEFER`.
4. The same candidate writer/latency/recovery/resource/liveness soft gate failing for the same mode/
   probe in all three valid seeds yields `PRESCREEN_NO_GO`. One or two failures yields `DEFER`.
   Harness/oracle/sampler/actuator/observer faults never become candidate failures by repetition.
5. The QR complete-population median/ratio/maximum rules above apply with checked integer arithmetic;
   their explicit `DEFER`/`PRESCREEN_NO_GO` rules take the same precedence.
6. `PRESCREEN_PASS` requires every baseline and activated replacement slot valid and passing, an
   exact non-synthetic prior `SMOKE_PASS`, a complete mechanism probe, complete artifact
   reconciliation, and both post-run owners accepting the derived result.

Owners attest the derived payload; they cannot manually select or weaken its disposition. A
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

## Docker Desktop/WSL smoke subset

Docker Desktop on this Windows host may run a smoke-only subset using the exact pinned Linux build
and a Docker volume. It checks harness construction, the four-table layout, schema/golden/oracle
agreement, one transaction in each mode, one `HOLD`, and one trial at each kill trigger against a
64-MiB base. Optional two- and five-second lane bursts may catch gross deadlocks.

Every such artifact is labelled `SMOKE_ONLY / NO_DECISION`. A named-volume database uses Docker's
managed ext4/VHDX/NTFS/shared-NVMe path (while the container root also uses overlayfs); it cannot
validate XFS quota, direct device writes, physical amplification, native-NVMe latency,
power loss, endurance, C2/C3, or the prescreen disposition. Passing Docker smoke is a prerequisite
for spending target-host time, not evidence that redb is suitable.

Cycle 16 implements a narrower construction lane, not this smoke subset. It has no crash actuator,
approval verifier, or result classifier. The canonical result and evidence boundary are recorded in
the [Cycle 16 carry-forward matrix](../reports/state-backend-carry-forward-matrix-2026-07-24.md#cycle-16-redb-construction-result).
This closes tool construction only; all writer-rate, crash, recovery, native-target, and disposition
questions above remain open.
