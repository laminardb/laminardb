# State backend qualification model v1

- **Status:** provisional C1 engineering contract; ineligible for qualification
- **Scope:** `tools/state-backend-qual` only
- **Approval needed before qualification execution:** workload owner and operations owner
- **Related decision:** [ADR-008](ADR-008-managed-vnode-keyed-state.md)

## Decision and authority boundary

The standalone qualification tool will contain a deterministic storage-semantics model before it
contains a Fjall or RocksDB adapter. The model gives later adapters identical batched point reads,
bounded scans, atomic mutations, and lifecycle operations. It is an oracle for storage behavior,
not a fallback backend or a model of complete SQL operator semantics.

Every model result says `NOT QUALIFICATION EVIDENCE` and
`qualification_eligible=false`. It contains no backend, latency, throughput, hardware, pass/fail,
or selection field. It cannot qualify an LSM, satisfy checkpoint or restore gates, establish
source/sink delivery, or relax `[LDB-4007]` or `[LDB-0013]`. The tool remains independent of
LaminarDB runtime crates, Arrow, DataFusion, Fjall, RocksDB, and async runtimes.

The checked-in profile is still an unapproved candidate. C1 code and goldens may be revised during
engineering review. Before qualification execution, named owners may change the profile or this
contract; approval then hashes the final profile plus the candidate-neutral runner contract/source
revision. Evidence records the exact built binary and lockfile separately. Before approval, work is
limited to builds and semantic adapter conformance; candidate performance, resource, fault,
endurance, and selection measurements are prohibited, not merely labelled diagnostic.

## Stable identities

Printable version identifiers do not contain a zero byte:

| Item | Value |
|---|---|
| model | `state-backend-reference/v1` |
| generator | `state-backend-workload/v1` |
| result schema | `state-backend-model-result/v1` |

Hash and canonical-encoding domains do include the displayed terminating zero byte:

| Item | Domain |
|---|---|
| model-input digest | `LDB-SBQ-MODEL-INPUT-V1\0` |
| counter block | `LDB-SBQ-COUNTER-V1\0` |
| entity suffix | `LDB-SBQ-ENTITY-V1\0` |
| request | `LDB-SBQ-REQUEST-V1\0` |
| observation | `LDB-SBQ-OBSERVATION-V1\0` |
| state digest | `LDB-SBQ-STATE-V1\0` |
| request stream | `LDB-SBQ-REQUEST-STREAM-V1\0` |
| observation stream | `LDB-SBQ-OBSERVATION-STREAM-V1\0` |
| combined trace | `LDB-SBQ-TRACE-V1\0` |

An incompatible semantic or encoding change requires new identities and goldens. Profile values
may change before approval without changing the algorithm version; their model-input digest and
result provenance change.

## Profile identity and bounded cases

The existing duplicate-key, JSON Schema, and semantic checks run first. Typed model values are
extracted from that same validated JSON value, not reparsed permissively.

Two hashes serve different purposes:

- `profile_sha256` hashes the exact input bytes and is result provenance. Formatting or approval
  metadata changes it.
- `model_input_sha256` hashes the model-input domain followed by every numeric field in the
  `workload` object in the order below, then `measurement.fixed_seeds`. Scalars are `u64`
  big-endian; a vector is `u32` count followed by `u64` elements. Formatting, status, approvals,
  environment, and evidence metadata do not affect generated requests.

The exact model-input order is `logical_state_bytes`, `batch_rows`, `target_batch_bytes`,
`hard_batch_bytes`, `compact_key_bytes`, `compact_state_bytes`, `variable_key_bytes`,
`variable_state_bytes`, `primary_vnode_count`, `metadata_edge_vnode_counts`,
`zipf_exponent_milli`, `hot_distribution_permille.one_key`, `.nine_keys`,
`.uniform_remainder`, `single_vnode_distinct_keys_permille`,
`timer_mix_permille.state_or_timer_mutation`, `.bounded_due_scan`, `.atomic_fire_delete`,
`timer_scan_max_rows`, `timer_scan_max_bytes`, `join_match_counts`,
`join_match_weights_permille`, and `fixed_seeds`.

A strict model-case JSON object has exactly these properties:

```json
{
  "scenario": "aggregate",
  "seed": 2026072201,
  "logical_state_bytes": 4294967296,
  "batch_rows": 128,
  "request_count": 16,
  "key_bytes": 32,
  "value_bytes": 208,
  "join_match_count": null
}
```

`scenario` is `aggregate`, `timer_window`, or `join`. Seed, logical state size, batch rows, key
width, value width, and non-null join match count must be members of their corresponding profile
vectors or compact-width fields. `join_match_count` is required and non-null only for `join`; it is
required and null otherwise. Counts and widths fit `u32`; other numeric fields are `u64`.

`request_count` is `1..=4096`, but that limit alone is not a memory bound. Before replay, the tool
sums each request's canonical encoded bytes, declared read capacity, and mutation bytes using
checked `u64` arithmetic. The total must be at most 64 MiB. Before constructing key, value, or
canonical-request payloads, it computes the exact post-deduplication shape with bounded metadata;
aggregate group count and join range count therefore use their actual distinct counts rather than
raw input rows. The validator streams digests and observations rather than retaining the trace.

C1 also rejects a model profile if any batch-row member exceeds 65,536, any compact or variable
workload key width exceeds 4,096 bytes, any compact or variable workload value width exceeds 65,536
bytes, or `hard_batch_bytes` exceeds 64 MiB. Restore maxima remain separate validation ceilings; no
payload is allocated merely from those maxima. A case is rejected if `request_count * batch_rows`
exceeds 4,194,304 logical rows. These are model-tool safety ceilings, not candidate qualification
thresholds, and do not replace the profile's C2 operation-count requirements. Direct ordinal
generation checks the named ordinal's exact charge; constructing a sequential replay additionally
enforces the cumulative 64 MiB bound.

There is no implicit workload cross-product. The approved runner must name its exact matrix and
pacing separately.

`logical_state_bytes` salts cardinality generation but C1 does not prefill or realize that target
state size. For join, `join_match_count` sets the probe cap; ordinary empty-start replay does not
promise that fanout. These labels are case inputs, not claims about exercised resident/spill size or
observed join matches.

## Logical storage semantics

### Keys and operations

Logical table tags are:

| Tag | Table |
|---:|---|
| `0x01` | aggregate state |
| `0x02` | window state |
| `0x03` | timer index |
| `0x04` | join left rows |
| `0x05` | join right rows |
| `0x06` | output bookkeeping |

Physical keyspace mapping is deliberately deferred to the approved C2 runner/adapter contract.

A logical key is `(table_tag, vnode_u32, opaque_key_bytes)`, ordered by unsigned tag, vnode, then
unsigned lexicographic bytes. Empty opaque keys and values are valid; this covers the zero-byte
encoded Null grouping key. A missing value is represented by `None`, so it remains distinct from a
present empty value. Vnodes must be below the active model limit and key/value widths must be at
most their active limits.

A batch contains `kind`, scenario, zero-based ordinal, logical input rows, `BatchLimits`, point-read
keys, ranges, and mutations. `kind` is `0x01` measured or `0x02` setup. Point keys, ranges by their
full encoded tuple, and mutation keys must each be strictly increasing and unique. A mutation is
`0x01` put or `0x02` delete; put replaces the full value and delete is idempotent.

A range is one table and vnode with `[start_inclusive, end_exclusive)`, `start < end`,
`max_rows > 0`, and `max_bytes > 0`. Results use logical-key order. A row's logical byte charge is
the opaque key length plus value length, with checked arithmetic. A row is returned only if both
limits still fit. If the first matching row exceeds `max_bytes`, execution returns
`RowTooLarge { required_bytes }`; it never returns an empty, non-progressing page. After at least
one row, an additional matching row produces `has_more=true`.

`BatchLimits` contains `request_bytes_max_u64`, `read_rows_max_u64`, `read_bytes_max_u64`, and
`mutation_bytes_max_u64`. It independently bounds canonical request bytes, returned read rows,
returned logical read bytes, and mutation bytes. Mutation charge is opaque key plus value for put
and opaque key for delete. Point-read charge is the returned opaque key plus present value; range
charge uses the rule above. A generated request sets request and mutation byte maxima to
`workload.hard_batch_bytes`; its read-row maximum is point-read count plus every range's maximum
rows, and its read-byte maximum is selected key plus value width per point read plus every range's
maximum bytes. Both sums are checked, and the declared read-byte maximum must fit the profile
hard-batch limit. Because limits are encoded in the request, they are part of its digest identity.

The complete request and limits are validated before reading. All reads then observe one immutable
pre-mutation cut. Only after every read succeeds do all mutations become visible together. Any
validation, oversized-row, injected pre-commit, or capacity error leaves the live-state digest
unchanged. A successful result cannot mix pre- and post-mutation values.

### Lifecycle operations

Observable reference behavior is:

- `snapshot` is an immutable complete state cut.
- `export_vnode` returns one vnode's records in canonical logical-key order.
- `restore_vnode` validates the entire stream before atomically replacing that vnode.
- `drop_vnode` idempotently removes that vnode from live state only.
- `persist` atomically replaces the durable image with the complete live cut.
- `crash_reopen` discards live-only changes and restores the last durable image.

`RestoreBudget` has four explicit `u64` maxima: records, cumulative opaque key bytes, cumulative
value bytes, and canonical record bytes. A canonical record charge is
`1 + 4 + 4 + key_len + 4 + value_len` for table tag, vnode, key length/key, and value length/value.
All four totals use checked arithmetic. Wrong vnode, unordered or duplicate keys, invalid widths,
or any exceeded budget rejects the complete restore without changing live state.

The model does not represent assignment ownership. A real cleanup must be fenced and resumable,
but C1 `drop_vnode` cannot prove serving exclusion. Nor does this in-memory lifecycle prove
filesystem persistence, torn-write handling, corruption recovery, or process-death behavior.

## Deterministic request generation

Scenario tags are aggregate `0x01`, timer/window `0x02`, and join `0x03`. A 32-byte counter block
is:

```text
SHA-256(
  "LDB-SBQ-COUNTER-V1\0" || model_input_sha256[32] || seed_u64_be ||
  scenario_u8 || request_ordinal_u64_be || row_ordinal_u32_be || lane_u32_be
)
```

`word(row,lane)` is the first eight bytes as big-endian `u64`. Consecutive counter lanes expand
values. Row `0xffffffff` is reserved for request-level choices. Generator and reference-oracle
execution, state cloning, and digesting are outside the measured candidate service interval.

Keys have a fixed base followed, when needed, by entity-suffix blocks. A suffix block is SHA-256 of
the entity domain, model-input digest, `seed_u64`, `table_u8`, `vnode_u32`, `component_count_u8`,
each component as `u64` big-endian, and `block_index_u32`. Blocks start at zero, are concatenated,
and the last is truncated to the exact remaining key width. Components are the base fields in table
order: one for aggregate/window and two for timer/output. Join components are the join identity,
zero-extended encoded event time, and full `u64` stable row. This suffix depends on logical
identity, never the row that happened to reference it.

| Key | Base layout | Minimum width |
|---|---|---:|
| aggregate/window | identity `u64` | 8 |
| timer | logical time `u64`, stable row `u64` | 16 |
| join | join identity `u64`, event time low `u32`, stable row low `u32` | 16 |
| output bookkeeping | arriving row `u64`, matched row `u64` | 16 |

A case whose selected width is below its scenario's minimum is rejected. The current candidate's
smallest width is 16 bytes. Generic model tests still cover empty keys independently of generation.

Generator lane use is fixed:

| Scope | Lane | Meaning |
|---|---:|---|
| request | 0 | aggregate single-vnode choice, timer mode, or join arriving side |
| request | 1 | aggregate shared vnode or timer due-scan vnode; unused by join |
| row | 0 | aggregate bucket selector or join identity; unused by timer |
| row | 1 | hot/uniform identity choice, timer due offset, or join event time |
| row | 256+ | value expansion |

A put value is exactly `value_bytes` counter-expanded bytes. For table tag `t`, block `b` uses row
ordinal of the lowest contributing row and lane `256 + (u32(t) << 16) + b`; the final block is
truncated. This separates values for different tables while preserving aggregate deduplication.
The active vnode count is `workload.primary_vnode_count`; active key and value maxima are
`restore_limits.encoded_key_bytes_max` and `.stored_state_bytes_max`.

For aggregate and join cases, `uniform_domain` is
`max(1, floor(logical_state_bytes / (key_bytes + value_bytes)).saturating_sub(10))`, with checked
nonzero width addition.

Aggregate generation uses `word(row,0) % 1000` against the cumulative configured one-key,
nine-key, and uniform thresholds. IDs are `0`, `1 + word(row,1) % 9`, or
`10 + word(row,1) % uniform_domain`.
If `word(0xffffffff,0) % 1000` selects the configured single-vnode case, every group uses
`word(0xffffffff,1) % vnode_count`; otherwise vnode is group ID modulo vnode count. The request
sorts and deduplicates groups, reads each once, and puts one replacement value generated from the
lowest contributing row ordinal.

Timer mode is `word(0xffffffff,0) % 1000` against the cumulative configured mutation, due-scan, and
fire/delete weights. Stable row ID is `(request_ordinal << 32) | row_ordinal`, and its vnode is the
stable row ID modulo the active vnode count. Mutation mode reads window keys and puts window plus
timer records in that vnode; timer time is
`request_ordinal + 1 + word(row,1) % 1024`. Due-scan mode emits one timer-index scan on
`word(0xffffffff,1) % vnode_count` from time zero through `request_ordinal + 1` exclusive, bounded
by the profile timer row/byte maxima. Fire/delete mode reconstructs the prior ordinal's window and
timer identities using `source_ordinal=max(0, request_ordinal-1)`, including the source ordinal in
the counter call for its due offset. It reads both, replaces window state using the current request
value expansion, and deletes timer state atomically; ordinal zero uses source ordinal zero. Scan
boundary bases are `(time=0,row=0)` and
`(time=request_ordinal+1,row=0)` with zero padding to the selected width. All generated lists are
sorted and deduplicated.

Every measured generator request encodes `logical_rows=case.batch_rows`, including a timer
due-scan request; the field identifies the selected logical batch size rather than returned rows.

Join arriving side is left for an even `word(0xffffffff,0)` and right otherwise. Stable row ID is
`(request_ordinal << 32) | row_ordinal`; join identity is `word(row,0) % uniform_domain`; event time
low is `((request_ordinal << 16) | (word(row,1) & 0xffff))` as `u32`. Vnode is join identity modulo
the active vnode count. Each distinct row scans the opposite table in that vnode for the same join
identity and the saturating event-time interval
`[event_time - 1024, event_time + 1025)`, then puts the arriving row. Scan `max_rows` is
`max(1, join_match_count)`, so the zero-match case remains a valid empty probe. Its `max_bytes` is
`max_rows * (key_bytes + value_bytes)`, checked; the case is rejected if this or total request read
capacity exceeds the hard-batch limit. Exact fanout is tested by inserting the generator's
opposite-side keys before execution, not performed by ordinary model-result replay. Range
boundaries use the stated join identity and event-time endpoints, row zero, and zero padding. The
event-time addition is checked. Point reads, ranges, and mutations from every generator are sorted
and deduplicated by the batch invariants before encoding.

The v1 aggregate generator covers the profile's explicit one/nine/uniform hot mix. It does not
claim to implement the separate Zipf parameter. **DKS-Q2-001** blocks qualification execution until
the workload owner approves a cross-platform deterministic Zipf sampler and assigns named cases to
hot-mix versus Zipf workloads. That addition requires a new generator identity and goldens, not an
unreviewed floating-point implementation.

## Canonical encodings and result

Integers are unsigned big-endian. `bytes` is `u32 length || payload`; a collection is `u32 count ||
elements`; booleans are exactly `0x00` or `0x01`. For canonical requests, counts and lengths are
checked before output allocation, the encoder reserves its exact checked length once, and the
result may not exceed the 64 MiB model-core ceiling.

A logical key is `table_u8 || vnode_u32 || bytes(key)`. A request is request domain, `kind_u8`,
`scenario_u8`, `ordinal_u64`, `logical_rows_u32`, the four `BatchLimits` values in the order defined
above, then point, range, and mutation collections. A range is
`table_u8 || vnode_u32 || bytes(start) || bytes(end) || max_rows_u32 || max_bytes_u64`. A mutation
is its tag, logical key, then `bytes(value)` only for put.

An observation is observation domain, kind, scenario, ordinal, ordered point results, then ordered
range results. A point result repeats its logical key, presence byte, and present value bytes. A
range result repeats the encoded range, its ordered logical-key/value rows, and `has_more`.

State SHA-256 input is its domain, `u64` record count, then canonical logical-key/value records.
Each stream digest is its domain, `u64` item count, then `u64 length || item`. The combined trace is
its domain, `request_count_u64`, then `request_length_u64 || request || observation_length_u64 ||
observation` for each pair. Hex output is exactly 64 lowercase characters.

The strict result schema requires the following JSON pointers. Each object rejects additional
properties, and every listed property is required:

| Pointer | Type or constant |
|---|---|
| `/schema_version` | `"state-backend-model-result/v1"` |
| `/notice` | `"NOT QUALIFICATION EVIDENCE"` |
| `/qualification_eligible` | `false` |
| `/versions/model` | `"state-backend-reference/v1"` |
| `/versions/generator` | `"state-backend-workload/v1"` |
| `/versions/request_encoding` | `"LDB-SBQ-REQUEST-V1"` |
| `/versions/observation_encoding` | `"LDB-SBQ-OBSERVATION-V1"` |
| `/versions/state_encoding` | `"LDB-SBQ-STATE-V1"` |
| `/profile/id` | nonempty string from the validated profile |
| `/profile/sha256` | 64 lowercase hexadecimal characters |
| `/profile/model_input_sha256` | 64 lowercase hexadecimal characters |
| `/case/*` | the eight properties and constraints in the model-case object above |
| `/counters/requests` | `u64` JSON integer |
| `/counters/logical_input_rows` | `u64` JSON integer |
| `/counters/point_reads` | `u64` JSON integer |
| `/counters/range_reads` | `u64` JSON integer |
| `/counters/puts` | `u64` JSON integer |
| `/counters/deletes` | `u64` JSON integer |
| `/counters/returned_point_values` | `u64` JSON integer |
| `/counters/returned_point_bytes` | `u64` JSON integer |
| `/counters/returned_range_rows` | `u64` JSON integer |
| `/counters/returned_range_bytes` | `u64` JSON integer |
| `/digests/requests_sha256` | 64 lowercase hexadecimal characters |
| `/digests/observations_sha256` | 64 lowercase hexadecimal characters |
| `/digests/trace_sha256` | 64 lowercase hexadecimal characters |
| `/digests/live_state_sha256` | 64 lowercase hexadecimal characters |

All counters are JSON integers in `u64` range. The model replay starts empty, executes only measured
generated requests, performs no implicit setup or persistence, streams its digests, and reports
final live state. The validator revalidates the profile, checks the case, regenerates the replay,
and compares every property; JSON Schema validation alone is insufficient. Duplicate JSON keys,
unknown fields, negative/floating values, malformed or uppercase hashes, and inconsistent counters
are errors. The CLI may validate a model result but exposes no candidate execution command.

## Fault vocabulary and required tests

Fault locations are stable zero-based `(phase, occurrence)` values. Occurrence counts eligible hook
visits independently per phase from zero across one fresh scenario replay and resets only when that
replay starts. The same one-shot injector and counters continue across an operation retry. An armed
target that was not reached makes the replay invalid; the runner must finalize the injector and
check this explicitly. Batch hooks fire immediately before the complete mutation install and
immediately after that install but
before acknowledgement. Persist hooks fire immediately before and immediately after the durable
image swap. `snapshot_open` fires before capturing/publishing the cut. Record hooks fire
immediately before copying, staging, or deleting the selected canonical record; records with smaller
occurrences have completed. A failed export publishes nothing, a failed restore leaves the active
vnode unchanged, and cleanup may have removed only smaller-occurrence physical records and must be
safe to retry under an external ownership fence. V1 phases are
`batch_before_commit`, `batch_after_commit_before_ack`, `persist_before`,
`persist_after_success_before_ack`, `snapshot_open`, `export_record`, `restore_record`, and
`cleanup_record`. C1 tests only semantic outcomes: atomic batches are complete pre/post, successful
persist is a complete durable cut, failed export is unpublished, restore is unchanged until full
replacement, and cleanup is idempotent. Real kills, torn files, `ENOSPC`, corruption, FD pressure,
ownership fences, and endurance remain C3 evidence under the
[Phase 0 plan](../plans/distributed-keyed-state-phase-0-execution.md).

Pre-success and record cuts return `InjectedFault` with the exact ordinal. The two post-success cuts
return `AmbiguousAfterSuccess` with the exact ordinal: the live or durable cut is complete even
though acknowledgement was lost. Retrying a batch may observe a different pre-mutation cut, so
this model does not turn an ambiguous acknowledgement into an exactly-once output claim. Invalid
batches, failed reads, and invalid restore inputs consume no batch or restore hooks; empty
record sets consume no record hooks.

C1 tests must cover frozen request/observation/state/trace goldens; direct versus sequential
generation; aggregate deduplication and timer modes; both join sides and manual 0/1/8/64 fanout;
point and range ordering; half-open and exact/max-plus-one scan limits; oversized-row errors;
checked-overflow and 64 MiB replay rejection; validate-before-mutate atomicity; empty keys/values;
immutable snapshots; vnode isolation; explicit restore budgets; persist/reopen cuts; and strict
result regeneration including duplicate/unknown-field rejection.

Backend runner policy, raw samples and derived quantiles, pass/invalid aggregation, resource
formulas, N/N-1 versions, physical keyspace layout, candidate-specific durability, and immutable
evidence retention are C2/C3 decisions in the Phase 0 plan. Backend-local observations cannot
satisfy product checkpoint, recovery, delivery, exactly-once, or the
[independent production soak](../testing/distributed-state-production-soak-charter.md).
