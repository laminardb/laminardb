# Managed state artifact format v1

- **Status:** Frozen admission-neutral codec contract; not selected by manifests
- **Parent decision:** [ADR-008](ADR-008-managed-vnode-keyed-state.md)
- **Byte order:** Big-endian for every integer

This is the normative byte layout for the first managed grouped-aggregate artifact and its
`VnodePartialV2` directory. Cycle 5 lands a private borrowed aggregate reader plus a full-buffer
fixture/reference encoder, and a private borrowed outer-directory reader plus a full-buffer
fixture/reference encoder. These encoders are not production streaming writers. The current
checkpoint manifest has no selector for these bytes, so a production reader or writer must not
sniff this magic or fall back from this format to legacy rkyv. A manifest-version/format-selector
change, rolling reader capability proof, trusted sealed-object composition, bounded object fetch,
and complete restore transaction are later admission blockers.

All offsets are from the start of the containing byte slice. All arithmetic is checked before
conversion to `usize` or allocation. Declared total length must equal the supplied slice exactly;
padding and trailing bytes are invalid. Reserved bytes must be zero. SHA-256 detects corruption but
does not authenticate an untrusted checkpoint store.

## Common values

| Value | Encoding |
|---|---:|
| partition ABI | `1` |
| managed aggregate codec ID | `1` (`COUNT_STAR_SUM_I64_APPEND_ONLY`) |
| managed aggregate codec version | `1` |
| managed key mode | `1` (`VNODE_KEYED`) |
| artifact kind | `0` NONE, `1` FULL, `2` DELTA, `3` EMPTY |
| directory entry kind | `1` BODY, `2` REFERENCE |
| state width | 24 bytes |

`vnode_count` and `vnode` are `u32` to match the assignment and sealed-partial APIs. A reader still
requires `1 <= vnode_count <= 65_535` and `vnode < vnode_count` under partition ABI v1.

An attempt is canonical only when `epoch == checkpoint_id != 0`. A DELTA parent must be canonical
and exactly `current - 1` in both fields. A REFERENCE parent must be canonical and strictly older,
but may skip checkpoints because an unchanged vnode can name an older sealed body. FULL and EMPTY
have no parent. The transition resolver counts each DELTA or REFERENCE edge against
`resolved_parent_links_max`; a FULL/EMPTY base has depth zero.

## Aggregate state contract

The codec-1 contract is exactly 64 bytes. It describes the only first-candidate semantic shape;
the stable operator digest identifies the selected direct SUM input expression.

| Offset | Width | Field | Required value |
|---:|---:|---|---|
| 0 | 8 | magic | `LDBMAC\0\0` |
| 8 | 2 | contract version | `1` |
| 10 | 2 | contract length | `64` |
| 12 | 4 | codec ID | `1` |
| 16 | 2 | codec version | `1` |
| 18 | 2 | partition ABI | `1` |
| 20 | 1 | key mode | `1` (`VNODE_KEYED`) |
| 21 | 1 | update mode | `1` (`APPEND_ONLY`) |
| 22 | 1 | count kind | `1` (`COUNT_STAR`) |
| 23 | 1 | reserved | `0` |
| 24 | 1 | sum kind | `1` (`SUM_INT64`) |
| 25 | 1 | SUM input nullable | `0` or `1` |
| 26 | 1 | COUNT output type | `1` (`INT64`) |
| 27 | 1 | COUNT output nullable | `0` |
| 28 | 1 | SUM output type | `1` (`INT64`) |
| 29 | 1 | SUM output nullable | `1` |
| 30 | 2 | state width | `24` |
| 32 | 32 | routing schema SHA-256 | digest of the exact `PartitionKeySchemaV1` bytes |

The contract is compared byte-for-byte with the immutable plan-owned expected contract after its
digest is checked. It is never inferred from artifact data.

## Managed aggregate envelope

Magic is `LDBMGA\0\0`, version is `1`, and the fixed header is 384 bytes.

| Offset | Width | Field |
|---:|---:|---|
| 0 | 8 | magic |
| 8 | 2 | version |
| 10 | 2 | header length (`384`) |
| 12 | 1 | artifact kind |
| 13 | 1 | key mode (`1`) |
| 14 | 2 | flags/reserved (`0`) |
| 16 | 8 | total length |
| 24 | 8 | attempt epoch |
| 32 | 8 | attempt checkpoint ID |
| 40 | 8 | parent epoch |
| 48 | 8 | parent checkpoint ID |
| 56 | 8 | assignment version |
| 64 | 2 | partition ABI (`1`) |
| 66 | 2 | codec version (`1`) |
| 68 | 4 | codec ID (`1`) |
| 72 | 4 | vnode count |
| 76 | 4 | claimed vnode |
| 80 | 8 | row count |
| 88 | 8 | aggregate key bytes |
| 96 | 8 | aggregate state bytes |
| 104 | 4 | state width (`24`) |
| 108 | 4 | reserved (`0`) |
| 112 | 8 | routing-schema offset (`384`) |
| 120 | 8 | routing-schema length |
| 128 | 8 | state-contract offset (routing end) |
| 136 | 8 | state-contract length (`64`) |
| 144 | 8 | rows offset (contract end) |
| 152 | 8 | rows length |
| 160 | 32 | assignment-certificate SHA-256 |
| 192 | 32 | stable operator-identity SHA-256 |
| 224 | 32 | stable state-table-identity SHA-256 |
| 256 | 32 | routing-schema SHA-256 |
| 288 | 32 | state-contract SHA-256 |
| 320 | 32 | rows SHA-256 |
| 352 | 32 | exact parent-entry SHA-256 |

The assignment digest is exactly `CheckpointAssignmentFence::digest()`, not its inner owner-map
digest. These are the **source checkpoint fence**, never the target rebalance assignment; the
target belongs only to transition metadata. Assignment version and the assignment, operator, and
state-table digests are nonzero. Exact stable operator/state-table identity derivation remains a
plan-contract blocker before a writer is enabled; the format treats each as a plan-owned opaque
32-byte value and a reader compares it with its expected value.

Sections are contiguous in routing, contract, rows order. The routing descriptor is nonempty and
is compared byte-for-byte with the cached `PartitionKeySchemaV1`. The contract is the exact 64-byte
record above. FULL and DELTA require at least one row; EMPTY is the only zero-row form and has zero
key/state/rows lengths plus `SHA256(empty)` for the rows digest. FULL/EMPTY parent fields and parent
digest are zero. DELTA uses the exact immediately preceding attempt and a nonzero parent-entry
digest.

Each row is:

```text
u32 key_length | key bytes | u64 COUNT(*) | u64 SUM non-null count | i64 SUM
```

The three state fields are big-endian and occupy exactly 24 bytes. A zero-length encoded key is
valid for a nonempty ABI-v1 `Null` grouping schema. Rows are strictly increasing by unsigned key
bytes; duplicates are invalid. Every key must map to the claimed vnode through
`PartitionKeyCodecV1::vnode_for_encoded` and the declared vnode count.

A persisted row requires `1 <= COUNT <= i64::MAX`, non-null count no greater than COUNT, and a zero
SUM when non-null count is zero. When the cached contract declares a non-nullable SUM input, the
non-null count must equal COUNT. A zero non-null count evaluates to SQL NULL; otherwise SUM is its
exact signed `i64` value. `state_bytes == row_count * 24`, and scanned row, key, and state totals must
equal the header. The landed reference encoder consumes already sorted rows and builds the complete
byte vector in memory; it is fixture infrastructure, not the production checkpoint writer. A future
writer sorts on a bounded blocking checkpoint worker or consumes an ordered LSM scan, never on the
event-loop hot path, and streams under the artifact budget.

## VnodePartialV2 directory

The current object-store provenance wrapper already uses `LDBVP2\0\0`; the inner directory
therefore uses distinct magic `LDBVPD\0\0`. Directory version is `2`, its header is 160 bytes, and
each entry is 168 bytes.

| Offset | Width | Field |
|---:|---:|---|
| 0 | 8 | magic |
| 8 | 2 | directory version (`2`) |
| 10 | 2 | header length (`160`) |
| 12 | 2 | entry length (`168`) |
| 14 | 2 | reserved (`0`) |
| 16 | 8 | total length |
| 24 | 8 | attempt epoch |
| 32 | 8 | attempt checkpoint ID |
| 40 | 8 | assignment version |
| 48 | 2 | partition ABI (`1`) |
| 50 | 2 | reserved (`0`) |
| 52 | 4 | vnode count |
| 56 | 4 | claimed vnode |
| 60 | 4 | entry count |
| 64 | 8 | directory offset (`160`) |
| 72 | 8 | directory length (`entry_count * 168`) |
| 80 | 8 | body offset (directory end) |
| 88 | 8 | body length |
| 96 | 32 | assignment-certificate SHA-256 |
| 128 | 32 | directory SHA-256 |

Entry count is nonzero, is no greater than `directory_entries_per_artifact_max`, and must equal the
plan-owned authoritative roster. Roster tuples are exact, unique, and sorted lexicographically by
raw `(operator digest, state-table digest, vnode)` bytes. Both identity digests are nonzero and every
entry vnode equals the header vnode. A directory with only references has an empty body region.

### Directory entry

| Offset | Width | Field |
|---:|---:|---|
| 0 | 32 | stable operator-identity SHA-256 |
| 32 | 32 | stable state-table-identity SHA-256 |
| 64 | 4 | vnode |
| 68 | 1 | entry kind |
| 69 | 1 | artifact kind |
| 70 | 2 | managed envelope version (`1`) |
| 72 | 8 | absolute body offset |
| 80 | 8 | body length |
| 88 | 32 | body SHA-256 |
| 120 | 8 | parent epoch |
| 128 | 8 | parent checkpoint ID |
| 136 | 32 | exact parent-entry SHA-256 |

BODY has a positive body length and FULL, DELTA, or EMPTY kind. BODY ranges appear contiguously in
directory order, start at the header body offset, and exactly cover the body region. Its body digest
matches the exact slice. FULL/EMPTY have zero parent fields; DELTA has an immediately preceding
canonical parent and nonzero parent-entry digest. The landed V2 reader stops after this outer
structural validation. Production composition must first verify the complete raw V2 payload against
the trusted seal/inventory digest, then invoke the manifest-selected inner decoder for every BODY;
that decoder verifies that the envelope repeats the same attempt, assignment, identity, vnode, kind,
and parent context.

REFERENCE has NONE artifact kind, zero body offset/length/digest, and an older canonical parent plus
nonzero parent-entry digest. REFERENCE never means EMPTY. The resolver fetches the exact named
parent, finds the same roster tuple, recomputes its contextual entry digest, and compares it before
following another edge.

The contextual entry digest is exactly:

```text
SHA256(
  "laminardb-vnode-partial-v2-entry-sha256\0" |
  containing_epoch_be |
  containing_checkpoint_id_be |
  containing_assignment_version_be |
  containing_partition_abi_be |
  containing_vnode_count_be |
  containing_claimed_vnode_be |
  containing_assignment_certificate_sha256 |
  exact_168_entry_bytes
)
```

Binding the containing context prevents transplanting a valid raw entry across attempts or
assignments. It deliberately does not include unrelated directory entries or bodies.

## Limits and composition

`encoded_artifact_bytes_max` applies to the complete raw V2 directory plus bodies, excluding the
existing 136-byte object-store provenance wrapper. Fetch admission checks the wrapper plus declared
payload with checked addition before GET. Row, key-byte, and state-byte artifact limits are totals
across every BODY in that V2 object. Production composition creates one caller-owned, non-`Copy`
mutable `AggregateObjectBudget` for the object and passes it through every inner aggregate decode;
resetting it per BODY is invalid. Directory metadata uses the metadata and entry caps. Chain bytes
and parent-link count accumulate across every fetched REFERENCE/DELTA edge.

The borrowed outer-directory reader receives an out-of-band expected context containing the source
attempt and assignment fence, ABI/count/vnode, and exact sorted plan roster. That is structural
validation, not artifact authentication or aggregate-state validation. A production composition
layer must receive bytes already matched to the trusted seal's top-level digest and manifest format,
then supply each BODY to the selected inner reader with its exact expected envelope, codec, routing
schema, state contract, identity, kind, and parent context while sharing the aggregate-object budget.
Passing only hashes stored inside the same payload is not sufficient proof. SHA-256 cannot protect a
deployment where an attacker may also rewrite the trust root.

Any change to partition ABI, vnode count, routing schema, state contract, or source assignment
certificate forces FULL/EMPTY rather than a DELTA. During whole-chain validation, DELTA is a latest
value/replacement record: for a key found in its parent, COUNT must strictly increase and SUM
non-null count cannot decrease. These cross-chain checks do not belong to the isolated row decoder.

The borrowed codecs validate bytes already admitted under those reservations. Their reference
encoders allocate complete output vectors and are only for frozen fixtures and conformance. None of
this makes the current whole-object `read_partial` bounded, verifies a trusted top-level seal,
resolves chains, invokes operators, installs state, alters manifest dispatch, or relaxes
`[LDB-4007]`. Whole-transition preflight must validate every roster entry, aggregate the per-object
counters, and finish every chain before any operator callback.

## Rolling compatibility

Release N may ship these private admission-neutral readers, reference encoders, and frozen goldens
without a production writer. A future N+1 streaming writer is enabled only after every checkpoint
participant advertises the manifest-selected reader capability and trusted sealed composition is
wired. Legacy rkyv remains reachable only through explicit legacy inventory proof for the admitted
global vnode-0 path. There is no magic-sniff fallback in either direction.
