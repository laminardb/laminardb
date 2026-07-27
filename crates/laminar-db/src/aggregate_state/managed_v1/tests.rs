use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray};
use arrow::datatypes::{DataType, Field};

use super::*;
use crate::aggregate_state::artifact_v1::{ParentLink, STATE_WIDTH};

const OPERATOR_ID: [u8; 32] = [0x22; 32];
const STATE_TABLE_ID: [u8; 32] = [0x33; 32];

fn assignment(version: u64) -> ManagedAssignmentFence {
    let marker = u8::try_from(version).unwrap();
    ManagedAssignmentFence::try_new(version, [marker; 32]).unwrap()
}

fn identity(
    routing_schema: &PartitionKeySchemaV1,
    vnode_count: NonZeroU32,
    vnode: u32,
    sum_input_nullable: bool,
) -> ManagedVnodeIdentityV1 {
    ManagedVnodeIdentityV1::try_new(
        routing_schema.clone(),
        AggregateContractV1::new(routing_schema, sum_input_nullable),
        OPERATOR_ID,
        STATE_TABLE_ID,
        vnode_count,
        vnode,
    )
    .unwrap()
}

fn schema() -> PartitionKeySchemaV1 {
    PartitionKeySchemaV1::try_new(&[Arc::new(Field::new("group", DataType::Binary, false))])
        .unwrap()
}

fn limits(payload: u64) -> ManagedVnodeLimits {
    ManagedVnodeLimits {
        max_rows: 64,
        max_encoded_key_bytes: 256,
        max_logical_payload_bytes: payload,
    }
}

fn budget() -> AggregateObjectBudget {
    AggregateObjectBudget {
        envelope_metadata_bytes_max: 4096,
        routing_schema_bytes_max: 1024,
        state_contract_bytes_max: 1024,
        encoded_key_bytes_max: 256,
        stored_state_bytes_max: 256,
        remaining_artifact_bytes: 1 << 20,
        remaining_rows: 1024,
        remaining_key_bytes: 1 << 16,
        remaining_state_bytes: 1 << 16,
    }
}

fn context(
    routing_schema: &PartitionKeySchemaV1,
    contract: AggregateContractV1,
    kind: ArtifactKind,
    assignment_version: u64,
    vnode_count: NonZeroU32,
    vnode: u32,
) -> ArtifactContext<'_> {
    ArtifactContext {
        kind,
        attempt: laminar_core::state::CheckpointAttempt::canonical(1),
        parent: None,
        assignment_version,
        assignment_certificate_sha256: assignment(assignment_version).certificate_sha256,
        operator_identity_sha256: OPERATOR_ID,
        state_table_identity_sha256: STATE_TABLE_ID,
        vnode_count,
        vnode,
        routing_schema,
        contract,
    }
}

fn encoded_key_for_vnode(vnode_count: NonZeroU32, wanted: u32, start: u32) -> Vec<u8> {
    let codec = PartitionKeyCodecV1::try_new([DataType::Binary]).unwrap();
    for candidate in start..10_000 {
        let bytes = candidate.to_be_bytes();
        let columns: Vec<ArrayRef> =
            vec![Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())]))];
        let rows = codec.encode_columns(&columns).unwrap();
        let key = rows.row(0).as_ref().to_vec();
        if PartitionKeyCodecV1::vnode_for_encoded(&key, vnode_count) == wanted {
            return key;
        }
    }
    panic!("no encoded key found for vnode {wanted}");
}

fn empty_store(
    routing_schema: &PartitionKeySchemaV1,
    vnode_count: NonZeroU32,
    vnode: u32,
    assignment_version: u64,
    limits: ManagedVnodeLimits,
) -> ManagedCountSumVnodeV1 {
    ManagedCountSumVnodeV1::empty(
        identity(routing_schema, vnode_count, vnode, true),
        assignment(assignment_version),
        limits,
    )
    .unwrap()
}

fn encoded_full(
    routing_schema: &PartitionKeySchemaV1,
    vnode_count: NonZeroU32,
    vnode: u32,
    assignment_version: u64,
    rows: &[AggregateRow<'_>],
) -> Vec<u8> {
    let contract = AggregateContractV1::new(routing_schema, true);
    artifact_v1::encode(
        context(
            routing_schema,
            contract,
            ArtifactKind::Full,
            assignment_version,
            vnode_count,
            vnode,
        ),
        rows,
        &mut budget(),
    )
    .unwrap()
}

fn publish_one(
    store: &mut ManagedCountSumVnodeV1,
    prepared: PreparedManagedVnodeChangeV1,
) -> Result<Option<ManagedCountSumVnodeV1>, ArtifactError> {
    let mut live = [store];
    let mut retired = publish_prepared_changes(&mut live, vec![prepared])?;
    Ok(retired.pop())
}

#[test]
fn append_batch_is_source_ordered_vnode_fenced_and_accounted() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(4).unwrap();
    let key = encoded_key_for_vnode(vnode_count, 0, 0);
    let other_vnode_key = encoded_key_for_vnode(vnode_count, 1, 0);
    assert_ne!(key, other_vnode_key);
    let mut store = empty_store(&routing, vnode_count, 0, 7, limits(4096));

    let assignment_error = store
        .apply_append_batch(
            assignment(8),
            &[GroupAppend {
                key: &key,
                sum_inputs: &[Some(1)],
            }],
        )
        .unwrap_err();
    assert_eq!(
        assignment_error,
        ArtifactError::Invalid("managed vnode assignment fence")
    );
    let vnode_error = store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &other_vnode_key,
                sum_inputs: &[Some(1)],
            }],
        )
        .unwrap_err();
    assert_eq!(
        vnode_error,
        ArtifactError::Invalid("managed append row vnode")
    );

    store
        .apply_append_batch(
            assignment(7),
            &[
                GroupAppend {
                    key: &key,
                    sum_inputs: &[Some(1)],
                },
                GroupAppend {
                    key: &key,
                    sum_inputs: &[None, Some(2)],
                },
            ],
        )
        .unwrap();
    let state = store.state(&key).unwrap();
    assert_eq!(state.count(), 3);
    assert_eq!(state.sum_non_null_count(), 2);
    assert_eq!(state.sum(), Some(3));
    assert_eq!(store.len(), 1);
    assert_eq!(
        store.logical_payload_bytes(),
        u64::try_from(key.len() + STATE_WIDTH).unwrap()
    );
}

#[test]
fn non_nullable_contract_rejects_null_before_any_batch_mutation() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let first = encoded_key_for_vnode(vnode_count, 0, 0);
    let second = encoded_key_for_vnode(vnode_count, 0, 1);
    assert_ne!(first, second);
    let mut store = ManagedCountSumVnodeV1::empty(
        identity(&routing, vnode_count, 0, false),
        assignment(7),
        limits(4096),
    )
    .unwrap();

    let error = store
        .apply_append_batch(
            assignment(7),
            &[
                GroupAppend {
                    key: &first,
                    sum_inputs: &[Some(3)],
                },
                GroupAppend {
                    key: &second,
                    sum_inputs: &[None],
                },
            ],
        )
        .unwrap_err();

    assert_eq!(error, ArtifactError::Invalid("non-null SUM count"));
    assert_eq!(store.len(), 0);
    assert_eq!(store.logical_payload_bytes(), 0);
}

#[test]
fn late_group_and_late_prefix_overflow_leave_live_state_unchanged() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let full_key = encoded_key_for_vnode(vnode_count, 0, 0);
    let innocent_key = encoded_key_for_vnode(vnode_count, 0, 1);
    assert_ne!(full_key, innocent_key);
    let max_count = i64::MAX.unsigned_abs();
    let full = encoded_full(
        &routing,
        vnode_count,
        0,
        7,
        &[AggregateRow {
            key: &full_key,
            state: CountSumStateV1::persisted(max_count, 1, 5).unwrap(),
        }],
    );
    let contract = AggregateContractV1::new(&routing, true);
    let mut store = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let prepared = store
        .prepare_replacement(
            &full,
            context(&routing, contract, ArtifactKind::Full, 7, vnode_count, 0),
            assignment(7),
            &mut budget(),
        )
        .unwrap();
    drop(publish_one(&mut store, prepared).unwrap());
    let before_bytes = store.logical_payload_bytes();

    let error = store
        .apply_append_batch(
            assignment(7),
            &[
                GroupAppend {
                    key: &innocent_key,
                    sum_inputs: &[Some(9)],
                },
                GroupAppend {
                    key: &full_key,
                    sum_inputs: &[None],
                },
            ],
        )
        .unwrap_err();
    assert_eq!(error, ArtifactError::CountOverflow);
    assert!(store.state(&innocent_key).is_none());
    assert_eq!(store.state(&full_key).unwrap().count(), max_count);
    assert_eq!(store.logical_payload_bytes(), before_bytes);

    let fresh_key = encoded_key_for_vnode(vnode_count, 0, 2);
    assert_ne!(fresh_key, full_key);
    assert_ne!(fresh_key, innocent_key);
    let error = store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &fresh_key,
                sum_inputs: &[Some(i64::MAX), Some(1)],
            }],
        )
        .unwrap_err();
    assert_eq!(error, ArtifactError::SumOverflow);
    assert!(store.state(&fresh_key).is_none());
    assert_eq!(store.logical_payload_bytes(), before_bytes);
}

#[test]
fn logical_payload_limit_is_checked_before_mutation() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let first = encoded_key_for_vnode(vnode_count, 0, 0);
    let second = encoded_key_for_vnode(vnode_count, 0, 1);
    assert_ne!(first, second);
    let exact = u64::try_from(first.len() + STATE_WIDTH).unwrap();
    let mut store = empty_store(&routing, vnode_count, 0, 7, limits(exact));
    let error = store
        .apply_append_batch(
            assignment(7),
            &[
                GroupAppend {
                    key: &first,
                    sum_inputs: &[None],
                },
                GroupAppend {
                    key: &second,
                    sum_inputs: &[Some(1)],
                },
            ],
        )
        .unwrap_err();
    assert_eq!(
        error,
        ArtifactError::Limit("managed logical payload byte limit")
    );
    assert_eq!(store.len(), 0, "the first group was preflight-only");
    assert_eq!(store.logical_payload_bytes(), 0);

    store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &first,
                sum_inputs: &[None],
            }],
        )
        .unwrap();
    let error = store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &second,
                sum_inputs: &[Some(1)],
            }],
        )
        .unwrap_err();
    assert_eq!(
        error,
        ArtifactError::Limit("managed logical payload byte limit")
    );
    assert_eq!(store.len(), 1);
    assert_eq!(store.logical_payload_bytes(), exact);
}

#[test]
fn full_and_empty_artifacts_replace_the_entire_vnode_image() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let donor_key = encoded_key_for_vnode(vnode_count, 0, 0);
    let stale_key = encoded_key_for_vnode(vnode_count, 0, 1);
    assert_ne!(donor_key, stale_key);
    let contract = AggregateContractV1::new(&routing, true);
    let mut donor = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    donor
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &donor_key,
                sum_inputs: &[None, Some(4)],
            }],
        )
        .unwrap();
    let full_context = context(&routing, contract, ArtifactKind::Full, 7, vnode_count, 0);
    let full = donor.freeze_full(full_context, &mut budget()).unwrap();

    let mut receiver = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    receiver
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &stale_key,
                sum_inputs: &[Some(99)],
            }],
        )
        .unwrap();
    let prepared = receiver
        .prepare_replacement(&full, full_context, assignment(8), &mut budget())
        .unwrap();
    let retired = publish_one(&mut receiver, prepared).unwrap().unwrap();
    assert!(receiver.state(&stale_key).is_none());
    assert_eq!(receiver.state(&donor_key).unwrap().sum(), Some(4));
    assert_eq!(receiver.assignment_version(), 8);
    assert!(retired.state(&stale_key).is_some());

    let empty_donor = empty_store(&routing, vnode_count, 0, 8, limits(4096));
    let empty_context = context(&routing, contract, ArtifactKind::Empty, 8, vnode_count, 0);
    let empty = empty_donor
        .freeze_full(empty_context, &mut budget())
        .unwrap();
    let prepared = receiver
        .prepare_replacement(&empty, empty_context, assignment(9), &mut budget())
        .unwrap();
    drop(publish_one(&mut receiver, prepared).unwrap());
    assert_eq!(receiver.len(), 0);
    assert_eq!(receiver.logical_payload_bytes(), 0);
    assert_eq!(receiver.assignment_version(), 9);
}

#[test]
fn failed_or_stale_prepare_never_replaces_live_state_and_reacquire_is_explicit() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let key = encoded_key_for_vnode(vnode_count, 0, 0);
    let contract = AggregateContractV1::new(&routing, true);
    let mut store = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &key,
                sum_inputs: &[Some(3)],
            }],
        )
        .unwrap();
    let full_context = context(&routing, contract, ArtifactKind::Full, 7, vnode_count, 0);
    let full = store.freeze_full(full_context, &mut budget()).unwrap();
    let truncated = &full[..full.len() - 1];
    assert!(store
        .prepare_replacement(truncated, full_context, assignment(8), &mut budget())
        .is_err());
    assert_eq!(store.state(&key).unwrap().sum(), Some(3));

    let prepared = store
        .prepare_replacement(&full, full_context, assignment(8), &mut budget())
        .unwrap();
    let revoke = store.prepare_revoke(assignment(8)).unwrap();
    let retired = publish_one(&mut store, revoke).unwrap().unwrap();
    assert_eq!(store.len(), 0);
    assert_eq!(store.assignment_version(), 8);
    assert!(!store.is_active());
    assert_eq!(
        store
            .apply_append_batch(
                assignment(8),
                &[GroupAppend {
                    key: &key,
                    sum_inputs: &[Some(4)],
                }],
            )
            .unwrap_err(),
        ArtifactError::Invalid("managed vnode is not active")
    );
    assert_eq!(
        store.freeze_full(full_context, &mut budget()).unwrap_err(),
        ArtifactError::Invalid("managed vnode is not active")
    );
    assert_eq!(
        publish_one(&mut store, prepared).unwrap_err(),
        ArtifactError::Invalid("stale managed vnode change")
    );

    let prepared = store
        .prepare_replacement(&full, full_context, assignment(8), &mut budget())
        .unwrap();
    drop(publish_one(&mut store, prepared).unwrap());
    assert_eq!(store.assignment_version(), 8);
    assert!(store.is_active());
    assert_eq!(store.state(&key).unwrap().sum(), Some(3));
    assert_eq!(retired.assignment_version(), 7);
}

#[test]
fn mutation_after_prepare_makes_the_replacement_stale() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let restored_key = encoded_key_for_vnode(vnode_count, 0, 0);
    let later_key = encoded_key_for_vnode(vnode_count, 0, 100);
    assert_ne!(restored_key, later_key);
    let contract = AggregateContractV1::new(&routing, true);
    let donor_rows = [AggregateRow {
        key: &restored_key,
        state: CountSumStateV1::persisted(1, 1, 3).unwrap(),
    }];
    let full = encoded_full(&routing, vnode_count, 0, 7, &donor_rows);
    let full_context = context(&routing, contract, ArtifactKind::Full, 7, vnode_count, 0);
    let mut store = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let prepared = store
        .prepare_replacement(&full, full_context, assignment(8), &mut budget())
        .unwrap();

    store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &later_key,
                sum_inputs: &[Some(9)],
            }],
        )
        .unwrap();

    assert_eq!(
        publish_one(&mut store, prepared).unwrap_err(),
        ArtifactError::Invalid("stale managed vnode change")
    );
    assert_eq!(store.state(&later_key).unwrap().sum(), Some(9));
    assert!(store.state(&restored_key).is_none());
}

#[test]
fn retained_vnode_advances_its_exact_fence_without_losing_state() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let key = encoded_key_for_vnode(vnode_count, 0, 0);
    let contract = AggregateContractV1::new(&routing, true);
    let mut store = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &key,
                sum_inputs: &[Some(5)],
            }],
        )
        .unwrap();

    let retained = store.prepare_retained_fence(assignment(8)).unwrap();
    assert!(publish_one(&mut store, retained).unwrap().is_none());
    assert_eq!(store.state(&key).unwrap().sum(), Some(5));
    assert_eq!(store.assignment_version(), 8);
    assert_eq!(
        store
            .apply_append_batch(
                assignment(7),
                &[GroupAppend {
                    key: &key,
                    sum_inputs: &[Some(1)],
                }],
            )
            .unwrap_err(),
        ArtifactError::Invalid("managed vnode assignment fence")
    );
    store
        .apply_append_batch(
            assignment(8),
            &[GroupAppend {
                key: &key,
                sum_inputs: &[Some(2)],
            }],
        )
        .unwrap();
    let checkpoint_context = context(&routing, contract, ArtifactKind::Full, 8, vnode_count, 0);
    assert!(!store
        .freeze_full(checkpoint_context, &mut budget())
        .unwrap()
        .is_empty());
    assert_eq!(store.state(&key).unwrap().sum(), Some(7));
}

#[test]
fn conflicting_same_version_certificate_is_rejected() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let contract = AggregateContractV1::new(&routing, true);
    let store = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let source_context = context(&routing, contract, ArtifactKind::Empty, 7, vnode_count, 0);
    let bytes = store.freeze_full(source_context, &mut budget()).unwrap();
    let conflicting = ManagedAssignmentFence::try_new(7, [0xaa; 32]).unwrap();

    assert_eq!(
        store
            .prepare_replacement(&bytes, source_context, conflicting, &mut budget())
            .unwrap_err(),
        ArtifactError::Invalid("managed restore context")
    );
}

#[test]
fn prepared_change_is_bound_to_one_live_shard_instance() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let contract = AggregateContractV1::new(&routing, true);
    let source = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let source_context = context(&routing, contract, ArtifactKind::Empty, 7, vnode_count, 0);
    let bytes = source.freeze_full(source_context, &mut budget()).unwrap();
    let prepared = source
        .prepare_replacement(&bytes, source_context, assignment(8), &mut budget())
        .unwrap();
    let mut separate_instance = empty_store(&routing, vnode_count, 0, 7, limits(4096));

    assert_eq!(
        publish_one(&mut separate_instance, prepared).unwrap_err(),
        ArtifactError::Invalid("stale managed vnode change")
    );
    assert_eq!(separate_instance.assignment_version(), 7);
    assert!(separate_instance.is_active());
}

#[test]
fn stale_later_vnode_prevents_any_transition_publication() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(2).unwrap();
    let second_key = encoded_key_for_vnode(vnode_count, 1, 0);
    let mut first = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let mut second = empty_store(&routing, vnode_count, 1, 7, limits(4096));
    let first_change = first.prepare_retained_fence(assignment(8)).unwrap();
    let stale_second_change = second.prepare_retained_fence(assignment(8)).unwrap();
    second
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &second_key,
                sum_inputs: &[Some(1)],
            }],
        )
        .unwrap();

    let mut live = [&mut first, &mut second];
    assert_eq!(
        publish_prepared_changes(&mut live, vec![first_change, stale_second_change]).unwrap_err(),
        ArtifactError::Invalid("stale managed vnode change")
    );
    assert_eq!(first.assignment_version(), 7);
    assert_eq!(second.assignment_version(), 7);
    assert!(first.is_active());
    assert_eq!(second.state(&second_key).unwrap().sum(), Some(1));
}

#[test]
fn mixed_assignment_targets_prevent_any_transition_publication() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(2).unwrap();
    let mut first = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let mut second = empty_store(&routing, vnode_count, 1, 7, limits(4096));
    let first_change = first.prepare_retained_fence(assignment(8)).unwrap();
    let second_change = second.prepare_retained_fence(assignment(9)).unwrap();

    let mut live = [&mut first, &mut second];
    assert_eq!(
        publish_prepared_changes(&mut live, vec![first_change, second_change]).unwrap_err(),
        ArtifactError::Invalid("managed transition assignment set")
    );
    assert_eq!(first.assignment_version(), 7);
    assert_eq!(second.assignment_version(), 7);
    assert!(first.is_active());
    assert!(second.is_active());
}

#[test]
fn prepared_replacement_cannot_cross_vnode_or_contract_destination() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(2).unwrap();
    let contract = AggregateContractV1::new(&routing, true);
    let source = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let source_context = context(&routing, contract, ArtifactKind::Empty, 7, vnode_count, 0);
    let bytes = source.freeze_full(source_context, &mut budget()).unwrap();
    let prepared = source
        .prepare_replacement(&bytes, source_context, assignment(8), &mut budget())
        .unwrap();
    let mut other_vnode = empty_store(&routing, vnode_count, 1, 7, limits(4096));
    assert_eq!(
        publish_one(&mut other_vnode, prepared).unwrap_err(),
        ArtifactError::Invalid("managed replacement destination")
    );
    assert_eq!(other_vnode.assignment_version(), 7);

    let source = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let prepared = source
        .prepare_replacement(&bytes, source_context, assignment(8), &mut budget())
        .unwrap();
    let mut other_contract = ManagedCountSumVnodeV1::empty(
        identity(&routing, vnode_count, 0, false),
        assignment(7),
        limits(4096),
    )
    .unwrap();
    assert_eq!(
        publish_one(&mut other_contract, prepared).unwrap_err(),
        ArtifactError::Invalid("managed replacement destination")
    );
    assert_eq!(other_contract.assignment_version(), 7);
}

#[test]
fn wrong_restore_context_or_decoded_payload_limit_preserves_live_state() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(2).unwrap();
    let vnode = 0;
    let first = encoded_key_for_vnode(vnode_count, vnode, 0);
    let second = encoded_key_for_vnode(vnode_count, vnode, 100);
    assert_ne!(first, second);
    let contract = AggregateContractV1::new(&routing, true);
    let mut sorted = [
        (
            first.as_slice(),
            CountSumStateV1::persisted(1, 1, 1).unwrap(),
        ),
        (
            second.as_slice(),
            CountSumStateV1::persisted(1, 1, 2).unwrap(),
        ),
    ];
    sorted.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let rows = sorted.map(|(key, state)| AggregateRow { key, state });
    let full = encoded_full(&routing, vnode_count, vnode, 7, &rows);
    let full_context = context(
        &routing,
        contract,
        ArtifactKind::Full,
        7,
        vnode_count,
        vnode,
    );

    let one_row_limit = u64::try_from(first.len() + STATE_WIDTH).unwrap();
    let mut store = empty_store(&routing, vnode_count, vnode, 7, limits(one_row_limit));
    store
        .apply_append_batch(
            assignment(7),
            &[GroupAppend {
                key: &first,
                sum_inputs: &[Some(9)],
            }],
        )
        .unwrap();
    let before = store.state(&first).unwrap();

    let mut wrong_contract = full_context;
    wrong_contract.contract = AggregateContractV1::new(&routing, false);
    assert_eq!(
        store
            .prepare_replacement(&full, wrong_contract, assignment(8), &mut budget())
            .unwrap_err(),
        ArtifactError::Invalid("managed restore context")
    );
    let mut wrong_vnode = full_context;
    wrong_vnode.vnode = 1;
    assert_eq!(
        store
            .prepare_replacement(&full, wrong_vnode, assignment(8), &mut budget())
            .unwrap_err(),
        ArtifactError::Invalid("managed restore context")
    );
    let mut wrong_identity = full_context;
    wrong_identity.operator_identity_sha256 = [0x44; 32];
    assert_eq!(
        store
            .prepare_replacement(&full, wrong_identity, assignment(8), &mut budget())
            .unwrap_err(),
        ArtifactError::Invalid("managed restore context")
    );
    assert_eq!(
        store
            .prepare_replacement(&full, full_context, assignment(8), &mut budget())
            .unwrap_err(),
        ArtifactError::Limit("managed logical payload byte limit")
    );

    assert_eq!(store.len(), 1);
    assert_eq!(store.state(&first), Some(before));
    assert!(store.state(&second).is_none());
    assert_eq!(store.logical_payload_bytes(), one_row_limit);
    assert_eq!(store.assignment_version(), 7);
}

#[test]
fn delta_or_parented_artifacts_are_not_accepted_as_authoritative_replacements() {
    let routing = schema();
    let vnode_count = NonZeroU32::new(1).unwrap();
    let store = empty_store(&routing, vnode_count, 0, 7, limits(4096));
    let contract = AggregateContractV1::new(&routing, true);
    let mut delta = context(&routing, contract, ArtifactKind::Delta, 7, vnode_count, 0);
    delta.parent = Some(ParentLink::new(
        laminar_core::state::CheckpointAttempt::canonical(0),
        [0x44; 32],
    ));
    assert_eq!(
        store
            .prepare_replacement(&[], delta, assignment(8), &mut budget())
            .unwrap_err(),
        ArtifactError::Invalid("managed restore context")
    );
}
