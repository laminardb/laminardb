//! Disconnected reference model for aggregate-v1 checkpoint transitions.
//!
//! Keys entering this model are already validated canonical, namespace-owned keys. The owned
//! `BTreeMap`s and clones are intentionally test-only and prove no hot-path, latency, or
//! bounded-memory property.

use std::collections::BTreeMap;

use laminar_core::checkpoint_decision::CheckpointVerdict;

use super::*;

type Rows = BTreeMap<Vec<u8>, CountSumStateV1>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Append {
    key: Vec<u8>,
    values: Vec<Option<i64>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EntryLink {
    attempt: u64,
    entry_sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CapturePlan {
    Body {
        kind: ArtifactKind,
        rows: Vec<(Vec<u8>, CountSumStateV1)>,
        parent: Option<EntryLink>,
    },
    Reference {
        parent: EntryLink,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CaptureView {
    attempt: u64,
    generation_ids: Vec<u64>,
    logical_rows: Vec<(Vec<u8>, CountSumStateV1)>,
    plan: CapturePlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Generation {
    id: u64,
    puts: Rows,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdmittedEntry {
    link: EntryLink,
    body_kind: Option<ArtifactKind>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LiveCapture {
    view: CaptureView,
    sealed_entry_sha256: Option<[u8; 32]>,
    decision_in_doubt: bool,
}

#[derive(Debug, Eq, PartialEq)]
enum OracleError {
    Artifact(ArtifactError),
    Invalid(&'static str),
}

impl From<ArtifactError> for OracleError {
    fn from(value: ArtifactError) -> Self {
        Self::Artifact(value)
    }
}

#[derive(Debug)]
struct Oracle {
    live: Rows,
    active: Generation,
    retained: BTreeMap<u64, Generation>,
    capture: Option<LiveCapture>,
    admitted: BTreeMap<u64, AdmittedEntry>,
    terminal_commit: BTreeMap<u64, bool>,
    highest_started_attempt: u64,
}

impl Oracle {
    fn new() -> Self {
        Self {
            live: Rows::new(),
            active: Generation {
                id: 1,
                puts: Rows::new(),
            },
            retained: BTreeMap::new(),
            capture: None,
            admitted: BTreeMap::new(),
            terminal_commit: BTreeMap::new(),
            highest_started_attempt: 0,
        }
    }

    fn apply_batch(&mut self, appends: &[Append]) -> Result<(), OracleError> {
        if self
            .capture
            .as_ref()
            .is_some_and(|capture| capture.decision_in_doubt)
        {
            return Err(OracleError::Invalid(
                "checkpoint decision requires recovery",
            ));
        }

        let mut scratch = Rows::new();
        for append in appends {
            if append.values.is_empty() {
                return Err(OracleError::Invalid("append contains no rows"));
            }
            let current = scratch
                .get(&append.key)
                .copied()
                .or_else(|| self.live.get(&append.key).copied())
                .unwrap_or_else(CountSumStateV1::empty);
            scratch.insert(append.key.clone(), current.preview_append(&append.values)?);
        }
        for (key, state) in scratch {
            self.live.insert(key.clone(), state);
            self.active.puts.insert(key, state);
        }
        Ok(())
    }

    fn begin_capture(&mut self, attempt: u64) -> Result<CaptureView, OracleError> {
        if self
            .capture
            .as_ref()
            .is_some_and(|capture| capture.decision_in_doubt)
        {
            return Err(OracleError::Invalid(
                "checkpoint decision requires recovery",
            ));
        }
        if self.capture.is_some() {
            return Err(OracleError::Invalid("another capture is live"));
        }
        // Allocation/fencing is caller-owned. A numeric gap denotes IDs already allocated and
        // burned outside this namespace; those IDs must never be supplied later.
        if attempt == 0 || attempt <= self.highest_started_attempt {
            return Err(OracleError::Invalid("attempt is zero or reused"));
        }

        let next_id = self
            .active
            .id
            .checked_add(1)
            .ok_or(OracleError::Invalid("generation ID overflow"))?;
        let frozen = std::mem::replace(
            &mut self.active,
            Generation {
                id: next_id,
                puts: Rows::new(),
            },
        );
        if !frozen.puts.is_empty() {
            self.retained.insert(frozen.id, frozen);
        }

        let mut dirty = Rows::new();
        for generation in self.retained.values() {
            for (key, state) in &generation.puts {
                dirty.insert(key.clone(), *state);
            }
        }
        let generation_ids = self.retained.keys().copied().collect::<Vec<_>>();
        let logical_rows = ordered_rows(&self.live);
        let plan = self.select_plan(attempt, &dirty, &logical_rows);
        let view = CaptureView {
            attempt,
            generation_ids,
            logical_rows,
            plan,
        };
        self.highest_started_attempt = attempt;
        self.capture = Some(LiveCapture {
            view: view.clone(),
            sealed_entry_sha256: None,
            decision_in_doubt: false,
        });
        Ok(view)
    }

    fn retry_materialization(&self, attempt: u64) -> Result<CaptureView, OracleError> {
        let capture = self
            .capture
            .as_ref()
            .filter(|capture| capture.view.attempt == attempt)
            .ok_or(OracleError::Invalid(
                "retry does not match the live capture",
            ))?;
        if capture.decision_in_doubt {
            return Err(OracleError::Invalid(
                "checkpoint decision requires recovery",
            ));
        }
        Ok(capture.view.clone())
    }

    /// Observes a seal already validated against the exact immutable inventory by the caller.
    fn observe_validated_seal(
        &mut self,
        attempt: u64,
        entry_sha256: [u8; 32],
    ) -> Result<(), OracleError> {
        if entry_sha256 == [0; 32] {
            return Err(OracleError::Invalid("validated seal digest is zero"));
        }
        let capture = self
            .capture
            .as_mut()
            .filter(|capture| capture.view.attempt == attempt)
            .ok_or(OracleError::Invalid("seal does not match the live capture"))?;
        match capture.sealed_entry_sha256 {
            None => capture.sealed_entry_sha256 = Some(entry_sha256),
            Some(existing) if existing == entry_sha256 => {}
            Some(_) => return Err(OracleError::Invalid("conflicting sealed entry digest")),
        }
        Ok(())
    }

    fn mark_decision_in_doubt(&mut self, attempt: u64) -> Result<(), OracleError> {
        let capture = self
            .capture
            .as_mut()
            .filter(|capture| capture.view.attempt == attempt)
            .ok_or(OracleError::Invalid(
                "ambiguous decision does not match the live capture",
            ))?;
        capture.decision_in_doubt = true;
        Ok(())
    }

    fn decide(&mut self, attempt: u64, verdict: CheckpointVerdict) -> Result<(), OracleError> {
        let commit = verdict == CheckpointVerdict::Commit;
        if let Some(existing) = self.terminal_commit.get(&attempt) {
            return if *existing == commit {
                Ok(())
            } else {
                Err(OracleError::Invalid("conflicting terminal verdict"))
            };
        }

        let capture = self
            .capture
            .as_ref()
            .filter(|capture| capture.view.attempt == attempt)
            .ok_or(OracleError::Invalid(
                "verdict does not match the live capture",
            ))?;
        if commit && capture.sealed_entry_sha256.is_none() {
            return Err(OracleError::Invalid("Commit precedes the exact seal"));
        }

        let capture = self.capture.take().expect("live capture was just checked");
        if commit {
            let entry_sha256 = capture
                .sealed_entry_sha256
                .expect("Commit seal was just checked");
            let body_kind = match capture.view.plan {
                CapturePlan::Body { kind, .. } => Some(kind),
                CapturePlan::Reference { .. } => None,
            };
            self.admitted.insert(
                attempt,
                AdmittedEntry {
                    link: EntryLink {
                        attempt,
                        entry_sha256,
                    },
                    body_kind,
                },
            );
            for generation_id in capture.view.generation_ids {
                self.retained.remove(&generation_id);
            }
        }
        self.terminal_commit.insert(attempt, commit);
        Ok(())
    }

    fn select_plan(
        &self,
        attempt: u64,
        dirty: &Rows,
        logical_rows: &[(Vec<u8>, CountSumStateV1)],
    ) -> CapturePlan {
        if dirty.is_empty() {
            if logical_rows.is_empty() {
                return CapturePlan::Body {
                    kind: ArtifactKind::Empty,
                    rows: Vec::new(),
                    parent: None,
                };
            }
            let parent = self
                .admitted
                .values()
                .rev()
                .find(|entry| {
                    matches!(
                        entry.body_kind,
                        Some(ArtifactKind::Full | ArtifactKind::Delta)
                    )
                })
                .expect("clean nonempty state must retain admitted BODY provenance");
            return CapturePlan::Reference {
                parent: parent.link,
            };
        }

        assert!(
            !logical_rows.is_empty(),
            "PUT-only dirty state cannot be logically empty"
        );
        if let Some(parent) = attempt
            .checked_sub(1)
            .and_then(|previous| self.admitted.get(&previous))
        {
            return CapturePlan::Body {
                kind: ArtifactKind::Delta,
                rows: ordered_rows(dirty),
                parent: Some(parent.link),
            };
        }
        CapturePlan::Body {
            kind: ArtifactKind::Full,
            rows: logical_rows.to_vec(),
            parent: None,
        }
    }
}

fn append(key: &[u8], values: &[Option<i64>]) -> Append {
    Append {
        key: key.to_vec(),
        values: values.to_vec(),
    }
}

fn state(count: u64, non_null: u64, sum: i64) -> CountSumStateV1 {
    CountSumStateV1::persisted(count, non_null, sum).unwrap()
}

fn ordered_rows(source: &Rows) -> Vec<(Vec<u8>, CountSumStateV1)> {
    source
        .iter()
        .map(|(key, state)| (key.clone(), *state))
        .collect()
}

fn admit(oracle: &mut Oracle, attempt: u64, digest_byte: u8) {
    oracle
        .observe_validated_seal(attempt, [digest_byte; 32])
        .unwrap();
    oracle.decide(attempt, CheckpointVerdict::Commit).unwrap();
}

#[test]
fn atomic_batch_and_cross_batch_put_coalescing() {
    let mut zero_row = Oracle::new();
    assert_eq!(
        zero_row.apply_batch(&[append(b"would-publish", &[Some(2)]), append(b"empty", &[]),]),
        Err(OracleError::Invalid("append contains no rows"))
    );
    assert!(zero_row.live.is_empty());
    assert_eq!(
        zero_row.active,
        Generation {
            id: 1,
            puts: Rows::new(),
        }
    );

    let mut rollback = Oracle::new();
    rollback
        .apply_batch(&[append(b"overflow", &[Some(i64::MAX)])])
        .unwrap();
    assert_eq!(
        rollback.apply_batch(&[
            append(b"would-publish", &[Some(2)]),
            append(b"overflow", &[Some(1)]),
        ]),
        Err(OracleError::Artifact(ArtifactError::SumOverflow))
    );
    assert_eq!(
        rollback.live,
        BTreeMap::from([(b"overflow".to_vec(), state(1, 1, i64::MAX))])
    );
    assert_eq!(
        rollback.active,
        Generation {
            id: 1,
            puts: BTreeMap::from([(b"overflow".to_vec(), state(1, 1, i64::MAX))]),
        }
    );

    let mut oracle = Oracle::new();
    oracle
        .apply_batch(&[
            append(b"z", &[Some(9)]),
            append(b"a", &[Some(1), None]),
            append(b"a", &[Some(4)]),
        ])
        .unwrap();
    oracle.apply_batch(&[append(b"z", &[Some(1)])]).unwrap();
    let expected = BTreeMap::from([
        (b"a".to_vec(), state(3, 2, 5)),
        (b"z".to_vec(), state(2, 2, 10)),
    ]);
    assert_eq!(oracle.live, expected);
    assert_eq!(oracle.active.puts, expected);
    assert_eq!(
        oracle.begin_capture(1).unwrap(),
        CaptureView {
            attempt: 1,
            generation_ids: vec![1],
            logical_rows: vec![
                (b"a".to_vec(), state(3, 2, 5)),
                (b"z".to_vec(), state(2, 2, 10)),
            ],
            plan: CapturePlan::Body {
                kind: ArtifactKind::Full,
                rows: vec![
                    (b"a".to_vec(), state(3, 2, 5)),
                    (b"z".to_vec(), state(2, 2, 10)),
                ],
                parent: None,
            },
        }
    );
}

#[test]
fn ancestry_retry_and_release_are_literal() {
    let mut empty = Oracle::new();
    assert_eq!(
        empty.begin_capture(1).unwrap(),
        CaptureView {
            attempt: 1,
            generation_ids: vec![],
            logical_rows: vec![],
            plan: CapturePlan::Body {
                kind: ArtifactKind::Empty,
                rows: vec![],
                parent: None,
            },
        }
    );
    admit(&mut empty, 1, 0x01);
    // Attempt 2 was allocated and burned before this namespace started a cut.
    assert_eq!(
        empty.begin_capture(3).unwrap().plan,
        CapturePlan::Body {
            kind: ArtifactKind::Empty,
            rows: vec![],
            parent: None,
        }
    );

    let mut clean_allocator_gap = Oracle::new();
    clean_allocator_gap
        .apply_batch(&[append(b"k", &[Some(1)])])
        .unwrap();
    clean_allocator_gap.begin_capture(1).unwrap();
    admit(&mut clean_allocator_gap, 1, 0x09);
    assert_eq!(
        clean_allocator_gap.begin_capture(3).unwrap().plan,
        CapturePlan::Reference {
            parent: EntryLink {
                attempt: 1,
                entry_sha256: [0x09; 32],
            },
        }
    );

    let mut dirty_allocator_gap = Oracle::new();
    dirty_allocator_gap
        .apply_batch(&[append(b"k", &[Some(1)])])
        .unwrap();
    dirty_allocator_gap.begin_capture(1).unwrap();
    admit(&mut dirty_allocator_gap, 1, 0x0a);
    dirty_allocator_gap
        .apply_batch(&[append(b"k", &[Some(2)])])
        .unwrap();
    assert_eq!(
        dirty_allocator_gap.begin_capture(3).unwrap().plan,
        CapturePlan::Body {
            kind: ArtifactKind::Full,
            rows: vec![(b"k".to_vec(), state(2, 2, 3))],
            parent: None,
        }
    );

    let mut chain = Oracle::new();
    chain.apply_batch(&[append(b"k", &[Some(1)])]).unwrap();
    assert_eq!(
        chain.begin_capture(1).unwrap(),
        CaptureView {
            attempt: 1,
            generation_ids: vec![1],
            logical_rows: vec![(b"k".to_vec(), state(1, 1, 1))],
            plan: CapturePlan::Body {
                kind: ArtifactKind::Full,
                rows: vec![(b"k".to_vec(), state(1, 1, 1))],
                parent: None,
            },
        }
    );
    assert_eq!(
        chain.begin_capture(2),
        Err(OracleError::Invalid("another capture is live"))
    );
    admit(&mut chain, 1, 0x11);
    assert_eq!(
        chain.begin_capture(2).unwrap(),
        CaptureView {
            attempt: 2,
            generation_ids: vec![],
            logical_rows: vec![(b"k".to_vec(), state(1, 1, 1))],
            plan: CapturePlan::Reference {
                parent: EntryLink {
                    attempt: 1,
                    entry_sha256: [0x11; 32],
                },
            },
        }
    );
    admit(&mut chain, 2, 0x22);
    chain.apply_batch(&[append(b"k", &[Some(5)])]).unwrap();
    assert_eq!(
        chain.begin_capture(3).unwrap().plan,
        CapturePlan::Body {
            kind: ArtifactKind::Delta,
            rows: vec![(b"k".to_vec(), state(2, 2, 6))],
            parent: Some(EntryLink {
                attempt: 2,
                entry_sha256: [0x22; 32],
            }),
        }
    );
    admit(&mut chain, 3, 0x33);
    chain.apply_batch(&[append(b"k", &[Some(6)])]).unwrap();
    assert_eq!(
        chain.begin_capture(4).unwrap().plan,
        CapturePlan::Body {
            kind: ArtifactKind::Delta,
            rows: vec![(b"k".to_vec(), state(3, 3, 12))],
            parent: Some(EntryLink {
                attempt: 3,
                entry_sha256: [0x33; 32],
            }),
        }
    );

    let mut sealed_abort_gap = Oracle::new();
    sealed_abort_gap
        .apply_batch(&[append(b"k", &[Some(1)])])
        .unwrap();
    sealed_abort_gap.begin_capture(1).unwrap();
    sealed_abort_gap
        .observe_validated_seal(1, [0x41; 32])
        .unwrap();
    sealed_abort_gap
        .decide(1, CheckpointVerdict::Abort)
        .unwrap();
    sealed_abort_gap
        .apply_batch(&[append(b"k", &[Some(2)])])
        .unwrap();
    assert_eq!(
        sealed_abort_gap.begin_capture(2).unwrap().plan,
        CapturePlan::Body {
            kind: ArtifactKind::Full,
            rows: vec![(b"k".to_vec(), state(2, 2, 3))],
            parent: None,
        }
    );

    let mut unchanged_abort = Oracle::new();
    unchanged_abort
        .apply_batch(&[append(b"k", &[Some(1)])])
        .unwrap();
    unchanged_abort.begin_capture(1).unwrap();
    admit(&mut unchanged_abort, 1, 0x51);
    unchanged_abort.begin_capture(2).unwrap();
    unchanged_abort
        .observe_validated_seal(2, [0x52; 32])
        .unwrap();
    unchanged_abort.decide(2, CheckpointVerdict::Abort).unwrap();
    assert_eq!(
        unchanged_abort.begin_capture(3).unwrap().plan,
        CapturePlan::Reference {
            parent: EntryLink {
                attempt: 1,
                entry_sha256: [0x51; 32],
            },
        }
    );

    let mut release = Oracle::new();
    release.apply_batch(&[append(b"a", &[Some(1)])]).unwrap();
    assert_eq!(release.begin_capture(1).unwrap().generation_ids, [1]);
    assert_eq!(
        release.decide(1, CheckpointVerdict::Commit),
        Err(OracleError::Invalid("Commit precedes the exact seal"))
    );
    assert_eq!(release.retained.keys().copied().collect::<Vec<_>>(), [1]);
    release.mark_decision_in_doubt(1).unwrap();
    assert_eq!(
        release.apply_batch(&[append(b"blocked", &[Some(9)])]),
        Err(OracleError::Invalid(
            "checkpoint decision requires recovery"
        ))
    );
    assert_eq!(
        release.retry_materialization(1),
        Err(OracleError::Invalid(
            "checkpoint decision requires recovery"
        ))
    );
    assert_eq!(
        release.begin_capture(2),
        Err(OracleError::Invalid(
            "checkpoint decision requires recovery"
        ))
    );
    release.decide(1, CheckpointVerdict::Abort).unwrap();
    assert_eq!(release.retained.keys().copied().collect::<Vec<_>>(), [1]);
    assert!(release.admitted.is_empty());

    release.apply_batch(&[append(b"b", &[Some(2)])]).unwrap();
    let second = release.begin_capture(2).unwrap();
    assert_eq!(
        second,
        CaptureView {
            attempt: 2,
            generation_ids: vec![1, 2],
            logical_rows: vec![
                (b"a".to_vec(), state(1, 1, 1)),
                (b"b".to_vec(), state(1, 1, 2)),
            ],
            plan: CapturePlan::Body {
                kind: ArtifactKind::Full,
                rows: vec![
                    (b"a".to_vec(), state(1, 1, 1)),
                    (b"b".to_vec(), state(1, 1, 2)),
                ],
                parent: None,
            },
        }
    );
    release.apply_batch(&[append(b"c", &[Some(3)])]).unwrap();
    assert_eq!(release.retry_materialization(2).unwrap(), second);
    release.observe_validated_seal(2, [0x72; 32]).unwrap();
    release.mark_decision_in_doubt(2).unwrap();
    release.decide(2, CheckpointVerdict::Commit).unwrap();
    assert!(release.retained.is_empty());
    assert_eq!(
        release.active,
        Generation {
            id: 3,
            puts: BTreeMap::from([(b"c".to_vec(), state(1, 1, 3))]),
        }
    );
    assert_eq!(release.decide(2, CheckpointVerdict::Commit), Ok(()));
    assert_eq!(
        release.decide(2, CheckpointVerdict::Abort),
        Err(OracleError::Invalid("conflicting terminal verdict"))
    );
}

#[test]
fn semantic_delta_plan_bridges_existing_inner_and_outer_codecs() {
    let routing = schema(DataType::Binary, false);
    let key = encoded_keys(
        &routing,
        &[false],
        &[Arc::new(BinaryArray::from(vec![Some(b"k" as &[u8])]))],
    )
    .remove(0);
    let mut oracle = Oracle::new();
    oracle.apply_batch(&[append(&key, &[Some(1)])]).unwrap();
    oracle.begin_capture(1).unwrap();
    oracle.observe_validated_seal(1, [0x44; 32]).unwrap();
    oracle.decide(1, CheckpointVerdict::Commit).unwrap();
    oracle.apply_batch(&[append(&key, &[Some(5)])]).unwrap();
    let capture = oracle.begin_capture(2).unwrap();
    let expected_parent = EntryLink {
        attempt: 1,
        entry_sha256: [0x44; 32],
    };
    assert_eq!(
        capture.plan,
        CapturePlan::Body {
            kind: ArtifactKind::Delta,
            rows: vec![(key.clone(), state(2, 2, 6))],
            parent: Some(expected_parent),
        }
    );

    let CapturePlan::Body {
        kind,
        rows,
        parent: Some(parent),
    } = &capture.plan
    else {
        panic!("literal DELTA plan changed shape");
    };
    let aggregate_rows = rows
        .iter()
        .map(|(key, state)| AggregateRow { key, state: *state })
        .collect::<Vec<_>>();
    let mut inner_context = context(&routing, *kind, capture.attempt, Some(parent.attempt));
    let exact_inner_parent = ParentLink::new(
        CheckpointAttempt::canonical(parent.attempt),
        parent.entry_sha256,
    );
    inner_context.parent = Some(exact_inner_parent);
    assert_eq!(inner_context.parent, Some(exact_inner_parent));
    let inner = encode(inner_context, &aggregate_rows, &mut budget()).unwrap();

    let roster = [partial_v2::ExpectedRosterEntry {
        operator_identity_sha256: [0x22; 32],
        state_table_identity_sha256: [0x33; 32],
        vnode: 0,
        managed_envelope_version: 1,
    }];
    let outer_context = partial_v2::ExpectedContext {
        attempt: CheckpointAttempt::canonical(2),
        assignment_version: 7,
        partitioning_abi_version: PARTITIONING_ABI_VERSION,
        vnode_count: 1,
        vnode: 0,
        assignment_certificate_sha256: [0x11; 32],
        roster: &roster,
    };
    let outer_parent = partial_v2::ParentEntryLink {
        attempt: CheckpointAttempt::canonical(parent.attempt),
        entry_sha256: parent.entry_sha256,
    };
    let outer = partial_v2::encode(
        outer_context,
        &[partial_v2::EncodeEntry {
            operator_identity_sha256: [0x22; 32],
            state_table_identity_sha256: [0x33; 32],
            payload: partial_v2::EncodeEntryPayload::Body {
                artifact_kind: partial_v2::ArtifactKind::Delta,
                body: &inner,
                parent: Some(outer_parent),
            },
        }],
        partial_limits(),
    )
    .unwrap();
    let decoded_outer = partial_v2::decode(&outer, outer_context, partial_limits()).unwrap();
    let decoded_entry = decoded_outer.entries().next().unwrap().unwrap();
    let partial_v2::DecodedEntryPayload::Body {
        artifact_kind: partial_v2::ArtifactKind::Delta,
        body,
        parent: Some(decoded_parent),
        ..
    } = decoded_entry.payload
    else {
        panic!("semantic DELTA did not round-trip through VnodePartialV2");
    };
    assert_eq!(decoded_parent, outer_parent);
    assert_eq!(
        decode(body, inner_context, &mut budget())
            .unwrap()
            .rows()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        aggregate_rows
    );
}
