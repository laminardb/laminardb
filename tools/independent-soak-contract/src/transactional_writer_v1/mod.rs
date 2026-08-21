//! Synchronous transactional-writer protocol model for validation fixtures.
//!
//! This is not a broker, connector, interval allocator, or performance model. It makes protocol
//! transitions and simulated committed visibility testable around the frozen v1 bytes.

use std::fmt;
use std::ops::Range;

use super::provenance_v1::{PreparedOutputAuthorityV1, ProvenanceError};
use super::wire_v1::{self, WireError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TransactionLimitsV1 {
    max_records: usize,
    max_modeled_bytes: usize,
}

impl TransactionLimitsV1 {
    pub(super) fn new(
        max_records: usize,
        max_modeled_bytes: usize,
    ) -> Result<Self, TransactionModelError> {
        if max_records == 0 || max_records > wire_v1::MAX_DATA_HEADERS_PER_BATCH {
            return Err(TransactionModelError::InvalidLimit("max_records"));
        }
        if max_modeled_bytes == 0 {
            return Err(TransactionModelError::InvalidLimit("max_modeled_bytes"));
        }
        Ok(Self {
            max_records,
            max_modeled_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DataRecordRefV1<'a> {
    pub partition: i32,
    pub operation_id: &'a [u8; 32],
    pub payload: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SimulatedOutcomeV1 {
    Confirmed,
    DefinitelyRejected,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FaultPointV1 {
    Begin,
    Send,
    Commit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SimulatedAttemptV1 {
    CommitConfirmed,
    ConfirmedAbortAt(FaultPointV1),
    OutcomeUnknownAt(FaultPointV1),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionKindV1 {
    Marker,
    Data,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionPhaseV1 {
    Begun,
    Staged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StableStateV1 {
    MarkerPending,
    DataOpen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PoisonPointV1 {
    Initialize,
    Begin,
    Send,
    Commit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriterStateV1 {
    Uninitialized,
    MarkerPending,
    DataOpen,
    TransactionInFlight {
        kind: TransactionKindV1,
        phase: TransactionPhaseV1,
        return_state: StableStateV1,
    },
    TerminalPoison {
        point: PoisonPointV1,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StableProducerScopeRefV1<'a> {
    deployment_uuid: &'a [u8; 16],
    pipeline_incarnation_id: &'a [u8; 16],
    sink_id: &'a str,
    shard_id: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ConfirmedIntervalV1<'a> {
    scope: StableProducerScopeRefV1<'a>,
    interval_id: &'a [u8; 16],
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct SimulatedDataRecordV1 {
    pub partition: i32,
    pub header: [u8; wire_v1::DATA_ENCODED_LEN],
    payload_start: usize,
    payload_len: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct SimulatedDataTransactionV1 {
    pub records: Vec<SimulatedDataRecordV1>,
    payload_bytes: Vec<u8>,
}

impl SimulatedDataTransactionV1 {
    pub(super) fn payload(&self, record_index: usize) -> Option<&[u8]> {
        let record = self.records.get(record_index)?;
        let end = record.payload_start.checked_add(record.payload_len)?;
        self.payload_bytes.get(record.payload_start..end)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CommittedBatchV1 {
    pub attempts: usize,
    pub first_sequence: u64,
    pub last_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CommittedMarkerRefV1<'a> {
    pub partitions: &'a [i32],
    pub envelope: &'a [u8],
}

#[derive(Debug)]
pub(super) struct FakeTransactionalWriterV1<'a> {
    authority: PreparedOutputAuthorityV1<'a>,
    affected_partitions: &'a [i32],
    limits: TransactionLimitsV1,
    marker_envelope: Vec<u8>,
    state: WriterStateV1,
    marker_confirmed: bool,
    next_sequence: Option<u64>,
    confirmed_data: Vec<SimulatedDataTransactionV1>,
}

impl<'a> FakeTransactionalWriterV1<'a> {
    pub(super) fn first(
        authority: PreparedOutputAuthorityV1<'a>,
        affected_partitions: &'a [i32],
        limits: TransactionLimitsV1,
    ) -> Result<Self, TransactionModelError> {
        if authority.marker_ref().predecessor_interval_id.is_some() {
            return Err(TransactionModelError::FirstIntervalHasPredecessor);
        }
        Self::new(authority, affected_partitions, limits)
    }

    pub(super) fn successor(
        authority: PreparedOutputAuthorityV1<'a>,
        predecessor: ConfirmedIntervalV1<'_>,
        affected_partitions: &'a [i32],
        limits: TransactionLimitsV1,
    ) -> Result<Self, TransactionModelError> {
        let marker = authority.marker_ref();
        let scope = scope_from_marker(marker);
        if scope != predecessor.scope {
            return Err(TransactionModelError::StableProducerScopeMismatch);
        }
        if marker.predecessor_interval_id != Some(predecessor.interval_id) {
            return Err(TransactionModelError::SuccessorPredecessorMismatch);
        }
        Self::new(authority, affected_partitions, limits)
    }

    fn new(
        authority: PreparedOutputAuthorityV1<'a>,
        affected_partitions: &'a [i32],
        limits: TransactionLimitsV1,
    ) -> Result<Self, TransactionModelError> {
        validate_partitions(affected_partitions)?;
        let marker = authority.marker_ref();
        let marker_len = wire_v1::encoded_marker_len(&marker)?;
        let modeled_bytes = marker_len.checked_mul(affected_partitions.len()).ok_or(
            TransactionModelError::ArithmeticOverflow("marker modeled bytes"),
        )?;
        validate_transaction_limits(affected_partitions.len(), modeled_bytes, limits)?;

        let mut marker_envelope = Vec::with_capacity(marker_len);
        wire_v1::encode_marker_into(&marker, &mut marker_envelope)?;
        Ok(Self {
            authority,
            affected_partitions,
            limits,
            marker_envelope,
            state: WriterStateV1::Uninitialized,
            marker_confirmed: false,
            next_sequence: Some(0),
            confirmed_data: Vec::new(),
        })
    }

    pub(super) fn initialize(
        &mut self,
        outcome: SimulatedOutcomeV1,
    ) -> Result<bool, TransactionModelError> {
        match self.state {
            WriterStateV1::TerminalPoison { point } => Err(TransactionModelError::Poisoned(point)),
            WriterStateV1::Uninitialized => match outcome {
                SimulatedOutcomeV1::Confirmed => {
                    self.state = WriterStateV1::MarkerPending;
                    Ok(true)
                }
                SimulatedOutcomeV1::DefinitelyRejected => Ok(false),
                SimulatedOutcomeV1::Ambiguous => {
                    self.poison(PoisonPointV1::Initialize);
                    Err(TransactionModelError::OutcomeUnknown(
                        PoisonPointV1::Initialize,
                    ))
                }
            },
            actual => Err(TransactionModelError::InvalidState {
                action: "initialize",
                actual,
            }),
        }
    }

    pub(super) fn commit_marker(
        &mut self,
        attempts: &[SimulatedAttemptV1],
    ) -> Result<usize, TransactionModelError> {
        self.require_state(WriterStateV1::MarkerPending, "commit marker")?;
        validate_attempt_script(attempts)?;
        let attempts = self.run_transaction(TransactionKindV1::Marker, attempts)?;
        self.marker_confirmed = true;
        Ok(attempts)
    }

    pub(super) fn commit_data_batch(
        &mut self,
        records: &[DataRecordRefV1<'_>],
        attempts: &[SimulatedAttemptV1],
    ) -> Result<CommittedBatchV1, TransactionModelError> {
        self.require_state(WriterStateV1::DataOpen, "commit data")?;
        validate_attempt_script(attempts)?;
        let modeled_bytes =
            checked_modeled_data_bytes(records.iter().map(|record| record.payload.len()))?;
        validate_transaction_limits(records.len(), modeled_bytes, self.limits)?;
        for record in records {
            if self
                .affected_partitions
                .binary_search(&record.partition)
                .is_err()
            {
                return Err(TransactionModelError::UnknownPartition(record.partition));
            }
            self.authority.project_data_header(record.operation_id, 0)?;
        }

        let reservation = self.preview_sequence_range(records.len())?;
        let mut encoded_records = Vec::with_capacity(records.len());
        let payload_bytes_len = records.iter().try_fold(0_usize, |total, record| {
            total
                .checked_add(record.payload.len())
                .ok_or(TransactionModelError::ArithmeticOverflow("payload bytes"))
        })?;
        let mut payload_offset = 0_usize;

        for (index, record) in records.iter().enumerate() {
            let index = u64::try_from(index)
                .map_err(|_| TransactionModelError::ArithmeticOverflow("sequence index"))?;
            let sequence = reservation
                .first
                .checked_add(index)
                .ok_or(TransactionModelError::ArithmeticOverflow("sequence value"))?;
            let header = self
                .authority
                .project_data_header(record.operation_id, sequence)?;
            let encoded = wire_v1::encode_data(&header)?;
            encoded_records.push(SimulatedDataRecordV1 {
                partition: record.partition,
                header: encoded,
                payload_start: payload_offset,
                payload_len: record.payload.len(),
            });
            payload_offset = payload_offset
                .checked_add(record.payload.len())
                .ok_or(TransactionModelError::ArithmeticOverflow("payload offset"))?;
        }

        let attempts = self.run_transaction(TransactionKindV1::Data, attempts)?;
        let mut payload_bytes = Vec::with_capacity(payload_bytes_len);
        for record in records {
            payload_bytes.extend_from_slice(record.payload);
        }
        self.confirmed_data.push(SimulatedDataTransactionV1 {
            records: encoded_records,
            payload_bytes,
        });
        self.next_sequence = reservation.next;
        Ok(CommittedBatchV1 {
            attempts,
            first_sequence: reservation.first,
            last_sequence: reservation.last,
        })
    }

    pub(super) fn confirmed_marker(
        &self,
    ) -> Result<CommittedMarkerRefV1<'_>, TransactionModelError> {
        if !self.marker_confirmed {
            return Err(TransactionModelError::MarkerNotConfirmed);
        }
        Ok(CommittedMarkerRefV1 {
            partitions: self.affected_partitions,
            envelope: &self.marker_envelope,
        })
    }

    pub(super) fn confirmed_interval(
        &self,
    ) -> Result<ConfirmedIntervalV1<'_>, TransactionModelError> {
        if !self.marker_confirmed {
            return Err(TransactionModelError::MarkerNotConfirmed);
        }
        let marker = self.authority.marker_ref();
        Ok(ConfirmedIntervalV1 {
            scope: scope_from_marker(marker),
            interval_id: marker.current_interval_id,
        })
    }

    /// Transactions whose simulated commit result was confirmed.
    ///
    /// Absence here after an ambiguous result is not evidence that a broker commit was absent.
    pub(super) fn confirmed_data(&self) -> &[SimulatedDataTransactionV1] {
        &self.confirmed_data
    }

    fn require_state(
        &self,
        expected: WriterStateV1,
        action: &'static str,
    ) -> Result<(), TransactionModelError> {
        match self.state {
            WriterStateV1::TerminalPoison { point } => Err(TransactionModelError::Poisoned(point)),
            actual if actual == expected => Ok(()),
            actual => Err(TransactionModelError::InvalidState { action, actual }),
        }
    }

    fn preview_sequence_range(
        &self,
        count: usize,
    ) -> Result<SequenceReservationV1, TransactionModelError> {
        if count == 0 {
            return Err(TransactionModelError::EmptyTransaction);
        }
        let first = self
            .next_sequence
            .ok_or(TransactionModelError::SequenceExhausted)?;
        let width = u64::try_from(count - 1)
            .map_err(|_| TransactionModelError::ArithmeticOverflow("sequence range"))?;
        let last = first
            .checked_add(width)
            .ok_or(TransactionModelError::SequenceExhausted)?;
        Ok(SequenceReservationV1 {
            first,
            last,
            next: last.checked_add(1),
        })
    }

    fn run_transaction(
        &mut self,
        kind: TransactionKindV1,
        attempts: &[SimulatedAttemptV1],
    ) -> Result<usize, TransactionModelError> {
        for (index, attempt) in attempts.iter().copied().enumerate() {
            match attempt {
                SimulatedAttemptV1::CommitConfirmed => {
                    self.begin_transaction(kind, SimulatedOutcomeV1::Confirmed)?;
                    self.send_transaction(SimulatedOutcomeV1::Confirmed)?;
                    self.commit_transaction(SimulatedOutcomeV1::Confirmed)?;
                    return Ok(index + 1);
                }
                SimulatedAttemptV1::ConfirmedAbortAt(point) => {
                    self.run_faulted_attempt(kind, point, SimulatedOutcomeV1::DefinitelyRejected)?;
                }
                SimulatedAttemptV1::OutcomeUnknownAt(point) => {
                    self.run_faulted_attempt(kind, point, SimulatedOutcomeV1::Ambiguous)?;
                    unreachable!("an ambiguous protocol result always returns an error");
                }
            }
        }
        unreachable!("a validated attempt script has a terminal final attempt")
    }

    fn run_faulted_attempt(
        &mut self,
        kind: TransactionKindV1,
        point: FaultPointV1,
        outcome: SimulatedOutcomeV1,
    ) -> Result<(), TransactionModelError> {
        match point {
            FaultPointV1::Begin => {
                self.begin_transaction(kind, outcome)?;
            }
            FaultPointV1::Send => {
                self.begin_transaction(kind, SimulatedOutcomeV1::Confirmed)?;
                self.send_transaction(outcome)?;
            }
            FaultPointV1::Commit => {
                self.begin_transaction(kind, SimulatedOutcomeV1::Confirmed)?;
                self.send_transaction(SimulatedOutcomeV1::Confirmed)?;
                self.commit_transaction(outcome)?;
            }
        }
        Ok(())
    }

    fn begin_transaction(
        &mut self,
        kind: TransactionKindV1,
        outcome: SimulatedOutcomeV1,
    ) -> Result<bool, TransactionModelError> {
        let return_state = match (self.state, kind) {
            (WriterStateV1::MarkerPending, TransactionKindV1::Marker) => {
                StableStateV1::MarkerPending
            }
            (WriterStateV1::DataOpen, TransactionKindV1::Data) => StableStateV1::DataOpen,
            (WriterStateV1::TerminalPoison { point }, _) => {
                return Err(TransactionModelError::Poisoned(point));
            }
            (actual, _) => {
                return Err(TransactionModelError::InvalidState {
                    action: "begin transaction",
                    actual,
                });
            }
        };
        match outcome {
            SimulatedOutcomeV1::Confirmed => {
                self.state = WriterStateV1::TransactionInFlight {
                    kind,
                    phase: TransactionPhaseV1::Begun,
                    return_state,
                };
                Ok(true)
            }
            SimulatedOutcomeV1::DefinitelyRejected => Ok(false),
            SimulatedOutcomeV1::Ambiguous => {
                self.poison(PoisonPointV1::Begin);
                Err(TransactionModelError::OutcomeUnknown(PoisonPointV1::Begin))
            }
        }
    }

    fn send_transaction(
        &mut self,
        outcome: SimulatedOutcomeV1,
    ) -> Result<bool, TransactionModelError> {
        let (kind, return_state) = match self.state {
            WriterStateV1::TransactionInFlight {
                kind,
                phase: TransactionPhaseV1::Begun,
                return_state,
            } => (kind, return_state),
            WriterStateV1::TerminalPoison { point } => {
                return Err(TransactionModelError::Poisoned(point));
            }
            actual => {
                return Err(TransactionModelError::InvalidState {
                    action: "send transaction",
                    actual,
                });
            }
        };
        match outcome {
            SimulatedOutcomeV1::Confirmed => {
                self.state = WriterStateV1::TransactionInFlight {
                    kind,
                    phase: TransactionPhaseV1::Staged,
                    return_state,
                };
                Ok(true)
            }
            SimulatedOutcomeV1::DefinitelyRejected => {
                self.state = stable_state(return_state);
                Ok(false)
            }
            SimulatedOutcomeV1::Ambiguous => {
                self.poison(PoisonPointV1::Send);
                Err(TransactionModelError::OutcomeUnknown(PoisonPointV1::Send))
            }
        }
    }

    fn commit_transaction(
        &mut self,
        outcome: SimulatedOutcomeV1,
    ) -> Result<bool, TransactionModelError> {
        let (kind, return_state) = match self.state {
            WriterStateV1::TransactionInFlight {
                kind,
                phase: TransactionPhaseV1::Staged,
                return_state,
            } => (kind, return_state),
            WriterStateV1::TerminalPoison { point } => {
                return Err(TransactionModelError::Poisoned(point));
            }
            actual => {
                return Err(TransactionModelError::InvalidState {
                    action: "commit transaction",
                    actual,
                });
            }
        };
        match outcome {
            SimulatedOutcomeV1::Confirmed => {
                self.state = match kind {
                    TransactionKindV1::Marker | TransactionKindV1::Data => WriterStateV1::DataOpen,
                };
                Ok(true)
            }
            SimulatedOutcomeV1::DefinitelyRejected => {
                self.state = stable_state(return_state);
                Ok(false)
            }
            SimulatedOutcomeV1::Ambiguous => {
                self.poison(PoisonPointV1::Commit);
                Err(TransactionModelError::OutcomeUnknown(PoisonPointV1::Commit))
            }
        }
    }

    fn abort_transaction(&mut self) -> Result<(), TransactionModelError> {
        match self.state {
            WriterStateV1::TransactionInFlight { return_state, .. } => {
                self.state = stable_state(return_state);
                Ok(())
            }
            WriterStateV1::TerminalPoison { point } => Err(TransactionModelError::Poisoned(point)),
            actual => Err(TransactionModelError::InvalidState {
                action: "abort transaction",
                actual,
            }),
        }
    }

    fn poison(&mut self, point: PoisonPointV1) {
        self.state = WriterStateV1::TerminalPoison { point };
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SequenceReservationV1 {
    first: u64,
    last: u64,
    next: Option<u64>,
}

fn stable_state(state: StableStateV1) -> WriterStateV1 {
    match state {
        StableStateV1::MarkerPending => WriterStateV1::MarkerPending,
        StableStateV1::DataOpen => WriterStateV1::DataOpen,
    }
}

fn scope_from_marker(marker: wire_v1::MarkerRef<'_>) -> StableProducerScopeRefV1<'_> {
    StableProducerScopeRefV1 {
        deployment_uuid: marker.deployment_uuid,
        pipeline_incarnation_id: marker.pipeline_incarnation_id,
        sink_id: marker.sink_id,
        shard_id: marker.shard_id,
    }
}

fn validate_partitions(partitions: &[i32]) -> Result<(), TransactionModelError> {
    if partitions.is_empty() {
        return Err(TransactionModelError::InvalidPartitions);
    }
    if partitions[0] < 0
        || partitions
            .windows(2)
            .any(|pair| pair[0] < 0 || pair[0] >= pair[1])
    {
        return Err(TransactionModelError::InvalidPartitions);
    }
    Ok(())
}

fn validate_transaction_limits(
    record_count: usize,
    modeled_bytes: usize,
    limits: TransactionLimitsV1,
) -> Result<(), TransactionModelError> {
    if record_count == 0 {
        return Err(TransactionModelError::EmptyTransaction);
    }
    if record_count > limits.max_records {
        return Err(TransactionModelError::LimitExceeded("record count"));
    }
    if modeled_bytes > limits.max_modeled_bytes {
        return Err(TransactionModelError::LimitExceeded("modeled bytes"));
    }
    Ok(())
}

fn checked_modeled_data_bytes(
    payload_lengths: impl IntoIterator<Item = usize>,
) -> Result<usize, TransactionModelError> {
    payload_lengths
        .into_iter()
        .try_fold(0_usize, |total, payload_len| {
            let record_bytes = wire_v1::DATA_ENCODED_LEN.checked_add(payload_len).ok_or(
                TransactionModelError::ArithmeticOverflow("data record modeled bytes"),
            )?;
            total
                .checked_add(record_bytes)
                .ok_or(TransactionModelError::ArithmeticOverflow(
                    "data transaction modeled bytes",
                ))
        })
}

pub(super) fn plan_data_chunks_v1(
    records: &[DataRecordRefV1<'_>],
    limits: TransactionLimitsV1,
) -> Result<Vec<Range<usize>>, TransactionModelError> {
    let mut ranges = Vec::new();
    let mut start = 0_usize;
    let mut bytes = 0_usize;

    for (index, record) in records.iter().enumerate() {
        let record_bytes = checked_modeled_data_bytes([record.payload.len()])?;
        if record_bytes > limits.max_modeled_bytes {
            return Err(TransactionModelError::LimitExceeded(
                "single record modeled bytes",
            ));
        }
        if index - start == limits.max_records {
            ranges.push(start..index);
            start = index;
            bytes = record_bytes;
            continue;
        }
        match bytes.checked_add(record_bytes) {
            Some(combined) if combined <= limits.max_modeled_bytes => bytes = combined,
            Some(_) | None => {
                ranges.push(start..index);
                start = index;
                bytes = record_bytes;
            }
        }
    }
    if start < records.len() {
        ranges.push(start..records.len());
    }
    Ok(ranges)
}

fn validate_attempt_script(attempts: &[SimulatedAttemptV1]) -> Result<(), TransactionModelError> {
    let Some((last, prefix)) = attempts.split_last() else {
        return Err(TransactionModelError::EmptyAttemptScript);
    };
    if prefix
        .iter()
        .any(|attempt| !matches!(attempt, SimulatedAttemptV1::ConfirmedAbortAt(_)))
    {
        return Err(TransactionModelError::NonCanonicalAttemptScript);
    }
    if matches!(last, SimulatedAttemptV1::ConfirmedAbortAt(_)) {
        return Err(TransactionModelError::UnresolvedAttemptScript);
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum TransactionModelError {
    InvalidLimit(&'static str),
    InvalidPartitions,
    FirstIntervalHasPredecessor,
    SuccessorPredecessorMismatch,
    StableProducerScopeMismatch,
    EmptyTransaction,
    LimitExceeded(&'static str),
    ArithmeticOverflow(&'static str),
    SequenceExhausted,
    UnknownPartition(i32),
    EmptyAttemptScript,
    NonCanonicalAttemptScript,
    UnresolvedAttemptScript,
    MarkerNotConfirmed,
    InvalidState {
        action: &'static str,
        actual: WriterStateV1,
    },
    OutcomeUnknown(PoisonPointV1),
    Poisoned(PoisonPointV1),
    Provenance(ProvenanceError),
    Wire(WireError),
}

impl fmt::Display for TransactionModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for TransactionModelError {}

impl From<ProvenanceError> for TransactionModelError {
    fn from(error: ProvenanceError) -> Self {
        Self::Provenance(error)
    }
}

impl From<WireError> for TransactionModelError {
    fn from(error: WireError) -> Self {
        Self::Wire(error)
    }
}

#[cfg(test)]
mod tests;
