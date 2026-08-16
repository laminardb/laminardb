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
mod tests {
    use super::*;
    use crate::provenance_v1::{
        prepare_output_authority_v1, AssignmentCertificateView, AssignmentParticipantRef,
        OutputMarkerInput, PipelineIdentityRef, ProcessLeaseView, RecoveryCheckpointView,
        RecoveryTerminal, VnodeOwnerRef, WriterIntervalInput,
    };

    const DEPLOYMENT: [u8; 16] = [1; 16];
    const OTHER_DEPLOYMENT: [u8; 16] = [21; 16];
    const INCARNATION: [u8; 16] = [2; 16];
    const OTHER_INCARNATION: [u8; 16] = [22; 16];
    const PIPELINE: [u8; 32] = [3; 32];
    const ASSIGNMENT_7: [u8; 32] = [
        0x5d, 0x96, 0xe2, 0x1c, 0x70, 0x59, 0xa1, 0x06, 0x71, 0x71, 0x46, 0xb0, 0x15, 0x15, 0xe9,
        0xb9, 0x7a, 0xe0, 0x88, 0x2c, 0x69, 0x03, 0x3d, 0x10, 0x8b, 0xc8, 0x2d, 0x77, 0xaa, 0x00,
        0x97, 0x2f,
    ];
    const COMMITTED_INDEX_DIGEST: [u8; 32] = [9; 32];
    const TOPOLOGY: [u8; 32] = [10; 32];
    const BOOT: [u8; 16] = [11; 16];
    const INTERVAL_A: [u8; 16] = [12; 16];
    const INTERVAL_B: [u8; 16] = [13; 16];
    const INTERVAL_C: [u8; 16] = [14; 16];
    const OPERATION_A: [u8; 32] = [15; 32];
    const OPERATION_B: [u8; 32] = [16; 32];
    const ZERO_OPERATION: [u8; 32] = [0; 32];
    const PLANNED_VNODES: [u16; 2] = [0, 2];
    const PARTITIONS: [i32; 3] = [0, 2, 4];
    const TWO_PARTITIONS: [i32; 2] = [0, 2];
    const OWNERS: [VnodeOwnerRef<'static>; 4] = [
        VnodeOwnerRef {
            vnode: 0,
            node_id: 41,
            boot_uuid: &BOOT,
        },
        VnodeOwnerRef {
            vnode: 1,
            node_id: 41,
            boot_uuid: &BOOT,
        },
        VnodeOwnerRef {
            vnode: 2,
            node_id: 41,
            boot_uuid: &BOOT,
        },
        VnodeOwnerRef {
            vnode: 3,
            node_id: 41,
            boot_uuid: &BOOT,
        },
    ];
    const PARTICIPANTS: [AssignmentParticipantRef<'static>; 1] = [AssignmentParticipantRef {
        node_id: 41,
        boot_uuid: &BOOT,
    }];

    fn prepared_authority<'a>(
        bitmap: &'a mut [u8; 1],
        current_interval: &'a [u8; 16],
        predecessor_interval: Option<&'a [u8; 16]>,
        deployment: &'a [u8; 16],
        incarnation: &'a [u8; 16],
        sink_id: &'a str,
        shard_id: &'a str,
    ) -> PreparedOutputAuthorityV1<'a> {
        let identity = PipelineIdentityRef {
            deployment_uuid: deployment,
            pipeline_incarnation_id: incarnation,
            pipeline_identity_version: wire_v1::PIPELINE_IDENTITY_VERSION,
            pipeline_identity_sha256: &PIPELINE,
        };
        let process = ProcessLeaseView {
            node_id: 41,
            boot_uuid: &BOOT,
            durable_process_term: 51,
        };
        prepare_output_authority_v1(
            OutputMarkerInput {
                identity,
                current_assignment: AssignmentCertificateView {
                    version: 7,
                    certificate_sha256: &ASSIGNMENT_7,
                    vnode_count: 4,
                    owners: &OWNERS,
                    participants: &PARTICIPANTS,
                },
                current_process: Some(process),
                recovery: RecoveryCheckpointView {
                    immutable: true,
                    terminal: RecoveryTerminal::Commit,
                    identity,
                    epoch: 61,
                    checkpoint_id: 61,
                    committed_index_sha256: &COMMITTED_INDEX_DIGEST,
                    base_assignment_version: 7,
                    base_assignment_certificate_sha256: &ASSIGNMENT_7,
                },
                interval: WriterIntervalInput {
                    current_interval_id: current_interval,
                    predecessor_interval_id: predecessor_interval,
                    claimed_writer: process,
                },
                topology_sha256: &TOPOLOGY,
                sink_id,
                operator_id: "aggregate-1",
                output_id: "grouped-count-sum",
                shard_id,
                planned_vnodes: &PLANNED_VNODES,
            },
            bitmap,
        )
        .unwrap()
    }

    fn roomy_limits() -> TransactionLimitsV1 {
        TransactionLimitsV1::new(16, 64 * 1024).unwrap()
    }

    fn ready_first<'a>(bitmap: &'a mut [u8; 1]) -> FakeTransactionalWriterV1<'a> {
        let authority = prepared_authority(
            bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let mut writer =
            FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
        assert!(writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap());
        assert_eq!(
            writer
                .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
                .unwrap(),
            1
        );
        writer
    }

    #[test]
    fn limits_and_chunk_planner_freeze_only_modeled_bytes() {
        assert_eq!(
            TransactionLimitsV1::new(0, 1),
            Err(TransactionModelError::InvalidLimit("max_records"))
        );
        assert_eq!(
            TransactionLimitsV1::new(wire_v1::MAX_DATA_HEADERS_PER_BATCH + 1, 1),
            Err(TransactionModelError::InvalidLimit("max_records"))
        );
        assert_eq!(
            TransactionLimitsV1::new(1, 0),
            Err(TransactionModelError::InvalidLimit("max_modeled_bytes"))
        );
        assert!(TransactionLimitsV1::new(wire_v1::MAX_DATA_HEADERS_PER_BATCH, 1).is_ok());

        let records = [
            DataRecordRefV1 {
                partition: 0,
                operation_id: &OPERATION_A,
                payload: b"a",
            },
            DataRecordRefV1 {
                partition: 2,
                operation_id: &OPERATION_B,
                payload: b"b",
            },
            DataRecordRefV1 {
                partition: 4,
                operation_id: &OPERATION_A,
                payload: b"c",
            },
        ];
        let count_limited = TransactionLimitsV1::new(2, 1_000).unwrap();
        assert_eq!(
            plan_data_chunks_v1(&records, count_limited).unwrap(),
            vec![0..2, 2..3]
        );
        let byte_limited =
            TransactionLimitsV1::new(3, 2 * (wire_v1::DATA_ENCODED_LEN + 1)).unwrap();
        assert_eq!(
            plan_data_chunks_v1(&records, byte_limited).unwrap(),
            vec![0..2, 2..3]
        );
        assert!(plan_data_chunks_v1(&[], byte_limited).unwrap().is_empty());

        let oversized = [DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"ab",
        }];
        let one_byte_short = TransactionLimitsV1::new(1, wire_v1::DATA_ENCODED_LEN + 1).unwrap();
        assert_eq!(
            plan_data_chunks_v1(&oversized, one_byte_short),
            Err(TransactionModelError::LimitExceeded(
                "single record modeled bytes"
            ))
        );
        assert!(matches!(
            checked_modeled_data_bytes([usize::MAX]),
            Err(TransactionModelError::ArithmeticOverflow(_))
        ));
        assert!(matches!(
            checked_modeled_data_bytes([usize::MAX - wire_v1::DATA_ENCODED_LEN, 1]),
            Err(TransactionModelError::ArithmeticOverflow(_))
        ));
    }

    #[test]
    fn marker_fanout_is_canonical_bounded_and_unsplittable() {
        let mut sizing_bitmap = [0_u8; 1];
        let sizing_authority = prepared_authority(
            &mut sizing_bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let marker_len = wire_v1::encoded_marker_len(&sizing_authority.marker_ref()).unwrap();

        let mut exact_bitmap = [0_u8; 1];
        let exact_authority = prepared_authority(
            &mut exact_bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert!(FakeTransactionalWriterV1::first(
            exact_authority,
            &PARTITIONS,
            TransactionLimitsV1::new(PARTITIONS.len(), marker_len * PARTITIONS.len()).unwrap(),
        )
        .is_ok());

        let mut count_bitmap = [0_u8; 1];
        let count_authority = prepared_authority(
            &mut count_bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert_eq!(
            FakeTransactionalWriterV1::first(
                count_authority,
                &PARTITIONS,
                TransactionLimitsV1::new(PARTITIONS.len() - 1, marker_len * PARTITIONS.len())
                    .unwrap(),
            )
            .unwrap_err(),
            TransactionModelError::LimitExceeded("record count")
        );

        let mut byte_bitmap = [0_u8; 1];
        let byte_authority = prepared_authority(
            &mut byte_bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert_eq!(
            FakeTransactionalWriterV1::first(
                byte_authority,
                &PARTITIONS,
                TransactionLimitsV1::new(PARTITIONS.len(), marker_len * PARTITIONS.len() - 1,)
                    .unwrap(),
            )
            .unwrap_err(),
            TransactionModelError::LimitExceeded("modeled bytes")
        );

        for partitions in [&[][..], &[0, 0][..], &[2, 0][..], &[-1, 0][..]] {
            let mut bitmap = [0_u8; 1];
            let authority = prepared_authority(
                &mut bitmap,
                &INTERVAL_A,
                None,
                &DEPLOYMENT,
                &INCARNATION,
                "sink-a",
                "shard-0",
            );
            assert_eq!(
                FakeTransactionalWriterV1::first(authority, partitions, roomy_limits())
                    .unwrap_err(),
                TransactionModelError::InvalidPartitions
            );
        }
    }

    #[test]
    fn first_marker_is_confirmed_before_any_data_opens() {
        let mut bitmap = [0_u8; 1];
        let authority = prepared_authority(
            &mut bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let mut writer =
            FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
        let record = [DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"alpha",
        }];

        assert!(matches!(
            writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::InvalidState { .. })
        ));
        assert!(!writer
            .initialize(SimulatedOutcomeV1::DefinitelyRejected)
            .unwrap());
        assert_eq!(writer.state, WriterStateV1::Uninitialized);
        assert!(writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap());
        assert!(matches!(
            writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::InvalidState { .. })
        ));

        let attempts = [
            SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Begin),
            SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Send),
            SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Commit),
            SimulatedAttemptV1::CommitConfirmed,
        ];
        assert_eq!(writer.commit_marker(&attempts).unwrap(), 4);
        assert_eq!(writer.state, WriterStateV1::DataOpen);
        let marker = writer.confirmed_marker().unwrap();
        assert_eq!(marker.partitions, &PARTITIONS);
        let decoded = wire_v1::decode_marker(marker.envelope).unwrap();
        assert_eq!(decoded.current_interval_id, &INTERVAL_A);
        assert_eq!(decoded.predecessor_interval_id, None);
        assert!(writer.confirmed_data().is_empty());
        assert_eq!(writer.next_sequence, Some(0));
    }

    #[test]
    fn transaction_in_flight_phases_and_invalid_actions_are_real_states() {
        let mut bitmap = [0_u8; 1];
        let authority = prepared_authority(
            &mut bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let mut writer =
            FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
        writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap();

        assert!(matches!(
            writer.begin_transaction(TransactionKindV1::Data, SimulatedOutcomeV1::Confirmed),
            Err(TransactionModelError::InvalidState { .. })
        ));
        writer
            .begin_transaction(TransactionKindV1::Marker, SimulatedOutcomeV1::Confirmed)
            .unwrap();
        assert_eq!(
            writer.state,
            WriterStateV1::TransactionInFlight {
                kind: TransactionKindV1::Marker,
                phase: TransactionPhaseV1::Begun,
                return_state: StableStateV1::MarkerPending,
            }
        );
        let before_invalid_commit = writer.state;
        assert!(matches!(
            writer.commit_transaction(SimulatedOutcomeV1::Confirmed),
            Err(TransactionModelError::InvalidState { .. })
        ));
        assert_eq!(writer.state, before_invalid_commit);
        writer.abort_transaction().unwrap();
        assert_eq!(writer.state, WriterStateV1::MarkerPending);

        writer
            .begin_transaction(TransactionKindV1::Marker, SimulatedOutcomeV1::Confirmed)
            .unwrap();
        writer
            .send_transaction(SimulatedOutcomeV1::Confirmed)
            .unwrap();
        assert_eq!(
            writer.state,
            WriterStateV1::TransactionInFlight {
                kind: TransactionKindV1::Marker,
                phase: TransactionPhaseV1::Staged,
                return_state: StableStateV1::MarkerPending,
            }
        );
        writer.abort_transaction().unwrap();
        assert_eq!(writer.state, WriterStateV1::MarkerPending);
        writer
            .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
            .unwrap();
        assert_eq!(writer.state, WriterStateV1::DataOpen);
        assert!(writer.confirmed_marker().is_ok());
    }

    #[test]
    fn deterministic_data_aborts_retry_identical_headers_and_range() {
        let mut bitmap = [0_u8; 1];
        let mut writer = ready_first(&mut bitmap);
        let records = [
            DataRecordRefV1 {
                partition: 2,
                operation_id: &OPERATION_A,
                payload: b"alpha",
            },
            DataRecordRefV1 {
                partition: 0,
                operation_id: &OPERATION_A,
                payload: b"beta",
            },
            DataRecordRefV1 {
                partition: 4,
                operation_id: &OPERATION_B,
                payload: b"gamma",
            },
        ];
        let attempts = [
            SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Begin),
            SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Send),
            SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Commit),
            SimulatedAttemptV1::CommitConfirmed,
        ];
        let committed = writer.commit_data_batch(&records, &attempts).unwrap();
        assert_eq!(
            committed,
            CommittedBatchV1 {
                attempts: 4,
                first_sequence: 0,
                last_sequence: 2,
            }
        );
        assert_eq!(writer.next_sequence, Some(3));
        assert_eq!(writer.confirmed_data().len(), 1);
        let transaction = &writer.confirmed_data()[0];
        assert_eq!(transaction.records.len(), 3);
        assert_eq!(transaction.payload(0), Some(&b"alpha"[..]));
        assert_eq!(transaction.payload(1), Some(&b"beta"[..]));
        assert_eq!(transaction.payload(2), Some(&b"gamma"[..]));
        let mut header_refs = Vec::new();
        for (index, record) in transaction.records.iter().enumerate() {
            let decoded = wire_v1::decode_data(&record.header).unwrap();
            assert_eq!(decoded.writer_interval_id, &INTERVAL_A);
            assert_eq!(decoded.admission_sequence, index as u64);
            header_refs.push(record.header.as_slice());
        }
        assert_eq!(
            wire_v1::validate_data_header_batch(&header_refs).unwrap(),
            3 * wire_v1::DATA_ENCODED_LEN
        );
    }

    #[test]
    fn ambiguous_initialize_marker_and_data_are_terminal_and_inert() {
        let mut initialize_bitmap = [0_u8; 1];
        let initialize_authority = prepared_authority(
            &mut initialize_bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let mut initialize_writer =
            FakeTransactionalWriterV1::first(initialize_authority, &PARTITIONS, roomy_limits())
                .unwrap();
        assert_eq!(
            initialize_writer.initialize(SimulatedOutcomeV1::Ambiguous),
            Err(TransactionModelError::OutcomeUnknown(
                PoisonPointV1::Initialize
            ))
        );
        assert_eq!(
            initialize_writer.initialize(SimulatedOutcomeV1::Confirmed),
            Err(TransactionModelError::Poisoned(PoisonPointV1::Initialize))
        );

        for point in [
            FaultPointV1::Begin,
            FaultPointV1::Send,
            FaultPointV1::Commit,
        ] {
            let mut bitmap = [0_u8; 1];
            let authority = prepared_authority(
                &mut bitmap,
                &INTERVAL_A,
                None,
                &DEPLOYMENT,
                &INCARNATION,
                "sink-a",
                "shard-0",
            );
            let mut writer =
                FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
            writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap();
            assert!(matches!(
                writer.commit_marker(&[SimulatedAttemptV1::OutcomeUnknownAt(point)]),
                Err(TransactionModelError::OutcomeUnknown(_))
            ));
            assert!(matches!(writer.state, WriterStateV1::TerminalPoison { .. }));
            assert_eq!(
                writer.confirmed_marker(),
                Err(TransactionModelError::MarkerNotConfirmed)
            );
            assert!(matches!(
                writer.commit_marker(&[SimulatedAttemptV1::CommitConfirmed]),
                Err(TransactionModelError::Poisoned(_))
            ));
            assert!(matches!(
                writer.initialize(SimulatedOutcomeV1::Confirmed),
                Err(TransactionModelError::Poisoned(_))
            ));
        }

        for point in [
            FaultPointV1::Begin,
            FaultPointV1::Send,
            FaultPointV1::Commit,
        ] {
            let mut bitmap = [0_u8; 1];
            let mut writer = ready_first(&mut bitmap);
            let record = [DataRecordRefV1 {
                partition: 0,
                operation_id: &OPERATION_A,
                payload: b"alpha",
            }];
            assert!(matches!(
                writer.commit_data_batch(&record, &[SimulatedAttemptV1::OutcomeUnknownAt(point)]),
                Err(TransactionModelError::OutcomeUnknown(_))
            ));
            assert!(writer.confirmed_data().is_empty());
            assert_eq!(writer.next_sequence, Some(0));
            assert!(matches!(
                writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
                Err(TransactionModelError::Poisoned(_))
            ));
            assert!(matches!(
                writer.commit_marker(&[SimulatedAttemptV1::CommitConfirmed]),
                Err(TransactionModelError::Poisoned(_))
            ));
        }
    }

    #[test]
    fn preflight_and_script_failures_have_no_state_or_sequence_effect() {
        let mut bitmap = [0_u8; 1];
        let mut writer = ready_first(&mut bitmap);
        let state = writer.state;
        let next = writer.next_sequence;

        assert_eq!(
            writer.commit_data_batch(&[], &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::EmptyTransaction)
        );
        let zero = [DataRecordRefV1 {
            partition: 0,
            operation_id: &ZERO_OPERATION,
            payload: b"zero",
        }];
        assert!(matches!(
            writer.commit_data_batch(&zero, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::Provenance(_))
        ));
        let wrong_partition = [DataRecordRefV1 {
            partition: 1,
            operation_id: &OPERATION_A,
            payload: b"wrong",
        }];
        assert_eq!(
            writer.commit_data_batch(&wrong_partition, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::UnknownPartition(1))
        );
        let valid = [DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"valid",
        }];
        assert_eq!(
            writer.commit_data_batch(&valid, &[]),
            Err(TransactionModelError::EmptyAttemptScript)
        );
        assert_eq!(
            writer.commit_data_batch(
                &valid,
                &[SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Send)]
            ),
            Err(TransactionModelError::UnresolvedAttemptScript)
        );
        assert_eq!(
            writer.commit_data_batch(
                &valid,
                &[
                    SimulatedAttemptV1::CommitConfirmed,
                    SimulatedAttemptV1::OutcomeUnknownAt(FaultPointV1::Commit),
                ]
            ),
            Err(TransactionModelError::NonCanonicalAttemptScript)
        );
        let too_many = vec![valid[0]; roomy_limits().max_records + 1];
        assert_eq!(
            writer.commit_data_batch(&too_many, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::LimitExceeded("record count"))
        );
        let oversized_payload = vec![0_u8; roomy_limits().max_modeled_bytes];
        let oversized = [DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: &oversized_payload,
        }];
        assert_eq!(
            writer.commit_data_batch(&oversized, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::LimitExceeded("modeled bytes"))
        );
        assert_eq!(writer.state, state);
        assert_eq!(writer.next_sequence, next);
        assert!(writer.confirmed_data().is_empty());
    }

    #[test]
    fn sequence_maximum_is_usable_once_then_explicitly_exhausted() {
        let record = [DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"max",
        }];
        let mut max_bitmap = [0_u8; 1];
        let mut max_writer = ready_first(&mut max_bitmap);
        max_writer.next_sequence = Some(u64::MAX);
        let committed = max_writer
            .commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed])
            .unwrap();
        assert_eq!(committed.first_sequence, u64::MAX);
        assert_eq!(committed.last_sequence, u64::MAX);
        assert_eq!(max_writer.next_sequence, None);
        assert_eq!(
            max_writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::SequenceExhausted)
        );
        assert_eq!(max_writer.confirmed_data().len(), 1);

        let two_records = [record[0], record[0]];
        let mut pair_bitmap = [0_u8; 1];
        let mut pair_writer = ready_first(&mut pair_bitmap);
        pair_writer.next_sequence = Some(u64::MAX - 1);
        let committed = pair_writer
            .commit_data_batch(&two_records, &[SimulatedAttemptV1::CommitConfirmed])
            .unwrap();
        assert_eq!(committed.first_sequence, u64::MAX - 1);
        assert_eq!(committed.last_sequence, u64::MAX);
        assert_eq!(pair_writer.next_sequence, None);

        let three_records = [record[0], record[0], record[0]];
        let mut overflow_bitmap = [0_u8; 1];
        let mut overflow_writer = ready_first(&mut overflow_bitmap);
        overflow_writer.next_sequence = Some(u64::MAX - 1);
        assert_eq!(
            overflow_writer
                .commit_data_batch(&three_records, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::SequenceExhausted)
        );
        assert_eq!(overflow_writer.next_sequence, Some(u64::MAX - 1));
        assert!(overflow_writer.confirmed_data().is_empty());
        assert_eq!(overflow_writer.state, WriterStateV1::DataOpen);
    }

    #[test]
    fn successor_replay_keeps_operation_bytes_but_rotates_interval_and_sequence() {
        let record = [DataRecordRefV1 {
            partition: 2,
            operation_id: &OPERATION_A,
            payload: b"stable-payload",
        }];
        let mut first_bitmap = [0_u8; 1];
        let mut first = ready_first(&mut first_bitmap);
        first
            .commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed])
            .unwrap();
        let predecessor = first.confirmed_interval().unwrap();

        let mut successor_bitmap = [0_u8; 1];
        let successor_authority = prepared_authority(
            &mut successor_bitmap,
            &INTERVAL_B,
            Some(&INTERVAL_A),
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let mut successor = FakeTransactionalWriterV1::successor(
            successor_authority,
            predecessor,
            &PARTITIONS,
            roomy_limits(),
        )
        .unwrap();
        successor.initialize(SimulatedOutcomeV1::Confirmed).unwrap();
        successor
            .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
            .unwrap();
        let marker =
            wire_v1::decode_marker(successor.confirmed_marker().unwrap().envelope).unwrap();
        assert_eq!(marker.current_interval_id, &INTERVAL_B);
        assert_eq!(marker.predecessor_interval_id, Some(&INTERVAL_A));
        let committed = successor
            .commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed])
            .unwrap();
        assert_eq!(committed.first_sequence, 0);

        let first_record = &first.confirmed_data()[0].records[0];
        let successor_record = &successor.confirmed_data()[0].records[0];
        let first_header = wire_v1::decode_data(&first_record.header).unwrap();
        let successor_header = wire_v1::decode_data(&successor_record.header).unwrap();
        assert_eq!(first_header.operation_id, successor_header.operation_id);
        assert_eq!(first_header.admission_sequence, 0);
        assert_eq!(successor_header.admission_sequence, 0);
        assert_ne!(
            first_header.writer_interval_id,
            successor_header.writer_interval_id
        );
        assert_eq!(
            first.confirmed_data()[0].payload(0),
            Some(&b"stable-payload"[..])
        );
        assert_eq!(
            successor.confirmed_data()[0].payload(0),
            Some(&b"stable-payload"[..])
        );
    }

    #[test]
    fn successor_requires_confirmed_predecessor_and_exact_stable_scope() {
        let mut first_bitmap = [0_u8; 1];
        let first = ready_first(&mut first_bitmap);
        let predecessor = first.confirmed_interval().unwrap();

        let mut first_with_predecessor_bitmap = [0_u8; 1];
        let first_with_predecessor = prepared_authority(
            &mut first_with_predecessor_bitmap,
            &INTERVAL_B,
            Some(&INTERVAL_A),
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert_eq!(
            FakeTransactionalWriterV1::first(first_with_predecessor, &PARTITIONS, roomy_limits())
                .unwrap_err(),
            TransactionModelError::FirstIntervalHasPredecessor
        );

        let mut wrong_predecessor_bitmap = [0_u8; 1];
        let wrong_predecessor = prepared_authority(
            &mut wrong_predecessor_bitmap,
            &INTERVAL_B,
            Some(&INTERVAL_C),
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert_eq!(
            FakeTransactionalWriterV1::successor(
                wrong_predecessor,
                predecessor,
                &PARTITIONS,
                roomy_limits(),
            )
            .unwrap_err(),
            TransactionModelError::SuccessorPredecessorMismatch
        );

        fn assert_scope_mismatch(
            predecessor: ConfirmedIntervalV1<'_>,
            deployment: &'static [u8; 16],
            incarnation: &'static [u8; 16],
            sink_id: &'static str,
            shard_id: &'static str,
        ) {
            let mut bitmap = [0_u8; 1];
            let authority = prepared_authority(
                &mut bitmap,
                &INTERVAL_B,
                Some(&INTERVAL_A),
                deployment,
                incarnation,
                sink_id,
                shard_id,
            );
            assert_eq!(
                FakeTransactionalWriterV1::successor(
                    authority,
                    predecessor,
                    &PARTITIONS,
                    roomy_limits(),
                )
                .unwrap_err(),
                TransactionModelError::StableProducerScopeMismatch
            );
        }
        assert_scope_mismatch(
            predecessor,
            &OTHER_DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert_scope_mismatch(
            predecessor,
            &DEPLOYMENT,
            &OTHER_INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert_scope_mismatch(predecessor, &DEPLOYMENT, &INCARNATION, "sink-b", "shard-0");
        assert_scope_mismatch(predecessor, &DEPLOYMENT, &INCARNATION, "sink-a", "shard-1");
    }

    #[test]
    fn explicit_chunk_execution_preserves_confirmed_prefix_on_later_ambiguity() {
        let records = [
            DataRecordRefV1 {
                partition: 0,
                operation_id: &OPERATION_A,
                payload: b"a",
            },
            DataRecordRefV1 {
                partition: 2,
                operation_id: &OPERATION_B,
                payload: b"b",
            },
            DataRecordRefV1 {
                partition: 0,
                operation_id: &OPERATION_A,
                payload: b"c",
            },
        ];
        let limits = TransactionLimitsV1::new(2, 64 * 1024).unwrap();
        let ranges = plan_data_chunks_v1(&records, limits).unwrap();
        assert_eq!(ranges, vec![0..2, 2..3]);

        let mut bitmap = [0_u8; 1];
        let authority = prepared_authority(
            &mut bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let mut writer =
            FakeTransactionalWriterV1::first(authority, &TWO_PARTITIONS, limits).unwrap();
        writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap();
        writer
            .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
            .unwrap();
        writer
            .commit_data_batch(
                &records[ranges[0].clone()],
                &[SimulatedAttemptV1::CommitConfirmed],
            )
            .unwrap();
        assert_eq!(writer.confirmed_data().len(), 1);
        assert_eq!(writer.next_sequence, Some(2));

        assert!(matches!(
            writer.commit_data_batch(
                &records[ranges[1].clone()],
                &[SimulatedAttemptV1::OutcomeUnknownAt(FaultPointV1::Commit)]
            ),
            Err(TransactionModelError::OutcomeUnknown(_))
        ));
        assert_eq!(writer.confirmed_data().len(), 1);
        assert_eq!(writer.next_sequence, Some(2));
        assert!(matches!(writer.state, WriterStateV1::TerminalPoison { .. }));
    }
}
