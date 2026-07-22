use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MODEL_VERSION: &str = "state-backend-reference/v1";
pub const REQUEST_ENCODING_VERSION: &str = "LDB-SBQ-REQUEST-V1";
pub const OBSERVATION_ENCODING_VERSION: &str = "LDB-SBQ-OBSERVATION-V1";
pub const STATE_ENCODING_VERSION: &str = "LDB-SBQ-STATE-V1";
pub const MAX_MODEL_CANONICAL_BYTES: u64 = 64 * 1024 * 1024;

const REQUEST_DOMAIN: &[u8] = b"LDB-SBQ-REQUEST-V1\0";
const OBSERVATION_DOMAIN: &[u8] = b"LDB-SBQ-OBSERVATION-V1\0";
const STATE_DOMAIN: &[u8] = b"LDB-SBQ-STATE-V1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Table {
    AggregateState,
    WindowState,
    TimerIndex,
    JoinLeftRows,
    JoinRightRows,
    OutputBookkeeping,
}

impl Table {
    pub const fn tag(self) -> u8 {
        match self {
            Self::AggregateState => 0x01,
            Self::WindowState => 0x02,
            Self::TimerIndex => 0x03,
            Self::JoinLeftRows => 0x04,
            Self::JoinRightRows => 0x05,
            Self::OutputBookkeeping => 0x06,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    Aggregate,
    TimerWindow,
    Join,
}

impl Scenario {
    pub const fn tag(self) -> u8 {
        match self {
            Self::Aggregate => 0x01,
            Self::TimerWindow => 0x02,
            Self::Join => 0x03,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchKind {
    Measured,
    Setup,
}

impl BatchKind {
    pub const fn tag(self) -> u8 {
        match self {
            Self::Measured => 0x01,
            Self::Setup => 0x02,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogicalKey {
    pub table: Table,
    pub vnode: u32,
    pub key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RangeRead {
    pub table: Table,
    pub vnode: u32,
    pub start_inclusive: Vec<u8>,
    pub end_exclusive: Vec<u8>,
    pub max_rows: u32,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Put { key: LogicalKey, value: Vec<u8> },
    Delete { key: LogicalKey },
}

impl Mutation {
    pub fn key(&self) -> &LogicalKey {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchLimits {
    pub request_bytes_max_u64: u64,
    pub read_rows_max_u64: u64,
    pub read_bytes_max_u64: u64,
    pub mutation_bytes_max_u64: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalBatch {
    pub kind: BatchKind,
    pub scenario: Scenario,
    pub ordinal: u64,
    pub logical_rows: u32,
    pub limits: BatchLimits,
    pub point_reads: Vec<LogicalKey>,
    pub ranges: Vec<RangeRead>,
    pub mutations: Vec<Mutation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointResult {
    pub key: LogicalKey,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeRow {
    pub key: LogicalKey,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeResult {
    pub request: RangeRead,
    pub rows: Vec<RangeRow>,
    pub has_more: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    pub kind: BatchKind,
    pub scenario: Scenario,
    pub ordinal: u64,
    pub point_results: Vec<PointResult>,
    pub range_results: Vec<RangeResult>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultPhase {
    BatchBeforeCommit,
    BatchAfterCommitBeforeAck,
    PersistBefore,
    PersistAfterSuccessBeforeAck,
    SnapshotOpen,
    ExportRecord,
    RestoreRecord,
    CleanupRecord,
}

impl FaultPhase {
    const COUNT: usize = 8;

    const fn index(self) -> usize {
        match self {
            Self::BatchBeforeCommit => 0,
            Self::BatchAfterCommitBeforeAck => 1,
            Self::PersistBefore => 2,
            Self::PersistAfterSuccessBeforeAck => 3,
            Self::SnapshotOpen => 4,
            Self::ExportRecord => 5,
            Self::RestoreRecord => 6,
            Self::CleanupRecord => 7,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::BatchBeforeCommit => "batch_before_commit",
            Self::BatchAfterCommitBeforeAck => "batch_after_commit_before_ack",
            Self::PersistBefore => "persist_before",
            Self::PersistAfterSuccessBeforeAck => "persist_after_success_before_ack",
            Self::SnapshotOpen => "snapshot_open",
            Self::ExportRecord => "export_record",
            Self::RestoreRecord => "restore_record",
            Self::CleanupRecord => "cleanup_record",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultOrdinal {
    pub phase: FaultPhase,
    pub occurrence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultInjector {
    target: Option<FaultOrdinal>,
    visits: [u64; FaultPhase::COUNT],
    fired: bool,
}

impl FaultInjector {
    pub const fn disabled() -> Self {
        Self {
            target: None,
            visits: [0; FaultPhase::COUNT],
            fired: false,
        }
    }

    pub const fn armed(target: FaultOrdinal) -> Self {
        Self {
            target: Some(target),
            visits: [0; FaultPhase::COUNT],
            fired: false,
        }
    }

    pub const fn target(&self) -> Option<FaultOrdinal> {
        self.target
    }

    pub const fn fired(&self) -> bool {
        self.fired
    }

    pub const fn visits(&self, phase: FaultPhase) -> u64 {
        self.visits[phase.index()]
    }

    pub fn verify_reached(&self) -> Result<(), ModelError> {
        if let Some(target) = self.target {
            if !self.fired {
                return Err(ModelError::FaultTargetNotReached {
                    target,
                    visits: self.visits(target.phase),
                });
            }
        }
        Ok(())
    }

    fn visit(&mut self, phase: FaultPhase) -> Result<(), ModelError> {
        let index = phase.index();
        let occurrence = self.visits[index];
        self.visits[index] = occurrence
            .checked_add(1)
            .ok_or_else(|| ModelError::invalid("fault occurrence counter overflow"))?;
        if let Some(target) = self.target {
            if !self.fired && target.phase == phase && target.occurrence == occurrence {
                self.fired = true;
                return Err(match phase {
                    FaultPhase::BatchAfterCommitBeforeAck
                    | FaultPhase::PersistAfterSuccessBeforeAck => {
                        ModelError::AmbiguousAfterSuccess { ordinal: target }
                    }
                    _ => ModelError::InjectedFault { ordinal: target },
                });
            }
        }
        Ok(())
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelError {
    Invalid(String),
    RowTooLarge { required_bytes: u64 },
    InjectedFault { ordinal: FaultOrdinal },
    AmbiguousAfterSuccess { ordinal: FaultOrdinal },
    FaultTargetNotReached { target: FaultOrdinal, visits: u64 },
}

impl ModelError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

impl Display for ModelError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::RowTooLarge { required_bytes } => {
                write!(formatter, "range row requires {required_bytes} bytes")
            }
            Self::InjectedFault { ordinal } => write!(
                formatter,
                "injected fault at {} occurrence {}",
                ordinal.phase.name(),
                ordinal.occurrence
            ),
            Self::AmbiguousAfterSuccess { ordinal } => write!(
                formatter,
                "operation succeeded before acknowledgement fault at {} occurrence {}",
                ordinal.phase.name(),
                ordinal.occurrence
            ),
            Self::FaultTargetNotReached { target, visits } => write!(
                formatter,
                "fault target {} occurrence {} was not reached after {visits} eligible visits",
                target.phase.name(),
                target.occurrence
            ),
        }
    }
}

impl std::error::Error for ModelError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreBudget {
    pub records_max_u64: u64,
    pub key_bytes_max_u64: u64,
    pub value_bytes_max_u64: u64,
    pub canonical_bytes_max_u64: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    vnode_count: u32,
    records: BTreeMap<LogicalKey, Vec<u8>>,
}

impl Snapshot {
    pub fn records(&self) -> &BTreeMap<LogicalKey, Vec<u8>> {
        &self.records
    }

    pub fn digest(&self) -> Result<[u8; 32], ModelError> {
        digest_records(&self.records)
    }

    pub fn export_vnode(&self, vnode: u32) -> Result<Vec<(LogicalKey, Vec<u8>)>, ModelError> {
        let mut faults = FaultInjector::disabled();
        self.export_vnode_with_fault(vnode, &mut faults)
    }

    pub fn export_vnode_with_fault(
        &self,
        vnode: u32,
        faults: &mut FaultInjector,
    ) -> Result<Vec<(LogicalKey, Vec<u8>)>, ModelError> {
        if vnode >= self.vnode_count {
            return Err(ModelError::invalid("vnode is outside the active range"));
        }

        let mut exported = Vec::new();
        for (key, value) in self.records.iter().filter(|(key, _)| key.vnode == vnode) {
            faults.visit(FaultPhase::ExportRecord)?;
            exported.push((key.clone(), value.clone()));
        }
        Ok(exported)
    }
}

#[derive(Clone, Debug)]
pub struct ReferenceModel {
    vnode_count: u32,
    key_bytes_max: u32,
    value_bytes_max: u32,
    live: BTreeMap<LogicalKey, Vec<u8>>,
    durable: BTreeMap<LogicalKey, Vec<u8>>,
}

impl ReferenceModel {
    pub fn new(
        vnode_count: u32,
        key_bytes_max: u32,
        value_bytes_max: u32,
    ) -> Result<Self, ModelError> {
        if vnode_count == 0 {
            return Err(ModelError::invalid("vnode count must be positive"));
        }
        Ok(Self {
            vnode_count,
            key_bytes_max,
            value_bytes_max,
            live: BTreeMap::new(),
            durable: BTreeMap::new(),
        })
    }

    pub fn execute(&mut self, batch: &LogicalBatch) -> Result<Observation, ModelError> {
        let mut faults = FaultInjector::disabled();
        self.execute_with_fault(batch, &mut faults)
    }

    pub fn execute_with_fault(
        &mut self,
        batch: &LogicalBatch,
        faults: &mut FaultInjector,
    ) -> Result<Observation, ModelError> {
        self.validate_batch(batch)?;
        let observation = self.observe(batch)?;
        faults.visit(FaultPhase::BatchBeforeCommit)?;
        // The single-threaded oracle exposes no observation or fallible hook inside this install.
        for mutation in &batch.mutations {
            match mutation {
                Mutation::Put { key, value } => {
                    self.live.insert(key.clone(), value.clone());
                }
                Mutation::Delete { key } => {
                    self.live.remove(key);
                }
            }
        }
        faults.visit(FaultPhase::BatchAfterCommitBeforeAck)?;
        Ok(observation)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            vnode_count: self.vnode_count,
            records: self.live.clone(),
        }
    }

    pub fn snapshot_with_fault(&self, faults: &mut FaultInjector) -> Result<Snapshot, ModelError> {
        faults.visit(FaultPhase::SnapshotOpen)?;
        Ok(self.snapshot())
    }

    pub fn restore_vnode(
        &mut self,
        vnode: u32,
        records: &[(LogicalKey, Vec<u8>)],
        budget: RestoreBudget,
    ) -> Result<(), ModelError> {
        let mut faults = FaultInjector::disabled();
        self.restore_vnode_with_fault(vnode, records, budget, &mut faults)
    }

    pub fn restore_vnode_with_fault(
        &mut self,
        vnode: u32,
        records: &[(LogicalKey, Vec<u8>)],
        budget: RestoreBudget,
        faults: &mut FaultInjector,
    ) -> Result<(), ModelError> {
        self.validate_vnode(vnode)?;
        let mut record_count = 0_u64;
        let mut key_bytes = 0_u64;
        let mut value_bytes = 0_u64;
        let mut canonical_bytes = 0_u64;
        let mut previous: Option<&LogicalKey> = None;

        for (key, value) in records {
            self.validate_key(key)?;
            self.validate_value(value)?;
            if key.vnode != vnode {
                return Err(ModelError::invalid(
                    "restore record belongs to another vnode",
                ));
            }
            if previous.is_some_and(|prior| prior >= key) {
                return Err(ModelError::invalid(
                    "restore records must be strictly increasing and unique",
                ));
            }
            previous = Some(key);

            record_count = checked_add(record_count, 1, "restore record count")?;
            key_bytes = checked_add(key_bytes, usize_to_u64(key.key.len())?, "restore key bytes")?;
            value_bytes = checked_add(
                value_bytes,
                usize_to_u64(value.len())?,
                "restore value bytes",
            )?;
            canonical_bytes = checked_add(
                canonical_bytes,
                canonical_record_charge(key, value)?,
                "restore canonical bytes",
            )?;
            if record_count > budget.records_max_u64
                || key_bytes > budget.key_bytes_max_u64
                || value_bytes > budget.value_bytes_max_u64
                || canonical_bytes > budget.canonical_bytes_max_u64
            {
                return Err(ModelError::invalid("restore budget exceeded"));
            }
        }

        let mut staged = BTreeMap::new();
        for (key, value) in records {
            faults.visit(FaultPhase::RestoreRecord)?;
            staged.insert(key.clone(), value.clone());
        }

        let mut next_live = self.live.clone();
        next_live.retain(|key, _| key.vnode != vnode);
        next_live.append(&mut staged);
        self.live = next_live;
        Ok(())
    }

    pub fn drop_vnode(&mut self, vnode: u32) -> Result<(), ModelError> {
        let mut faults = FaultInjector::disabled();
        self.drop_vnode_with_fault(vnode, &mut faults)
    }

    pub fn drop_vnode_with_fault(
        &mut self,
        vnode: u32,
        faults: &mut FaultInjector,
    ) -> Result<(), ModelError> {
        self.validate_vnode(vnode)?;
        let selected: Vec<_> = self
            .live
            .keys()
            .filter(|key| key.vnode == vnode)
            .cloned()
            .collect();
        for key in selected {
            faults.visit(FaultPhase::CleanupRecord)?;
            self.live.remove(&key);
        }
        Ok(())
    }

    pub fn persist(&mut self) {
        let mut faults = FaultInjector::disabled();
        self.persist_with_fault(&mut faults)
            .expect("disabled fault injector cannot fail");
    }

    pub fn persist_with_fault(&mut self, faults: &mut FaultInjector) -> Result<(), ModelError> {
        let next_durable = self.live.clone();
        faults.visit(FaultPhase::PersistBefore)?;
        self.durable = next_durable;
        faults.visit(FaultPhase::PersistAfterSuccessBeforeAck)?;
        Ok(())
    }

    pub fn crash_reopen(&mut self) {
        self.live.clone_from(&self.durable);
    }

    pub fn live_records(&self) -> &BTreeMap<LogicalKey, Vec<u8>> {
        &self.live
    }

    pub fn durable_records(&self) -> &BTreeMap<LogicalKey, Vec<u8>> {
        &self.durable
    }

    pub fn live_digest(&self) -> Result<[u8; 32], ModelError> {
        digest_records(&self.live)
    }

    pub fn durable_digest(&self) -> Result<[u8; 32], ModelError> {
        digest_records(&self.durable)
    }

    fn validate_batch(&self, batch: &LogicalBatch) -> Result<(), ModelError> {
        let request_bytes = encoded_request_len(batch)?;
        validate_request_byte_limits(request_bytes, batch.limits.request_bytes_max_u64)?;

        ensure_strictly_increasing(&batch.point_reads, "point reads")?;
        ensure_strictly_increasing(&batch.ranges, "range reads")?;

        for key in &batch.point_reads {
            self.validate_key(key)?;
        }
        for range in &batch.ranges {
            self.validate_vnode(range.vnode)?;
            if range.start_inclusive >= range.end_exclusive {
                return Err(ModelError::invalid(
                    "range start must be before its exclusive end",
                ));
            }
            if range.max_rows == 0 || range.max_bytes == 0 {
                return Err(ModelError::invalid(
                    "range row and byte limits must be positive",
                ));
            }
            self.validate_opaque_key(&range.start_inclusive)?;
            self.validate_opaque_key(&range.end_exclusive)?;
        }

        let mut previous_mutation: Option<&LogicalKey> = None;
        let mut mutation_bytes = 0_u64;
        for mutation in &batch.mutations {
            let key = mutation.key();
            self.validate_key(key)?;
            if previous_mutation.is_some_and(|previous| previous >= key) {
                return Err(ModelError::invalid(
                    "mutations must be strictly increasing and unique by key",
                ));
            }
            previous_mutation = Some(key);
            let mut charge = usize_to_u64(key.key.len())?;
            if let Mutation::Put { value, .. } = mutation {
                self.validate_value(value)?;
                charge = checked_add(charge, usize_to_u64(value.len())?, "mutation charge")?;
            }
            mutation_bytes = checked_add(mutation_bytes, charge, "mutation bytes")?;
        }
        if mutation_bytes > batch.limits.mutation_bytes_max_u64 {
            return Err(ModelError::invalid("mutation byte limit exceeded"));
        }

        Ok(())
    }

    fn observe(&self, batch: &LogicalBatch) -> Result<Observation, ModelError> {
        let mut read_rows = 0_u64;
        let mut read_bytes = 0_u64;
        let mut point_results = Vec::with_capacity(batch.point_reads.len());
        for key in &batch.point_reads {
            let value = self.live.get(key).cloned();
            let mut charge = usize_to_u64(key.key.len())?;
            if let Some(value) = &value {
                charge = checked_add(charge, usize_to_u64(value.len())?, "point read charge")?;
            }
            charge_read_limits(batch.limits, &mut read_rows, &mut read_bytes, charge)?;
            point_results.push(PointResult {
                key: key.clone(),
                value,
            });
        }

        let mut range_results = Vec::with_capacity(batch.ranges.len());
        for range in &batch.ranges {
            let start = LogicalKey {
                table: range.table,
                vnode: range.vnode,
                key: range.start_inclusive.clone(),
            };
            let end = LogicalKey {
                table: range.table,
                vnode: range.vnode,
                key: range.end_exclusive.clone(),
            };
            let mut rows = Vec::new();
            let mut range_bytes = 0_u64;
            let mut has_more = false;
            for (key, value) in self.live.range(start..end) {
                let charge = checked_add(
                    usize_to_u64(key.key.len())?,
                    usize_to_u64(value.len())?,
                    "range row charge",
                )?;
                if rows.len()
                    >= usize::try_from(range.max_rows).map_err(|_| {
                        ModelError::invalid("range max_rows does not fit platform usize")
                    })?
                {
                    has_more = true;
                    break;
                }
                let next_range_bytes = checked_add(range_bytes, charge, "range result bytes")?;
                if next_range_bytes > range.max_bytes {
                    if rows.is_empty() {
                        return Err(ModelError::RowTooLarge {
                            required_bytes: charge,
                        });
                    }
                    has_more = true;
                    break;
                }
                charge_read_limits(batch.limits, &mut read_rows, &mut read_bytes, charge)?;
                range_bytes = next_range_bytes;
                rows.push(RangeRow {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
            range_results.push(RangeResult {
                request: range.clone(),
                rows,
                has_more,
            });
        }

        Ok(Observation {
            kind: batch.kind,
            scenario: batch.scenario,
            ordinal: batch.ordinal,
            point_results,
            range_results,
        })
    }

    fn validate_key(&self, key: &LogicalKey) -> Result<(), ModelError> {
        self.validate_vnode(key.vnode)?;
        self.validate_opaque_key(&key.key)
    }

    fn validate_opaque_key(&self, key: &[u8]) -> Result<(), ModelError> {
        if key.len()
            > usize::try_from(self.key_bytes_max)
                .map_err(|_| ModelError::invalid("key limit does not fit platform usize"))?
        {
            return Err(ModelError::invalid(
                "logical key width exceeds active limit",
            ));
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<(), ModelError> {
        if value.len()
            > usize::try_from(self.value_bytes_max)
                .map_err(|_| ModelError::invalid("value limit does not fit platform usize"))?
        {
            return Err(ModelError::invalid("value width exceeds active limit"));
        }
        Ok(())
    }

    fn validate_vnode(&self, vnode: u32) -> Result<(), ModelError> {
        if vnode >= self.vnode_count {
            return Err(ModelError::invalid("vnode is outside the active range"));
        }
        Ok(())
    }
}

pub fn encode_request(batch: &LogicalBatch) -> Result<Vec<u8>, ModelError> {
    let expected_bytes = encoded_request_len(batch)?;
    validate_canonical_request_ceiling(expected_bytes)?;
    let expected_capacity = usize::try_from(expected_bytes)
        .map_err(|_| ModelError::invalid("canonical request length does not fit platform usize"))?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(expected_capacity)
        .map_err(|_| ModelError::invalid("canonical request allocation failed"))?;
    encode_request_into(&mut output, batch)?;
    let actual_bytes = usize_to_u64(output.len())?;
    if actual_bytes != expected_bytes {
        return Err(ModelError::invalid(format!(
            "canonical request length accounting mismatch: expected {expected_bytes}, encoded {actual_bytes}"
        )));
    }
    Ok(output)
}

pub fn encoded_request_len(batch: &LogicalBatch) -> Result<u64, ModelError> {
    let mut output = EncodedLength::default();
    encode_request_into(&mut output, batch)?;
    Ok(output.bytes)
}

fn encode_request_into<O: CanonicalOutput>(
    output: &mut O,
    batch: &LogicalBatch,
) -> Result<(), ModelError> {
    output.write_bytes(REQUEST_DOMAIN)?;
    output.write_byte(batch.kind.tag())?;
    output.write_byte(batch.scenario.tag())?;
    output.write_bytes(&batch.ordinal.to_be_bytes())?;
    output.write_bytes(&batch.logical_rows.to_be_bytes())?;
    output.write_bytes(&batch.limits.request_bytes_max_u64.to_be_bytes())?;
    output.write_bytes(&batch.limits.read_rows_max_u64.to_be_bytes())?;
    output.write_bytes(&batch.limits.read_bytes_max_u64.to_be_bytes())?;
    output.write_bytes(&batch.limits.mutation_bytes_max_u64.to_be_bytes())?;

    encode_count(output, batch.point_reads.len(), "point read")?;
    for key in &batch.point_reads {
        encode_logical_key(output, key)?;
    }
    encode_count(output, batch.ranges.len(), "range read")?;
    for range in &batch.ranges {
        encode_range(output, range)?;
    }
    encode_count(output, batch.mutations.len(), "mutation")?;
    for mutation in &batch.mutations {
        match mutation {
            Mutation::Put { key, value } => {
                output.write_byte(0x01)?;
                encode_logical_key(output, key)?;
                encode_bytes(output, value)?;
            }
            Mutation::Delete { key } => {
                output.write_byte(0x02)?;
                encode_logical_key(output, key)?;
            }
        }
    }
    Ok(())
}

pub fn encode_observation(observation: &Observation) -> Result<Vec<u8>, ModelError> {
    let mut output = Vec::new();
    output.extend_from_slice(OBSERVATION_DOMAIN);
    output.push(observation.kind.tag());
    output.push(observation.scenario.tag());
    output.extend_from_slice(&observation.ordinal.to_be_bytes());

    encode_count(&mut output, observation.point_results.len(), "point result")?;
    for result in &observation.point_results {
        encode_logical_key(&mut output, &result.key)?;
        match &result.value {
            Some(value) => {
                output.push(0x01);
                encode_bytes(&mut output, value)?;
            }
            None => output.push(0x00),
        }
    }
    encode_count(&mut output, observation.range_results.len(), "range result")?;
    for result in &observation.range_results {
        encode_range(&mut output, &result.request)?;
        encode_count(&mut output, result.rows.len(), "range row")?;
        for row in &result.rows {
            encode_logical_key(&mut output, &row.key)?;
            encode_bytes(&mut output, &row.value)?;
        }
        output.push(u8::from(result.has_more));
    }
    Ok(output)
}

pub fn digest_records(records: &BTreeMap<LogicalKey, Vec<u8>>) -> Result<[u8; 32], ModelError> {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    hasher.update(usize_to_u64(records.len())?.to_be_bytes());
    for (key, value) in records {
        let mut record = Vec::new();
        encode_logical_key(&mut record, key)?;
        encode_bytes(&mut record, value)?;
        hasher.update(record);
    }
    Ok(hasher.finalize().into())
}

fn validate_request_byte_limits(
    encoded_bytes: u64,
    declared_max_bytes: u64,
) -> Result<(), ModelError> {
    if declared_max_bytes > MAX_MODEL_CANONICAL_BYTES {
        return Err(ModelError::invalid(format!(
            "declared canonical request byte limit {declared_max_bytes} exceeds model maximum {MAX_MODEL_CANONICAL_BYTES}"
        )));
    }
    validate_canonical_request_ceiling(encoded_bytes)?;
    if encoded_bytes > declared_max_bytes {
        return Err(ModelError::invalid("canonical request byte limit exceeded"));
    }
    Ok(())
}

fn validate_canonical_request_ceiling(encoded_bytes: u64) -> Result<(), ModelError> {
    if encoded_bytes > MAX_MODEL_CANONICAL_BYTES {
        return Err(ModelError::invalid(format!(
            "canonical request is {encoded_bytes} bytes; model maximum is {MAX_MODEL_CANONICAL_BYTES}"
        )));
    }
    Ok(())
}

trait CanonicalOutput {
    fn write_byte(&mut self, value: u8) -> Result<(), ModelError>;
    fn write_bytes(&mut self, value: &[u8]) -> Result<(), ModelError>;
}

impl CanonicalOutput for Vec<u8> {
    fn write_byte(&mut self, value: u8) -> Result<(), ModelError> {
        self.push(value);
        Ok(())
    }

    fn write_bytes(&mut self, value: &[u8]) -> Result<(), ModelError> {
        self.extend_from_slice(value);
        Ok(())
    }
}

#[derive(Default)]
struct EncodedLength {
    bytes: u64,
}

impl CanonicalOutput for EncodedLength {
    fn write_byte(&mut self, _value: u8) -> Result<(), ModelError> {
        self.bytes = checked_add(self.bytes, 1, "canonical request length")?;
        Ok(())
    }

    fn write_bytes(&mut self, value: &[u8]) -> Result<(), ModelError> {
        self.bytes = checked_add(
            self.bytes,
            usize_to_u64(value.len())?,
            "canonical request length",
        )?;
        Ok(())
    }
}

fn encode_range<O: CanonicalOutput>(output: &mut O, range: &RangeRead) -> Result<(), ModelError> {
    output.write_byte(range.table.tag())?;
    output.write_bytes(&range.vnode.to_be_bytes())?;
    encode_bytes(output, &range.start_inclusive)?;
    encode_bytes(output, &range.end_exclusive)?;
    output.write_bytes(&range.max_rows.to_be_bytes())?;
    output.write_bytes(&range.max_bytes.to_be_bytes())?;
    Ok(())
}

fn encode_logical_key<O: CanonicalOutput>(
    output: &mut O,
    key: &LogicalKey,
) -> Result<(), ModelError> {
    output.write_byte(key.table.tag())?;
    output.write_bytes(&key.vnode.to_be_bytes())?;
    encode_bytes(output, &key.key)
}

fn encode_bytes<O: CanonicalOutput>(output: &mut O, value: &[u8]) -> Result<(), ModelError> {
    let length = u32::try_from(value.len())
        .map_err(|_| ModelError::invalid("byte payload length exceeds u32"))?;
    output.write_bytes(&length.to_be_bytes())?;
    output.write_bytes(value)
}

fn encode_count<O: CanonicalOutput>(
    output: &mut O,
    count: usize,
    label: &str,
) -> Result<(), ModelError> {
    let count = u32::try_from(count)
        .map_err(|_| ModelError::invalid(format!("{label} count exceeds u32")))?;
    output.write_bytes(&count.to_be_bytes())
}

fn ensure_strictly_increasing<T: Ord>(values: &[T], label: &str) -> Result<(), ModelError> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ModelError::invalid(format!(
            "{label} must be strictly increasing and unique"
        )));
    }
    Ok(())
}

fn charge_read_limits(
    limits: BatchLimits,
    rows: &mut u64,
    bytes: &mut u64,
    charge: u64,
) -> Result<(), ModelError> {
    let next_rows = checked_add(*rows, 1, "returned read rows")?;
    let next_bytes = checked_add(*bytes, charge, "returned read bytes")?;
    if next_rows > limits.read_rows_max_u64 || next_bytes > limits.read_bytes_max_u64 {
        return Err(ModelError::invalid("returned read limit exceeded"));
    }
    *rows = next_rows;
    *bytes = next_bytes;
    Ok(())
}

fn canonical_record_charge(key: &LogicalKey, value: &[u8]) -> Result<u64, ModelError> {
    let fixed = 1_u64 + 4 + 4 + 4;
    let with_key = checked_add(fixed, usize_to_u64(key.key.len())?, "record charge")?;
    checked_add(with_key, usize_to_u64(value.len())?, "record charge")
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64, ModelError> {
    left.checked_add(right)
        .ok_or_else(|| ModelError::invalid(format!("{label} overflow")))
}

fn usize_to_u64(value: usize) -> Result<u64, ModelError> {
    u64::try_from(value).map_err(|_| ModelError::invalid("usize does not fit u64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(table: Table, vnode: u32, bytes: &[u8]) -> LogicalKey {
        LogicalKey {
            table,
            vnode,
            key: bytes.to_vec(),
        }
    }

    fn limits() -> BatchLimits {
        BatchLimits {
            request_bytes_max_u64: 16_384,
            read_rows_max_u64: 64,
            read_bytes_max_u64: 16_384,
            mutation_bytes_max_u64: 16_384,
        }
    }

    fn batch(mutations: Vec<Mutation>) -> LogicalBatch {
        LogicalBatch {
            kind: BatchKind::Measured,
            scenario: Scenario::Aggregate,
            ordinal: 0,
            logical_rows: 1,
            limits: limits(),
            point_reads: Vec::new(),
            ranges: Vec::new(),
            mutations,
        }
    }

    fn fault(phase: FaultPhase, occurrence: u64) -> FaultInjector {
        FaultInjector::armed(FaultOrdinal { phase, occurrence })
    }

    fn injected(phase: FaultPhase, occurrence: u64) -> ModelError {
        ModelError::InjectedFault {
            ordinal: FaultOrdinal { phase, occurrence },
        }
    }

    fn ambiguous(phase: FaultPhase, occurrence: u64) -> ModelError {
        ModelError::AmbiguousAfterSuccess {
            ordinal: FaultOrdinal { phase, occurrence },
        }
    }

    fn not_reached(phase: FaultPhase, occurrence: u64, visits: u64) -> ModelError {
        ModelError::FaultTargetNotReached {
            target: FaultOrdinal { phase, occurrence },
            visits,
        }
    }

    fn generous_restore_budget() -> RestoreBudget {
        RestoreBudget {
            records_max_u64: 64,
            key_bytes_max_u64: 4_096,
            value_bytes_max_u64: 4_096,
            canonical_bytes_max_u64: 16_384,
        }
    }

    #[test]
    fn fault_phase_names_match_the_v1_wire_vocabulary() {
        let phases = [
            FaultPhase::BatchBeforeCommit,
            FaultPhase::BatchAfterCommitBeforeAck,
            FaultPhase::PersistBefore,
            FaultPhase::PersistAfterSuccessBeforeAck,
            FaultPhase::SnapshotOpen,
            FaultPhase::ExportRecord,
            FaultPhase::RestoreRecord,
            FaultPhase::CleanupRecord,
        ];
        assert_eq!(
            serde_json::to_value(phases).unwrap(),
            serde_json::json!([
                "batch_before_commit",
                "batch_after_commit_before_ack",
                "persist_before",
                "persist_after_success_before_ack",
                "snapshot_open",
                "export_record",
                "restore_record",
                "cleanup_record"
            ])
        );
    }

    #[test]
    fn empty_key_and_value_are_distinct_from_missing() {
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let empty = key(Table::AggregateState, 0, b"");
        model
            .execute(&batch(vec![Mutation::Put {
                key: empty.clone(),
                value: Vec::new(),
            }]))
            .unwrap();
        let mut read = batch(Vec::new());
        read.point_reads = vec![empty.clone(), key(Table::AggregateState, 0, b"x")];
        let observed = model.execute(&read).unwrap();
        assert_eq!(observed.point_results[0].value, Some(Vec::new()));
        assert_eq!(observed.point_results[1].value, None);
    }

    #[test]
    fn batch_fault_ordinals_produce_only_complete_pre_or_post_cuts() {
        let deleted = key(Table::AggregateState, 0, b"a");
        let first_put = key(Table::AggregateState, 0, b"b");
        let second_put = key(Table::AggregateState, 0, b"c");
        let retained = key(Table::AggregateState, 0, b"d");
        let mut baseline = ReferenceModel::new(4, 64, 64).unwrap();
        baseline
            .execute(&batch(vec![
                Mutation::Put {
                    key: deleted.clone(),
                    value: b"old".to_vec(),
                },
                Mutation::Put {
                    key: retained,
                    value: b"keep".to_vec(),
                },
            ]))
            .unwrap();
        let change = batch(vec![
            Mutation::Delete {
                key: deleted.clone(),
            },
            Mutation::Put {
                key: first_put,
                value: b"first".to_vec(),
            },
            Mutation::Put {
                key: second_put,
                value: b"second".to_vec(),
            },
        ]);
        let pre_records = baseline.live_records().clone();
        let pre_digest = baseline.live_digest().unwrap();
        let mut expected_post = baseline.clone();
        expected_post.execute(&change).unwrap();
        let post_records = expected_post.live_records().clone();
        let post_digest = expected_post.live_digest().unwrap();

        let mut model = baseline.clone();
        let mut before = fault(FaultPhase::BatchBeforeCommit, 0);
        assert_eq!(
            model.execute_with_fault(&change, &mut before),
            Err(injected(FaultPhase::BatchBeforeCommit, 0))
        );
        assert_eq!(model.live_digest().unwrap(), pre_digest);
        assert_eq!(model.live_records(), &pre_records);
        assert!(before.fired());
        assert_eq!(before.visits(FaultPhase::BatchBeforeCommit), 1);
        assert_eq!(before.verify_reached(), Ok(()));

        let mut model = baseline;
        let mut after = fault(FaultPhase::BatchAfterCommitBeforeAck, 0);
        assert_eq!(
            model.execute_with_fault(&change, &mut after),
            Err(ambiguous(FaultPhase::BatchAfterCommitBeforeAck, 0))
        );
        assert_eq!(model.live_digest().unwrap(), post_digest);
        assert_eq!(model.live_records(), &post_records);
        assert!(after.fired());
        assert_eq!(after.visits(FaultPhase::BatchAfterCommitBeforeAck), 1);
        assert_eq!(after.verify_reached(), Ok(()));
    }

    #[test]
    fn range_is_half_open_and_reports_more_at_exact_row_cap() {
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let records = [b"a".as_slice(), b"b", b"c"]
            .into_iter()
            .map(|bytes| Mutation::Put {
                key: key(Table::TimerIndex, 1, bytes),
                value: vec![1],
            })
            .collect();
        model.execute(&batch(records)).unwrap();
        let mut scan = batch(Vec::new());
        scan.ranges = vec![RangeRead {
            table: Table::TimerIndex,
            vnode: 1,
            start_inclusive: b"a".to_vec(),
            end_exclusive: b"c".to_vec(),
            max_rows: 1,
            max_bytes: 10,
        }];
        let result = model.execute(&scan).unwrap();
        assert_eq!(result.range_results[0].rows.len(), 1);
        assert!(result.range_results[0].has_more);
        assert_eq!(result.range_results[0].rows[0].key.key, b"a");
    }

    #[test]
    fn oversized_first_range_row_is_an_error_and_does_not_mutate() {
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let record = key(Table::TimerIndex, 1, b"a");
        model
            .execute(&batch(vec![Mutation::Put {
                key: record.clone(),
                value: vec![7; 8],
            }]))
            .unwrap();
        let pre = model.live_digest().unwrap();
        let mut scan = batch(vec![Mutation::Delete {
            key: record.clone(),
        }]);
        scan.ranges = vec![RangeRead {
            table: Table::TimerIndex,
            vnode: 1,
            start_inclusive: b"a".to_vec(),
            end_exclusive: b"b".to_vec(),
            max_rows: 1,
            max_bytes: 8,
        }];
        let mut faults = fault(FaultPhase::BatchBeforeCommit, 0);
        assert_eq!(
            model.execute_with_fault(&scan, &mut faults),
            Err(ModelError::RowTooLarge { required_bytes: 9 })
        );
        assert_eq!(model.live_digest().unwrap(), pre);
        assert_eq!(faults.visits(FaultPhase::BatchBeforeCommit), 0);
        assert_eq!(faults.visits(FaultPhase::BatchAfterCommitBeforeAck), 0);
        assert_eq!(
            faults.verify_reached(),
            Err(not_reached(FaultPhase::BatchBeforeCommit, 0, 0))
        );
    }

    #[test]
    fn invalid_late_mutation_is_validated_before_any_change() {
        let mut model = ReferenceModel::new(4, 4, 4).unwrap();
        let first = key(Table::AggregateState, 0, b"a");
        let invalid = key(Table::AggregateState, 0, b"zzzzz");
        let request = batch(vec![
            Mutation::Put {
                key: first,
                value: vec![1],
            },
            Mutation::Put {
                key: invalid,
                value: vec![2],
            },
        ]);
        let pre = model.live_digest().unwrap();
        let mut faults = fault(FaultPhase::BatchBeforeCommit, 0);
        assert!(model.execute_with_fault(&request, &mut faults).is_err());
        assert_eq!(model.live_digest().unwrap(), pre);
        assert_eq!(faults.visits(FaultPhase::BatchBeforeCommit), 0);
        assert_eq!(faults.visits(FaultPhase::BatchAfterCommitBeforeAck), 0);
        assert_eq!(
            faults.verify_reached(),
            Err(not_reached(FaultPhase::BatchBeforeCommit, 0, 0))
        );
    }

    #[test]
    fn snapshot_restore_drop_and_reopen_have_explicit_cuts() {
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let zero = key(Table::AggregateState, 0, b"a");
        let one = key(Table::AggregateState, 1, b"b");
        model
            .execute(&batch(vec![
                Mutation::Put {
                    key: zero.clone(),
                    value: vec![1],
                },
                Mutation::Put {
                    key: one.clone(),
                    value: vec![2],
                },
            ]))
            .unwrap();
        model.persist();
        let snapshot = model.snapshot();
        model.drop_vnode(0).unwrap();
        assert!(model.live_records().get(&zero).is_none());
        assert!(snapshot.records().contains_key(&zero));
        model.crash_reopen();
        assert!(model.live_records().contains_key(&zero));

        let replacement = vec![(zero.clone(), vec![9])];
        model
            .restore_vnode(
                0,
                &replacement,
                RestoreBudget {
                    records_max_u64: 1,
                    key_bytes_max_u64: 1,
                    value_bytes_max_u64: 1,
                    canonical_bytes_max_u64: 15,
                },
            )
            .unwrap();
        assert_eq!(model.live_records().get(&zero), Some(&vec![9]));
        assert_eq!(model.live_records().get(&one), Some(&vec![2]));
    }

    #[test]
    fn persist_fault_ordinals_preserve_complete_durable_cuts() {
        let first = key(Table::AggregateState, 0, b"a");
        let second = key(Table::AggregateState, 0, b"b");
        let first_write = batch(vec![Mutation::Put {
            key: first.clone(),
            value: vec![1],
        }]);
        let second_write = batch(vec![Mutation::Put {
            key: second.clone(),
            value: vec![2],
        }]);
        let third = key(Table::AggregateState, 0, b"c");
        let third_write = batch(vec![Mutation::Put {
            key: third.clone(),
            value: vec![3],
        }]);

        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let mut before = fault(FaultPhase::PersistBefore, 1);
        model.execute(&first_write).unwrap();
        model.persist_with_fault(&mut before).unwrap();
        let first_durable = model.durable_digest().unwrap();
        model.execute(&second_write).unwrap();
        assert_eq!(
            model.persist_with_fault(&mut before),
            Err(injected(FaultPhase::PersistBefore, 1))
        );
        assert_eq!(model.durable_digest().unwrap(), first_durable);
        assert_eq!(before.verify_reached(), Ok(()));
        model.crash_reopen();
        assert!(model.live_records().contains_key(&first));
        assert!(!model.live_records().contains_key(&second));

        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let mut after = fault(FaultPhase::PersistAfterSuccessBeforeAck, 1);
        model.execute(&first_write).unwrap();
        model.persist_with_fault(&mut after).unwrap();
        model.execute(&second_write).unwrap();
        assert_eq!(
            model.persist_with_fault(&mut after),
            Err(ambiguous(FaultPhase::PersistAfterSuccessBeforeAck, 1))
        );
        assert!(model.durable_records().contains_key(&first));
        assert!(model.durable_records().contains_key(&second));
        assert_eq!(after.verify_reached(), Ok(()));
        model.execute(&third_write).unwrap();
        assert!(model.live_records().contains_key(&third));
        model.crash_reopen();
        assert!(model.live_records().contains_key(&second));
        assert!(!model.live_records().contains_key(&third));
    }

    #[test]
    fn snapshot_retry_captures_mutation_made_after_failed_open() {
        let first = key(Table::WindowState, 0, b"a");
        let second = key(Table::WindowState, 0, b"b");
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        model
            .execute(&batch(vec![Mutation::Put {
                key: first.clone(),
                value: vec![1],
            }]))
            .unwrap();

        let mut faults = fault(FaultPhase::SnapshotOpen, 0);
        assert_eq!(
            model.snapshot_with_fault(&mut faults),
            Err(injected(FaultPhase::SnapshotOpen, 0))
        );
        assert!(faults.fired());
        assert_eq!(faults.visits(FaultPhase::SnapshotOpen), 1);
        assert_eq!(faults.visits(FaultPhase::ExportRecord), 0);
        assert_eq!(faults.verify_reached(), Ok(()));

        model
            .execute(&batch(vec![Mutation::Put {
                key: second.clone(),
                value: vec![2],
            }]))
            .unwrap();
        let retry_cut = model.snapshot_with_fault(&mut faults).unwrap();
        assert_eq!(faults.visits(FaultPhase::SnapshotOpen), 2);
        assert!(retry_cut.records().contains_key(&first));
        assert!(retry_cut.records().contains_key(&second));
    }

    #[test]
    fn export_fault_publishes_nothing_and_retries_from_an_immutable_snapshot() {
        let vnode_records = [b"a".as_slice(), b"b", b"c"];
        let mutations = vnode_records
            .iter()
            .enumerate()
            .map(|(index, bytes)| Mutation::Put {
                key: key(Table::JoinLeftRows, 0, bytes),
                value: vec![u8::try_from(index).unwrap()],
            })
            .collect();
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        model.execute(&batch(mutations)).unwrap();
        let snapshot = model.snapshot();

        model.drop_vnode(0).unwrap();
        let mut faults = fault(FaultPhase::ExportRecord, 1);
        assert_eq!(
            snapshot.export_vnode_with_fault(0, &mut faults),
            Err(injected(FaultPhase::ExportRecord, 1))
        );

        let exported = snapshot.export_vnode_with_fault(0, &mut faults).unwrap();
        assert_eq!(exported.len(), 3);
        assert_eq!(
            exported
                .iter()
                .map(|(key, _)| key.key.as_slice())
                .collect::<Vec<_>>(),
            vnode_records
        );
        assert!(model.live_records().is_empty());
    }

    #[test]
    fn restore_fault_leaves_active_vnode_unchanged_until_full_replacement() {
        let old = key(Table::JoinRightRows, 0, b"old");
        let other = key(Table::JoinRightRows, 1, b"keep");
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        model
            .execute(&batch(vec![
                Mutation::Put {
                    key: old.clone(),
                    value: vec![9],
                },
                Mutation::Put {
                    key: other.clone(),
                    value: vec![8],
                },
            ]))
            .unwrap();
        let before = model.live_records().clone();
        let replacement = [b"a".as_slice(), b"b", b"c"]
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| {
                (
                    key(Table::JoinRightRows, 0, bytes),
                    vec![u8::try_from(index).unwrap()],
                )
            })
            .collect::<Vec<_>>();

        let mut faults = fault(FaultPhase::RestoreRecord, 1);
        assert_eq!(
            model
                .restore_vnode_with_fault(0, &replacement, generous_restore_budget(), &mut faults,),
            Err(injected(FaultPhase::RestoreRecord, 1))
        );
        assert_eq!(model.live_records(), &before);

        model
            .restore_vnode_with_fault(0, &replacement, generous_restore_budget(), &mut faults)
            .unwrap();
        assert!(!model.live_records().contains_key(&old));
        assert!(model.live_records().contains_key(&other));
        for (key, value) in replacement {
            assert_eq!(model.live_records().get(&key), Some(&value));
        }
    }

    #[test]
    fn cleanup_fault_removes_only_a_canonical_prefix_and_retry_is_idempotent() {
        let removed = [b"a".as_slice(), b"b", b"c"]
            .into_iter()
            .map(|bytes| key(Table::OutputBookkeeping, 0, bytes))
            .collect::<Vec<_>>();
        let retained = key(Table::OutputBookkeeping, 1, b"keep");
        let mut mutations = removed
            .iter()
            .enumerate()
            .map(|(index, key)| Mutation::Put {
                key: key.clone(),
                value: vec![u8::try_from(index).unwrap()],
            })
            .collect::<Vec<_>>();
        mutations.push(Mutation::Put {
            key: retained.clone(),
            value: vec![9],
        });
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        model.execute(&batch(mutations)).unwrap();

        let mut faults = fault(FaultPhase::CleanupRecord, 1);
        assert_eq!(
            model.drop_vnode_with_fault(0, &mut faults),
            Err(injected(FaultPhase::CleanupRecord, 1))
        );
        assert!(!model.live_records().contains_key(&removed[0]));
        assert!(model.live_records().contains_key(&removed[1]));
        assert!(model.live_records().contains_key(&removed[2]));
        assert!(model.live_records().contains_key(&retained));

        model.drop_vnode_with_fault(0, &mut faults).unwrap();
        model.drop_vnode_with_fault(0, &mut faults).unwrap();
        assert!(removed
            .iter()
            .all(|key| !model.live_records().contains_key(key)));
        assert!(model.live_records().contains_key(&retained));
    }

    #[test]
    fn invalid_restore_is_fully_rejected_before_record_hooks_or_replacement() {
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let old = key(Table::AggregateState, 0, b"old");
        model
            .execute(&batch(vec![Mutation::Put {
                key: old.clone(),
                value: vec![1],
            }]))
            .unwrap();
        let records = vec![
            (key(Table::AggregateState, 0, b"z"), vec![2]),
            (key(Table::AggregateState, 0, b"a"), vec![3]),
        ];
        let mut unsorted_faults = fault(FaultPhase::RestoreRecord, 0);
        assert!(model
            .restore_vnode_with_fault(0, &records, generous_restore_budget(), &mut unsorted_faults,)
            .is_err());
        assert_eq!(unsorted_faults.visits(FaultPhase::RestoreRecord), 0);
        assert_eq!(
            unsorted_faults.verify_reached(),
            Err(not_reached(FaultPhase::RestoreRecord, 0, 0))
        );

        let sorted = vec![
            (key(Table::AggregateState, 0, b"a"), vec![3]),
            (key(Table::AggregateState, 0, b"z"), vec![2]),
        ];
        let mut one_record = generous_restore_budget();
        one_record.records_max_u64 = 1;
        let mut budget_faults = fault(FaultPhase::RestoreRecord, 0);
        assert!(model
            .restore_vnode_with_fault(0, &sorted, one_record, &mut budget_faults)
            .is_err());
        assert_eq!(budget_faults.visits(FaultPhase::RestoreRecord), 0);
        assert_eq!(
            budget_faults.verify_reached(),
            Err(not_reached(FaultPhase::RestoreRecord, 0, 0))
        );
        assert_eq!(model.live_records().get(&old), Some(&vec![1]));
    }

    #[test]
    fn empty_lifecycle_operations_consume_no_record_fault_ordinals() {
        let snapshot = ReferenceModel::new(2, 64, 64).unwrap().snapshot();
        let mut export_faults = fault(FaultPhase::ExportRecord, 0);
        assert_eq!(
            snapshot.export_vnode_with_fault(0, &mut export_faults),
            Ok(Vec::new())
        );
        assert_eq!(export_faults.visits(FaultPhase::ExportRecord), 0);
        assert_eq!(
            export_faults.verify_reached(),
            Err(not_reached(FaultPhase::ExportRecord, 0, 0))
        );

        let mut model = ReferenceModel::new(2, 64, 64).unwrap();
        let empty_digest = model.live_digest().unwrap();
        let mut restore_faults = fault(FaultPhase::RestoreRecord, 0);
        assert_eq!(
            model.restore_vnode_with_fault(0, &[], generous_restore_budget(), &mut restore_faults,),
            Ok(())
        );
        assert_eq!(restore_faults.visits(FaultPhase::RestoreRecord), 0);
        assert_eq!(
            restore_faults.verify_reached(),
            Err(not_reached(FaultPhase::RestoreRecord, 0, 0))
        );
        assert_eq!(model.live_digest().unwrap(), empty_digest);

        let mut cleanup_faults = fault(FaultPhase::CleanupRecord, 0);
        assert_eq!(model.drop_vnode_with_fault(0, &mut cleanup_faults), Ok(()));
        assert_eq!(cleanup_faults.visits(FaultPhase::CleanupRecord), 0);
        assert_eq!(
            cleanup_faults.verify_reached(),
            Err(not_reached(FaultPhase::CleanupRecord, 0, 0))
        );
        assert_eq!(model.live_digest().unwrap(), empty_digest);
    }

    #[test]
    fn fault_counters_are_independent_across_mixed_phases() {
        let seed = [b"a".as_slice(), b"b", b"c", b"d"]
            .into_iter()
            .map(|bytes| Mutation::Put {
                key: key(Table::AggregateState, 0, bytes),
                value: vec![1],
            })
            .collect();
        let mut model = ReferenceModel::new(2, 64, 64).unwrap();
        let mut faults = fault(FaultPhase::CleanupRecord, 99);
        model.execute_with_fault(&batch(seed), &mut faults).unwrap();
        model.persist_with_fault(&mut faults).unwrap();
        model.persist_with_fault(&mut faults).unwrap();
        model.snapshot_with_fault(&mut faults).unwrap();
        model.snapshot_with_fault(&mut faults).unwrap();
        let snapshot = model.snapshot_with_fault(&mut faults).unwrap();
        assert_eq!(
            snapshot
                .export_vnode_with_fault(0, &mut faults)
                .unwrap()
                .len(),
            4
        );

        let replacement = [b"a".as_slice(), b"b", b"c", b"d", b"e"]
            .into_iter()
            .map(|bytes| (key(Table::JoinLeftRows, 0, bytes), vec![2]))
            .collect::<Vec<_>>();
        model
            .restore_vnode_with_fault(0, &replacement, generous_restore_budget(), &mut faults)
            .unwrap();
        model
            .execute(&batch(vec![Mutation::Put {
                key: key(Table::JoinLeftRows, 0, b"f"),
                value: vec![2],
            }]))
            .unwrap();
        model.drop_vnode_with_fault(0, &mut faults).unwrap();

        assert_eq!(faults.visits(FaultPhase::BatchBeforeCommit), 1);
        assert_eq!(faults.visits(FaultPhase::BatchAfterCommitBeforeAck), 1);
        assert_eq!(faults.visits(FaultPhase::PersistBefore), 2);
        assert_eq!(faults.visits(FaultPhase::PersistAfterSuccessBeforeAck), 2);
        assert_eq!(faults.visits(FaultPhase::SnapshotOpen), 3);
        assert_eq!(faults.visits(FaultPhase::ExportRecord), 4);
        assert_eq!(faults.visits(FaultPhase::RestoreRecord), 5);
        assert_eq!(faults.visits(FaultPhase::CleanupRecord), 6);
        assert_eq!(
            faults.verify_reached(),
            Err(not_reached(FaultPhase::CleanupRecord, 99, 6))
        );
    }

    #[test]
    fn request_collections_reject_out_of_order_and_duplicate_entries() {
        let a = key(Table::AggregateState, 0, b"a");
        let b = key(Table::AggregateState, 0, b"b");
        let range_a = RangeRead {
            table: Table::AggregateState,
            vnode: 0,
            start_inclusive: b"a".to_vec(),
            end_exclusive: b"b".to_vec(),
            max_rows: 1,
            max_bytes: 64,
        };
        let range_b = RangeRead {
            start_inclusive: b"b".to_vec(),
            end_exclusive: b"c".to_vec(),
            ..range_a.clone()
        };

        let mut point_unsorted = batch(Vec::new());
        point_unsorted.point_reads = vec![b.clone(), a.clone()];
        let mut point_duplicate = batch(Vec::new());
        point_duplicate.point_reads = vec![a.clone(), a.clone()];
        let mut range_unsorted = batch(Vec::new());
        range_unsorted.ranges = vec![range_b.clone(), range_a.clone()];
        let mut range_duplicate = batch(Vec::new());
        range_duplicate.ranges = vec![range_a.clone(), range_a];
        let mutation_unsorted = batch(vec![
            Mutation::Put {
                key: b,
                value: vec![2],
            },
            Mutation::Put {
                key: a.clone(),
                value: vec![1],
            },
        ]);
        let mutation_duplicate = batch(vec![
            Mutation::Put {
                key: a.clone(),
                value: vec![1],
            },
            Mutation::Delete { key: a },
        ]);
        let cases = [
            (
                "point reads out of order",
                point_unsorted,
                "point reads must be strictly increasing and unique",
            ),
            (
                "duplicate point read",
                point_duplicate,
                "point reads must be strictly increasing and unique",
            ),
            (
                "range reads out of order",
                range_unsorted,
                "range reads must be strictly increasing and unique",
            ),
            (
                "duplicate range read",
                range_duplicate,
                "range reads must be strictly increasing and unique",
            ),
            (
                "mutations out of order",
                mutation_unsorted,
                "mutations must be strictly increasing and unique by key",
            ),
            (
                "duplicate mutation key",
                mutation_duplicate,
                "mutations must be strictly increasing and unique by key",
            ),
        ];

        let mut model = ReferenceModel::new(1, 64, 64).unwrap();
        let empty_digest = model.live_digest().unwrap();
        for (name, request, message) in cases {
            assert_eq!(
                model.execute(&request),
                Err(ModelError::Invalid(message.to_owned())),
                "{name}"
            );
            assert_eq!(model.live_digest().unwrap(), empty_digest, "{name}");
        }
    }

    #[test]
    fn range_scan_accepts_exact_bytes_and_stops_before_max_plus_one() {
        let mut model = ReferenceModel::new(1, 64, 64).unwrap();
        model
            .execute(&batch(vec![
                Mutation::Put {
                    key: key(Table::TimerIndex, 0, b"a"),
                    value: vec![1, 2],
                },
                Mutation::Put {
                    key: key(Table::TimerIndex, 0, b"b"),
                    value: vec![3, 4, 5],
                },
            ]))
            .unwrap();
        let scan = |max_bytes| {
            let mut request = batch(Vec::new());
            request.ranges = vec![RangeRead {
                table: Table::TimerIndex,
                vnode: 0,
                start_inclusive: b"a".to_vec(),
                end_exclusive: b"c".to_vec(),
                max_rows: 2,
                max_bytes,
            }];
            request
        };

        let capped = model.execute(&scan(6)).unwrap();
        assert_eq!(capped.range_results[0].rows.len(), 1);
        assert_eq!(capped.range_results[0].rows[0].key.key, b"a");
        assert!(capped.range_results[0].has_more);

        let exact = model.execute(&scan(7)).unwrap();
        assert_eq!(exact.range_results[0].rows.len(), 2);
        assert!(!exact.range_results[0].has_more);
    }

    #[test]
    fn restore_budget_dimensions_accept_exact_and_reject_actual_plus_one() {
        #[derive(Clone, Copy)]
        enum Dimension {
            Records,
            KeyBytes,
            ValueBytes,
            CanonicalBytes,
        }

        let old = key(Table::AggregateState, 0, b"old");
        let retained = key(Table::AggregateState, 1, b"keep");
        let mut baseline = ReferenceModel::new(2, 4, 3).unwrap();
        baseline
            .execute(&batch(vec![
                Mutation::Put {
                    key: old.clone(),
                    value: vec![9],
                },
                Mutation::Put {
                    key: retained.clone(),
                    value: vec![8],
                },
            ]))
            .unwrap();
        let replacement = vec![
            (key(Table::AggregateState, 0, b"a"), vec![1, 2]),
            (key(Table::AggregateState, 0, b"bb"), vec![3, 4, 5]),
        ];
        let budget = |dimension, maximum| {
            let mut budget = RestoreBudget {
                records_max_u64: u64::MAX,
                key_bytes_max_u64: u64::MAX,
                value_bytes_max_u64: u64::MAX,
                canonical_bytes_max_u64: u64::MAX,
            };
            match dimension {
                Dimension::Records => budget.records_max_u64 = maximum,
                Dimension::KeyBytes => budget.key_bytes_max_u64 = maximum,
                Dimension::ValueBytes => budget.value_bytes_max_u64 = maximum,
                Dimension::CanonicalBytes => budget.canonical_bytes_max_u64 = maximum,
            }
            budget
        };

        for (name, dimension, exact) in [
            ("records", Dimension::Records, 2),
            ("key bytes", Dimension::KeyBytes, 3),
            ("value bytes", Dimension::ValueBytes, 5),
            ("canonical bytes", Dimension::CanonicalBytes, 34),
        ] {
            let mut accepted = baseline.clone();
            assert_eq!(
                accepted.restore_vnode(0, &replacement, budget(dimension, exact)),
                Ok(()),
                "exact {name} budget"
            );
            assert!(!accepted.live_records().contains_key(&old));
            assert!(accepted.live_records().contains_key(&retained));
            assert_eq!(
                accepted
                    .live_records()
                    .iter()
                    .filter(|(key, _)| key.vnode == 0)
                    .count(),
                2
            );

            let mut rejected = baseline.clone();
            let before_records = rejected.live_records().clone();
            let before_digest = rejected.live_digest().unwrap();
            assert_eq!(
                rejected.restore_vnode(0, &replacement, budget(dimension, exact - 1)),
                Err(ModelError::Invalid("restore budget exceeded".to_owned())),
                "{name} budget plus one"
            );
            assert_eq!(rejected.live_records(), &before_records, "{name}");
            assert_eq!(rejected.live_digest().unwrap(), before_digest, "{name}");
        }
    }

    #[test]
    fn invalid_restore_records_preserve_the_full_live_state() {
        let old = key(Table::AggregateState, 0, b"old");
        let retained = key(Table::AggregateState, 1, b"keep");
        let mut baseline = ReferenceModel::new(2, 4, 3).unwrap();
        baseline
            .execute(&batch(vec![
                Mutation::Put {
                    key: old,
                    value: vec![9],
                },
                Mutation::Put {
                    key: retained,
                    value: vec![8],
                },
            ]))
            .unwrap();
        let duplicate = key(Table::AggregateState, 0, b"a");
        let cases = [
            (
                "wrong vnode",
                vec![(key(Table::AggregateState, 1, b"a"), vec![1])],
                "restore record belongs to another vnode",
            ),
            (
                "duplicate key",
                vec![(duplicate.clone(), vec![1]), (duplicate, vec![2])],
                "restore records must be strictly increasing and unique",
            ),
            (
                "key width",
                vec![(key(Table::AggregateState, 0, b"wide!"), vec![1])],
                "logical key width exceeds active limit",
            ),
            (
                "value width",
                vec![(key(Table::AggregateState, 0, b"a"), vec![1; 4])],
                "value width exceeds active limit",
            ),
        ];

        for (name, records, message) in cases {
            let mut model = baseline.clone();
            let before_records = model.live_records().clone();
            let before_digest = model.live_digest().unwrap();
            assert_eq!(
                model.restore_vnode(0, &records, generous_restore_budget()),
                Err(ModelError::Invalid(message.to_owned())),
                "{name}"
            );
            assert_eq!(model.live_records(), &before_records, "{name}");
            assert_eq!(model.live_digest().unwrap(), before_digest, "{name}");
        }
    }

    #[test]
    fn lifecycle_record_order_is_canonical_across_tables() {
        let tables = [
            Table::AggregateState,
            Table::WindowState,
            Table::TimerIndex,
            Table::JoinLeftRows,
            Table::JoinRightRows,
            Table::OutputBookkeeping,
        ];
        let keys = tables.map(|table| key(table, 0, b"k"));
        let mutations = keys
            .iter()
            .map(|key| Mutation::Put {
                key: key.clone(),
                value: vec![key.table.tag()],
            })
            .collect();
        let mut model = ReferenceModel::new(1, 64, 64).unwrap();
        model.execute(&batch(mutations)).unwrap();
        let exported = model.snapshot().export_vnode(0).unwrap();
        assert_eq!(
            exported
                .iter()
                .map(|(key, _)| key.table)
                .collect::<Vec<_>>(),
            tables
        );

        let mut restored = ReferenceModel::new(1, 64, 64).unwrap();
        restored
            .restore_vnode(0, &exported, generous_restore_budget())
            .unwrap();
        assert_eq!(
            restored
                .live_records()
                .keys()
                .map(|key| key.table)
                .collect::<Vec<_>>(),
            tables
        );

        let mut faults = fault(FaultPhase::CleanupRecord, 2);
        assert_eq!(
            model.drop_vnode_with_fault(0, &mut faults),
            Err(injected(FaultPhase::CleanupRecord, 2))
        );
        assert!(keys[..2]
            .iter()
            .all(|key| !model.live_records().contains_key(key)));
        assert!(keys[2..]
            .iter()
            .all(|key| model.live_records().contains_key(key)));
        model.drop_vnode_with_fault(0, &mut faults).unwrap();
        assert!(model.live_records().is_empty());
    }

    #[test]
    fn request_encoding_binds_limits_and_is_deterministic() {
        let request = batch(vec![Mutation::Put {
            key: key(Table::AggregateState, 0, b"a"),
            value: vec![1],
        }]);
        let encoded = encode_request(&request).unwrap();
        assert!(encoded.starts_with(REQUEST_DOMAIN));
        assert_eq!(
            encoded_request_len(&request).unwrap(),
            u64::try_from(encoded.len()).unwrap()
        );
        assert_eq!(encoded, encode_request(&request).unwrap());
        let mut changed = request;
        changed.limits.read_bytes_max_u64 += 1;
        assert_ne!(encoded, encode_request(&changed).unwrap());
    }

    #[test]
    fn declared_request_limit_accepts_exact_and_rejects_one_byte_over() {
        let model = ReferenceModel::new(1, 64, 64).unwrap();
        let mut request = batch(vec![Mutation::Put {
            key: key(Table::AggregateState, 0, b"key"),
            value: b"value".to_vec(),
        }]);
        let exact_bytes = encoded_request_len(&request).unwrap();
        request.limits.request_bytes_max_u64 = exact_bytes;
        assert_eq!(encoded_request_len(&request).unwrap(), exact_bytes);
        assert_eq!(model.validate_batch(&request), Ok(()));

        request.limits.request_bytes_max_u64 = exact_bytes - 1;
        assert_eq!(encoded_request_len(&request).unwrap(), exact_bytes);
        assert_eq!(
            model.validate_batch(&request),
            Err(ModelError::Invalid(
                "canonical request byte limit exceeded".to_owned()
            ))
        );

        request.limits.request_bytes_max_u64 = MAX_MODEL_CANONICAL_BYTES + 1;
        assert_eq!(
            model.validate_batch(&request),
            Err(ModelError::Invalid(format!(
                "declared canonical request byte limit {} exceeds model maximum {MAX_MODEL_CANONICAL_BYTES}",
                MAX_MODEL_CANONICAL_BYTES + 1
            )))
        );
    }

    #[test]
    fn canonical_request_ceiling_accepts_exact_and_rejects_max_plus_one() {
        assert_eq!(
            validate_canonical_request_ceiling(MAX_MODEL_CANONICAL_BYTES),
            Ok(())
        );
        assert_eq!(
            validate_canonical_request_ceiling(MAX_MODEL_CANONICAL_BYTES + 1),
            Err(ModelError::Invalid(format!(
                "canonical request is {} bytes; model maximum is {MAX_MODEL_CANONICAL_BYTES}",
                MAX_MODEL_CANONICAL_BYTES + 1
            )))
        );
    }
}
