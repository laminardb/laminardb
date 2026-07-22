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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitFault {
    None,
    BeforeCommit,
    AfterCommitBeforeAck,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultOrdinal {
    pub phase: FaultPhase,
    pub occurrence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelError {
    Invalid(String),
    RowTooLarge { required_bytes: u64 },
    InjectedBeforeCommit,
    AmbiguousAfterCommit,
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
            Self::InjectedBeforeCommit => formatter.write_str("injected before commit"),
            Self::AmbiguousAfterCommit => {
                formatter.write_str("commit completed before acknowledgement was lost")
            }
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
    records: BTreeMap<LogicalKey, Vec<u8>>,
}

impl Snapshot {
    pub fn records(&self) -> &BTreeMap<LogicalKey, Vec<u8>> {
        &self.records
    }

    pub fn digest(&self) -> Result<[u8; 32], ModelError> {
        digest_records(&self.records)
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
        self.execute_with_fault(batch, CommitFault::None)
    }

    pub fn execute_with_fault(
        &mut self,
        batch: &LogicalBatch,
        fault: CommitFault,
    ) -> Result<Observation, ModelError> {
        self.validate_batch(batch)?;
        let observation = self.observe(batch)?;
        if fault == CommitFault::BeforeCommit {
            return Err(ModelError::InjectedBeforeCommit);
        }

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

        if fault == CommitFault::AfterCommitBeforeAck {
            return Err(ModelError::AmbiguousAfterCommit);
        }
        Ok(observation)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            records: self.live.clone(),
        }
    }

    pub fn export_vnode(&self, vnode: u32) -> Result<Vec<(LogicalKey, Vec<u8>)>, ModelError> {
        self.validate_vnode(vnode)?;
        Ok(self
            .live
            .iter()
            .filter(|(key, _)| key.vnode == vnode)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    pub fn restore_vnode(
        &mut self,
        vnode: u32,
        records: &[(LogicalKey, Vec<u8>)],
        budget: RestoreBudget,
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

        self.live.retain(|key, _| key.vnode != vnode);
        for (key, value) in records {
            self.live.insert(key.clone(), value.clone());
        }
        Ok(())
    }

    pub fn drop_vnode(&mut self, vnode: u32) -> Result<(), ModelError> {
        self.validate_vnode(vnode)?;
        self.live.retain(|key, _| key.vnode != vnode);
        Ok(())
    }

    pub fn persist(&mut self) {
        self.durable.clone_from(&self.live);
    }

    pub fn persist_with_fault(&mut self, fault: CommitFault) -> Result<(), ModelError> {
        if fault == CommitFault::BeforeCommit {
            return Err(ModelError::InjectedBeforeCommit);
        }
        self.persist();
        if fault == CommitFault::AfterCommitBeforeAck {
            return Err(ModelError::AmbiguousAfterCommit);
        }
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
    fn before_and_after_commit_faults_are_only_complete_cuts() {
        let mut model = ReferenceModel::new(4, 64, 64).unwrap();
        let record = key(Table::AggregateState, 0, b"a");
        let write = batch(vec![Mutation::Put {
            key: record.clone(),
            value: b"value".to_vec(),
        }]);
        let pre = model.live_digest().unwrap();
        assert_eq!(
            model.execute_with_fault(&write, CommitFault::BeforeCommit),
            Err(ModelError::InjectedBeforeCommit)
        );
        assert_eq!(model.live_digest().unwrap(), pre);
        assert_eq!(
            model.execute_with_fault(&write, CommitFault::AfterCommitBeforeAck),
            Err(ModelError::AmbiguousAfterCommit)
        );
        assert_eq!(model.live_records().get(&record), Some(&b"value".to_vec()));
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
        assert_eq!(
            model.execute(&scan),
            Err(ModelError::RowTooLarge { required_bytes: 9 })
        );
        assert_eq!(model.live_digest().unwrap(), pre);
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
        assert!(model.execute(&request).is_err());
        assert_eq!(model.live_digest().unwrap(), pre);
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
    fn restore_rejects_unsorted_or_over_budget_without_replacing() {
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
        let generous = RestoreBudget {
            records_max_u64: 10,
            key_bytes_max_u64: 100,
            value_bytes_max_u64: 100,
            canonical_bytes_max_u64: 1_000,
        };
        assert!(model.restore_vnode(0, &records, generous).is_err());
        assert_eq!(model.live_records().get(&old), Some(&vec![1]));
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
        let mut request = batch(vec![Mutation::Put {
            key: key(Table::AggregateState, 0, b""),
            value: Vec::new(),
        }]);
        request.limits.request_bytes_max_u64 = MAX_MODEL_CANONICAL_BYTES;
        request.limits.mutation_bytes_max_u64 = MAX_MODEL_CANONICAL_BYTES;
        let envelope_bytes = encoded_request_len(&request).unwrap();
        let payload_bytes = usize::try_from(MAX_MODEL_CANONICAL_BYTES - envelope_bytes).unwrap();
        let Mutation::Put { value, .. } = &mut request.mutations[0] else {
            panic!("test request must contain a put")
        };
        value.reserve_exact(payload_bytes + 1);
        value.resize(payload_bytes, 0);

        assert_eq!(
            encoded_request_len(&request).unwrap(),
            MAX_MODEL_CANONICAL_BYTES
        );
        let encoded = encode_request(&request).unwrap();
        assert_eq!(
            u64::try_from(encoded.len()).unwrap(),
            MAX_MODEL_CANONICAL_BYTES
        );
        drop(encoded);

        let Mutation::Put { value, .. } = &mut request.mutations[0] else {
            panic!("test request must contain a put")
        };
        value.push(0);
        assert_eq!(
            encoded_request_len(&request).unwrap(),
            MAX_MODEL_CANONICAL_BYTES + 1
        );
        assert_eq!(
            encode_request(&request),
            Err(ModelError::Invalid(format!(
                "canonical request is {} bytes; model maximum is {MAX_MODEL_CANONICAL_BYTES}",
                MAX_MODEL_CANONICAL_BYTES + 1
            )))
        );
    }
}
