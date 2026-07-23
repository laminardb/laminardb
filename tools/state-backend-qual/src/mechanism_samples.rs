use std::io::{Cursor, Read};

use crate::artifact_reader::{ExactReader, PopulationHasher};
use crate::CheckErrors;

pub const MAX_MECHANISM_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;

const DEBT_DOMAIN: &[u8] = b"LDB-SBQ-MAINTENANCE-DEBT-V1\0";
const STALL_DOMAIN: &[u8] = b"LDB-SBQ-STALL-INTERVALS-V1\0";
const DEVICE_IO_DOMAIN: &[u8] = b"LDB-SBQ-TARGET-DEVICE-IO-V1\0";
const DIGEST_BYTES: usize = 32;
const COUNT_BYTES: usize = 8;

const DEBT_FIXED_RECORD_BYTES: usize = 32;
const DEBT_NOMINAL_SAMPLE: u8 = 0x00;
const DEBT_PRE_MEASUREMENT: u8 = 0x10;
const DEBT_WRITE_STOP: u8 = 0x11;
const DEBT_LAST_TERMINAL: u8 = 0x12;
const DEBT_MEASURED_END: u8 = 0x13;
const DEBT_TAIL_STABLE: u8 = 0x14;
const DEBT_TAIL_DEADLINE: u8 = 0x15;

const STALL_RECORD_BYTES: usize = 32;
const DEVICE_IO_RECORD_BYTES: usize = 112;
const MAX_DEVICE_IO_SHARDS: u32 = 256;
const DEVICE_IO_ANOMALY_FLAGS_MASK: u32 = 0x7f;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceDebtSummary {
    pub record_count: u64,
    pub maximum_total_debt_bytes: u64,
    pub maximum_observation_skew_ns: u64,
    pub tail_reached_deadline: bool,
    pub nominal_population_sha256: [u8; 32],
    pub cut_population_sha256: [u8; 32],
    pub normalized_tail_tag: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StallIntervalsSummary {
    pub record_count: u64,
    pub union_stall_ns: u64,
    pub stall_time_permille: u64,
    pub measurement_start_offset_ns: u64,
    pub measurement_end_offset_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetDeviceIoSummary {
    pub target_major: u32,
    pub target_minor: u32,
    pub measurement_start_offset_ns: u64,
    pub measurement_end_offset_ns: u64,
    pub capture_end_offset_ns: u64,
    pub shard_count: u32,
    pub issued_count: u64,
    pub maximum_issue_to_terminal_ns: u64,
    pub maximum_issue_to_terminal_ms: u64,
    pub error_count: u64,
    pub incomplete_count: u64,
    pub trace_anomaly_count: u64,
    pub trace_anomaly_flags: u32,
}

/// Validates the byte-normalized debt population bound to one exact mechanism mapping.
///
/// Records are all nominal common-resource-v2 samples followed by pre-measurement,
/// write-stop, last-terminal, measured-end, and exactly one stable/deadline tail cut.
/// Each mechanism contributes one direct unsigned-byte gauge. The gate value is the
/// checked sum across mechanisms at each observation, then the maximum across every
/// record. There is no unit conversion or implicit zero.
pub fn validate_maintenance_debt_samples_v1(
    bytes: &[u8],
    expected_mapping_sha256: &[u8; 32],
    mechanism_count: u32,
    expected_nominal_samples: u64,
    maximum_skew_ns: u64,
) -> Result<MaintenanceDebtSummary, CheckErrors> {
    let byte_length = u64::try_from(bytes.len())
        .map_err(|_| CheckErrors::one("maintenance debt length does not fit u64"))?;
    validate_maintenance_debt_samples_v1_reader(
        Cursor::new(bytes),
        byte_length,
        expected_mapping_sha256,
        mechanism_count,
        expected_nominal_samples,
        maximum_skew_ns,
    )
}

/// Streams the maintenance-debt wire with one bounded record buffer.
pub fn validate_maintenance_debt_samples_v1_reader<R: Read>(
    reader: R,
    byte_length: u64,
    expected_mapping_sha256: &[u8; 32],
    mechanism_count: u32,
    expected_nominal_samples: u64,
    maximum_skew_ns: u64,
) -> Result<MaintenanceDebtSummary, CheckErrors> {
    if mechanism_count == 0 || mechanism_count > 16 {
        return Err(CheckErrors::one(
            "maintenance debt mechanism count must be in 1..=16",
        ));
    }
    let record_bytes = DEBT_FIXED_RECORD_BYTES
        .checked_add(
            usize::try_from(mechanism_count)
                .ok()
                .and_then(|count| count.checked_mul(8))
                .ok_or_else(|| CheckErrors::one("maintenance debt record width overflow"))?,
        )
        .ok_or_else(|| CheckErrors::one("maintenance debt record width overflow"))?;
    let mut input = ExactReader::new(
        reader,
        byte_length,
        MAX_MECHANISM_ARTIFACT_BYTES,
        "maintenance debt",
    )?;
    if input.read_vec(DEBT_DOMAIN.len())? != DEBT_DOMAIN {
        return Err(CheckErrors::one("maintenance debt domain mismatch"));
    }
    if input.read_array::<DIGEST_BYTES>()? != *expected_mapping_sha256 {
        return Err(CheckErrors::one("maintenance debt mapping sha256 mismatch"));
    }
    let encoded_mechanisms = u32::from_be_bytes(input.read_array()?);
    if encoded_mechanisms != mechanism_count {
        return Err(CheckErrors::one(format!(
            "maintenance debt mechanism count is {encoded_mechanisms}; expected {mechanism_count}"
        )));
    }
    let record_count = u64::from_be_bytes(input.read_array()?);
    let expected_records = expected_nominal_samples
        .checked_add(5)
        .ok_or_else(|| CheckErrors::one("maintenance debt expected record count overflow"))?;
    if record_count != expected_records {
        return Err(CheckErrors::one(format!(
            "maintenance debt record count is {record_count}; expected {expected_records}"
        )));
    }
    let header_bytes = DEBT_DOMAIN
        .len()
        .checked_add(DIGEST_BYTES + 4 + COUNT_BYTES)
        .ok_or_else(|| CheckErrors::one("maintenance debt header width overflow"))?;
    input.require_total_length(exact_stream_length(
        header_bytes,
        record_bytes,
        record_count,
        "maintenance debt",
    )?)?;

    let mut maximum_total = 0_u64;
    let mut maximum_skew = 0_u64;
    let mut tail_reached_deadline = false;
    let mut nominal_population = PopulationHasher::new(expected_nominal_samples);
    let mut cut_population = PopulationHasher::new(5);
    let mut normalized_tail_tag = 0_u8;
    let mut record = vec![0_u8; record_bytes];
    for index in 0..record_count {
        input.read_into(&mut record)?;
        let tag = record[0];
        if record[1..8].iter().any(|byte| *byte != 0) {
            return Err(CheckErrors::one(format!(
                "maintenance debt record {index} has nonzero reserved bytes"
            )));
        }
        let population_index = u64_at(&record, 8);
        let expected_tag = if index < expected_nominal_samples {
            DEBT_NOMINAL_SAMPLE
        } else {
            match index - expected_nominal_samples {
                0 => DEBT_PRE_MEASUREMENT,
                1 => DEBT_WRITE_STOP,
                2 => DEBT_LAST_TERMINAL,
                3 => DEBT_MEASURED_END,
                4 => {
                    if matches!(tag, DEBT_TAIL_STABLE | DEBT_TAIL_DEADLINE) {
                        tag
                    } else {
                        DEBT_TAIL_STABLE
                    }
                }
                _ => unreachable!("record count fixed above"),
            }
        };
        if tag != expected_tag {
            return Err(CheckErrors::one(format!(
                "maintenance debt record {index} tag is 0x{tag:02x}; expected 0x{expected_tag:02x}"
            )));
        }
        let expected_population_index = if tag == DEBT_NOMINAL_SAMPLE { index } else { 0 };
        if population_index != expected_population_index {
            return Err(CheckErrors::one(format!(
                "maintenance debt record {index} population index is {population_index}; expected {expected_population_index}"
            )));
        }
        let begin = u64_at(&record, 16);
        let end = u64_at(&record, 24);
        let skew = validate_bracket(begin, end, maximum_skew_ns, "maintenance debt", index)?;
        maximum_skew = maximum_skew.max(skew);
        let mut total = 0_u64;
        for mechanism in 0..mechanism_count {
            let value = u64_at(&record, DEBT_FIXED_RECORD_BYTES + mechanism as usize * 8);
            total = total.checked_add(value).ok_or_else(|| {
                CheckErrors::one(format!(
                    "maintenance debt record {index} mechanism sum overflow"
                ))
            })?;
        }
        maximum_total = maximum_total.max(total);
        tail_reached_deadline = tag == DEBT_TAIL_DEADLINE;
        if tag == DEBT_NOMINAL_SAMPLE {
            nominal_population.update(0, population_index, begin, end);
        } else {
            let normalized_tag = tag - DEBT_PRE_MEASUREMENT;
            cut_population.update(normalized_tag, 0, begin, end);
            if matches!(tag, DEBT_TAIL_STABLE | DEBT_TAIL_DEADLINE) {
                normalized_tail_tag = normalized_tag;
            }
        }
    }
    input.finish()?;

    Ok(MaintenanceDebtSummary {
        record_count,
        maximum_total_debt_bytes: maximum_total,
        maximum_observation_skew_ns: maximum_skew,
        tail_reached_deadline,
        nominal_population_sha256: nominal_population.finish(),
        cut_population_sha256: cut_population.finish(),
        normalized_tail_tag,
    })
}

/// Validates complete candidate stall intervals and computes their measurement-window union.
pub fn validate_stall_intervals_v1(
    bytes: &[u8],
    expected_mapping_sha256: &[u8; 32],
    mechanism_count: u32,
) -> Result<StallIntervalsSummary, CheckErrors> {
    let byte_length = u64::try_from(bytes.len())
        .map_err(|_| CheckErrors::one("stall interval length does not fit u64"))?;
    validate_stall_intervals_v1_reader(
        Cursor::new(bytes),
        byte_length,
        expected_mapping_sha256,
        mechanism_count,
    )
}

/// Streams the canonical stall-interval wire with bounded per-mechanism state.
pub fn validate_stall_intervals_v1_reader<R: Read>(
    reader: R,
    byte_length: u64,
    expected_mapping_sha256: &[u8; 32],
    mechanism_count: u32,
) -> Result<StallIntervalsSummary, CheckErrors> {
    if mechanism_count == 0 || mechanism_count > 16 {
        return Err(CheckErrors::one("stall mechanism count must be in 1..=16"));
    }
    let mut input = ExactReader::new(
        reader,
        byte_length,
        MAX_MECHANISM_ARTIFACT_BYTES,
        "stall interval",
    )?;
    if input.read_vec(STALL_DOMAIN.len())? != STALL_DOMAIN {
        return Err(CheckErrors::one("stall interval domain mismatch"));
    }
    if input.read_array::<DIGEST_BYTES>()? != *expected_mapping_sha256 {
        return Err(CheckErrors::one("stall interval mapping sha256 mismatch"));
    }
    let encoded_mechanisms = u32::from_be_bytes(input.read_array()?);
    if encoded_mechanisms != mechanism_count {
        return Err(CheckErrors::one(format!(
            "stall mechanism count is {encoded_mechanisms}; expected {mechanism_count}"
        )));
    }
    let measurement_start = u64::from_be_bytes(input.read_array()?);
    let measurement_end = u64::from_be_bytes(input.read_array()?);
    if measurement_start >= measurement_end {
        return Err(CheckErrors::one(
            "stall measurement interval must be nonempty",
        ));
    }
    let record_count = u64::from_be_bytes(input.read_array()?);
    let header_bytes = STALL_DOMAIN
        .len()
        .checked_add(DIGEST_BYTES + 4 + 8 + 8 + COUNT_BYTES)
        .ok_or_else(|| CheckErrors::one("stall interval header width overflow"))?;
    input.require_total_length(exact_stream_length(
        header_bytes,
        STALL_RECORD_BYTES,
        record_count,
        "stall interval",
    )?)?;

    let mechanism_len = usize::try_from(mechanism_count)
        .map_err(|_| CheckErrors::one("stall mechanism count does not fit usize"))?;
    let mut next_sequences = vec![0_u64; mechanism_len];
    let mut previous_order: Option<(u64, u64, u32, u64)> = None;
    let mut union_start: Option<u64> = None;
    let mut union_end = 0_u64;
    let mut union_ns = 0_u64;

    for index in 0..record_count {
        let record = input.read_array::<STALL_RECORD_BYTES>()?;
        let mechanism = u32_at(&record, 0);
        if mechanism >= mechanism_count {
            return Err(CheckErrors::one(format!(
                "stall interval {index} mechanism index {mechanism} is outside 0..{mechanism_count}"
            )));
        }
        if record[4..8].iter().any(|byte| *byte != 0) {
            return Err(CheckErrors::one(format!(
                "stall interval {index} has nonzero reserved bytes"
            )));
        }
        let source_sequence = u64_at(&record, 8);
        let expected_sequence = next_sequences[mechanism as usize];
        if source_sequence != expected_sequence {
            return Err(CheckErrors::one(format!(
                "stall interval {index} source sequence is {source_sequence}; expected {expected_sequence} for mechanism {mechanism}"
            )));
        }
        next_sequences[mechanism as usize] = expected_sequence
            .checked_add(1)
            .ok_or_else(|| CheckErrors::one("stall source sequence overflow"))?;
        let start = u64_at(&record, 16);
        let end = u64_at(&record, 24);
        if start >= end {
            return Err(CheckErrors::one(format!(
                "stall interval {index} must be nonempty"
            )));
        }
        if start >= measurement_end || end <= measurement_start {
            return Err(CheckErrors::one(format!(
                "stall interval {index} does not intersect the measurement interval"
            )));
        }
        if end > measurement_end {
            return Err(CheckErrors::one(format!(
                "stall interval {index} ends after measurement_end instead of using the canonical censored boundary"
            )));
        }
        let order = (start, end, mechanism, source_sequence);
        if previous_order.is_some_and(|previous| previous > order) {
            return Err(CheckErrors::one(
                "stall intervals must be canonically ordered by start, end, mechanism, sequence",
            ));
        }
        previous_order = Some(order);

        let clipped_start = start.max(measurement_start);
        let clipped_end = end;
        match union_start {
            None => {
                union_start = Some(clipped_start);
                union_end = clipped_end;
            }
            Some(current_start) if clipped_start > union_end => {
                union_ns = union_ns
                    .checked_add(union_end - current_start)
                    .ok_or_else(|| CheckErrors::one("stall interval union overflow"))?;
                union_start = Some(clipped_start);
                union_end = clipped_end;
            }
            Some(_) => union_end = union_end.max(clipped_end),
        }
    }
    if let Some(current_start) = union_start {
        union_ns = union_ns
            .checked_add(union_end - current_start)
            .ok_or_else(|| CheckErrors::one("stall interval union overflow"))?;
    }
    input.finish()?;
    let measurement_ns = measurement_end - measurement_start;
    let stall_time_permille = ceil_ratio_milli(union_ns, measurement_ns)?;

    Ok(StallIntervalsSummary {
        record_count,
        union_stall_ns: union_ns,
        stall_time_permille,
        measurement_start_offset_ns: measurement_start,
        measurement_end_offset_ns: measurement_end,
    })
}

/// Validates the objective population behind profile-v3's
/// `target_device_io_latency_max_ms` gate.
///
/// The population is every read, write, or flush request issued to one exclusive target
/// device during `[measurement_start, measurement_end)`. The pinned tracer maintains bounded
/// per-issue state and exact per-shard counts/maxima; it does not emit one hot-path record per
/// I/O. No causal exclusions are allowed. Incomplete requests are censored at `capture_end`;
/// they remain a candidate failure and their duration is only a lower bound. The numerical
/// gate is the maximum issue-to-terminal duration, rounded up to whole milliseconds.
pub fn validate_target_device_io_v1(
    bytes: &[u8],
    expected_profile_sha256: &[u8; 32],
) -> Result<TargetDeviceIoSummary, CheckErrors> {
    let byte_length = u64::try_from(bytes.len())
        .map_err(|_| CheckErrors::one("target device I/O length does not fit u64"))?;
    validate_target_device_io_v1_reader(Cursor::new(bytes), byte_length, expected_profile_sha256)
}

/// Streams the bounded per-shard target-device summary with constant parser memory.
pub fn validate_target_device_io_v1_reader<R: Read>(
    reader: R,
    byte_length: u64,
    expected_profile_sha256: &[u8; 32],
) -> Result<TargetDeviceIoSummary, CheckErrors> {
    let mut input = ExactReader::new(
        reader,
        byte_length,
        MAX_MECHANISM_ARTIFACT_BYTES,
        "target device I/O",
    )?;
    if input.read_vec(DEVICE_IO_DOMAIN.len())? != DEVICE_IO_DOMAIN {
        return Err(CheckErrors::one("target device I/O domain mismatch"));
    }
    if input.read_array::<DIGEST_BYTES>()? != *expected_profile_sha256 {
        return Err(CheckErrors::one(
            "target device I/O profile sha256 mismatch",
        ));
    }
    let target_major = u32::from_be_bytes(input.read_array()?);
    let target_minor = u32::from_be_bytes(input.read_array()?);
    if target_major == 0 {
        return Err(CheckErrors::one(
            "target device I/O major number must be nonzero",
        ));
    }
    let measurement_start = u64::from_be_bytes(input.read_array()?);
    let measurement_end = u64::from_be_bytes(input.read_array()?);
    let capture_end = u64::from_be_bytes(input.read_array()?);
    if measurement_start >= measurement_end || measurement_end > capture_end {
        return Err(CheckErrors::one(
            "target device I/O requires measurement_start < measurement_end <= capture_end",
        ));
    }
    let shard_count = u32::from_be_bytes(input.read_array()?);
    if shard_count == 0 || shard_count > MAX_DEVICE_IO_SHARDS {
        return Err(CheckErrors::one(
            "target device I/O shard count must be in 1..=256",
        ));
    }
    let trace_anomaly_flags = u32::from_be_bytes(input.read_array()?);
    if trace_anomaly_flags & !DEVICE_IO_ANOMALY_FLAGS_MASK != 0 {
        return Err(CheckErrors::one(format!(
            "target device I/O header has unknown anomaly flags 0x{trace_anomaly_flags:08x}"
        )));
    }
    let header_bytes = DEVICE_IO_DOMAIN
        .len()
        .checked_add(DIGEST_BYTES + 4 + 4 + 8 + 8 + 8 + 4 + 4)
        .ok_or_else(|| CheckErrors::one("target device I/O header width overflow"))?;
    input.require_total_length(exact_stream_length(
        header_bytes,
        DEVICE_IO_RECORD_BYTES,
        u64::from(shard_count),
        "target device I/O",
    )?)?;

    let mut issued_count = 0_u64;
    let mut maximum_ns = 0_u64;
    let mut error_count = 0_u64;
    let mut incomplete_count = 0_u64;
    let mut trace_anomaly_count = 0_u64;
    let mut tracked_count = 0_u64;
    let mut untracked_count = 0_u64;
    for index in 0..u64::from(shard_count) {
        let record = input.read_array::<DEVICE_IO_RECORD_BYTES>()?;
        let shard = u32_at(&record, 0);
        if u64::from(shard) != index {
            return Err(CheckErrors::one(format!(
                "target device I/O shard is {shard}; expected {index}"
            )));
        }
        if u32_at(&record, 4) != 0 {
            return Err(CheckErrors::one(format!(
                "target device I/O record {index} has nonzero reserved bytes"
            )));
        }
        let issued = u64_at(&record, 8);
        let success = u64_at(&record, 16);
        let errors = u64_at(&record, 24);
        let incomplete = u64_at(&record, 32);
        let untracked = u64_at(&record, 40);
        let orphan_completions = u64_at(&record, 48);
        let duplicate_issues = u64_at(&record, 56);
        let tracked = checked_sum_u64([success, errors, incomplete], "device tracked count")?;
        issued_count = checked_add_u64(issued_count, issued, "device issued count")?;
        tracked_count = checked_add_u64(tracked_count, tracked, "device tracked count")?;
        untracked_count =
            checked_add_u64(untracked_count, untracked, "device untracked issue count")?;
        error_count = checked_add_u64(error_count, errors, "device error count")?;
        incomplete_count =
            checked_add_u64(incomplete_count, incomplete, "device incomplete count")?;
        trace_anomaly_count = checked_add_u64(
            trace_anomaly_count,
            checked_sum_u64(
                [untracked, orphan_completions, duplicate_issues],
                "device trace anomaly count",
            )?,
            "device trace anomaly count",
        )?;

        let duration = u64_at(&record, 64);
        let issue = u64_at(&record, 72);
        let terminal = u64_at(&record, 80);
        let logical_bytes = u64_at(&record, 88);
        let sequence = u64_at(&record, 96);
        let operation = record[104];
        let status = record[105];
        let witness_present = record[106];
        if record[107..112].iter().any(|byte| *byte != 0) {
            return Err(CheckErrors::one(format!(
                "target device I/O record {index} has nonzero trailing reserved bytes"
            )));
        }
        if tracked == 0 {
            if witness_present != 0
                || [duration, issue, terminal, logical_bytes, sequence]
                    .into_iter()
                    .any(|value| value != 0)
                || operation != 0
                || status != 0
            {
                return Err(CheckErrors::one(format!(
                    "target device I/O empty shard {index} has a maximum witness"
                )));
            }
            continue;
        }
        if witness_present != 1 {
            return Err(CheckErrors::one(format!(
                "target device I/O nonempty shard {index} lacks a maximum witness"
            )));
        }
        if issue < measurement_start || issue >= measurement_end {
            return Err(CheckErrors::one(format!(
                "target device I/O shard {index} maximum issue is outside measurement"
            )));
        }
        if terminal < issue || terminal > capture_end || duration != terminal - issue {
            return Err(CheckErrors::one(format!(
                "target device I/O shard {index} maximum duration/witness is inconsistent"
            )));
        }
        match operation {
            0x00 | 0x01 if logical_bytes == 0 => {
                return Err(CheckErrors::one(format!(
                    "target device I/O shard {index} read/write witness bytes must be positive"
                )))
            }
            0x02 if logical_bytes != 0 => {
                return Err(CheckErrors::one(format!(
                    "target device I/O shard {index} flush witness bytes must be zero"
                )))
            }
            0x00..=0x02 => {}
            _ => {
                return Err(CheckErrors::one(format!(
                "target device I/O shard {index} has unsupported operation tag 0x{operation:02x}"
            )))
            }
        }
        match status {
            0x00 if success > 0 => {}
            0x01 if errors > 0 => {}
            0x02 if incomplete > 0 && terminal == capture_end => {}
            0x00..=0x02 => {
                return Err(CheckErrors::one(format!(
                    "target device I/O shard {index} witness status has no matching population"
                )))
            }
            _ => {
                return Err(CheckErrors::one(format!(
                    "target device I/O shard {index} has unsupported status tag 0x{status:02x}"
                )))
            }
        }
        maximum_ns = maximum_ns.max(duration);
    }
    input.finish()?;
    if issued_count == 0 {
        return Err(CheckErrors::one(
            "target device I/O population is empty and cannot evaluate the gate",
        ));
    }
    if tracked_count == 0 {
        return Err(CheckErrors::one(
            "target device I/O population has no tracked request and cannot evaluate the gate",
        ));
    }
    let classified_count =
        checked_add_u64(tracked_count, untracked_count, "device classified count")?;
    if issued_count != classified_count {
        return Err(CheckErrors::one(format!(
            "target device I/O issued count is {issued_count}; global classified count is {classified_count}"
        )));
    }
    let maximum_ms = maximum_ns
        .checked_add(999_999)
        .ok_or_else(|| CheckErrors::one("target device I/O millisecond rounding overflow"))?
        / 1_000_000;

    Ok(TargetDeviceIoSummary {
        target_major,
        target_minor,
        measurement_start_offset_ns: measurement_start,
        measurement_end_offset_ns: measurement_end,
        capture_end_offset_ns: capture_end,
        shard_count,
        issued_count,
        maximum_issue_to_terminal_ns: maximum_ns,
        maximum_issue_to_terminal_ms: maximum_ms,
        error_count,
        incomplete_count,
        trace_anomaly_count,
        trace_anomaly_flags,
    })
}

fn checked_add_u64(left: u64, right: u64, label: &str) -> Result<u64, CheckErrors> {
    left.checked_add(right)
        .ok_or_else(|| CheckErrors::one(format!("{label} overflow")))
}

fn checked_sum_u64(values: impl IntoIterator<Item = u64>, label: &str) -> Result<u64, CheckErrors> {
    values
        .into_iter()
        .try_fold(0_u64, |sum, value| checked_add_u64(sum, value, label))
}

fn exact_stream_length(
    header_bytes: usize,
    record_bytes: usize,
    record_count: u64,
    label: &str,
) -> Result<u64, CheckErrors> {
    let header = u64::try_from(header_bytes)
        .map_err(|_| CheckErrors::one(format!("{label} header length does not fit u64")))?;
    let width = u64::try_from(record_bytes)
        .map_err(|_| CheckErrors::one(format!("{label} record length does not fit u64")))?;
    width
        .checked_mul(record_count)
        .and_then(|records| header.checked_add(records))
        .ok_or_else(|| CheckErrors::one(format!("{label} encoded length overflow")))
}

fn validate_bracket(
    begin: u64,
    end: u64,
    maximum_skew_ns: u64,
    label: &str,
    index: u64,
) -> Result<u64, CheckErrors> {
    let skew = end
        .checked_sub(begin)
        .ok_or_else(|| CheckErrors::one(format!("{label} record {index} begins after it ends")))?;
    if skew > maximum_skew_ns {
        return Err(CheckErrors::one(format!(
            "{label} record {index} skew {skew} exceeds {maximum_skew_ns} ns"
        )));
    }
    Ok(skew)
}

fn ceil_ratio_milli(numerator: u64, denominator: u64) -> Result<u64, CheckErrors> {
    let scaled = u128::from(numerator)
        .checked_mul(1_000)
        .ok_or_else(|| CheckErrors::one("permille numerator overflow"))?;
    let denominator = u128::from(denominator);
    let quotient = scaled / denominator;
    let rounded = quotient + u128::from(scaled % denominator != 0);
    u64::try_from(rounded).map_err(|_| CheckErrors::one("permille result exceeds u64"))
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use sha2::{Digest as _, Sha256};

    use super::*;

    const HASH: [u8; 32] = [0x11; 32];

    fn append_u32(bytes: &mut Vec<u8>, value: u32) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn append_u64(bytes: &mut Vec<u8>, value: u64) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn debt_stream(tail: u8) -> Vec<u8> {
        let mut bytes = DEBT_DOMAIN.to_vec();
        bytes.extend_from_slice(&HASH);
        append_u32(&mut bytes, 2);
        append_u64(&mut bytes, 7);
        for (index, tag) in [
            DEBT_NOMINAL_SAMPLE,
            DEBT_NOMINAL_SAMPLE,
            DEBT_PRE_MEASUREMENT,
            DEBT_WRITE_STOP,
            DEBT_LAST_TERMINAL,
            DEBT_MEASURED_END,
            tail,
        ]
        .into_iter()
        .enumerate()
        {
            bytes.push(tag);
            bytes.extend_from_slice(&[0; 7]);
            append_u64(
                &mut bytes,
                if tag == DEBT_NOMINAL_SAMPLE {
                    index as u64
                } else {
                    0
                },
            );
            append_u64(&mut bytes, 100 + index as u64 * 10);
            append_u64(&mut bytes, 105 + index as u64 * 10);
            append_u64(&mut bytes, index as u64);
            append_u64(&mut bytes, index as u64 * 2);
        }
        bytes
    }

    fn stall_stream() -> Vec<u8> {
        let mut bytes = STALL_DOMAIN.to_vec();
        bytes.extend_from_slice(&HASH);
        append_u32(&mut bytes, 2);
        append_u64(&mut bytes, 100);
        append_u64(&mut bytes, 200);
        append_u64(&mut bytes, 3);
        for (mechanism, sequence, start, end) in [
            (0_u32, 0_u64, 90_u64, 120_u64),
            (1, 0, 115, 130),
            (0, 1, 150, 170),
        ] {
            append_u32(&mut bytes, mechanism);
            append_u32(&mut bytes, 0);
            append_u64(&mut bytes, sequence);
            append_u64(&mut bytes, start);
            append_u64(&mut bytes, end);
        }
        bytes
    }

    fn device_stream(status: u8) -> Vec<u8> {
        let mut bytes = DEVICE_IO_DOMAIN.to_vec();
        bytes.extend_from_slice(&HASH);
        append_u32(&mut bytes, 259);
        append_u32(&mut bytes, 0);
        append_u64(&mut bytes, 100_000_000);
        append_u64(&mut bytes, 200_000_000);
        append_u64(&mut bytes, 300_000_000);
        append_u32(&mut bytes, 2);
        append_u32(&mut bytes, 0);
        for (
            shard,
            issued,
            success,
            incomplete,
            duration,
            issue,
            terminal,
            logical_bytes,
            sequence,
            operation,
            state,
        ) in [
            (
                0_u32,
                2_u64,
                2_u64,
                0_u64,
                10_000_000_u64,
                110_000_000_u64,
                120_000_000_u64,
                4096_u64,
                0_u64,
                0_u8,
                0_u8,
            ),
            (
                1_u32,
                1,
                u64::from(status == 0),
                u64::from(status == 2),
                if status == 2 {
                    170_000_000
                } else {
                    131_000_000
                },
                130_000_000,
                if status == 2 {
                    300_000_000
                } else {
                    261_000_000
                },
                0,
                1,
                2,
                status,
            ),
        ] {
            append_u32(&mut bytes, shard);
            append_u32(&mut bytes, 0);
            append_u64(&mut bytes, issued);
            append_u64(&mut bytes, success);
            append_u64(&mut bytes, 0);
            append_u64(&mut bytes, incomplete);
            append_u64(&mut bytes, 0);
            append_u64(&mut bytes, 0);
            append_u64(&mut bytes, 0);
            append_u64(&mut bytes, duration);
            append_u64(&mut bytes, issue);
            append_u64(&mut bytes, terminal);
            append_u64(&mut bytes, logical_bytes);
            append_u64(&mut bytes, sequence);
            bytes.push(operation);
            bytes.push(state);
            bytes.push(1);
            bytes.extend_from_slice(&[0; 5]);
        }
        bytes
    }

    #[test]
    fn debt_sum_then_max_and_tail_disposition_are_exact() {
        assert_eq!(
            Sha256::digest(debt_stream(DEBT_TAIL_STABLE)).as_slice(),
            &[
                0x22, 0xa0, 0x84, 0xf2, 0x32, 0x2f, 0x10, 0xc8, 0x71, 0xfc, 0x67, 0xd5, 0xe6, 0x03,
                0xbf, 0x2a, 0x57, 0xd8, 0x26, 0x46, 0x8f, 0x75, 0xc0, 0xca, 0x70, 0x1e, 0x5e, 0xea,
                0x51, 0x74, 0xd0, 0x4c,
            ]
        );
        let stable =
            validate_maintenance_debt_samples_v1(&debt_stream(DEBT_TAIL_STABLE), &HASH, 2, 2, 5)
                .unwrap();
        assert_eq!(stable.record_count, 7);
        assert_eq!(stable.maximum_total_debt_bytes, 18);
        assert_eq!(stable.maximum_observation_skew_ns, 5);
        assert!(!stable.tail_reached_deadline);

        let deadline =
            validate_maintenance_debt_samples_v1(&debt_stream(DEBT_TAIL_DEADLINE), &HASH, 2, 2, 5)
                .unwrap();
        assert!(deadline.tail_reached_deadline);
    }

    #[test]
    fn stall_intervals_are_clipped_and_unioned_before_permille_rounding() {
        assert_eq!(
            Sha256::digest(stall_stream()).as_slice(),
            &[
                0x55, 0xb1, 0x2e, 0x5f, 0xdd, 0x0b, 0xdd, 0x24, 0x55, 0x03, 0x8d, 0xe1, 0x63, 0xbe,
                0xfb, 0x89, 0xe9, 0x9a, 0xce, 0xeb, 0x58, 0x43, 0x01, 0xe2, 0x40, 0x43, 0xaf, 0x5f,
                0xbf, 0xa8, 0x9b, 0x01,
            ]
        );
        let summary = validate_stall_intervals_v1(&stall_stream(), &HASH, 2).unwrap();
        assert_eq!(summary.record_count, 3);
        assert_eq!(summary.union_stall_ns, 50);
        assert_eq!(summary.stall_time_permille, 500);

        let mut active_at_end = stall_stream();
        let header = STALL_DOMAIN.len() + DIGEST_BYTES + 4 + 8 + 8 + COUNT_BYTES;
        let last_end = header + 2 * STALL_RECORD_BYTES + 24;
        active_at_end[last_end..last_end + 8].copy_from_slice(&200_u64.to_be_bytes());
        let summary = validate_stall_intervals_v1(&active_at_end, &HASH, 2).unwrap();
        assert_eq!(summary.union_stall_ns, 80);
        assert_eq!(summary.stall_time_permille, 800);

        active_at_end[last_end..last_end + 8].copy_from_slice(&201_u64.to_be_bytes());
        assert!(validate_stall_intervals_v1(&active_at_end, &HASH, 2)
            .unwrap_err()
            .to_string()
            .contains("canonical censored boundary"));
    }

    #[test]
    fn target_device_population_uses_maximum_and_preserves_incomplete_failure() {
        assert_eq!(
            Sha256::digest(device_stream(0)).as_slice(),
            &[
                0x80, 0xbd, 0x48, 0x99, 0xc5, 0x4b, 0xc4, 0x72, 0x55, 0xd8, 0x29, 0x25, 0x2b, 0x94,
                0x1d, 0x5c, 0x32, 0x7b, 0x87, 0x92, 0x77, 0xe3, 0x0e, 0x42, 0x3e, 0x55, 0x84, 0x2a,
                0x7e, 0xdc, 0x90, 0x08,
            ]
        );
        let summary = validate_target_device_io_v1(&device_stream(0), &HASH).unwrap();
        assert_eq!(summary.shard_count, 2);
        assert_eq!(summary.issued_count, 3);
        assert_eq!(summary.maximum_issue_to_terminal_ns, 131_000_000);
        assert_eq!(summary.maximum_issue_to_terminal_ms, 131);
        assert_eq!(summary.error_count, 0);
        assert_eq!(summary.incomplete_count, 0);
        assert_eq!(summary.trace_anomaly_count, 0);

        let summary = validate_target_device_io_v1(&device_stream(2), &HASH).unwrap();
        assert_eq!(summary.incomplete_count, 1);
        assert_eq!(summary.maximum_issue_to_terminal_ns, 170_000_000);
        assert_eq!(summary.maximum_issue_to_terminal_ms, 170);
    }

    #[test]
    fn target_device_trace_anomalies_are_preserved_for_attempt_classification() {
        let mut bytes = device_stream(0);
        let header = DEVICE_IO_DOMAIN.len() + DIGEST_BYTES + 4 + 4 + 8 + 8 + 8 + 4 + 4;
        bytes[header + 8..header + 16].copy_from_slice(&3_u64.to_be_bytes());
        bytes[header + 40..header + 48].copy_from_slice(&1_u64.to_be_bytes());
        bytes[header - 4..header].copy_from_slice(&0x05_u32.to_be_bytes());
        let summary = validate_target_device_io_v1(&bytes, &HASH).unwrap();
        assert_eq!(summary.issued_count, 4);
        assert_eq!(summary.trace_anomaly_count, 1);
        assert_eq!(summary.trace_anomaly_flags, 0x05);
    }

    #[test]
    fn target_device_counts_are_global_not_cross_cpu_issue_shard_updates() {
        let mut bytes = device_stream(0);
        let header = DEVICE_IO_DOMAIN.len() + DIGEST_BYTES + 4 + 4 + 8 + 8 + 8 + 4 + 4;
        let first = header;
        let second = header + DEVICE_IO_RECORD_BYTES;
        bytes[first + 8..first + 16].copy_from_slice(&3_u64.to_be_bytes());
        bytes[first + 16..first + 24].copy_from_slice(&1_u64.to_be_bytes());
        bytes[second + 8..second + 16].copy_from_slice(&0_u64.to_be_bytes());
        bytes[second + 16..second + 24].copy_from_slice(&2_u64.to_be_bytes());
        let summary = validate_target_device_io_v1(&bytes, &HASH).unwrap();
        assert_eq!(summary.issued_count, 3);
        assert_eq!(summary.trace_anomaly_count, 0);
    }

    #[test]
    fn artifacts_reject_wrong_binding_shape_order_and_trailing_bytes() {
        let mut debt = debt_stream(DEBT_TAIL_STABLE);
        debt[DEBT_DOMAIN.len()] ^= 1;
        assert!(validate_maintenance_debt_samples_v1(&debt, &HASH, 2, 2, 5).is_err());

        let mut debt = debt_stream(DEBT_TAIL_STABLE);
        debt.push(0);
        assert!(validate_maintenance_debt_samples_v1(&debt, &HASH, 2, 2, 5).is_err());

        let mut stalls = stall_stream();
        let header = STALL_DOMAIN.len() + DIGEST_BYTES + 4 + 8 + 8 + COUNT_BYTES;
        stalls[header + STALL_RECORD_BYTES + 16..header + STALL_RECORD_BYTES + 24]
            .copy_from_slice(&80_u64.to_be_bytes());
        assert!(validate_stall_intervals_v1(&stalls, &HASH, 2).is_err());

        let mut device = device_stream(0);
        let header = DEVICE_IO_DOMAIN.len() + DIGEST_BYTES + 4 + 4 + 8 + 8 + 8 + 4 + 4;
        device[header + DEVICE_IO_RECORD_BYTES + 104] = 3;
        assert!(validate_target_device_io_v1(&device, &HASH).is_err());

        let mut device = device_stream(0);
        device[header - 4..header].copy_from_slice(&0x80_u32.to_be_bytes());
        assert!(validate_target_device_io_v1(&device, &HASH).is_err());
    }

    #[test]
    fn debt_overflow_and_device_empty_population_fail_closed() {
        let mut debt = debt_stream(DEBT_TAIL_STABLE);
        let header = DEBT_DOMAIN.len() + DIGEST_BYTES + 4 + COUNT_BYTES;
        debt[header + DEBT_FIXED_RECORD_BYTES..header + DEBT_FIXED_RECORD_BYTES + 8]
            .copy_from_slice(&u64::MAX.to_be_bytes());
        debt[header + DEBT_FIXED_RECORD_BYTES + 8..header + DEBT_FIXED_RECORD_BYTES + 16]
            .copy_from_slice(&1_u64.to_be_bytes());
        assert!(validate_maintenance_debt_samples_v1(&debt, &HASH, 2, 2, 5).is_err());

        let mut empty = device_stream(0);
        let header = DEVICE_IO_DOMAIN.len() + DIGEST_BYTES + 4 + 4 + 8 + 8 + 8 + 4 + 4;
        for shard in 0..2 {
            let offset = header + shard * DEVICE_IO_RECORD_BYTES;
            empty[offset + 8..offset + DEVICE_IO_RECORD_BYTES].fill(0);
        }
        assert!(validate_target_device_io_v1(&empty, &HASH).is_err());
    }
}
