//! Intrinsic validation for the finite C2 latency-sample wire.
//!
//! This module does not validate a runner plan, conservation, cadence, gates, attempt
//! classification, or qualification eligibility.

use crate::CheckErrors;

const LATENCY_SAMPLES_DOMAIN: &[u8; 27] = b"LDB-SBQ-LATENCY-SAMPLES-V1\0";
const LATENCY_SAMPLES_HEADER_BYTES: usize = 35;
const LATENCY_SAMPLE_RECORD_BYTES: usize = 58;

pub const LATENCY_SAMPLE_OUTCOME_COUNT: usize = 11;
const STAGE_MASKS: [u8; 5] = [0x00, 0x01, 0x03, 0x07, 0x0f];

const ORDINAL_OFFSET: usize = 0;
const SCHEDULED_OFFSET: usize = 8;
const ENQUEUED_OFFSET: usize = 16;
const DISPATCH_START_OFFSET: usize = 24;
const SERVICE_START_OFFSET: usize = 32;
const CANDIDATE_RETURN_OFFSET: usize = 40;
const TERMINAL_OFFSET: usize = 48;
const STAGE_MASK_OFFSET: usize = 56;
const OUTCOME_OFFSET: usize = 57;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatencySamplesSummary {
    pub record_count: u64,
    pub returned_records: u64,
    pub maximum_scheduled_offset_ns: u64,
    pub maximum_terminal_offset_ns: u64,
    pub outcome_counts: [u64; LATENCY_SAMPLE_OUTCOME_COUNT],
}

/// Validates the provisional C2 latency-sample wire without retaining records.
///
/// `maximum_records` is a caller-owned allocation/evidence bound. The wire's declared count must
/// be nonzero and no greater than that bound.
pub fn validate_latency_samples(
    bytes: &[u8],
    maximum_records: u64,
) -> Result<LatencySamplesSummary, CheckErrors> {
    if bytes.len() < LATENCY_SAMPLES_DOMAIN.len() {
        return Err(CheckErrors::one("latency samples domain is truncated"));
    }
    if &bytes[..LATENCY_SAMPLES_DOMAIN.len()] != LATENCY_SAMPLES_DOMAIN {
        return Err(CheckErrors::one("latency samples domain is invalid"));
    }
    if bytes.len() < LATENCY_SAMPLES_HEADER_BYTES {
        return Err(CheckErrors::one("latency samples header is truncated"));
    }

    let record_count = read_u64(bytes, LATENCY_SAMPLES_DOMAIN.len());
    if record_count == 0 {
        return Err(CheckErrors::one(
            "latency samples record count must be nonzero",
        ));
    }
    if record_count > maximum_records {
        return Err(CheckErrors::one(format!(
            "latency samples record count {record_count} exceeds caller maximum {maximum_records}"
        )));
    }

    let record_count_usize = usize::try_from(record_count)
        .map_err(|_| CheckErrors::one("latency samples record count does not fit usize"))?;
    let expected_length = record_count_usize
        .checked_mul(LATENCY_SAMPLE_RECORD_BYTES)
        .and_then(|records| LATENCY_SAMPLES_HEADER_BYTES.checked_add(records))
        .ok_or_else(|| CheckErrors::one("latency samples encoded length overflows usize"))?;
    if bytes.len() != expected_length {
        return Err(CheckErrors::one(format!(
            "latency samples length is {}; expected {expected_length}",
            bytes.len()
        )));
    }

    let mut summary = LatencySamplesSummary {
        record_count,
        returned_records: 0,
        maximum_scheduled_offset_ns: 0,
        maximum_terminal_offset_ns: 0,
        outcome_counts: [0; LATENCY_SAMPLE_OUTCOME_COUNT],
    };

    for (index, record) in bytes[LATENCY_SAMPLES_HEADER_BYTES..]
        .chunks_exact(LATENCY_SAMPLE_RECORD_BYTES)
        .enumerate()
    {
        let expected_ordinal = u64::try_from(index)
            .map_err(|_| CheckErrors::one("latency sample ordinal does not fit u64"))?;
        let ordinal = read_u64(record, ORDINAL_OFFSET);
        if ordinal != expected_ordinal {
            return Err(record_error(
                expected_ordinal,
                format!("ordinal is {ordinal}; expected {expected_ordinal}"),
            ));
        }

        let scheduled = read_u64(record, SCHEDULED_OFFSET);
        let enqueued = read_u64(record, ENQUEUED_OFFSET);
        let dispatch_start = read_u64(record, DISPATCH_START_OFFSET);
        let service_start = read_u64(record, SERVICE_START_OFFSET);
        let candidate_return = read_u64(record, CANDIDATE_RETURN_OFFSET);
        let terminal = read_u64(record, TERMINAL_OFFSET);
        let stage_mask = record[STAGE_MASK_OFFSET];
        let outcome = record[OUTCOME_OFFSET];

        if !STAGE_MASKS.contains(&stage_mask) {
            return Err(record_error(
                expected_ordinal,
                format!("stage mask 0x{stage_mask:02x} is not a valid prefix mask"),
            ));
        }

        let optional_stages = [
            (0x01, "enqueued", enqueued),
            (0x02, "dispatch_start", dispatch_start),
            (0x04, "service_start", service_start),
            (0x08, "candidate_return", candidate_return),
        ];
        let mut previous_name = "scheduled";
        let mut previous = scheduled;
        for (bit, name, timestamp) in optional_stages {
            if stage_mask & bit == 0 {
                if timestamp != 0 {
                    return Err(record_error(
                        expected_ordinal,
                        format!("absent {name} timestamp must be zero"),
                    ));
                }
                continue;
            }
            if timestamp < previous {
                return Err(record_error(
                    expected_ordinal,
                    format!("{name} timestamp precedes {previous_name}"),
                ));
            }
            previous_name = name;
            previous = timestamp;
        }
        if terminal < previous {
            return Err(record_error(
                expected_ordinal,
                format!("terminal timestamp precedes {previous_name}"),
            ));
        }

        if !outcome_accepts_mask(outcome, stage_mask) {
            if usize::from(outcome) >= LATENCY_SAMPLE_OUTCOME_COUNT {
                return Err(record_error(
                    expected_ordinal,
                    format!("outcome tag 0x{outcome:02x} is unknown"),
                ));
            }
            return Err(record_error(
                expected_ordinal,
                format!(
                    "outcome tag 0x{outcome:02x} is incompatible with stage mask 0x{stage_mask:02x}"
                ),
            ));
        }

        summary.outcome_counts[usize::from(outcome)] += 1;
        if stage_mask == 0x0f {
            summary.returned_records += 1;
        }
        summary.maximum_scheduled_offset_ns = summary.maximum_scheduled_offset_ns.max(scheduled);
        summary.maximum_terminal_offset_ns = summary.maximum_terminal_offset_ns.max(terminal);
    }

    Ok(summary)
}

fn outcome_accepts_mask(outcome: u8, stage_mask: u8) -> bool {
    match outcome {
        0x00..=0x02 => stage_mask == 0x0f,
        0x03 => stage_mask == 0x00,
        0x04 => stage_mask == 0x01,
        0x05 => stage_mask == 0x07,
        0x06 => matches!(stage_mask, 0x00 | 0x01 | 0x03 | 0x07),
        0x07 => stage_mask == 0x03,
        0x08 | 0x09 => STAGE_MASKS.contains(&stage_mask),
        0x0a => stage_mask == 0x07,
        _ => false,
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_be_bytes(encoded)
}

fn record_error(ordinal: u64, message: impl std::fmt::Display) -> CheckErrors {
    CheckErrors::one(format!("latency sample {ordinal}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct TestRecord {
        ordinal: u64,
        scheduled: u64,
        enqueued: u64,
        dispatch_start: u64,
        service_start: u64,
        candidate_return: u64,
        terminal: u64,
        stage_mask: u8,
        outcome: u8,
    }

    fn record(ordinal: u64, stage_mask: u8, outcome: u8) -> TestRecord {
        TestRecord {
            ordinal,
            scheduled: 10,
            enqueued: u64::from(stage_mask & 0x01 != 0) * 20,
            dispatch_start: u64::from(stage_mask & 0x02 != 0) * 30,
            service_start: u64::from(stage_mask & 0x04 != 0) * 40,
            candidate_return: u64::from(stage_mask & 0x08 != 0) * 50,
            terminal: 60,
            stage_mask,
            outcome,
        }
    }

    fn encode(records: &[TestRecord]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            LATENCY_SAMPLES_HEADER_BYTES + LATENCY_SAMPLE_RECORD_BYTES * records.len(),
        );
        bytes.extend_from_slice(LATENCY_SAMPLES_DOMAIN);
        bytes.extend_from_slice(&u64::try_from(records.len()).unwrap().to_be_bytes());
        for record in records {
            for value in [
                record.ordinal,
                record.scheduled,
                record.enqueued,
                record.dispatch_start,
                record.service_start,
                record.candidate_return,
                record.terminal,
            ] {
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            bytes.push(record.stage_mask);
            bytes.push(record.outcome);
        }
        bytes
    }

    fn set_u64(bytes: &mut [u8], record_index: usize, offset: usize, value: u64) {
        let start =
            LATENCY_SAMPLES_HEADER_BYTES + record_index * LATENCY_SAMPLE_RECORD_BYTES + offset;
        bytes[start..start + 8].copy_from_slice(&value.to_be_bytes());
    }

    #[test]
    fn validates_all_outcome_shapes_and_summarizes_without_retaining_records() {
        let shapes = [
            (0x00, 0x0f),
            (0x01, 0x0f),
            (0x02, 0x0f),
            (0x03, 0x00),
            (0x04, 0x01),
            (0x05, 0x07),
            (0x06, 0x00),
            (0x06, 0x01),
            (0x06, 0x03),
            (0x06, 0x07),
            (0x07, 0x03),
            (0x08, 0x00),
            (0x08, 0x01),
            (0x08, 0x03),
            (0x08, 0x07),
            (0x08, 0x0f),
            (0x09, 0x00),
            (0x09, 0x01),
            (0x09, 0x03),
            (0x09, 0x07),
            (0x09, 0x0f),
            (0x0a, 0x07),
        ];
        let records = shapes
            .iter()
            .enumerate()
            .map(|(ordinal, (outcome, mask))| {
                record(u64::try_from(ordinal).unwrap(), *mask, *outcome)
            })
            .collect::<Vec<_>>();

        let summary = validate_latency_samples(&encode(&records), records.len() as u64).unwrap();

        assert_eq!(summary.record_count, records.len() as u64);
        assert_eq!(summary.returned_records, 5);
        assert_eq!(summary.maximum_scheduled_offset_ns, 10);
        assert_eq!(summary.maximum_terminal_offset_ns, 60);
        assert_eq!(summary.outcome_counts[0x06], 4);
        assert_eq!(summary.outcome_counts[0x08], 5);
        assert_eq!(summary.outcome_counts[0x09], 5);
        assert_eq!(summary.outcome_counts[0x0a], 1);
        assert_eq!(
            summary.outcome_counts.iter().sum::<u64>(),
            records.len() as u64
        );
    }

    #[test]
    fn rejects_every_truncation_and_trailing_length() {
        let bytes = encode(&[record(0, 0x0f, 0x00)]);
        for length in 0..bytes.len() {
            assert!(
                validate_latency_samples(&bytes[..length], 1).is_err(),
                "accepted truncation at {length}"
            );
        }
        for trailing in 1..=LATENCY_SAMPLE_RECORD_BYTES {
            let mut changed = bytes.clone();
            changed.resize(bytes.len() + trailing, 0);
            assert!(
                validate_latency_samples(&changed, 1).is_err(),
                "accepted {trailing} trailing bytes"
            );
        }
    }

    #[test]
    fn rejects_domain_mutations_and_invalid_counts() {
        let bytes = encode(&[record(0, 0x0f, 0x00)]);
        for index in 0..LATENCY_SAMPLES_DOMAIN.len() {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            assert!(
                validate_latency_samples(&changed, 1).is_err(),
                "accepted domain mutation at {index}"
            );
        }

        let mut zero = bytes.clone();
        zero[LATENCY_SAMPLES_DOMAIN.len()..LATENCY_SAMPLES_HEADER_BYTES].fill(0);
        assert!(validate_latency_samples(&zero, 1).is_err());

        assert!(validate_latency_samples(&bytes, 0).is_err());
        assert!(validate_latency_samples(&bytes, 1).is_ok());

        let mut too_many = bytes.clone();
        too_many[LATENCY_SAMPLES_DOMAIN.len()..LATENCY_SAMPLES_HEADER_BYTES]
            .copy_from_slice(&2_u64.to_be_bytes());
        assert!(validate_latency_samples(&too_many, 2).is_err());

        let mut overflowing = bytes;
        overflowing[LATENCY_SAMPLES_DOMAIN.len()..LATENCY_SAMPLES_HEADER_BYTES]
            .copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(validate_latency_samples(&overflowing, u64::MAX).is_err());
    }

    #[test]
    fn requires_zero_based_contiguous_ordinals() {
        let base = encode(&[record(0, 0x0f, 0x00), record(1, 0x0f, 0x00)]);
        for (record_index, ordinal) in [(0, 1), (1, 0), (1, 2)] {
            let mut changed = base.clone();
            set_u64(&mut changed, record_index, ORDINAL_OFFSET, ordinal);
            assert!(
                validate_latency_samples(&changed, 2).is_err(),
                "accepted ordinal {ordinal} at record {record_index}"
            );
        }
    }

    #[test]
    fn accepts_only_prefix_stage_masks() {
        for mask in u8::MIN..=u8::MAX {
            let bytes = encode(&[record(0, mask, 0x08)]);
            assert_eq!(
                validate_latency_samples(&bytes, 1).is_ok(),
                STAGE_MASKS.contains(&mask),
                "unexpected result for mask 0x{mask:02x}"
            );
        }
    }

    #[test]
    fn enforces_the_complete_outcome_mask_matrix() {
        fn expected(outcome: u8, mask: u8) -> bool {
            match outcome {
                0x00..=0x02 => mask == 0x0f,
                0x03 => mask == 0x00,
                0x04 => mask == 0x01,
                0x05 => mask == 0x07,
                0x06 => matches!(mask, 0x00 | 0x01 | 0x03 | 0x07),
                0x07 => mask == 0x03,
                0x08 | 0x09 => STAGE_MASKS.contains(&mask),
                0x0a => mask == 0x07,
                _ => false,
            }
        }

        for outcome in u8::MIN..=u8::MAX {
            for mask in STAGE_MASKS {
                let bytes = encode(&[record(0, mask, outcome)]);
                assert_eq!(
                    validate_latency_samples(&bytes, 1).is_ok(),
                    expected(outcome, mask),
                    "unexpected result for outcome 0x{outcome:02x}, mask 0x{mask:02x}"
                );
            }
        }
    }

    #[test]
    fn absent_stage_timestamps_must_be_zero() {
        let stage_offsets = [
            (0x01, ENQUEUED_OFFSET),
            (0x02, DISPATCH_START_OFFSET),
            (0x04, SERVICE_START_OFFSET),
            (0x08, CANDIDATE_RETURN_OFFSET),
        ];
        for mask in STAGE_MASKS {
            for (bit, offset) in stage_offsets {
                if mask & bit != 0 {
                    continue;
                }
                let mut bytes = encode(&[record(0, mask, 0x08)]);
                set_u64(&mut bytes, 0, offset, 1);
                assert!(
                    validate_latency_samples(&bytes, 1).is_err(),
                    "accepted nonzero absent stage bit 0x{bit:02x} for mask 0x{mask:02x}"
                );
            }
        }
    }

    #[test]
    fn timestamps_must_be_nondecreasing_across_present_stages() {
        let base = encode(&[record(0, 0x0f, 0x00)]);
        for (offset, value) in [
            (ENQUEUED_OFFSET, 9),
            (DISPATCH_START_OFFSET, 19),
            (SERVICE_START_OFFSET, 29),
            (CANDIDATE_RETURN_OFFSET, 39),
            (TERMINAL_OFFSET, 49),
        ] {
            let mut changed = base.clone();
            set_u64(&mut changed, 0, offset, value);
            assert!(
                validate_latency_samples(&changed, 1).is_err(),
                "accepted decreasing timestamp at offset {offset}"
            );
        }

        let mut no_stages = encode(&[record(0, 0x00, 0x03)]);
        set_u64(&mut no_stages, 0, TERMINAL_OFFSET, 9);
        assert!(validate_latency_samples(&no_stages, 1).is_err());

        let equal = TestRecord {
            ordinal: 0,
            scheduled: 7,
            enqueued: 7,
            dispatch_start: 7,
            service_start: 7,
            candidate_return: 7,
            terminal: 7,
            stage_mask: 0x0f,
            outcome: 0x00,
        };
        assert!(validate_latency_samples(&encode(&[equal]), 1).is_ok());

        let zero_present_stages = TestRecord {
            ordinal: 0,
            scheduled: 0,
            enqueued: 0,
            dispatch_start: 0,
            service_start: 0,
            candidate_return: 0,
            terminal: 0,
            stage_mask: 0x0f,
            outcome: 0x00,
        };
        assert!(validate_latency_samples(&encode(&[zero_present_stages]), 1).is_ok());
    }
}
