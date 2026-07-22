//! Intrinsic validation for the C2 resource-sample and resource-cut wires.
//!
//! Caller-supplied bounds are assumed to come from a future validated plan. This module does not
//! derive write-stop, tail, formula, attempt-classification, or qualification decisions.

use crate::CheckErrors;

const RESOURCE_SAMPLES_DOMAIN: &[u8] = b"LDB-SBQ-RESOURCE-SAMPLES-V1\0";
const RESOURCE_CUTS_DOMAIN: &[u8] = b"LDB-SBQ-RESOURCE-CUTS-V1\0";
const COUNT_BYTES: usize = 8;
const SAMPLE_FIELD_COUNT: usize = 22;
const SAMPLE_RECORD_BYTES: usize = SAMPLE_FIELD_COUNT * 8;
const CUT_RESERVED_BYTES: usize = 7;
const CUT_FIELD_COUNT: usize = 15;
const CUT_RECORD_BYTES: usize = 1 + CUT_RESERVED_BYTES + CUT_FIELD_COUNT * 8;
const REQUIRED_CUT_RECORDS: u64 = 5;

const SAMPLE_INDEX: usize = 0;
const SAMPLE_BEGIN_NS: usize = 1;
const SAMPLE_END_NS: usize = 2;

const CUT_BEGIN_NS: usize = 0;
const CUT_END_NS: usize = 1;

const _: [(); 28] = [(); RESOURCE_SAMPLES_DOMAIN.len()];
const _: [(); 25] = [(); RESOURCE_CUTS_DOMAIN.len()];
const _: [(); 176] = [(); SAMPLE_RECORD_BYTES];
const _: [(); 128] = [(); CUT_RECORD_BYTES];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceSamplesSummary {
    pub record_count: u64,
    pub maximum_skew_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceCutsSummary {
    pub record_count: u64,
    pub resource_tail_tag: u8,
    pub maximum_skew_ns: u64,
}

/// Validates the provisional C2 resource-sample wire without allocating per record.
pub fn validate_resource_samples(
    bytes: &[u8],
    maximum_records: u64,
    maximum_skew_ns: u64,
) -> Result<ResourceSamplesSummary, CheckErrors> {
    let (record_count, records) = validate_stream_envelope(
        bytes,
        RESOURCE_SAMPLES_DOMAIN,
        SAMPLE_RECORD_BYTES,
        maximum_records,
        "resource samples",
    )?;

    let mut maximum_skew = 0;

    for (index, record) in records.chunks_exact(SAMPLE_RECORD_BYTES).enumerate() {
        let wire_index = field_u64(record, SAMPLE_INDEX);
        let expected_index = u64::try_from(index)
            .map_err(|_| CheckErrors::one("resource sample index does not fit u64"))?;
        if wire_index != expected_index {
            return Err(CheckErrors::one(format!(
                "resource sample {index} has index {wire_index}; expected {expected_index}"
            )));
        }

        let begin = field_u64(record, SAMPLE_BEGIN_NS);
        let end = field_u64(record, SAMPLE_END_NS);
        let skew = validate_bracket(begin, end, maximum_skew_ns, "resource sample", index)?;
        maximum_skew = maximum_skew.max(skew);
    }

    Ok(ResourceSamplesSummary {
        record_count,
        maximum_skew_ns: maximum_skew,
    })
}

/// Validates the provisional C2 resource-cut wire without allocating per record.
pub fn validate_resource_cuts(
    bytes: &[u8],
    maximum_records: u64,
    maximum_skew_ns: u64,
) -> Result<ResourceCutsSummary, CheckErrors> {
    let (record_count, records) = validate_stream_envelope(
        bytes,
        RESOURCE_CUTS_DOMAIN,
        CUT_RECORD_BYTES,
        maximum_records,
        "resource cuts",
    )?;
    if record_count != REQUIRED_CUT_RECORDS {
        return Err(CheckErrors::one(format!(
            "resource cuts record count is {record_count}; expected {REQUIRED_CUT_RECORDS}"
        )));
    }

    let mut maximum_skew = 0;
    let mut tail_tag = 0;

    for (index, record) in records.chunks_exact(CUT_RECORD_BYTES).enumerate() {
        let tag = record[0];
        let expected_tag = u8::try_from(index)
            .map_err(|_| CheckErrors::one("resource cut index does not fit u8"))?;
        if index < 4 {
            if tag != expected_tag {
                return Err(CheckErrors::one(format!(
                    "resource cut {index} has tag 0x{tag:02x}; expected 0x{expected_tag:02x}"
                )));
            }
        } else if !matches!(tag, 0x04 | 0x05) {
            return Err(CheckErrors::one(format!(
                "resource tail cut has tag 0x{tag:02x}; expected 0x04 or 0x05"
            )));
        } else {
            tail_tag = tag;
        }

        if record[1..1 + CUT_RESERVED_BYTES]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(CheckErrors::one(format!(
                "resource cut {index} has nonzero reserved bytes"
            )));
        }

        let fields = &record[1 + CUT_RESERVED_BYTES..];
        let begin = field_u64(fields, CUT_BEGIN_NS);
        let end = field_u64(fields, CUT_END_NS);
        let skew = validate_bracket(begin, end, maximum_skew_ns, "resource cut", index)?;
        maximum_skew = maximum_skew.max(skew);
    }

    Ok(ResourceCutsSummary {
        record_count,
        resource_tail_tag: tail_tag,
        maximum_skew_ns: maximum_skew,
    })
}

fn validate_stream_envelope<'a>(
    bytes: &'a [u8],
    domain: &[u8],
    record_bytes: usize,
    maximum_records: u64,
    label: &str,
) -> Result<(u64, &'a [u8]), CheckErrors> {
    if bytes.len() < domain.len() {
        return Err(CheckErrors::one(format!("{label} domain is truncated")));
    }
    if &bytes[..domain.len()] != domain {
        return Err(CheckErrors::one(format!("{label} domain is invalid")));
    }

    let header_bytes = domain
        .len()
        .checked_add(COUNT_BYTES)
        .ok_or_else(|| CheckErrors::one(format!("{label} header length overflow")))?;
    if bytes.len() < header_bytes {
        return Err(CheckErrors::one(format!("{label} header is truncated")));
    }
    let record_count = u64_at(bytes, domain.len());
    if record_count > maximum_records {
        return Err(CheckErrors::one(format!(
            "{label} record count {record_count} exceeds caller maximum {maximum_records}"
        )));
    }

    let count = usize::try_from(record_count).map_err(|_| {
        CheckErrors::one(format!(
            "{label} encoded length overflow: record count does not fit usize"
        ))
    })?;
    let expected_bytes = count
        .checked_mul(record_bytes)
        .and_then(|records| header_bytes.checked_add(records))
        .ok_or_else(|| CheckErrors::one(format!("{label} encoded length overflow")))?;
    if bytes.len() != expected_bytes {
        return Err(CheckErrors::one(format!(
            "{label} length is {}; expected {expected_bytes}",
            bytes.len()
        )));
    }

    Ok((record_count, &bytes[header_bytes..]))
}

fn validate_bracket(
    begin: u64,
    end: u64,
    maximum_skew_ns: u64,
    label: &str,
    index: usize,
) -> Result<u64, CheckErrors> {
    let skew = end.checked_sub(begin).ok_or_else(|| {
        CheckErrors::one(format!(
            "{label} {index} begins at {begin} after it ends at {end}"
        ))
    })?;
    if skew > maximum_skew_ns {
        return Err(CheckErrors::one(format!(
            "{label} {index} skew {skew} ns exceeds caller maximum {maximum_skew_ns} ns"
        )));
    }
    Ok(skew)
}

fn field_u64(record: &[u8], field: usize) -> u64 {
    u64_at(record, field * 8)
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_be_bytes(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
    }

    fn sample_stream(record_count: u64) -> Vec<u8> {
        let mut bytes = Vec::from(RESOURCE_SAMPLES_DOMAIN);
        bytes.extend_from_slice(&record_count.to_be_bytes());
        for index in 0..record_count {
            let mut record = [0_u8; SAMPLE_RECORD_BYTES];
            let begin = 100 + index * 20;
            set_u64(&mut record, SAMPLE_INDEX * 8, index);
            set_u64(&mut record, SAMPLE_BEGIN_NS * 8, begin);
            set_u64(&mut record, SAMPLE_END_NS * 8, begin + 5);
            bytes.extend_from_slice(&record);
        }
        bytes
    }

    fn cut_stream(tail_tag: u8) -> Vec<u8> {
        let mut bytes = Vec::from(RESOURCE_CUTS_DOMAIN);
        bytes.extend_from_slice(&REQUIRED_CUT_RECORDS.to_be_bytes());
        for index in 0..REQUIRED_CUT_RECORDS {
            let mut record = [0_u8; CUT_RECORD_BYTES];
            record[0] = if index == 4 {
                tail_tag
            } else {
                u8::try_from(index).unwrap()
            };
            let fields = &mut record[1 + CUT_RESERVED_BYTES..];
            let begin = 100 + index * 20;
            set_u64(fields, CUT_BEGIN_NS * 8, begin);
            set_u64(fields, CUT_END_NS * 8, begin + 5);
            for (field, value) in [
                (2, 1_000 + index),
                (6, 2_000 + index),
                (3, 3_000 + index),
                (4, 4_000 + index),
                (11, 5_000 + index),
            ] {
                set_u64(fields, field * 8, value);
            }
            bytes.extend_from_slice(&record);
        }
        bytes
    }

    #[test]
    fn accepts_sample_stream_and_returns_compact_summary() {
        let summary = validate_resource_samples(&sample_stream(3), 3, 5).unwrap();
        assert_eq!(
            summary,
            ResourceSamplesSummary {
                record_count: 3,
                maximum_skew_ns: 5,
            }
        );
    }

    #[test]
    fn accepts_stable_and_deadline_cut_streams() {
        for tail_tag in [0x04, 0x05] {
            let summary = validate_resource_cuts(&cut_stream(tail_tag), 5, 5).unwrap();
            assert_eq!(summary.record_count, 5);
            assert_eq!(summary.resource_tail_tag, tail_tag);
            assert_eq!(summary.maximum_skew_ns, 5);
        }
    }

    #[test]
    fn rejects_domain_mutations_every_truncation_and_trailing_bytes() {
        let sample = sample_stream(1);
        for length in 0..sample.len() {
            assert!(validate_resource_samples(&sample[..length], 1, 5).is_err());
        }
        let cuts = cut_stream(0x04);
        for length in 0..cuts.len() {
            assert!(validate_resource_cuts(&cuts[..length], 5, 5).is_err());
        }

        for index in 0..RESOURCE_SAMPLES_DOMAIN.len() {
            let mut wrong_domain = sample.clone();
            wrong_domain[index] ^= 1;
            assert!(validate_resource_samples(&wrong_domain, 1, 5).is_err());
        }
        for index in 0..RESOURCE_CUTS_DOMAIN.len() {
            let mut wrong_domain = cuts.clone();
            wrong_domain[index] ^= 1;
            assert!(validate_resource_cuts(&wrong_domain, 5, 5).is_err());
        }

        for trailing in 1..=SAMPLE_RECORD_BYTES {
            let mut changed = sample.clone();
            changed.resize(sample.len() + trailing, 0);
            assert!(validate_resource_samples(&changed, 1, 5).is_err());
        }
        for trailing in 1..=CUT_RECORD_BYTES {
            let mut changed = cuts.clone();
            changed.resize(cuts.len() + trailing, 0);
            assert!(validate_resource_cuts(&changed, 5, 5).is_err());
        }
    }

    #[test]
    fn enforces_count_caps_and_length_overflow_while_allowing_empty_samples() {
        assert!(validate_resource_samples(&sample_stream(1), 0, 5)
            .unwrap_err()
            .to_string()
            .contains("caller maximum"));
        assert_eq!(
            validate_resource_samples(&sample_stream(0), 0, 5).unwrap(),
            ResourceSamplesSummary {
                record_count: 0,
                maximum_skew_ns: 0,
            }
        );
        assert!(validate_resource_cuts(&cut_stream(0x04), 4, 5)
            .unwrap_err()
            .to_string()
            .contains("caller maximum"));

        let mut overflow = Vec::from(RESOURCE_SAMPLES_DOMAIN);
        overflow.extend_from_slice(&u64::MAX.to_be_bytes());
        assert!(validate_resource_samples(&overflow, u64::MAX, 5)
            .unwrap_err()
            .to_string()
            .contains("overflow"));
    }

    #[test]
    fn rejects_sample_index_and_skew() {
        let mut wrong_index = sample_stream(2);
        let second = RESOURCE_SAMPLES_DOMAIN.len() + COUNT_BYTES + SAMPLE_RECORD_BYTES;
        set_u64(&mut wrong_index, second + SAMPLE_INDEX * 8, 7);
        assert!(validate_resource_samples(&wrong_index, 2, 5)
            .unwrap_err()
            .to_string()
            .contains("expected 1"));

        let mut excessive_skew = sample_stream(1);
        let first = RESOURCE_SAMPLES_DOMAIN.len() + COUNT_BYTES;
        set_u64(&mut excessive_skew, first + SAMPLE_END_NS * 8, 106);
        assert!(validate_resource_samples(&excessive_skew, 1, 5)
            .unwrap_err()
            .to_string()
            .contains("skew"));
    }

    #[test]
    fn sample_cross_record_time_and_counter_relationships_are_deferred() {
        let first = RESOURCE_SAMPLES_DOMAIN.len() + COUNT_BYTES;
        let second = first + SAMPLE_RECORD_BYTES;
        let mut samples = sample_stream(2);
        set_u64(&mut samples, second + SAMPLE_BEGIN_NS * 8, 104);
        set_u64(&mut samples, second + SAMPLE_END_NS * 8, 105);
        set_u64(&mut samples, first + 3 * 8, 1_000);
        set_u64(&mut samples, second + 3 * 8, 999);
        assert!(validate_resource_samples(&samples, 2, 5).is_ok());
    }

    #[test]
    fn rejects_cut_count_tags_reserved_bytes_and_skew() {
        let mut wrong_count = cut_stream(0x04);
        set_u64(&mut wrong_count, RESOURCE_CUTS_DOMAIN.len(), 4);
        wrong_count.truncate(RESOURCE_CUTS_DOMAIN.len() + COUNT_BYTES + 4 * CUT_RECORD_BYTES);
        assert!(validate_resource_cuts(&wrong_count, 5, 5)
            .unwrap_err()
            .to_string()
            .contains("expected 5"));

        let header = RESOURCE_CUTS_DOMAIN.len() + COUNT_BYTES;
        let mut wrong_tag = cut_stream(0x04);
        wrong_tag[header + CUT_RECORD_BYTES] = 0x02;
        assert!(validate_resource_cuts(&wrong_tag, 5, 5)
            .unwrap_err()
            .to_string()
            .contains("expected 0x01"));

        let wrong_tail_tag = cut_stream(0x06);
        assert!(validate_resource_cuts(&wrong_tail_tag, 5, 5)
            .unwrap_err()
            .to_string()
            .contains("expected 0x04 or 0x05"));

        let mut reserved = cut_stream(0x04);
        reserved[header + 1] = 1;
        assert!(validate_resource_cuts(&reserved, 5, 5)
            .unwrap_err()
            .to_string()
            .contains("reserved"));

        let mut skew = cut_stream(0x04);
        let fields = header + 1 + CUT_RESERVED_BYTES;
        set_u64(&mut skew, fields + CUT_END_NS * 8, 106);
        assert!(validate_resource_cuts(&skew, 5, 5)
            .unwrap_err()
            .to_string()
            .contains("skew"));
    }

    #[test]
    fn cut_tag_order_does_not_imply_time_or_counter_order() {
        let header = RESOURCE_CUTS_DOMAIN.len() + COUNT_BYTES;
        let mut regression = cut_stream(0x05);
        let second_fields = header + CUT_RECORD_BYTES + 1 + CUT_RESERVED_BYTES;
        set_u64(&mut regression, second_fields + CUT_BEGIN_NS * 8, 50);
        set_u64(&mut regression, second_fields + CUT_END_NS * 8, 55);
        set_u64(&mut regression, second_fields + 4 * 8, 3_999);
        assert!(validate_resource_cuts(&regression, 5, 5).is_ok());
    }

    #[test]
    fn rejects_inverted_sample_and_cut_brackets() {
        let mut inverted = sample_stream(1);
        let first = RESOURCE_SAMPLES_DOMAIN.len() + COUNT_BYTES;
        set_u64(&mut inverted, first + SAMPLE_END_NS * 8, 99);
        assert!(validate_resource_samples(&inverted, 1, 5)
            .unwrap_err()
            .to_string()
            .contains("after it ends"));

        let mut inverted = cut_stream(0x04);
        let first = RESOURCE_CUTS_DOMAIN.len() + COUNT_BYTES + 1 + CUT_RESERVED_BYTES;
        set_u64(&mut inverted, first + CUT_END_NS * 8, 99);
        assert!(validate_resource_cuts(&inverted, 5, 5)
            .unwrap_err()
            .to_string()
            .contains("after it ends"));
    }
}
