use state_backend_qual::latency_samples::validate_latency_samples;
use state_backend_qual::resource_samples::{
    validate_common_resource_cuts_v2, validate_common_resource_samples_v2, validate_resource_cuts,
    validate_resource_samples,
};

fn append_u64s(bytes: &mut Vec<u8>, values: &[u64]) {
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
}

#[test]
fn hand_authored_synthetic_runner_wires_match_v1() {
    let mut latency = b"LDB-SBQ-LATENCY-SAMPLES-V1\0".to_vec();
    append_u64s(&mut latency, &[1]);
    append_u64s(&mut latency, &[0, 10, 20, 30, 40, 50, 60]);
    latency.extend_from_slice(&[0x0f, 0x00]);
    assert_eq!(latency.len(), 93);
    let latency_summary = validate_latency_samples(&latency, 1).unwrap();
    assert_eq!(latency_summary.record_count, 1);
    assert_eq!(latency_summary.returned_records, 1);

    let mut resources = b"LDB-SBQ-RESOURCE-SAMPLES-V1\0".to_vec();
    append_u64s(&mut resources, &[1]);
    append_u64s(
        &mut resources,
        &[
            0, 100, 105, 1_000, 2_000, 2_500, 3_000, 4_000, 5_000, 6_000, 7, 8_000, 8_100, 9_000,
            10_000, 11, 12, 13, 14, 15, 16_000, 15_900,
        ],
    );
    assert_eq!(resources.len(), 212);
    let resource_summary = validate_resource_samples(&resources, 1, 5).unwrap();
    assert_eq!(resource_summary.record_count, 1);
    assert_eq!(resource_summary.maximum_skew_ns, 5);

    let mut cuts = b"LDB-SBQ-RESOURCE-CUTS-V1\0".to_vec();
    append_u64s(&mut cuts, &[5]);
    for tag in 0_u8..=4 {
        cuts.push(tag);
        cuts.extend_from_slice(&[0; 7]);
        let begin = 100 + u64::from(tag) * 20;
        append_u64s(
            &mut cuts,
            &[
                begin,
                begin + 5,
                1_000 + u64::from(tag),
                2_000 + u64::from(tag),
                3_000 + u64::from(tag),
                4_000 + u64::from(tag),
                5_000 + u64::from(tag),
                6_000 + u64::from(tag),
                6_100 + u64::from(tag),
                7_000 + u64::from(tag),
                6_900 + u64::from(tag),
                8_000 + u64::from(tag),
                9_000 + u64::from(tag),
                10_000 + u64::from(tag),
                11_000 + u64::from(tag),
            ],
        );
    }
    assert_eq!(cuts.len(), 673);
    let cut_summary = validate_resource_cuts(&cuts, 5, 5).unwrap();
    assert_eq!(cut_summary.record_count, 5);
    assert_eq!(cut_summary.resource_tail_tag, 0x04);
}

#[test]
fn hand_authored_candidate_neutral_common_resource_wires_match_v2() {
    let mut resources = b"LDB-SBQ-RESOURCE-SAMPLES-V2\0".to_vec();
    append_u64s(&mut resources, &[1]);
    append_u64s(
        &mut resources,
        &[
            0, 100, 105, 1_000, 2_000, 2_500, 3_000, 4_000, 5_000, 6_000, 7, 8_000, 8_100, 11, 12,
            13, 14, 15, 16_000, 15_900,
        ],
    );
    assert_eq!(resources.len(), 196);
    let resource_summary = validate_common_resource_samples_v2(&resources, 1, 5).unwrap();
    assert_eq!(resource_summary.record_count, 1);
    assert_eq!(resource_summary.maximum_skew_ns, 5);

    let mut cuts = b"LDB-SBQ-RESOURCE-CUTS-V2\0".to_vec();
    append_u64s(&mut cuts, &[5]);
    for tag in 0_u8..=4 {
        cuts.push(tag);
        cuts.extend_from_slice(&[0; 7]);
        let begin = 100 + u64::from(tag) * 20;
        append_u64s(
            &mut cuts,
            &[
                begin,
                begin + 5,
                1_000 + u64::from(tag),
                2_000 + u64::from(tag),
                3_000 + u64::from(tag),
                4_000 + u64::from(tag),
                5_000 + u64::from(tag),
                6_000 + u64::from(tag),
                6_100 + u64::from(tag),
                7_000 + u64::from(tag),
                6_900 + u64::from(tag),
                10_000 + u64::from(tag),
                11_000 + u64::from(tag),
            ],
        );
    }
    assert_eq!(cuts.len(), 593);
    let cut_summary = validate_common_resource_cuts_v2(&cuts, 5, 5).unwrap();
    assert_eq!(cut_summary.record_count, 5);
    assert_eq!(cut_summary.resource_tail_tag, 0x04);
    assert_eq!(cut_summary.maximum_skew_ns, 5);
}
