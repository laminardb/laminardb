use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use state_backend_qual::mechanism_bundle::validate_mechanism_bundle_path;
use state_backend_qual::mechanism_samples::{
    validate_maintenance_debt_samples_v1_reader, validate_stall_intervals_v1_reader,
    validate_target_device_io_v1_reader,
};
use state_backend_qual::resource_samples::{
    validate_common_resource_cuts_v2_reader, validate_common_resource_samples_v2_reader,
};

const PROFILE: &[u8] = include_bytes!("../profiles/linux-nvme-v3.candidate.json");
const OBSERVED_MAPPING: &[u8] = include_bytes!("fixtures/mechanism-mapping-observed-v1.json");
const DETACHED_BUNDLE_INPUT: &[u8] = include_bytes!("fixtures/mechanism-bundle-v1/bundle.json");
const DETACHED_SAMPLES: &str = include_str!("fixtures/mechanism-bundle-v1/common-samples-v2.hex");
const DETACHED_CUTS: &str = include_str!("fixtures/mechanism-bundle-v1/common-cuts-v2.hex");
const DETACHED_DEBT: &str = include_str!("fixtures/mechanism-bundle-v1/maintenance-debt-v1.hex");
const DETACHED_STALLS: &str = include_str!("fixtures/mechanism-bundle-v1/stall-intervals-v1.hex");
const DETACHED_DEVICE: &str = include_str!("fixtures/mechanism-bundle-v1/target-device-io-v1.hex");

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct FixtureDirectory(PathBuf);

impl FixtureDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "state-backend-mechanism-bundle-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self, file_name: &str) -> PathBuf {
        self.0.join(file_name)
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct BundleFixture {
    directory: FixtureDirectory,
    input: Value,
}

impl BundleFixture {
    fn detached_observed() -> Self {
        let directory = FixtureDirectory::new();
        for (file_name, bytes) in [
            ("profile.json", PROFILE.to_vec()),
            ("mapping.json", OBSERVED_MAPPING.to_vec()),
            ("common-samples.bin", decode_hex(DETACHED_SAMPLES)),
            ("common-cuts.bin", decode_hex(DETACHED_CUTS)),
            ("debt.bin", decode_hex(DETACHED_DEBT)),
            ("stalls.bin", decode_hex(DETACHED_STALLS)),
            ("device.bin", decode_hex(DETACHED_DEVICE)),
        ] {
            std::fs::write(directory.path(file_name), bytes).unwrap();
        }
        Self {
            directory,
            input: serde_json::from_slice(DETACHED_BUNDLE_INPUT).unwrap(),
        }
    }

    fn observed(device: Vec<u8>) -> Self {
        Self::from_mapping(OBSERVED_MAPPING.to_vec(), true, device)
    }

    fn not_applicable(device: Vec<u8>) -> Self {
        let mut mapping: Value = serde_json::from_slice(OBSERVED_MAPPING).unwrap();
        let descriptor = |role: &str, byte: char| {
            json!({
                "role": role,
                "byte_length": 1,
                "sha256": byte.to_string().repeat(64),
                "media_type": "application/octet-stream"
            })
        };
        mapping["background_maintenance_debt"] = json!({
            "kind": "not_applicable",
            "reason_code": "no-background-maintenance-mechanism-in-exact-build",
            "claim_scope": "complete-candidate-process",
            "source_proof": descriptor("mechanism-source-proof", 'a'),
            "configuration_proof": descriptor("mechanism-configuration-proof", 'b'),
            "bounded_probe_proof": descriptor("mechanism-bounded-probe-proof", 'c')
        });
        mapping["engine_pressure_stalls"] = json!({
            "kind": "not_applicable",
            "reason_code": "no-engine-pressure-stall-mechanism-in-exact-build",
            "claim_scope": "complete-candidate-process",
            "source_proof": descriptor("mechanism-source-proof", 'a'),
            "configuration_proof": descriptor("mechanism-configuration-proof", 'b'),
            "bounded_probe_proof": descriptor("mechanism-bounded-probe-proof", 'c')
        });
        Self::from_mapping(serde_json::to_vec_pretty(&mapping).unwrap(), false, device)
    }

    fn from_mapping(mapping: Vec<u8>, observed: bool, device: Vec<u8>) -> Self {
        let directory = FixtureDirectory::new();
        let profile_hash = sha256(PROFILE);
        let mapping_hash = sha256(&mapping);
        let samples = common_samples();
        let cuts = common_cuts(0x04);
        let debt = observed.then(|| maintenance_debt(&mapping_hash, 0x14));
        let stalls = observed.then(|| stall_intervals(&mapping_hash));

        let artifacts = [
            ("profile.json", PROFILE),
            ("mapping.json", mapping.as_slice()),
            ("common-samples.bin", samples.as_slice()),
            ("common-cuts.bin", cuts.as_slice()),
            ("device.bin", device.as_slice()),
        ];
        for (file_name, bytes) in artifacts {
            std::fs::write(directory.path(file_name), bytes).unwrap();
        }
        if let Some(bytes) = &debt {
            std::fs::write(directory.path("debt.bin"), bytes).unwrap();
        }
        if let Some(bytes) = &stalls {
            std::fs::write(directory.path("stalls.bin"), bytes).unwrap();
        }

        let input = json!({
            "schema_version": "state-backend-mechanism-bundle-validation-input/v1",
            "notice": "NOT QUALIFICATION EVIDENCE",
            "record_class": "synthetic_fixture",
            "fixture_ineligible": true,
            "status": "candidate_unapproved",
            "qualification_eligible": false,
            "validation_authorizes_execution": false,
            "bundle_id": "synthetic/mechanism-bundle-v1",
            "candidate_id": "synthetic-candidate",
            "clock": {
                "source": "CLOCK_MONOTONIC_RAW",
                "origin_reading_ns": 10_000,
                "measured_phase_start_offset_ns": 1_000_000_000_u64,
                "measured_elapsed_ns": 1_000_000_000_u64,
                "write_stop_offset_ns": 2_000_000_000_u64,
                "last_terminal_offset_ns": 1_900_000_000_u64,
                "device_capture_end_offset_ns": 2_100_000_000_u64
            },
            "limits": {
                "expected_nominal_resource_samples": 1,
                "resource_observation_skew_max_ns": 5
            },
            "target_device": {"major": 259, "minor": 0},
            "artifacts": {
                "profile": descriptor("qualification-profile", "profile.json", PROFILE),
                "mechanism_mapping": descriptor("mechanism-mapping", "mapping.json", &mapping),
                "common_resource_samples": descriptor("common-resource-samples", "common-samples.bin", &samples),
                "common_resource_cuts": descriptor("common-resource-cuts", "common-cuts.bin", &cuts),
                "maintenance_debt_samples": debt.as_ref().map(|bytes| descriptor("maintenance-debt-samples", "debt.bin", bytes)),
                "stall_intervals": stalls.as_ref().map(|bytes| descriptor("stall-intervals", "stalls.bin", bytes)),
                "target_device_io": descriptor("target-device-io", "device.bin", &device)
            }
        });

        assert_eq!(
            profile_hash,
            hex_digest("d5faf06c9c63d6334b93c05c5bb75400753afd12140184b02cc4137f4ca37b0b")
        );
        Self { directory, input }
    }

    fn write_input(&self) -> PathBuf {
        let path = self.directory.path("bundle.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&self.input).unwrap()).unwrap();
        path
    }

    fn replace_artifact(&mut self, pointer: &str, file_name: &str, bytes: &[u8]) {
        std::fs::write(self.directory.path(file_name), bytes).unwrap();
        let descriptor = self.input.pointer_mut(pointer).unwrap();
        descriptor["byte_length"] = u64::try_from(bytes.len()).unwrap().into();
        descriptor["sha256"] = hex(&sha256(bytes)).into();
    }
}

#[test]
fn detached_bundle_golden_is_valid_but_never_evidence() {
    assert_eq!(
        hex(&sha256(OBSERVED_MAPPING)),
        "89b6cbc44a7b6848efcefd81af75b04d816cc6cfd58d6ab6b83d90bb3a58f2e9"
    );
    assert_eq!(
        hex(&sha256(&decode_hex(DETACHED_SAMPLES))),
        "e278774268e094cdf74aacb3f641096766b9537650a138287a765652d1d7ad90"
    );
    assert_eq!(
        hex(&sha256(&decode_hex(DETACHED_CUTS))),
        "577b80cb81cd85f3a352c4e51697c06a9d25d46110724c0f1d6bdc6b1d6386a1"
    );
    assert_eq!(
        hex(&sha256(&decode_hex(DETACHED_DEBT))),
        "af8fb80f070e44c8e1df58cd71605fd3ccf474e638ee4eca100806813aef73ad"
    );
    assert_eq!(
        hex(&sha256(&decode_hex(DETACHED_STALLS))),
        "cb67885ce072443a57591470b345969915fab7713bbafade218a366107511bc6"
    );
    assert_eq!(
        hex(&sha256(&decode_hex(DETACHED_DEVICE))),
        "e2ef25ed790beba03a3f7a30cadabe465af7d7c343b8b8a3ff766918267439f5"
    );
    let fixture = BundleFixture::detached_observed();
    let input_path = fixture.directory.path("bundle.json");
    std::fs::write(&input_path, DETACHED_BUNDLE_INPUT).unwrap();
    let summary = validate_mechanism_bundle_path(&input_path).unwrap();
    assert_eq!(summary.observation_state, "no_adverse_signal");
    assert!(summary.candidate_fail_reasons.is_empty());
    assert_eq!(summary.maximum_debt_bytes, Some(14));
    assert_eq!(summary.stall_time_permille, Some(1));
    assert_eq!(summary.target_device_io_maximum_ms, 250);

    let output = Command::new(env!("CARGO_BIN_EXE_state-backend-qual"))
        .arg("validate-mechanism-bundle")
        .arg(input_path)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "NOT QUALIFICATION EVIDENCE\nVALID_INELIGIBLE_MECHANISM_BUNDLE \
         bundle=synthetic/mechanism-bundle-v1 profile=linux-nvme-v3 \
         mapping=synthetic-observed-v1 candidate=synthetic-candidate \
         observation_state=no_adverse_signal fail_reasons=none samples=1 debt_bytes=14 \
         stall_permille=1 device_ms=250\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn bundle_separates_adverse_candidate_signals_from_invalid_observation() {
    let deadline_device = target_device(250_000_001, 1, 0, 0);
    let mut fixture = BundleFixture::observed(deadline_device);
    let cuts = common_cuts(0x05);
    fixture.replace_artifact("/artifacts/common_resource_cuts", "common-cuts.bin", &cuts);
    let debt = maintenance_debt(&sha256(OBSERVED_MAPPING), 0x15);
    fixture.replace_artifact("/artifacts/maintenance_debt_samples", "debt.bin", &debt);
    let summary = validate_mechanism_bundle_path(&fixture.write_input()).unwrap();
    assert_eq!(summary.observation_state, "candidate_failure_signal");
    assert_eq!(
        summary.candidate_fail_reasons,
        [
            "resource-tail-deadline",
            "target-device-latency-gate",
            "target-device-error"
        ]
    );

    let mut invalid = BundleFixture::observed(target_device(1, 0, 0, 0x01));
    let error = validate_mechanism_bundle_path(&invalid.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("target device trace is invalid"));

    invalid.input["clock"]["origin_reading_ns"] = u64::MAX.into();
    let error = validate_mechanism_bundle_path(&invalid.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("absolute measured phase start clock reading overflow"));

    let mut invalid = BundleFixture::not_applicable(target_device(1, 0, 0, 0));
    invalid.input["clock"]["origin_reading_ns"] = 1.into();
    let mut samples = common_samples();
    samples[44..52].copy_from_slice(&(u64::MAX - 5).to_be_bytes());
    samples[52..60].copy_from_slice(&u64::MAX.to_be_bytes());
    invalid.replace_artifact(
        "/artifacts/common_resource_samples",
        "common-samples.bin",
        &samples,
    );
    let error = validate_mechanism_bundle_path(&invalid.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("absolute common resource sample clock reading overflow"));
}

#[test]
fn population_alignment_and_mapping_arm_presence_fail_closed() {
    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    let mut samples = common_samples();
    samples[44..52].copy_from_slice(&1_500_000_001_u64.to_be_bytes());
    samples[52..60].copy_from_slice(&1_500_000_006_u64.to_be_bytes());
    fixture.replace_artifact(
        "/artifacts/common_resource_samples",
        "common-samples.bin",
        &samples,
    );
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("nominal population does not match"));

    let fixture = BundleFixture::not_applicable(target_device(1, 0, 0, 0));
    let summary = validate_mechanism_bundle_path(&fixture.write_input()).unwrap();
    assert_eq!(summary.maximum_debt_bytes, None);
    assert_eq!(summary.stall_time_permille, None);

    let mut absent = BundleFixture::observed(target_device(1, 0, 0, 0));
    absent.input["artifacts"]["maintenance_debt_samples"] = Value::Null;
    let error = validate_mechanism_bundle_path(&absent.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("is observed but its artifact is absent"));
}

#[test]
fn descriptors_clock_device_and_tail_bindings_fail_closed() {
    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    fixture.input["artifacts"]["profile"]["sha256"] = "a".repeat(64).into();
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("qualification-profile sha256"));

    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    fixture.input["artifacts"]["target_device_io"]["role"] = "common-resource-cuts".into();
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("must have role `target-device-io`"));

    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    fixture.input["artifacts"]["target_device_io"]["file_name"] = "common-cuts.bin".into();
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("reused across roles"));

    let fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    std::fs::remove_file(fixture.directory.path("device.bin")).unwrap();
    std::fs::create_dir(fixture.directory.path("device.bin")).unwrap();
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be a regular file before it is opened"));

    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    fixture.input["clock"]["measured_elapsed_ns"] = 999_999_999_u64.into();
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("write-stop offset must be within the measured phase"));

    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    fixture.input["target_device"]["major"] = 260.into();
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("identity does not match"));

    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    fixture.input["fixture_ineligible"] = false.into();
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("fixture_ineligible"));

    let mut fixture = BundleFixture::observed(target_device(1, 0, 0, 0));
    let deadline_cuts = common_cuts(0x05);
    fixture.replace_artifact(
        "/artifacts/common_resource_cuts",
        "common-cuts.bin",
        &deadline_cuts,
    );
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("debt cut population does not match"));

    let fixture = BundleFixture::observed(target_device(600_000_000, 0, 1, 0));
    let summary = validate_mechanism_bundle_path(&fixture.write_input()).unwrap();
    assert!(summary
        .candidate_fail_reasons
        .contains(&"target-device-incomplete".to_owned()));

    let mut fixture = BundleFixture::not_applicable(target_device(1, 0, 0, 0));
    let mut cuts = common_cuts(0x04);
    set_common_cut_bracket(&mut cuts, 3, 1_999_999_999, 2_000_000_004);
    fixture.replace_artifact("/artifacts/common_resource_cuts", "common-cuts.bin", &cuts);
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("measured-end resource cut begins before measured phase end"));

    let mut fixture = BundleFixture::not_applicable(target_device(1, 0, 0, 0));
    let mut cuts = common_cuts(0x04);
    set_common_cut_bracket(&mut cuts, 2, 2_000_000_004, 2_000_000_009);
    fixture.replace_artifact("/artifacts/common_resource_cuts", "common-cuts.bin", &cuts);
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("resource cut observation 1 overlaps or follows observation 2"));

    let mut fixture = BundleFixture::not_applicable(target_device(1, 0, 0, 0));
    fixture.input["clock"]["origin_reading_ns"] = 1.into();
    let mut cuts = common_cuts(0x04);
    set_common_cut_bracket(&mut cuts, 4, u64::MAX - 5, u64::MAX);
    fixture.replace_artifact("/artifacts/common_resource_cuts", "common-cuts.bin", &cuts);
    let error = validate_mechanism_bundle_path(&fixture.write_input())
        .unwrap_err()
        .to_string();
    assert!(error.contains("absolute common resource cut 4 end clock reading overflow"));

    let tail_gate_end = 2_000_000_000_u64 + 1_800_000_000_000;
    for (end, expected_signal) in [(tail_gate_end, false), (tail_gate_end + 1, true)] {
        let mut fixture = BundleFixture::not_applicable(target_device(1, 0, 0, 0));
        let mut cuts = common_cuts(0x04);
        set_common_cut_bracket(&mut cuts, 4, end - 5, end);
        fixture.replace_artifact("/artifacts/common_resource_cuts", "common-cuts.bin", &cuts);
        let summary = validate_mechanism_bundle_path(&fixture.write_input()).unwrap();
        assert_eq!(
            summary
                .candidate_fail_reasons
                .contains(&"resource-tail-clear-gate".to_owned()),
            expected_signal
        );
    }
}

#[test]
fn streaming_readers_reject_every_truncation_and_use_small_buffers() {
    let mapping_hash = sha256(OBSERVED_MAPPING);
    let profile_hash = sha256(PROFILE);
    let samples = common_samples();
    let cuts = common_cuts(0x04);
    let debt = maintenance_debt(&mapping_hash, 0x14);
    let stalls = stall_intervals(&mapping_hash);
    let device = target_device(1, 0, 0, 0);

    for prefix in 0..samples.len() {
        assert!(validate_common_resource_samples_v2_reader(
            Cursor::new(&samples[..prefix]),
            prefix as u64,
            1,
            5,
        )
        .is_err());
    }
    for prefix in 0..cuts.len() {
        assert!(validate_common_resource_cuts_v2_reader(
            Cursor::new(&cuts[..prefix]),
            prefix as u64,
            5,
            5,
        )
        .is_err());
    }
    for prefix in 0..debt.len() {
        assert!(validate_maintenance_debt_samples_v1_reader(
            Cursor::new(&debt[..prefix]),
            prefix as u64,
            &mapping_hash,
            1,
            1,
            5,
        )
        .is_err());
    }
    for prefix in 0..stalls.len() {
        assert!(validate_stall_intervals_v1_reader(
            Cursor::new(&stalls[..prefix]),
            prefix as u64,
            &mapping_hash,
            1,
        )
        .is_err());
    }
    for prefix in 0..device.len() {
        assert!(validate_target_device_io_v1_reader(
            Cursor::new(&device[..prefix]),
            prefix as u64,
            &profile_hash,
        )
        .is_err());
    }

    let mut trailing = samples.clone();
    trailing.push(0);
    assert!(validate_common_resource_samples_v2_reader(
        Cursor::new(&trailing),
        trailing.len() as u64,
        1,
        5,
    )
    .is_err());
    let mut trailing = cuts.clone();
    trailing.push(0);
    assert!(validate_common_resource_cuts_v2_reader(
        Cursor::new(&trailing),
        trailing.len() as u64,
        5,
        5,
    )
    .is_err());
    let mut trailing = debt.clone();
    trailing.push(0);
    assert!(validate_maintenance_debt_samples_v1_reader(
        Cursor::new(&trailing),
        trailing.len() as u64,
        &mapping_hash,
        1,
        1,
        5,
    )
    .is_err());
    let mut trailing = stalls.clone();
    trailing.push(0);
    assert!(validate_stall_intervals_v1_reader(
        Cursor::new(&trailing),
        trailing.len() as u64,
        &mapping_hash,
        1,
    )
    .is_err());
    let mut trailing = device.clone();
    trailing.push(0);
    assert!(validate_target_device_io_v1_reader(
        Cursor::new(&trailing),
        trailing.len() as u64,
        &profile_hash,
    )
    .is_err());

    let mut sample_reader = TrackingReader::new(&samples, 7);
    validate_common_resource_samples_v2_reader(&mut sample_reader, samples.len() as u64, 1, 5)
        .unwrap();
    let mut cut_reader = TrackingReader::new(&cuts, 7);
    validate_common_resource_cuts_v2_reader(&mut cut_reader, cuts.len() as u64, 5, 5).unwrap();
    let mut debt_reader = TrackingReader::new(&debt, 7);
    validate_maintenance_debt_samples_v1_reader(
        &mut debt_reader,
        debt.len() as u64,
        &mapping_hash,
        1,
        1,
        5,
    )
    .unwrap();
    let mut stall_reader = TrackingReader::new(&stalls, 7);
    validate_stall_intervals_v1_reader(&mut stall_reader, stalls.len() as u64, &mapping_hash, 1)
        .unwrap();
    let mut device_reader = TrackingReader::new(&device, 7);
    validate_target_device_io_v1_reader(&mut device_reader, device.len() as u64, &profile_hash)
        .unwrap();
    for reader in [
        sample_reader,
        cut_reader,
        debt_reader,
        stall_reader,
        device_reader,
    ] {
        assert!(reader.maximum_request <= 160);
        assert!(reader.read_calls > 1);
    }
}

struct TrackingReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    chunk: usize,
    maximum_request: usize,
    read_calls: usize,
}

impl<'a> TrackingReader<'a> {
    fn new(bytes: &'a [u8], chunk: usize) -> Self {
        Self {
            bytes,
            offset: 0,
            chunk,
            maximum_request: 0,
            read_calls: 0,
        }
    }
}

impl Read for TrackingReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.maximum_request = self.maximum_request.max(output.len());
        self.read_calls += 1;
        let remaining = &self.bytes[self.offset..];
        let count = remaining.len().min(output.len()).min(self.chunk);
        output[..count].copy_from_slice(&remaining[..count]);
        self.offset += count;
        Ok(count)
    }
}

fn common_samples() -> Vec<u8> {
    let mut bytes = b"LDB-SBQ-RESOURCE-SAMPLES-V2\0".to_vec();
    append_u64(&mut bytes, 1);
    for value in [
        0,
        1_500_000_000,
        1_500_000_005,
        1_000,
        2_000,
        2_500,
        3_000,
        4_000,
        5_000,
        6_000,
        7,
        8_000,
        8_100,
        11,
        12,
        13,
        14,
        15,
        16_000,
        15_900,
    ] {
        append_u64(&mut bytes, value);
    }
    assert_eq!(bytes.len(), 196);
    bytes
}

fn common_cuts(tail_tag: u8) -> Vec<u8> {
    let mut bytes = b"LDB-SBQ-RESOURCE-CUTS-V2\0".to_vec();
    append_u64(&mut bytes, 5);
    for index in 0_u8..5 {
        let tag = if index == 4 { tail_tag } else { index };
        bytes.push(tag);
        bytes.extend_from_slice(&[0; 7]);
        let begin = common_cut_begin(usize::from(index));
        append_u64(&mut bytes, begin);
        append_u64(&mut bytes, begin + 5);
        for value in 0_u64..11 {
            append_u64(&mut bytes, 1_000 + value + u64::from(index));
        }
    }
    assert_eq!(bytes.len(), 593);
    bytes
}

fn maintenance_debt(mapping_hash: &[u8; 32], tail_tag: u8) -> Vec<u8> {
    let mut bytes = b"LDB-SBQ-MAINTENANCE-DEBT-V1\0".to_vec();
    bytes.extend_from_slice(mapping_hash);
    append_u32(&mut bytes, 1);
    append_u64(&mut bytes, 6);
    for (index, tag) in [0x00_u8, 0x10, 0x11, 0x12, 0x13, tail_tag]
        .into_iter()
        .enumerate()
    {
        bytes.push(tag);
        bytes.extend_from_slice(&[0; 7]);
        append_u64(&mut bytes, 0);
        let begin = if index == 0 {
            1_500_000_000
        } else {
            common_cut_begin(index - 1)
        };
        append_u64(&mut bytes, begin);
        append_u64(&mut bytes, begin + 5);
        append_u64(&mut bytes, 9 + index as u64);
    }
    assert_eq!(bytes.len(), 312);
    bytes
}

fn stall_intervals(mapping_hash: &[u8; 32]) -> Vec<u8> {
    let mut bytes = b"LDB-SBQ-STALL-INTERVALS-V1\0".to_vec();
    bytes.extend_from_slice(mapping_hash);
    append_u32(&mut bytes, 1);
    append_u64(&mut bytes, 1_000_000_000);
    append_u64(&mut bytes, 2_000_000_000);
    append_u64(&mut bytes, 1);
    append_u32(&mut bytes, 0);
    append_u32(&mut bytes, 0);
    append_u64(&mut bytes, 0);
    append_u64(&mut bytes, 1_200_000_000);
    append_u64(&mut bytes, 1_200_000_001);
    assert_eq!(bytes.len(), 119);
    bytes
}

fn target_device(
    duration_ns: u64,
    error_count: u64,
    incomplete_count: u64,
    anomaly_flags: u32,
) -> Vec<u8> {
    let mut bytes = b"LDB-SBQ-TARGET-DEVICE-IO-V1\0".to_vec();
    bytes.extend_from_slice(&sha256(PROFILE));
    append_u32(&mut bytes, 259);
    append_u32(&mut bytes, 0);
    append_u64(&mut bytes, 1_000_000_000);
    append_u64(&mut bytes, 2_000_000_000);
    append_u64(&mut bytes, 2_100_000_000);
    append_u32(&mut bytes, 1);
    append_u32(&mut bytes, anomaly_flags);
    append_u32(&mut bytes, 0);
    append_u32(&mut bytes, 0);
    append_u64(&mut bytes, 1);
    let success = u64::from(error_count == 0 && incomplete_count == 0);
    append_u64(&mut bytes, success);
    append_u64(&mut bytes, error_count);
    append_u64(&mut bytes, incomplete_count);
    append_u64(&mut bytes, 0);
    append_u64(&mut bytes, 0);
    append_u64(&mut bytes, 0);
    append_u64(&mut bytes, duration_ns);
    append_u64(&mut bytes, 1_500_000_000);
    let terminal = if incomplete_count == 0 {
        1_500_000_000 + duration_ns
    } else {
        2_100_000_000
    };
    append_u64(&mut bytes, terminal);
    append_u64(&mut bytes, 4_096);
    append_u64(&mut bytes, 0);
    bytes.push(0x01);
    bytes.push(if error_count != 0 {
        0x01
    } else if incomplete_count != 0 {
        0x02
    } else {
        0x00
    });
    bytes.push(1);
    bytes.extend_from_slice(&[0; 5]);
    assert_eq!(bytes.len(), 212);
    bytes
}

fn descriptor(role: &str, file_name: &str, bytes: &[u8]) -> Value {
    json!({
        "role": role,
        "file_name": file_name,
        "byte_length": bytes.len(),
        "sha256": hex(&sha256(bytes)),
        "media_type": if file_name.ends_with(".json") { "application/json" } else { "application/octet-stream" }
    })
}

fn common_cut_begin(index: usize) -> u64 {
    [
        999_999_990,
        2_000_000_000,
        2_000_000_010,
        2_000_000_020,
        2_000_000_030,
    ][index]
}

fn set_common_cut_bracket(bytes: &mut [u8], index: usize, begin: u64, end: u64) {
    let record = 33 + index * 112;
    bytes[record + 8..record + 16].copy_from_slice(&begin.to_be_bytes());
    bytes[record + 16..record + 24].copy_from_slice(&end.to_be_bytes());
}

fn append_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn append_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_digest(source: &str) -> [u8; 32] {
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&source[index * 2..index * 2 + 2], 16).unwrap();
    }
    digest
}

fn decode_hex(source: &str) -> Vec<u8> {
    let digits = source
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert!(digits.len().is_multiple_of(2));
    digits
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}
