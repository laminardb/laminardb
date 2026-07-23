//! Synthetic mechanism-bundle validation for a trusted, quiescent local fixture directory.
//!
//! Final entries must be regular files and are bound by exact length and SHA-256. This ineligible
//! tool is not a hostile-directory API; an approved validator requires race-free handle-relative,
//! no-follow opens in addition to the content checks implemented here.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::artifact_reader::HashingReader;
use crate::mechanism_mapping::{
    validate_mechanism_mapping, MechanismMappingSummary, MAX_MECHANISM_MAPPING_BYTES,
};
use crate::mechanism_samples::{
    validate_maintenance_debt_samples_v1_reader, validate_stall_intervals_v1_reader,
    validate_target_device_io_v1_reader, MaintenanceDebtSummary, StallIntervalsSummary,
    TargetDeviceIoSummary, MAX_MECHANISM_ARTIFACT_BYTES,
};
use crate::resource_samples::{
    validate_common_resource_cuts_v2_reader, validate_common_resource_samples_v2_reader,
    CommonResourceCutsV2Summary, CommonResourceSamplesV2Summary, ObservationBracket,
    MAX_RESOURCE_ARTIFACT_BYTES,
};
use crate::{
    decode_unique_json, reject_non_u64_numbers, reject_placeholder_strings,
    validated_profile_value, CheckErrors, MAX_PROFILE_BYTES,
};

pub const MAX_MECHANISM_BUNDLE_INPUT_BYTES: usize = 262_144;
const STREAM_BUFFER_BYTES: usize = 64 * 1024;

const SCHEMA_VERSION: &str = "state-backend-mechanism-bundle-validation-input/v1";
const INPUT_SCHEMA: &str =
    include_str!("../schema/mechanism-bundle-validation-input-v1.schema.json");

const PROFILE_ROLE: &str = "qualification-profile";
const MAPPING_ROLE: &str = "mechanism-mapping";
const COMMON_SAMPLES_ROLE: &str = "common-resource-samples";
const COMMON_CUTS_ROLE: &str = "common-resource-cuts";
const DEBT_ROLE: &str = "maintenance-debt-samples";
const STALL_ROLE: &str = "stall-intervals";
const DEVICE_ROLE: &str = "target-device-io";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MechanismBundleSummary {
    pub bundle_id: String,
    pub profile_id: String,
    pub mapping_id: String,
    pub candidate_id: String,
    pub observation_state: String,
    pub candidate_fail_reasons: Vec<String>,
    pub common_resource_samples: u64,
    pub maximum_debt_bytes: Option<u64>,
    pub stall_time_permille: Option<u64>,
    pub target_device_io_maximum_ms: u64,
}

#[derive(Clone, Debug)]
struct Descriptor {
    role: String,
    file_name: String,
    byte_length: u64,
    sha256: [u8; 32],
    media_type: String,
}

#[derive(Clone, Debug)]
struct BundleInput {
    bundle_id: String,
    candidate_id: String,
    origin_reading_ns: u64,
    measured_phase_start_offset_ns: u64,
    measured_elapsed_ns: u64,
    write_stop_offset_ns: u64,
    last_terminal_offset_ns: u64,
    device_capture_end_offset_ns: u64,
    expected_nominal_resource_samples: u64,
    resource_observation_skew_max_ns: u64,
    target_major: u32,
    target_minor: u32,
    profile: Descriptor,
    mapping: Descriptor,
    common_samples: Descriptor,
    common_cuts: Descriptor,
    debt: Option<Descriptor>,
    stalls: Option<Descriptor>,
    device: Descriptor,
}

/// Validates an explicitly ineligible content-addressed mechanism bundle.
///
/// This command validates wire and cross-artifact consistency only. It cannot approve a runner
/// plan, source proof, candidate execution, backend selection, or production admission.
pub fn validate_mechanism_bundle_path(
    input_path: &Path,
) -> Result<MechanismBundleSummary, CheckErrors> {
    let input_bytes = read_small_file(
        input_path,
        u64::try_from(MAX_MECHANISM_BUNDLE_INPUT_BYTES)
            .map_err(|_| CheckErrors::one("bundle input maximum does not fit u64"))?,
        "mechanism bundle validation input",
    )?;
    let input = decode_input(&input_bytes)?;
    let base = input_path.parent().unwrap_or_else(|| Path::new("."));

    validate_descriptor_roles(&input)?;
    validate_unique_file_names(&input)?;

    let profile_bytes = read_bound_descriptor(base, &input.profile, MAX_PROFILE_BYTES as u64)?;
    let profile = validated_profile_value(&profile_bytes)?;
    if text(&profile, "/schema_version") != "distributed-state-qual/v3" {
        return Err(CheckErrors::one(
            "mechanism bundle requires an exact distributed-state-qual/v3 profile",
        ));
    }
    let profile_id = text(&profile, "/profile_id").to_owned();
    let profile_sha256: [u8; 32] = Sha256::digest(&profile_bytes).into();

    let mapping_bytes = read_bound_descriptor(
        base,
        &input.mapping,
        u64::try_from(MAX_MECHANISM_MAPPING_BYTES)
            .map_err(|_| CheckErrors::one("mapping maximum does not fit u64"))?,
    )?;
    let mapping_sha256: [u8; 32] = Sha256::digest(&mapping_bytes).into();
    let mapping = validate_mechanism_mapping(&profile_bytes, &mapping_bytes)?;
    if mapping.candidate_id != input.candidate_id {
        return Err(CheckErrors::one(
            "bundle candidate_id does not match the mechanism mapping",
        ));
    }

    validate_arm_presence(&mapping, &input)?;

    let common_samples = validate_common_samples(base, &input)?;
    if common_samples.record_count != input.expected_nominal_resource_samples {
        return Err(CheckErrors::one(format!(
            "common resource sample count is {}; expected {} from the validation input",
            common_samples.record_count, input.expected_nominal_resource_samples
        )));
    }
    let common_cuts = validate_common_cuts(base, &input)?;

    let debt = input
        .debt
        .as_ref()
        .map(|descriptor| validate_debt(base, descriptor, &mapping_sha256, &mapping, &input))
        .transpose()?;
    if let Some(debt) = debt {
        if debt.nominal_population_sha256 != common_samples.observation_population_sha256 {
            return Err(CheckErrors::one(
                "maintenance debt nominal population does not match common resource samples",
            ));
        }
        if debt.cut_population_sha256 != common_cuts.observation_population_sha256 {
            return Err(CheckErrors::one(
                "maintenance debt cut population does not match common resource cuts",
            ));
        }
        if debt.normalized_tail_tag != common_cuts.resource_tail_tag {
            return Err(CheckErrors::one(
                "maintenance debt tail disposition does not match common resource cuts",
            ));
        }
    }

    let expected_measurement_end = input
        .measured_phase_start_offset_ns
        .checked_add(input.measured_elapsed_ns)
        .ok_or_else(|| CheckErrors::one("measured phase end offset overflow"))?;
    validate_claimed_timeline(&input, expected_measurement_end)?;
    validate_cut_chronology(
        &common_cuts.observation_brackets,
        &input,
        expected_measurement_end,
    )?;
    validate_artifact_clock_bounds(&input, &common_samples, &common_cuts)?;
    if input.device_capture_end_offset_ns < expected_measurement_end {
        return Err(CheckErrors::one(
            "device capture end precedes measured phase end",
        ));
    }

    let stalls = input
        .stalls
        .as_ref()
        .map(|descriptor| validate_stalls(base, descriptor, &mapping_sha256, &mapping, &input))
        .transpose()?;
    if let Some(stalls) = stalls {
        require_measurement_window(
            stalls.measurement_start_offset_ns,
            stalls.measurement_end_offset_ns,
            input.measured_phase_start_offset_ns,
            expected_measurement_end,
            "stall intervals",
        )?;
    }

    let device = validate_device(base, &input.device, &profile_sha256)?;
    require_measurement_window(
        device.measurement_start_offset_ns,
        device.measurement_end_offset_ns,
        input.measured_phase_start_offset_ns,
        expected_measurement_end,
        "target device I/O",
    )?;
    if device.capture_end_offset_ns != input.device_capture_end_offset_ns {
        return Err(CheckErrors::one(
            "target device I/O capture end does not match the validation input",
        ));
    }
    if device.target_major != input.target_major || device.target_minor != input.target_minor {
        return Err(CheckErrors::one(
            "target device I/O identity does not match the validation input",
        ));
    }
    if device.trace_anomaly_flags != 0 || device.trace_anomaly_count != 0 {
        return Err(CheckErrors::one(format!(
            "target device trace is invalid: flags=0x{:02x}, anomaly_count={}",
            device.trace_anomaly_flags, device.trace_anomaly_count
        )));
    }

    let mut candidate_fail_reasons = Vec::new();
    if common_cuts.resource_tail_tag == 0x05 {
        candidate_fail_reasons.push("resource-tail-deadline".to_owned());
    }
    let tail_end = common_cuts.observation_brackets[4].end_offset_ns;
    let tail_duration_ns = tail_end
        .checked_sub(input.write_stop_offset_ns)
        .ok_or_else(|| CheckErrors::one("resource tail ends before write stop"))?;
    let tail_gate_ns = u64_value(&profile, "/resource_gates/resource_tail_clear_max_seconds")
        .checked_mul(1_000_000_000)
        .ok_or_else(|| CheckErrors::one("resource tail clear gate overflows nanoseconds"))?;
    if tail_duration_ns > tail_gate_ns {
        candidate_fail_reasons.push("resource-tail-clear-gate".to_owned());
    }
    let debt_gate = u64_value(
        &profile,
        "/resource_gates/background_maintenance_debt_max_bytes",
    );
    if debt.is_some_and(|summary| summary.maximum_total_debt_bytes > debt_gate) {
        candidate_fail_reasons.push("background-maintenance-debt-gate".to_owned());
    }
    let stall_gate = u64_value(
        &profile,
        "/resource_gates/engine_pressure_stall_time_max_permille",
    );
    if stalls.is_some_and(|summary| summary.stall_time_permille > stall_gate) {
        candidate_fail_reasons.push("engine-pressure-stall-gate".to_owned());
    }
    let device_gate = u64_value(&profile, "/resource_gates/target_device_io_latency_max_ms");
    if device.maximum_issue_to_terminal_ms > device_gate {
        candidate_fail_reasons.push("target-device-latency-gate".to_owned());
    }
    if device.error_count != 0 {
        candidate_fail_reasons.push("target-device-error".to_owned());
    }
    if device.incomplete_count != 0 {
        candidate_fail_reasons.push("target-device-incomplete".to_owned());
    }

    Ok(MechanismBundleSummary {
        bundle_id: input.bundle_id,
        profile_id,
        mapping_id: mapping.mapping_id,
        candidate_id: input.candidate_id,
        observation_state: if candidate_fail_reasons.is_empty() {
            "no_adverse_signal".to_owned()
        } else {
            "candidate_failure_signal".to_owned()
        },
        candidate_fail_reasons,
        common_resource_samples: common_samples.record_count,
        maximum_debt_bytes: debt.map(|summary| summary.maximum_total_debt_bytes),
        stall_time_permille: stalls.map(|summary| summary.stall_time_permille),
        target_device_io_maximum_ms: device.maximum_issue_to_terminal_ms,
    })
}

fn decode_input(bytes: &[u8]) -> Result<BundleInput, CheckErrors> {
    let value = decode_unique_json(
        bytes,
        MAX_MECHANISM_BUNDLE_INPUT_BYTES,
        "mechanism bundle validation input",
    )?;
    let schema: Value = serde_json::from_str(INPUT_SCHEMA)
        .map_err(|error| CheckErrors::one(format!("decode embedded bundle schema: {error}")))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| CheckErrors::one(format!("compile embedded bundle schema: {error}")))?;
    let schema_errors = validator
        .iter_errors(&value)
        .map(|error| format!("schema {}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    if !schema_errors.is_empty() {
        return Err(CheckErrors::many(schema_errors));
    }
    let mut errors = Vec::new();
    reject_placeholder_strings(&value, "", &mut errors);
    reject_non_u64_numbers(&value, "", &mut errors);
    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }
    if text(&value, "/schema_version") != SCHEMA_VERSION {
        return Err(CheckErrors::one("mechanism bundle schema version mismatch"));
    }

    Ok(BundleInput {
        bundle_id: text(&value, "/bundle_id").to_owned(),
        candidate_id: text(&value, "/candidate_id").to_owned(),
        origin_reading_ns: u64_value(&value, "/clock/origin_reading_ns"),
        measured_phase_start_offset_ns: u64_value(&value, "/clock/measured_phase_start_offset_ns"),
        measured_elapsed_ns: u64_value(&value, "/clock/measured_elapsed_ns"),
        write_stop_offset_ns: u64_value(&value, "/clock/write_stop_offset_ns"),
        last_terminal_offset_ns: u64_value(&value, "/clock/last_terminal_offset_ns"),
        device_capture_end_offset_ns: u64_value(&value, "/clock/device_capture_end_offset_ns"),
        expected_nominal_resource_samples: u64_value(
            &value,
            "/limits/expected_nominal_resource_samples",
        ),
        resource_observation_skew_max_ns: u64_value(
            &value,
            "/limits/resource_observation_skew_max_ns",
        ),
        target_major: u32_value(&value, "/target_device/major")?,
        target_minor: u32_value(&value, "/target_device/minor")?,
        profile: descriptor(&value, "/artifacts/profile")?,
        mapping: descriptor(&value, "/artifacts/mechanism_mapping")?,
        common_samples: descriptor(&value, "/artifacts/common_resource_samples")?,
        common_cuts: descriptor(&value, "/artifacts/common_resource_cuts")?,
        debt: optional_descriptor(&value, "/artifacts/maintenance_debt_samples")?,
        stalls: optional_descriptor(&value, "/artifacts/stall_intervals")?,
        device: descriptor(&value, "/artifacts/target_device_io")?,
    })
}

fn validate_common_samples(
    base: &Path,
    input: &BundleInput,
) -> Result<CommonResourceSamplesV2Summary, CheckErrors> {
    validate_stream_descriptor(
        base,
        &input.common_samples,
        MAX_RESOURCE_ARTIFACT_BYTES,
        |reader| {
            validate_common_resource_samples_v2_reader(
                reader,
                input.common_samples.byte_length,
                input.expected_nominal_resource_samples,
                input.resource_observation_skew_max_ns,
            )
        },
    )
}

fn validate_common_cuts(
    base: &Path,
    input: &BundleInput,
) -> Result<CommonResourceCutsV2Summary, CheckErrors> {
    validate_stream_descriptor(
        base,
        &input.common_cuts,
        MAX_RESOURCE_ARTIFACT_BYTES,
        |reader| {
            validate_common_resource_cuts_v2_reader(
                reader,
                input.common_cuts.byte_length,
                5,
                input.resource_observation_skew_max_ns,
            )
        },
    )
}

fn validate_debt(
    base: &Path,
    descriptor: &Descriptor,
    mapping_sha256: &[u8; 32],
    mapping: &MechanismMappingSummary,
    input: &BundleInput,
) -> Result<MaintenanceDebtSummary, CheckErrors> {
    let mechanism_count = u32::try_from(mapping.debt_mechanism_count)
        .map_err(|_| CheckErrors::one("debt mechanism count does not fit u32"))?;
    validate_stream_descriptor(base, descriptor, MAX_MECHANISM_ARTIFACT_BYTES, |reader| {
        validate_maintenance_debt_samples_v1_reader(
            reader,
            descriptor.byte_length,
            mapping_sha256,
            mechanism_count,
            input.expected_nominal_resource_samples,
            input.resource_observation_skew_max_ns,
        )
    })
}

fn validate_stalls(
    base: &Path,
    descriptor: &Descriptor,
    mapping_sha256: &[u8; 32],
    mapping: &MechanismMappingSummary,
    _input: &BundleInput,
) -> Result<StallIntervalsSummary, CheckErrors> {
    let mechanism_count = u32::try_from(mapping.stall_mechanism_count)
        .map_err(|_| CheckErrors::one("stall mechanism count does not fit u32"))?;
    validate_stream_descriptor(base, descriptor, MAX_MECHANISM_ARTIFACT_BYTES, |reader| {
        validate_stall_intervals_v1_reader(
            reader,
            descriptor.byte_length,
            mapping_sha256,
            mechanism_count,
        )
    })
}

fn validate_device(
    base: &Path,
    descriptor: &Descriptor,
    profile_sha256: &[u8; 32],
) -> Result<TargetDeviceIoSummary, CheckErrors> {
    validate_stream_descriptor(base, descriptor, MAX_MECHANISM_ARTIFACT_BYTES, |reader| {
        validate_target_device_io_v1_reader(reader, descriptor.byte_length, profile_sha256)
    })
}

fn validate_stream_descriptor<T>(
    base: &Path,
    descriptor: &Descriptor,
    maximum_bytes: u64,
    validate: impl FnOnce(&mut HashingReader<BufReader<File>>) -> Result<T, CheckErrors>,
) -> Result<T, CheckErrors> {
    let file = open_bound_file(base, descriptor, maximum_bytes)?;
    let mut reader = HashingReader::new(BufReader::with_capacity(STREAM_BUFFER_BYTES, file));
    let summary = validate(&mut reader)?;
    let (sha256, bytes_read) = reader.finish();
    if bytes_read != descriptor.byte_length {
        return Err(CheckErrors::one(format!(
            "{} parser read {bytes_read} bytes; descriptor declares {}",
            descriptor.role, descriptor.byte_length
        )));
    }
    if sha256 != descriptor.sha256 {
        return Err(CheckErrors::one(format!(
            "{} sha256 does not match its descriptor",
            descriptor.role
        )));
    }
    Ok(summary)
}

fn read_bound_descriptor(
    base: &Path,
    descriptor: &Descriptor,
    maximum_bytes: u64,
) -> Result<Vec<u8>, CheckErrors> {
    let file = open_bound_file(base, descriptor, maximum_bytes)?;
    let capacity = usize::try_from(descriptor.byte_length)
        .map_err(|_| CheckErrors::one(format!("{} length does not fit usize", descriptor.role)))?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut reader = HashingReader::new(file);
    (&mut reader)
        .take(descriptor.byte_length + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| CheckErrors::one(format!("read {}: {error}", descriptor.role)))?;
    let (sha256, bytes_read) = reader.finish();
    if bytes_read != descriptor.byte_length || bytes.len() != capacity {
        return Err(CheckErrors::one(format!(
            "{} changed length while it was read",
            descriptor.role
        )));
    }
    if sha256 != descriptor.sha256 {
        return Err(CheckErrors::one(format!(
            "{} sha256 does not match its descriptor",
            descriptor.role
        )));
    }
    Ok(bytes)
}

fn open_bound_file(
    base: &Path,
    descriptor: &Descriptor,
    maximum_bytes: u64,
) -> Result<File, CheckErrors> {
    if descriptor.byte_length > maximum_bytes {
        return Err(CheckErrors::one(format!(
            "{} is {} bytes; maximum is {maximum_bytes}",
            descriptor.role, descriptor.byte_length
        )));
    }
    let path = artifact_path(base, &descriptor.file_name);
    let path_metadata = std::fs::symlink_metadata(&path)
        .map_err(|error| CheckErrors::one(format!("stat {}: {error}", path.display())))?;
    if path_metadata.file_type().is_symlink() || is_windows_reparse_point(&path_metadata) {
        return Err(CheckErrors::one(format!(
            "{} must not be a symlink or reparse point",
            path.display()
        )));
    }
    if !path_metadata.is_file() {
        return Err(CheckErrors::one(format!(
            "{} must be a regular file before it is opened",
            path.display()
        )));
    }
    let file = File::open(&path)
        .map_err(|error| CheckErrors::one(format!("open {}: {error}", path.display())))?;
    let metadata = file
        .metadata()
        .map_err(|error| CheckErrors::one(format!("stat {}: {error}", path.display())))?;
    if !metadata.is_file() {
        return Err(CheckErrors::one(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    if metadata.len() != descriptor.byte_length {
        return Err(CheckErrors::one(format!(
            "{} is {} bytes; descriptor declares {}",
            path.display(),
            metadata.len(),
            descriptor.byte_length
        )));
    }
    Ok(file)
}

fn read_small_file(path: &Path, maximum_bytes: u64, label: &str) -> Result<Vec<u8>, CheckErrors> {
    let path_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| CheckErrors::one(format!("stat {}: {error}", path.display())))?;
    if path_metadata.file_type().is_symlink() || is_windows_reparse_point(&path_metadata) {
        return Err(CheckErrors::one(format!(
            "{label} must not be a symlink or reparse point"
        )));
    }
    if !path_metadata.is_file() {
        return Err(CheckErrors::one(format!(
            "{label} must be a regular file before it is opened"
        )));
    }
    let file = File::open(path)
        .map_err(|error| CheckErrors::one(format!("open {}: {error}", path.display())))?;
    let metadata = file
        .metadata()
        .map_err(|error| CheckErrors::one(format!("stat {}: {error}", path.display())))?;
    if !metadata.is_file() || metadata.len() > maximum_bytes {
        return Err(CheckErrors::one(format!(
            "{label} is not a regular file within the {maximum_bytes}-byte cap"
        )));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| CheckErrors::one(format!("{label} length does not fit usize")))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| CheckErrors::one(format!("read {}: {error}", path.display())))?;
    if bytes.len() != capacity {
        return Err(CheckErrors::one(format!(
            "{label} changed length while it was read"
        )));
    }
    Ok(bytes)
}

fn validate_descriptor_roles(input: &BundleInput) -> Result<(), CheckErrors> {
    for (descriptor, role, media_type) in [
        (&input.profile, PROFILE_ROLE, "application/json"),
        (&input.mapping, MAPPING_ROLE, "application/json"),
        (
            &input.common_samples,
            COMMON_SAMPLES_ROLE,
            "application/octet-stream",
        ),
        (
            &input.common_cuts,
            COMMON_CUTS_ROLE,
            "application/octet-stream",
        ),
        (&input.device, DEVICE_ROLE, "application/octet-stream"),
    ] {
        require_descriptor_role(descriptor, role, media_type)?;
    }
    if let Some(descriptor) = &input.debt {
        require_descriptor_role(descriptor, DEBT_ROLE, "application/octet-stream")?;
    }
    if let Some(descriptor) = &input.stalls {
        require_descriptor_role(descriptor, STALL_ROLE, "application/octet-stream")?;
    }
    Ok(())
}

fn require_descriptor_role(
    descriptor: &Descriptor,
    expected_role: &str,
    expected_media_type: &str,
) -> Result<(), CheckErrors> {
    if descriptor.role != expected_role || descriptor.media_type != expected_media_type {
        return Err(CheckErrors::one(format!(
            "descriptor {} must have role `{expected_role}` and media type `{expected_media_type}`",
            descriptor.file_name
        )));
    }
    Ok(())
}

fn validate_unique_file_names(input: &BundleInput) -> Result<(), CheckErrors> {
    let descriptors = [
        Some(&input.profile),
        Some(&input.mapping),
        Some(&input.common_samples),
        Some(&input.common_cuts),
        input.debt.as_ref(),
        input.stalls.as_ref(),
        Some(&input.device),
    ];
    let mut names = BTreeSet::new();
    for descriptor in descriptors.into_iter().flatten() {
        if !names.insert(descriptor.file_name.as_str()) {
            return Err(CheckErrors::one(format!(
                "artifact file name `{}` is reused across roles",
                descriptor.file_name
            )));
        }
    }
    Ok(())
}

fn validate_arm_presence(
    mapping: &MechanismMappingSummary,
    input: &BundleInput,
) -> Result<(), CheckErrors> {
    require_arm_presence(
        "background_maintenance_debt",
        &mapping.background_maintenance_debt_kind,
        input.debt.is_some(),
    )?;
    require_arm_presence(
        "engine_pressure_stalls",
        &mapping.engine_pressure_stalls_kind,
        input.stalls.is_some(),
    )
}

fn require_arm_presence(label: &str, kind: &str, present: bool) -> Result<(), CheckErrors> {
    match (kind, present) {
        ("observed", true) | ("not_applicable", false) => Ok(()),
        ("observed", false) => Err(CheckErrors::one(format!(
            "{label} is observed but its artifact is absent"
        ))),
        ("not_applicable", true) => Err(CheckErrors::one(format!(
            "{label} is not_applicable but an artifact is present"
        ))),
        _ => Err(CheckErrors::one(format!(
            "{label} has unsupported mapping kind `{kind}`"
        ))),
    }
}

fn require_measurement_window(
    actual_start: u64,
    actual_end: u64,
    expected_start: u64,
    expected_end: u64,
    label: &str,
) -> Result<(), CheckErrors> {
    if actual_start != expected_start || actual_end != expected_end {
        return Err(CheckErrors::one(format!(
            "{label} measurement window [{actual_start},{actual_end}) does not match [{expected_start},{expected_end})"
        )));
    }
    Ok(())
}

fn validate_claimed_timeline(
    input: &BundleInput,
    expected_measurement_end: u64,
) -> Result<(), CheckErrors> {
    if input.write_stop_offset_ns < input.measured_phase_start_offset_ns
        || input.write_stop_offset_ns > expected_measurement_end
    {
        return Err(CheckErrors::one(
            "write-stop offset must be within the measured phase",
        ));
    }
    if input.last_terminal_offset_ns < input.measured_phase_start_offset_ns
        || input.last_terminal_offset_ns > expected_measurement_end
    {
        return Err(CheckErrors::one(
            "last-terminal offset must be within the measured phase",
        ));
    }
    for (label, offset) in [
        ("measured phase start", input.measured_phase_start_offset_ns),
        ("measured phase end", expected_measurement_end),
        ("write stop", input.write_stop_offset_ns),
        ("last terminal", input.last_terminal_offset_ns),
        ("device capture", input.device_capture_end_offset_ns),
    ] {
        input
            .origin_reading_ns
            .checked_add(offset)
            .ok_or_else(|| CheckErrors::one(format!("absolute {label} clock reading overflow")))?;
    }
    Ok(())
}

fn validate_cut_chronology(
    cuts: &[ObservationBracket; 5],
    input: &BundleInput,
    expected_measurement_end: u64,
) -> Result<(), CheckErrors> {
    if cuts[0].end_offset_ns > input.measured_phase_start_offset_ns {
        return Err(CheckErrors::one(
            "pre-measurement resource cut extends into the measured phase",
        ));
    }
    if cuts[1].begin_offset_ns < input.write_stop_offset_ns {
        return Err(CheckErrors::one(
            "write-stop resource cut begins before the claimed write-stop event",
        ));
    }
    if cuts[2].begin_offset_ns < input.last_terminal_offset_ns {
        return Err(CheckErrors::one(
            "last-terminal resource cut begins before the claimed last terminal",
        ));
    }
    if cuts[3].begin_offset_ns < expected_measurement_end {
        return Err(CheckErrors::one(
            "measured-end resource cut begins before measured phase end",
        ));
    }
    if cuts[4].begin_offset_ns < cuts[3].end_offset_ns {
        return Err(CheckErrors::one(
            "resource-tail cut begins before the measured-end observation completes",
        ));
    }
    for (index, pair) in cuts.windows(2).enumerate() {
        if pair[0].end_offset_ns > pair[1].begin_offset_ns {
            return Err(CheckErrors::one(format!(
                "resource cut observation {index} overlaps or follows observation {}",
                index + 1
            )));
        }
    }
    Ok(())
}

fn validate_artifact_clock_bounds(
    input: &BundleInput,
    samples: &CommonResourceSamplesV2Summary,
    cuts: &CommonResourceCutsV2Summary,
) -> Result<(), CheckErrors> {
    input
        .origin_reading_ns
        .checked_add(samples.maximum_observation_end_offset_ns)
        .ok_or_else(|| {
            CheckErrors::one("absolute common resource sample clock reading overflow")
        })?;
    for (index, bracket) in cuts.observation_brackets.iter().enumerate() {
        for (edge, offset) in [
            ("begin", bracket.begin_offset_ns),
            ("end", bracket.end_offset_ns),
        ] {
            input.origin_reading_ns.checked_add(offset).ok_or_else(|| {
                CheckErrors::one(format!(
                    "absolute common resource cut {index} {edge} clock reading overflow"
                ))
            })?;
        }
    }

    // Debt brackets are bound to these common populations by their domain-separated digests.
    // Stall offsets are <= measurement_end; device issue/terminal offsets are <= capture_end.
    Ok(())
}

fn descriptor(value: &Value, pointer: &str) -> Result<Descriptor, CheckErrors> {
    let object = value
        .pointer(pointer)
        .ok_or_else(|| CheckErrors::one(format!("missing descriptor at {pointer}")))?;
    Ok(Descriptor {
        role: text(object, "/role").to_owned(),
        file_name: text(object, "/file_name").to_owned(),
        byte_length: u64_value(object, "/byte_length"),
        sha256: parse_sha256(text(object, "/sha256"))?,
        media_type: text(object, "/media_type").to_owned(),
    })
}

fn optional_descriptor(value: &Value, pointer: &str) -> Result<Option<Descriptor>, CheckErrors> {
    match value.pointer(pointer) {
        Some(Value::Null) => Ok(None),
        Some(_) => descriptor(value, pointer).map(Some),
        None => Err(CheckErrors::one(format!(
            "missing optional descriptor at {pointer}"
        ))),
    }
}

fn parse_sha256(source: &str) -> Result<[u8; 32], CheckErrors> {
    if source.len() != 64 {
        return Err(CheckErrors::one("sha256 must have 64 lowercase hex digits"));
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&source[offset..offset + 2], 16)
            .map_err(|_| CheckErrors::one("sha256 contains invalid hex"))?;
    }
    Ok(digest)
}

fn artifact_path(base: &Path, file_name: &str) -> PathBuf {
    base.join(file_name)
}

#[cfg(windows)]
fn is_windows_reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_windows_reparse_point(_metadata: &std::fs::Metadata) -> bool {
    false
}

fn text<'a>(value: &'a Value, pointer: &str) -> &'a str {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_else(|| unreachable!("schema requires string at {pointer}"))
}

fn u64_value(value: &Value, pointer: &str) -> u64 {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| unreachable!("schema requires u64 at {pointer}"))
}

fn u32_value(value: &Value, pointer: &str) -> Result<u32, CheckErrors> {
    u32::try_from(u64_value(value, pointer))
        .map_err(|_| CheckErrors::one(format!("value at {pointer} does not fit u32")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_validation_input_schema_is_valid_and_ineligible() {
        let schema: Value = serde_json::from_str(INPUT_SCHEMA).unwrap();
        jsonschema::draft202012::meta::validate(&schema).unwrap();
        assert_eq!(
            schema["properties"]["record_class"]["const"],
            "synthetic_fixture"
        );
        assert_eq!(
            schema["properties"]["qualification_eligible"]["const"],
            false
        );
        assert_eq!(
            schema["properties"]["validation_authorizes_execution"]["const"],
            false
        );
    }
}
