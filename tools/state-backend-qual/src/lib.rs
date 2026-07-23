#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};

use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

mod artifact_reader;

pub mod latency_samples;
pub mod mechanism_bundle;
pub mod mechanism_mapping;
pub mod mechanism_samples;
pub mod model;
pub mod model_result;
pub mod resource_samples;
pub mod workload;

#[cfg(all(test, feature = "zipf-feasibility"))]
mod zipf_candidate;

pub const NOTICE: &str = "NOT QUALIFICATION EVIDENCE";
pub const MAX_PROFILE_BYTES: usize = 1_048_576;
pub const MAX_MODEL_RESULT_BYTES: usize = 1_048_576;

const PROFILE_SCHEMA_V1: &str = include_str!("../schema/profile-v1.schema.json");
const PROFILE_SCHEMA_V2: &str = include_str!("../schema/profile-v2.schema.json");
const PROFILE_SCHEMA_V3: &str = include_str!("../schema/profile-v3.schema.json");

#[derive(Debug)]
pub struct CheckErrors {
    messages: Vec<String>,
}

impl CheckErrors {
    pub(crate) fn one(message: impl Into<String>) -> Self {
        Self {
            messages: vec![message.into()],
        }
    }

    pub(crate) fn many(mut messages: Vec<String>) -> Self {
        messages.sort();
        messages.dedup();
        Self { messages }
    }
}

impl Display for CheckErrors {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.messages.join("; "))
    }
}

impl std::error::Error for CheckErrors {}

#[derive(Debug, PartialEq, Eq)]
pub struct ProfileSummary {
    pub schema_version: String,
    pub profile_id: String,
    pub status: String,
    pub qualification_eligible: bool,
}

/// A JSON value decoder that rejects duplicate object keys at every depth.
/// `serde_json::Value` alone would silently retain the last duplicate.
struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UniqueValueVisitor;

        impl<'de> Visitor<'de> for UniqueValueVisitor {
            type Value = UniqueValue;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value without duplicate object keys")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .map(UniqueValue)
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(value.to_owned())
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value)))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                UniqueValue::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(UniqueValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(UniqueValue(Value::Array(values)))
            }

            fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = Map::new();
                while let Some(key) = object.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(A::Error::custom(format!("duplicate object key `{key}`")));
                    }
                    let UniqueValue(value) = object.next_value()?;
                    values.insert(key, value);
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }

        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

pub fn validate_profile(bytes: &[u8]) -> Result<ProfileSummary, CheckErrors> {
    let profile = validated_profile_value(bytes)?;

    Ok(ProfileSummary {
        schema_version: text_at(&profile, "/schema_version").to_owned(),
        profile_id: text_at(&profile, "/profile_id").to_owned(),
        status: text_at(&profile, "/status").to_owned(),
        qualification_eligible: bool_at(&profile, "/qualification_eligible"),
    })
}

pub(crate) fn validated_profile_value(bytes: &[u8]) -> Result<Value, CheckErrors> {
    let profile = decode_unique_json(bytes, MAX_PROFILE_BYTES, "profile")?;
    let schema_source = match profile.pointer("/schema_version").and_then(Value::as_str) {
        Some("distributed-state-qual/v1") => PROFILE_SCHEMA_V1,
        Some("distributed-state-qual/v2") => PROFILE_SCHEMA_V2,
        Some("distributed-state-qual/v3") => PROFILE_SCHEMA_V3,
        Some(version) => {
            return Err(CheckErrors::one(format!(
                "profile schema version `{version}` is unsupported"
            )))
        }
        None => {
            return Err(CheckErrors::one(
                "profile schema version is missing or invalid",
            ))
        }
    };
    let schema: Value = serde_json::from_str(schema_source)
        .map_err(|error| CheckErrors::one(format!("decode embedded schema: {error}")))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| CheckErrors::one(format!("compile embedded schema: {error}")))?;

    let schema_errors = validator
        .iter_errors(&profile)
        .map(|error| format!("schema {}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    if !schema_errors.is_empty() {
        return Err(CheckErrors::many(schema_errors));
    }

    let mut errors = Vec::new();
    reject_placeholder_strings(&profile, "", &mut errors);
    reject_non_u64_numbers(&profile, "", &mut errors);
    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }
    check_semantics(&profile, &mut errors);
    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }

    Ok(profile)
}

pub(crate) fn decode_unique_json(
    bytes: &[u8],
    maximum_bytes: usize,
    label: &str,
) -> Result<Value, CheckErrors> {
    if bytes.len() > maximum_bytes {
        return Err(CheckErrors::one(format!(
            "{label} is {} bytes; maximum is {maximum_bytes}",
            bytes.len()
        )));
    }
    let UniqueValue(value) = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode {label}: {error}")))?;
    Ok(value)
}

fn check_semantics(profile: &Value, errors: &mut Vec<String>) {
    for path in [
        "/latency_gates/resident_request_us",
        "/latency_gates/spill_request_us",
        "/latency_gates/hot_vnode_request_us",
        "/latency_gates/timer_join_range_request_us",
        "/latency_gates/state_queue_wait_us",
        "/checkpoint_gates/local_generation_freeze_us",
        "/checkpoint_gates/delta_export_ms",
        "/checkpoint_gates/sink_flush_ms",
        "/recovery_gates/owner_failover_ms",
    ] {
        check_latency(profile, path, errors);
    }

    for path in [
        "/workload/logical_state_bytes",
        "/workload/batch_rows",
        "/workload/variable_key_bytes",
        "/workload/variable_state_bytes",
        "/workload/metadata_edge_vnode_counts",
        "/workload/join_match_counts",
        "/measurement/fixed_seeds",
    ] {
        check_sorted_unique(profile, path, errors);
    }

    check_less_than(
        profile,
        "/environment/cgroup/memory_high_bytes",
        "/resource_gates/process_memory_max_bytes",
        errors,
    );
    check_less_than(
        profile,
        "/resource_gates/process_memory_max_bytes",
        "/environment/cgroup/memory_max_bytes",
        errors,
    );
    check_at_most(
        profile,
        "/environment/cgroup/memory_max_bytes",
        "/environment/physical_memory_bytes",
        errors,
    );
    check_at_most(
        profile,
        "/environment/project_quota_bytes",
        "/environment/local_nvme_bytes",
        errors,
    );
    check_at_most(
        profile,
        "/workload/target_batch_bytes",
        "/workload/hard_batch_bytes",
        errors,
    );
    check_at_most(
        profile,
        "/workload/timer_scan_max_bytes",
        "/workload/hard_batch_bytes",
        errors,
    );

    let disk_paths = [
        "/resource_gates/normal_disk_bytes",
        "/resource_gates/pressure_disk_bytes",
        "/resource_gates/hard_stop_disk_bytes",
        "/environment/project_quota_bytes",
    ];
    check_strict_chain(profile, &disk_paths, errors);

    let state_sizes = numbers_at(profile, "/workload/logical_state_bytes");
    let block_cache = number_at(profile, "/store_layout/block_cache_bytes_total");
    let physical_memory = number_at(profile, "/environment/physical_memory_bytes");
    if state_sizes.first().is_none_or(|value| *value > block_cache) {
        errors.push("resident state size must fit the total block-cache budget".to_owned());
    }
    if state_sizes
        .last()
        .is_none_or(|value| *value <= physical_memory)
    {
        errors.push("workload must include a state size larger than physical RAM".to_owned());
    }
    let concurrent_memory_envelope = checked_sum([
        block_cache,
        number_at(profile, "/store_layout/write_buffer_bytes_total"),
        number_at(profile, "/product_runtime_limits/state_queue_max_bytes"),
        number_at(profile, "/product_runtime_limits/output_buffer_max_bytes"),
        number_at(
            profile,
            "/product_runtime_limits/global_restore_scratch_bytes_max",
        ),
        number_at(
            profile,
            "/product_runtime_limits/global_encoded_restore_bytes_max",
        ),
    ]);
    if concurrent_memory_envelope
        .is_none_or(|bytes| bytes > number_at(profile, "/environment/cgroup/memory_high_bytes"))
    {
        errors.push("concurrent memory envelope exceeds cgroup memory.high".to_owned());
    }
    let largest_state = state_sizes.last().copied().unwrap_or_default();
    let space_amplification = number_at(profile, "/resource_gates/space_amplification_milli");
    if scale_ceil_milli(largest_state, space_amplification)
        .is_none_or(|required| required > number_at(profile, "/resource_gates/normal_disk_bytes"))
    {
        errors.push("largest state plus space amplification exceeds normal disk budget".to_owned());
    }

    if number_at(profile, "/workload/primary_vnode_count")
        != number_at(profile, "/store_layout/logical_vnode_count")
    {
        errors.push("workload primary vnode count must match store layout".to_owned());
    }

    let encoded_key_max = number_at(profile, "/restore_limits/encoded_key_bytes_max");
    if number_at(profile, "/workload/compact_key_bytes") > encoded_key_max
        || numbers_at(profile, "/workload/variable_key_bytes")
            .into_iter()
            .any(|bytes| bytes > encoded_key_max)
    {
        errors.push("workload key width exceeds the artifact key cap".to_owned());
    }
    let stored_state_max = number_at(profile, "/restore_limits/stored_state_bytes_max");
    if number_at(profile, "/workload/compact_state_bytes") > stored_state_max
        || numbers_at(profile, "/workload/variable_state_bytes")
            .into_iter()
            .any(|bytes| bytes > stored_state_max)
    {
        errors.push("workload state width exceeds the artifact state cap".to_owned());
    }

    check_sum(
        profile,
        &[
            "/workload/hot_distribution_permille/one_key",
            "/workload/hot_distribution_permille/nine_keys",
            "/workload/hot_distribution_permille/uniform_remainder",
        ],
        1000,
        "hot distribution",
        errors,
    );
    check_sum(
        profile,
        &[
            "/workload/timer_mix_permille/state_or_timer_mutation",
            "/workload/timer_mix_permille/bounded_due_scan",
            "/workload/timer_mix_permille/atomic_fire_delete",
        ],
        1000,
        "timer mix",
        errors,
    );
    let matches = numbers_at(profile, "/workload/join_match_counts");
    let match_weights = numbers_at(profile, "/workload/join_match_weights_permille");
    if matches.len() != match_weights.len() {
        errors.push("join match counts and weights must have equal length".to_owned());
    }
    if checked_sum(match_weights.iter().copied()) != Some(1000) {
        errors.push("join match weights must sum to 1000 permille".to_owned());
    }

    let repetitions = number_at(profile, "/measurement/paired_repetitions");
    let per_repetition = number_at(profile, "/measurement/minimum_requests_per_repetition");
    let minimum_total = number_at(profile, "/measurement/minimum_requests_total");
    if repetitions < 5
        || number_at(profile, "/measurement/warmup_seconds") < 900
        || number_at(profile, "/measurement/measured_seconds") < 1800
        || per_repetition < 200_000
        || minimum_total < 1_000_000
    {
        errors.push("measurement policy is below the frozen minimum".to_owned());
    }
    if per_repetition
        .checked_mul(repetitions)
        .is_none_or(|required| minimum_total < required)
    {
        errors.push("minimum total requests do not cover every repetition".to_owned());
    }
    let seed_count =
        u64::try_from(numbers_at(profile, "/measurement/fixed_seeds").len()).unwrap_or(u64::MAX);
    if seed_count != repetitions {
        errors.push("exactly one fixed seed is required per paired repetition".to_owned());
    }

    check_less_than(
        profile,
        "/checkpoint_gates/cadence_seconds",
        "/checkpoint_gates/deadline_seconds",
        errors,
    );
    let sink_flush_ms = number_at(profile, "/checkpoint_gates/sink_flush_ms/max");
    let deadline_ms = number_at(profile, "/checkpoint_gates/deadline_seconds").checked_mul(1000);
    if deadline_ms.is_none_or(|deadline| sink_flush_ms >= deadline) {
        errors.push("sink flush maximum must fit inside checkpoint deadline".to_owned());
    }
    check_less_than(
        profile,
        "/resource_gates/candidate_endurance_seconds",
        "/resource_gates/selected_backend_endurance_seconds",
        errors,
    );

    check_at_most(
        profile,
        "/product_runtime_limits/restore_task_scratch_bytes_max",
        "/product_runtime_limits/global_restore_scratch_bytes_max",
        errors,
    );
    check_at_most(
        profile,
        "/product_runtime_limits/transition_metadata_bytes_max",
        "/product_runtime_limits/global_restore_scratch_bytes_max",
        errors,
    );
    let task_reservation = number_at(
        profile,
        "/product_runtime_limits/restore_task_scratch_bytes_max",
    );
    let decoder_count = number_at(
        profile,
        "/product_runtime_limits/concurrent_restore_decoders_max",
    );
    let global_reservation = number_at(
        profile,
        "/product_runtime_limits/global_restore_scratch_bytes_max",
    );
    if task_reservation
        .checked_mul(decoder_count)
        .is_none_or(|reserved| reserved > global_reservation)
    {
        errors.push("concurrent restore task reservations exceed the global cap".to_owned());
    }
    let operators = number_at(
        profile,
        "/product_runtime_limits/operators_per_transition_max",
    );
    let vnodes = number_at(profile, "/product_runtime_limits/vnodes_per_transition_max");
    let pairs = number_at(profile, "/product_runtime_limits/operator_vnode_pairs_max");
    if operators
        .checked_mul(vnodes)
        .is_none_or(|maximum| pairs > maximum)
    {
        errors.push("operator-vnode pair cap exceeds the product cap".to_owned());
    }
    let artifact_bytes = number_at(profile, "/restore_limits/encoded_artifact_bytes_max");
    let chain_bytes = number_at(profile, "/restore_limits/encoded_chain_bytes_max");
    let global_encoded = number_at(
        profile,
        "/product_runtime_limits/global_encoded_restore_bytes_max",
    );
    if artifact_bytes > chain_bytes || chain_bytes > global_encoded {
        errors
            .push("artifact and chain bytes must fit the global encoded restore budget".to_owned());
    }
    for path in [
        "/restore_limits/key_bytes_per_artifact_max",
        "/restore_limits/state_bytes_per_artifact_max",
    ] {
        if number_at(profile, path) > artifact_bytes {
            errors.push(format!("{path} must fit the encoded artifact cap"));
        }
    }
    let rows_per_artifact = number_at(profile, "/restore_limits/rows_per_artifact_max");
    let rows_per_transition = number_at(profile, "/product_runtime_limits/rows_per_transition_max");
    if rows_per_artifact
        .checked_mul(pairs)
        .is_none_or(|maximum| rows_per_transition > maximum)
    {
        errors.push("transition row cap exceeds artifact/pair capacity".to_owned());
    }
    let largest_batch = numbers_at(profile, "/workload/batch_rows")
        .last()
        .copied()
        .unwrap_or_default();
    if number_at(
        profile,
        "/product_runtime_limits/output_records_per_batch_max",
    ) > largest_batch
    {
        errors.push("output record cap exceeds the largest admitted input batch".to_owned());
    }
}

fn check_latency(profile: &Value, path: &str, errors: &mut Vec<String>) {
    let values = ["p50", "p95", "p99", "p999", "max"]
        .map(|quantile| number_at(profile, &format!("{path}/{quantile}")));
    if !values.windows(2).all(|pair| pair[0] <= pair[1]) {
        errors.push(format!("{path} quantiles must be nondecreasing"));
    }
}

fn check_sorted_unique(profile: &Value, path: &str, errors: &mut Vec<String>) {
    let values = numbers_at(profile, path);
    if !values.windows(2).all(|pair| pair[0] < pair[1]) {
        errors.push(format!("{path} must be sorted and unique"));
    }
}

fn check_less_than(profile: &Value, lower: &str, upper: &str, errors: &mut Vec<String>) {
    if number_at(profile, lower) >= number_at(profile, upper) {
        errors.push(format!("{lower} must be less than {upper}"));
    }
}

fn check_at_most(profile: &Value, lower: &str, upper: &str, errors: &mut Vec<String>) {
    if number_at(profile, lower) > number_at(profile, upper) {
        errors.push(format!("{lower} must not exceed {upper}"));
    }
}

fn check_strict_chain(profile: &Value, paths: &[&str], errors: &mut Vec<String>) {
    let values = paths
        .iter()
        .map(|path| number_at(profile, path))
        .collect::<Vec<_>>();
    if !values.windows(2).all(|pair| pair[0] < pair[1]) {
        errors.push(format!("{} must be strictly ordered", paths.join(", ")));
    }
}

fn check_sum(
    profile: &Value,
    paths: &[&str],
    expected: u64,
    label: &str,
    errors: &mut Vec<String>,
) {
    if checked_sum(paths.iter().map(|path| number_at(profile, path))) != Some(expected) {
        errors.push(format!("{label} must sum to {expected} permille"));
    }
}

fn checked_sum(values: impl IntoIterator<Item = u64>) -> Option<u64> {
    values
        .into_iter()
        .try_fold(0_u64, |sum, value| sum.checked_add(value))
}

fn scale_ceil_milli(value: u64, multiplier_milli: u64) -> Option<u64> {
    value
        .checked_mul(multiplier_milli)?
        .checked_add(999)?
        .checked_div(1000)
}

pub(crate) fn reject_placeholder_strings(value: &Value, path: &str, errors: &mut Vec<String>) {
    match value {
        Value::String(text) => {
            let normalized = text.trim().to_ascii_uppercase();
            if matches!(normalized.as_str(), "TBD" | "TODO" | "UNKNOWN") {
                errors.push(format!("placeholder value at {path}"));
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                reject_placeholder_strings(child, &format!("{path}/{index}"), errors);
            }
        }
        Value::Object(values) => {
            for (key, child) in values {
                reject_placeholder_strings(child, &format!("{path}/{key}"), errors);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

pub(crate) fn reject_non_u64_numbers(value: &Value, path: &str, errors: &mut Vec<String>) {
    match value {
        Value::Number(number) if number.as_u64().is_none() => {
            errors.push(format!("non-u64 numerical value at {path}"));
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                reject_non_u64_numbers(child, &format!("{path}/{index}"), errors);
            }
        }
        Value::Object(values) => {
            for (key, child) in values {
                reject_non_u64_numbers(child, &format!("{path}/{key}"), errors);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn value_at<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    value
        .pointer(pointer)
        .unwrap_or_else(|| unreachable!("schema requires {pointer}"))
}

fn number_at(value: &Value, pointer: &str) -> u64 {
    value_at(value, pointer)
        .as_u64()
        .unwrap_or_else(|| unreachable!("schema requires unsigned integer at {pointer}"))
}

fn numbers_at(value: &Value, pointer: &str) -> Vec<u64> {
    value_at(value, pointer)
        .as_array()
        .unwrap_or_else(|| unreachable!("schema requires array at {pointer}"))
        .iter()
        .map(|item| {
            item.as_u64()
                .unwrap_or_else(|| unreachable!("schema requires unsigned integers at {pointer}"))
        })
        .collect()
}

fn text_at<'a>(value: &'a Value, pointer: &str) -> &'a str {
    value_at(value, pointer)
        .as_str()
        .unwrap_or_else(|| unreachable!("schema requires string at {pointer}"))
}

fn bool_at(value: &Value, pointer: &str) -> bool {
    value_at(value, pointer)
        .as_bool()
        .unwrap_or_else(|| unreachable!("schema requires boolean at {pointer}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE: &[u8] = include_bytes!("../profiles/linux-nvme-v1.candidate.json");
    const PROFILE_V2: &[u8] = include_bytes!("../profiles/linux-nvme-v2.candidate.json");
    const PROFILE_V3: &[u8] = include_bytes!("../profiles/linux-nvme-v3.candidate.json");

    fn profile_value() -> Value {
        serde_json::from_slice(PROFILE).unwrap()
    }

    fn mutated(mutator: impl FnOnce(&mut Value)) -> Vec<u8> {
        let mut profile = profile_value();
        mutator(&mut profile);
        serde_json::to_vec(&profile).unwrap()
    }

    fn mutated_v2(mutator: impl FnOnce(&mut Value)) -> Vec<u8> {
        let mut profile: Value = serde_json::from_slice(PROFILE_V2).unwrap();
        mutator(&mut profile);
        serde_json::to_vec(&profile).unwrap()
    }

    fn mutated_v3(mutator: impl FnOnce(&mut Value)) -> Vec<u8> {
        let mut profile: Value = serde_json::from_slice(PROFILE_V3).unwrap();
        mutator(&mut profile);
        serde_json::to_vec(&profile).unwrap()
    }

    #[test]
    fn committed_candidate_is_valid_but_ineligible() {
        let summary = validate_profile(PROFILE).unwrap();
        assert_eq!(summary.schema_version, "distributed-state-qual/v1");
        assert_eq!(summary.profile_id, "linux-nvme-v1");
        assert_eq!(summary.status, "candidate_unapproved");
        assert!(!summary.qualification_eligible);
    }

    #[test]
    fn committed_v2_candidate_is_valid_but_ineligible() {
        let summary = validate_profile(PROFILE_V2).unwrap();
        assert_eq!(summary.schema_version, "distributed-state-qual/v2");
        assert_eq!(summary.profile_id, "linux-nvme-v2");
        assert_eq!(summary.status, "candidate_unapproved");
        assert!(!summary.qualification_eligible);
    }

    #[test]
    fn committed_v3_candidate_is_valid_but_ineligible() {
        let summary = validate_profile(PROFILE_V3).unwrap();
        assert_eq!(summary.schema_version, "distributed-state-qual/v3");
        assert_eq!(summary.profile_id, "linux-nvme-v3");
        assert_eq!(summary.status, "candidate_unapproved");
        assert!(!summary.qualification_eligible);
    }

    #[test]
    fn v3_has_only_truthful_additive_field_replacements_from_v2() {
        let profile_v2: Value = serde_json::from_slice(PROFILE_V2).unwrap();
        let mut profile_v3: Value = serde_json::from_slice(PROFILE_V3).unwrap();
        profile_v3["schema_version"] = profile_v2["schema_version"].clone();
        profile_v3["profile_id"] = profile_v2["profile_id"].clone();
        let v3_gate = profile_v3["resource_gates"]
            .as_object_mut()
            .unwrap()
            .remove("target_device_io_latency_max_ms")
            .unwrap();
        profile_v3["resource_gates"]
            .as_object_mut()
            .unwrap()
            .insert("unexplained_storage_pause_max_ms".to_owned(), v3_gate);
        let measurement = profile_v3["measurement"].as_object_mut().unwrap();
        measurement.remove("open_loop_due_to_return").unwrap();
        measurement
            .remove("synthetic_coordinated_omission_correction")
            .unwrap();
        measurement.insert("coordinated_omission_corrected".to_owned(), true.into());
        assert_eq!(profile_v3, profile_v2);

        let schema_v2: Value = serde_json::from_str(PROFILE_SCHEMA_V2).unwrap();
        let mut schema_v3: Value = serde_json::from_str(PROFILE_SCHEMA_V3).unwrap();
        schema_v3["$id"] = schema_v2["$id"].clone();
        schema_v3["title"] = schema_v2["title"].clone();
        schema_v3["properties"]["schema_version"] =
            schema_v2["properties"]["schema_version"].clone();
        let required = schema_v3["$defs"]["resourceGates"]["required"]
            .as_array_mut()
            .unwrap();
        let field = required
            .iter_mut()
            .find(|field| field.as_str() == Some("target_device_io_latency_max_ms"))
            .unwrap();
        *field = "unexplained_storage_pause_max_ms".into();
        let v3_gate = schema_v3["$defs"]["resourceGates"]["properties"]
            .as_object_mut()
            .unwrap()
            .remove("target_device_io_latency_max_ms")
            .unwrap();
        schema_v3["$defs"]["resourceGates"]["properties"]
            .as_object_mut()
            .unwrap()
            .insert("unexplained_storage_pause_max_ms".to_owned(), v3_gate);
        let required = schema_v3["$defs"]["measurement"]["required"]
            .as_array_mut()
            .unwrap();
        let open_loop = required
            .iter_mut()
            .find(|field| field.as_str() == Some("open_loop_due_to_return"))
            .unwrap();
        *open_loop = "coordinated_omission_corrected".into();
        let synthetic = required
            .iter()
            .position(|field| field.as_str() == Some("synthetic_coordinated_omission_correction"))
            .unwrap();
        required.remove(synthetic);
        let measurement = schema_v3["$defs"]["measurement"]["properties"]
            .as_object_mut()
            .unwrap();
        measurement.remove("open_loop_due_to_return").unwrap();
        measurement
            .remove("synthetic_coordinated_omission_correction")
            .unwrap();
        measurement.insert(
            "coordinated_omission_corrected".to_owned(),
            serde_json::json!({"const": true}),
        );
        assert_eq!(schema_v3, schema_v2);
    }

    #[test]
    fn profile_versions_reject_each_others_resource_gate_vocabulary() {
        let bytes = mutated_v2(|profile| {
            let gates = profile["resource_gates"].as_object_mut().unwrap();
            let value = gates
                .remove("background_maintenance_debt_max_bytes")
                .unwrap();
            gates.insert("compaction_debt_max_bytes".to_owned(), value);
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated_v3(|profile| {
            let gates = profile["resource_gates"].as_object_mut().unwrap();
            let value = gates.remove("target_device_io_latency_max_ms").unwrap();
            gates.insert("unexplained_storage_pause_max_ms".to_owned(), value);
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated_v2(|profile| {
            let gates = profile["resource_gates"].as_object_mut().unwrap();
            let value = gates.remove("unexplained_storage_pause_max_ms").unwrap();
            gates.insert("target_device_io_latency_max_ms".to_owned(), value);
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            let gates = profile["resource_gates"].as_object_mut().unwrap();
            let value = gates.remove("compaction_debt_max_bytes").unwrap();
            gates.insert("background_maintenance_debt_max_bytes".to_owned(), value);
        });
        assert!(validate_profile(&bytes).is_err());
    }

    #[test]
    fn rejects_unknown_profile_schema_version_before_semantic_checks() {
        let bytes = mutated_v2(|profile| {
            profile["schema_version"] = "distributed-state-qual/v4".into();
        });
        assert!(validate_profile(&bytes)
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
    }

    #[test]
    fn rejects_claimed_eligibility_and_unapproved_owner() {
        let bytes = mutated(|profile| profile["qualification_eligible"] = Value::Bool(true));
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["approvals"]["workload_owner"] = Value::String("owner".to_owned());
        });
        assert!(validate_profile(&bytes).is_err());
    }

    #[test]
    fn rejects_unknown_missing_and_duplicate_fields() {
        let bytes = mutated(|profile| profile["measured_result"] = Value::Bool(true));
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile.as_object_mut().unwrap().remove("preflight");
        });
        assert!(validate_profile(&bytes).is_err());

        let source = String::from_utf8(PROFILE.to_vec()).unwrap();
        let duplicate = source.replacen(
            "{\n  \"schema_version\"",
            "{\n  \"notice\": \"NOT QUALIFICATION EVIDENCE\",\n  \"schema_version\"",
            1,
        );
        let error = validate_profile(duplicate.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("duplicate object key `notice`"));
    }

    #[test]
    fn rejects_non_slug_profile_identifiers() {
        let too_long = "a".repeat(65);
        for identifier in ["line\nbreak", "UPPERCASE", too_long.as_str()] {
            let bytes = mutated(|profile| profile["profile_id"] = identifier.into());
            assert!(validate_profile(&bytes).is_err(), "{identifier:?}");
        }
    }

    #[test]
    fn rejects_zero_negative_fraction_overflow_and_placeholder() {
        let bytes = mutated(|profile| profile["workload"]["target_batch_bytes"] = 0.into());
        assert!(validate_profile(&bytes).is_err());

        let mut source = String::from_utf8(PROFILE.to_vec()).unwrap();
        source = source.replacen(
            "\"target_batch_bytes\": 4194304",
            "\"target_batch_bytes\": -1",
            1,
        );
        assert!(validate_profile(source.as_bytes()).is_err());

        let mut source = String::from_utf8(PROFILE.to_vec()).unwrap();
        source = source.replacen(
            "\"target_batch_bytes\": 4194304",
            "\"target_batch_bytes\": 1.5",
            1,
        );
        assert!(validate_profile(source.as_bytes()).is_err());

        let mut source = String::from_utf8(PROFILE.to_vec()).unwrap();
        source = source.replacen(
            "\"target_batch_bytes\": 4194304",
            "\"target_batch_bytes\": 18446744073709551616",
            1,
        );
        assert!(validate_profile(source.as_bytes()).is_err());

        let bytes = mutated(|profile| profile["environment"]["provider"] = "TBD".into());
        assert!(validate_profile(&bytes).is_err());
    }

    #[test]
    fn hostile_sizes_and_semantic_arithmetic_fail_without_panicking() {
        let bytes = vec![b' '; MAX_PROFILE_BYTES + 1];
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["workload"]["join_match_weights_permille"] =
                serde_json::json!([u64::MAX, u64::MAX, 1, 1]);
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["checkpoint_gates"]["deadline_seconds"] = u64::MAX.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["measurement"]["paired_repetitions"] = u64::MAX.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["store_layout"]["block_cache_bytes_total"] = u64::MAX.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["resource_gates"]["space_amplification_milli"] = u64::MAX.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["product_runtime_limits"]["concurrent_restore_decoders_max"] = u64::MAX.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["product_runtime_limits"]["operators_per_transition_max"] = u64::MAX.into();
        });
        assert!(validate_profile(&bytes).is_err());
    }

    #[test]
    fn rejects_inverted_quantiles_and_resource_caps() {
        let bytes = mutated(|profile| {
            profile["latency_gates"]["resident_request_us"]["p95"] = 100.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["environment"]["cgroup"]["memory_high_bytes"] =
                profile["environment"]["cgroup"]["memory_max_bytes"].clone();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["resource_gates"]["pressure_disk_bytes"] =
                profile["resource_gates"]["normal_disk_bytes"].clone();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["environment"]["project_quota_bytes"] = profile["environment"]
                ["local_nvme_bytes"]
                .as_u64()
                .unwrap()
                .saturating_add(1)
                .into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["store_layout"]["block_cache_bytes_total"] = 1024.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["resource_gates"]["normal_disk_bytes"] = 107374182400_u64.into();
        });
        assert!(validate_profile(&bytes).is_err());
    }

    #[test]
    fn rejects_unsorted_or_duplicate_workload_dimensions() {
        let bytes = mutated(|profile| {
            profile["workload"]["batch_rows"] = serde_json::json!([1000, 128, 8192]);
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["workload"]["variable_key_bytes"] = serde_json::json!([16, 64, 64]);
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["measurement"]["fixed_seeds"]
                .as_array_mut()
                .unwrap()
                .push(2026072206_u64.into());
        });
        assert!(validate_profile(&bytes).is_err());
    }

    #[test]
    fn rejects_incoherent_restore_and_output_caps() {
        let bytes = mutated(|profile| {
            profile["product_runtime_limits"]["global_encoded_restore_bytes_max"] = 1048576.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["restore_limits"]["key_bytes_per_artifact_max"] = 536870913_u64.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["product_runtime_limits"]["output_records_per_batch_max"] = 8193.into();
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["product_runtime_limits"]["global_restore_scratch_bytes_max"] = u64::MAX.into();
        });
        assert!(validate_profile(&bytes)
            .unwrap_err()
            .to_string()
            .contains("concurrent memory envelope exceeds cgroup memory.high"));

        let bytes = mutated(|profile| {
            profile["product_runtime_limits"]["transition_metadata_bytes_max"] =
                2147483649_u64.into();
        });
        assert!(validate_profile(&bytes)
            .unwrap_err()
            .to_string()
            .contains("transition_metadata_bytes_max must not exceed"));

        let bytes = mutated(|profile| {
            profile["workload"]["variable_key_bytes"] = serde_json::json!([16, 64, 256, 4097]);
        });
        assert!(validate_profile(&bytes).is_err());

        let bytes = mutated(|profile| {
            profile["workload"]["variable_state_bytes"] = serde_json::json!([64, 256, 1024, 65537]);
        });
        assert!(validate_profile(&bytes).is_err());
    }
}

#[cfg(test)]
mod redb_prescreen_contract_tests {
    use super::*;

    const APPROVAL_SCHEMA: &str = include_str!("../schema/redb-prescreen-approval-v1.schema.json");
    const RESULT_SCHEMA: &str = include_str!("../schema/redb-prescreen-result-v1.schema.json");
    const APPROVAL_FIXTURE: &str =
        include_str!("../tests/fixtures/redb-prescreen-approval-v1.synthetic.json");
    const RESULT_FIXTURE: &str =
        include_str!("../tests/fixtures/redb-prescreen-result-v1.synthetic.json");

    fn strict_value(source: &str, label: &str) -> Value {
        decode_unique_json(source.as_bytes(), 1_048_576, label).unwrap()
    }

    fn checked_schema(source: &str, label: &str) -> Value {
        let schema = strict_value(source, label);
        jsonschema::draft202012::meta::validate(&schema).unwrap();
        schema
    }

    fn descriptor(role: &str, byte: char) -> Value {
        serde_json::json!({
            "role": role,
            "byte_length": 1234,
            "sha256": byte.to_string().repeat(64),
            "media_type": "application/json"
        })
    }

    #[test]
    fn redb_prescreen_schemas_and_synthetic_fixtures_are_strict_and_ineligible() {
        let approval_schema = checked_schema(APPROVAL_SCHEMA, "redb approval schema");
        let result_schema = checked_schema(RESULT_SCHEMA, "redb result schema");
        let approval = strict_value(APPROVAL_FIXTURE, "redb approval fixture");
        let result = strict_value(RESULT_FIXTURE, "redb result fixture");

        assert!(jsonschema::draft202012::is_valid(
            &approval_schema,
            &approval
        ));
        assert!(jsonschema::draft202012::is_valid(&result_schema, &result));
        assert_eq!(approval["fixture_ineligible"], true);
        assert_eq!(result["fixture_ineligible"], true);
        assert_eq!(result["disposition"], "DEFER");
        assert_eq!(approval["evidence_scope"]["production_eligible"], false);
        assert_eq!(result["evidence_scope"]["independent_soak_eligible"], false);

        let duplicate = APPROVAL_FIXTURE.replacen(
            "  \"notice\": \"NOT QUALIFICATION EVIDENCE\",",
            "  \"notice\": \"NOT QUALIFICATION EVIDENCE\",\n  \"notice\": \"NOT QUALIFICATION EVIDENCE\",",
            1,
        );
        assert!(
            decode_unique_json(duplicate.as_bytes(), 1_048_576, "duplicate approval")
                .unwrap_err()
                .to_string()
                .contains("duplicate object key `notice`")
        );
    }

    #[test]
    fn redb_prescreen_approval_keeps_native_smoke_owners_and_scope_fail_closed() {
        let schema = checked_schema(APPROVAL_SCHEMA, "redb approval schema");
        let approval = strict_value(APPROVAL_FIXTURE, "redb approval fixture");

        let mut docker = approval.clone();
        docker["run_class"] = "docker_smoke_no_decision".into();
        docker["prior_smoke_result"] = Value::Null;
        assert!(jsonschema::draft202012::is_valid(&schema, &docker));

        let mut missing_smoke = approval.clone();
        missing_smoke["prior_smoke_result"] = Value::Null;
        assert!(!jsonschema::draft202012::is_valid(&schema, &missing_smoke));

        for pointer in [
            "/evidence_scope/qualification_eligible",
            "/evidence_scope/production_eligible",
            "/evidence_scope/independent_soak_eligible",
            "/evidence_scope/checkpoint_exactly_once_eligible",
        ] {
            let mut mutation = approval.clone();
            *mutation.pointer_mut(pointer).unwrap() = true.into();
            assert!(!jsonschema::draft202012::is_valid(&schema, &mutation));
        }

        let mut collapsed_owner_namespace = approval.clone();
        collapsed_owner_namespace["owners"]["operations_owner"]["owner_id"] =
            approval["owners"]["workload_owner"]["owner_id"].clone();
        assert!(!jsonschema::draft202012::is_valid(
            &schema,
            &collapsed_owner_namespace
        ));

        let mut unknown = approval;
        unknown["execution_authorized"] = true.into();
        assert!(!jsonschema::draft202012::is_valid(&schema, &unknown));
    }

    #[test]
    fn redb_prescreen_result_separates_docker_native_bounds_and_production() {
        let schema = checked_schema(RESULT_SCHEMA, "redb result schema");
        let docker = strict_value(RESULT_FIXTURE, "redb result fixture");

        let mut docker_pass = docker.clone();
        docker_pass["disposition"] = "PRESCREEN_PASS".into();
        assert!(!jsonschema::draft202012::is_valid(&schema, &docker_pass));

        let mut synthetic_smoke_pass = docker.clone();
        synthetic_smoke_pass["disposition"] = "SMOKE_PASS".into();
        assert!(!jsonschema::draft202012::is_valid(
            &schema,
            &synthetic_smoke_pass
        ));

        let mut docker_probe = docker.clone();
        docker_probe["mechanism_probe"] =
            descriptor("redb-prescreen-bounded-mechanism-probe-result", '1');
        assert!(!jsonschema::draft202012::is_valid(&schema, &docker_probe));

        let mut native = docker.clone();
        native["record_class"] = "prescreen_record".into();
        native["fixture_ineligible"] = false.into();
        native["run_class"] = "native_prescreen_decision".into();
        native["prior_smoke_result"] = descriptor("redb-prescreen-reviewed-smoke-result", '2');
        native["mechanism_probe"] =
            descriptor("redb-prescreen-bounded-mechanism-probe-result", '3');
        native["disposition"] = "PRESCREEN_NO_GO".into();
        assert!(jsonschema::draft202012::is_valid(&schema, &native));

        let mut synthetic_native_pass = native.clone();
        synthetic_native_pass["record_class"] = "synthetic_fixture".into();
        synthetic_native_pass["fixture_ineligible"] = true.into();
        synthetic_native_pass["disposition"] = "PRESCREEN_PASS".into();
        assert!(!jsonschema::draft202012::is_valid(
            &schema,
            &synthetic_native_pass
        ));

        let mut missing_probe = native.clone();
        missing_probe["mechanism_probe"] = Value::Null;
        assert!(!jsonschema::draft202012::is_valid(&schema, &missing_probe));

        let mut bounded = native.clone();
        bounded["bounds"]["hard_bound_hit"] = true.into();
        bounded["bounds"]["hit_codes"] = serde_json::json!(["attempt_deadline"]);
        assert!(!jsonschema::draft202012::is_valid(&schema, &bounded));
        bounded["disposition"] = "DEFER".into();
        assert!(jsonschema::draft202012::is_valid(&schema, &bounded));

        for pointer in [
            "/evidence_scope/candidate_admission_eligible",
            "/evidence_scope/production_eligible",
            "/evidence_scope/independent_soak_eligible",
            "/evidence_scope/source_sink_delivery_eligible",
        ] {
            let mut mutation = native.clone();
            *mutation.pointer_mut(pointer).unwrap() = true.into();
            assert!(!jsonschema::draft202012::is_valid(&schema, &mutation));
        }

        let mut unknown = native;
        unknown["production_ready"] = true.into();
        assert!(!jsonschema::draft202012::is_valid(&schema, &unknown));
    }
}
