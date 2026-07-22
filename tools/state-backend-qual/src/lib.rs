#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};

use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

pub const NOTICE: &str = "NOT QUALIFICATION EVIDENCE";

const PROFILE_SCHEMA: &str = include_str!("../schema/profile-v1.schema.json");
const MAX_PROFILE_BYTES: usize = 1_048_576;

#[derive(Debug)]
pub struct CheckErrors {
    messages: Vec<String>,
}

impl CheckErrors {
    fn one(message: impl Into<String>) -> Self {
        Self {
            messages: vec![message.into()],
        }
    }

    fn many(mut messages: Vec<String>) -> Self {
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
    if bytes.len() > MAX_PROFILE_BYTES {
        return Err(CheckErrors::one(format!(
            "profile is {} bytes; maximum is {MAX_PROFILE_BYTES}",
            bytes.len()
        )));
    }
    let UniqueValue(profile) = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode profile: {error}")))?;
    let schema: Value = serde_json::from_str(PROFILE_SCHEMA)
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

    Ok(ProfileSummary {
        schema_version: text_at(&profile, "/schema_version").to_owned(),
        profile_id: text_at(&profile, "/profile_id").to_owned(),
        status: text_at(&profile, "/status").to_owned(),
        qualification_eligible: bool_at(&profile, "/qualification_eligible"),
    })
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
    let memory_max = number_at(profile, "/environment/cgroup/memory_max_bytes");
    let physical_memory = number_at(profile, "/environment/physical_memory_bytes");
    if state_sizes
        .first()
        .is_some_and(|value| *value >= memory_max)
    {
        errors.push("workload must include a cache-resident state size".to_owned());
    }
    if state_sizes
        .last()
        .is_none_or(|value| *value <= physical_memory)
    {
        errors.push("workload must include a state size larger than physical RAM".to_owned());
    }

    if number_at(profile, "/workload/primary_vnode_count")
        != number_at(profile, "/store_layout/logical_vnode_count")
    {
        errors.push("workload primary vnode count must match store layout".to_owned());
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
    if seed_count < repetitions {
        errors.push("one fixed seed is required per paired repetition".to_owned());
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
        "/restore_limits/restore_task_reservation_bytes_max",
        "/restore_limits/global_restore_reservation_bytes_max",
        errors,
    );
    let task_reservation = number_at(
        profile,
        "/restore_limits/restore_task_reservation_bytes_max",
    );
    let decoder_count = number_at(profile, "/restore_limits/concurrent_restore_decoders_max");
    let global_reservation = number_at(
        profile,
        "/restore_limits/global_restore_reservation_bytes_max",
    );
    if task_reservation
        .checked_mul(decoder_count)
        .is_none_or(|reserved| reserved > global_reservation)
    {
        errors.push("concurrent restore task reservations exceed the global cap".to_owned());
    }
    let operators = number_at(profile, "/restore_limits/operators_per_transition_max");
    let vnodes = number_at(profile, "/restore_limits/vnodes_per_transition_max");
    let pairs = number_at(profile, "/restore_limits/operator_vnode_pairs_max");
    if operators
        .checked_mul(vnodes)
        .is_none_or(|maximum| pairs > maximum)
    {
        errors.push("operator-vnode pair cap exceeds the product cap".to_owned());
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

fn reject_placeholder_strings(value: &Value, path: &str, errors: &mut Vec<String>) {
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

fn reject_non_u64_numbers(value: &Value, path: &str, errors: &mut Vec<String>) {
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

    fn profile_value() -> Value {
        serde_json::from_slice(PROFILE).unwrap()
    }

    fn mutated(mutator: impl FnOnce(&mut Value)) -> Vec<u8> {
        let mut profile = profile_value();
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
    }

    #[test]
    fn dependency_manifest_stays_runtime_neutral() {
        let manifest = include_str!("../Cargo.toml");
        for forbidden in ["fjall", "rocksdb", "arrow", "datafusion", "laminar"] {
            assert!(!manifest.contains(forbidden));
        }
        assert!(!manifest.contains("path ="));
        assert!(!manifest.contains("workspace = true"));
    }
}
