use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Formatter;

use serde::de::{Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::model::{
    encoded_request_len as model_encoded_request_len, BatchKind, BatchLimits, LogicalBatch,
    LogicalKey, Mutation, RangeRead, Scenario, Table,
};
use crate::{validated_profile_value, CheckErrors};

pub const GENERATOR_VERSION: &str = "state-backend-workload/v1";
pub const MAX_REQUEST_COUNT: u32 = 4_096;
pub const MAX_REPLAY_ACCOUNTED_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_MODEL_BATCH_ROWS: u32 = 65_536;
pub const MAX_MODEL_KEY_BYTES: u32 = 4_096;
pub const MAX_MODEL_VALUE_BYTES: u32 = 65_536;
pub const MAX_MODEL_HARD_BATCH_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_MODEL_LOGICAL_ROWS: u64 = 4 * 1024 * 1024;

const MODEL_INPUT_DOMAIN: &[u8] = b"LDB-SBQ-MODEL-INPUT-V1\0";
const COUNTER_DOMAIN: &[u8] = b"LDB-SBQ-COUNTER-V1\0";
const ENTITY_DOMAIN: &[u8] = b"LDB-SBQ-ENTITY-V1\0";
const REQUEST_ROW: u32 = u32::MAX;
const PERMILLE: u64 = 1_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ModelCase {
    pub scenario: Scenario,
    pub seed: u64,
    pub logical_state_bytes: u64,
    pub batch_rows: u32,
    pub request_count: u32,
    pub key_bytes: u32,
    pub value_bytes: u32,
    pub join_match_count: Option<u32>,
}

impl<'de> Deserialize<'de> for ModelCase {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ModelCaseVisitor;

        impl<'de> Visitor<'de> for ModelCaseVisitor {
            type Value = ModelCase;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a strict state-backend model case")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut scenario = None;
                let mut seed = None;
                let mut logical_state_bytes = None;
                let mut batch_rows = None;
                let mut request_count = None;
                let mut key_bytes = None;
                let mut value_bytes = None;
                let mut join_match_count = None;

                while let Some(field) = map.next_key::<String>()? {
                    match field.as_str() {
                        "scenario" => set_once(&mut scenario, map.next_value()?, &field)?,
                        "seed" => set_once(&mut seed, map.next_value()?, &field)?,
                        "logical_state_bytes" => {
                            set_once(&mut logical_state_bytes, map.next_value()?, &field)?
                        }
                        "batch_rows" => set_once(&mut batch_rows, map.next_value()?, &field)?,
                        "request_count" => set_once(&mut request_count, map.next_value()?, &field)?,
                        "key_bytes" => set_once(&mut key_bytes, map.next_value()?, &field)?,
                        "value_bytes" => set_once(&mut value_bytes, map.next_value()?, &field)?,
                        "join_match_count" => {
                            let value = map.next_value::<Option<u32>>()?;
                            set_once(&mut join_match_count, value, &field)?;
                        }
                        _ => return Err(A::Error::unknown_field(&field, MODEL_CASE_FIELDS)),
                    }
                }

                Ok(ModelCase {
                    scenario: required(scenario, "scenario")?,
                    seed: required(seed, "seed")?,
                    logical_state_bytes: required(logical_state_bytes, "logical_state_bytes")?,
                    batch_rows: required(batch_rows, "batch_rows")?,
                    request_count: required(request_count, "request_count")?,
                    key_bytes: required(key_bytes, "key_bytes")?,
                    value_bytes: required(value_bytes, "value_bytes")?,
                    join_match_count: required(join_match_count, "join_match_count")?,
                })
            }
        }

        deserializer.deserialize_map(ModelCaseVisitor)
    }
}

const MODEL_CASE_FIELDS: &[&str] = &[
    "scenario",
    "seed",
    "logical_state_bytes",
    "batch_rows",
    "request_count",
    "key_bytes",
    "value_bytes",
    "join_match_count",
];

fn set_once<E, T>(slot: &mut Option<T>, value: T, field: &str) -> Result<(), E>
where
    E: serde::de::Error,
{
    if slot.replace(value).is_some() {
        return Err(E::custom(format!("duplicate field `{field}`")));
    }
    Ok(())
}

fn required<E, T>(value: Option<T>, field: &'static str) -> Result<T, E>
where
    E: serde::de::Error,
{
    value.ok_or_else(|| E::missing_field(field))
}

#[derive(Clone, Copy, Debug)]
pub struct HotDistribution {
    pub one_key: u64,
    pub nine_keys: u64,
    pub uniform_remainder: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct TimerMix {
    pub state_or_timer_mutation: u64,
    pub bounded_due_scan: u64,
    pub atomic_fire_delete: u64,
}

/// Typed inputs extracted only after the profile's duplicate-key, schema, and
/// semantic validation has succeeded.
#[derive(Clone, Debug)]
pub struct ModelProfile {
    pub profile_id: String,
    pub profile_sha256: [u8; 32],
    pub model_input_sha256: [u8; 32],
    pub logical_state_bytes: Vec<u64>,
    pub batch_rows: Vec<u32>,
    pub target_batch_bytes: u64,
    pub hard_batch_bytes: u64,
    pub compact_key_bytes: u32,
    pub compact_state_bytes: u32,
    pub variable_key_bytes: Vec<u32>,
    pub variable_state_bytes: Vec<u32>,
    pub primary_vnode_count: u32,
    pub metadata_edge_vnode_counts: Vec<u32>,
    pub zipf_exponent_milli: u64,
    pub hot_distribution: HotDistribution,
    pub single_vnode_distinct_keys_permille: u64,
    pub timer_mix: TimerMix,
    pub timer_scan_max_rows: u32,
    pub timer_scan_max_bytes: u64,
    pub join_match_counts: Vec<u32>,
    pub join_match_weights_permille: Vec<u64>,
    pub fixed_seeds: Vec<u64>,
    pub encoded_key_bytes_max: u32,
    pub stored_state_bytes_max: u32,
}

impl ModelProfile {
    pub fn from_profile_bytes(bytes: &[u8]) -> Result<Self, CheckErrors> {
        let value = validated_profile_value(bytes)?;
        Self::from_validated_value(bytes, &value)
    }

    pub(crate) fn from_validated_value(
        exact_profile_bytes: &[u8],
        profile: &Value,
    ) -> Result<Self, CheckErrors> {
        let profile_id = text(profile, "/profile_id")?.to_owned();
        let logical_state_bytes = u64_vector(profile, "/workload/logical_state_bytes")?;
        let batch_rows = u32_vector(profile, "/workload/batch_rows")?;
        let target_batch_bytes = u64_value(profile, "/workload/target_batch_bytes")?;
        let hard_batch_bytes = u64_value(profile, "/workload/hard_batch_bytes")?;
        let compact_key_bytes = u32_value(profile, "/workload/compact_key_bytes")?;
        let compact_state_bytes = u32_value(profile, "/workload/compact_state_bytes")?;
        let variable_key_bytes = u32_vector(profile, "/workload/variable_key_bytes")?;
        let variable_state_bytes = u32_vector(profile, "/workload/variable_state_bytes")?;
        let primary_vnode_count = u32_value(profile, "/workload/primary_vnode_count")?;
        let metadata_edge_vnode_counts =
            u32_vector(profile, "/workload/metadata_edge_vnode_counts")?;
        let zipf_exponent_milli = u64_value(profile, "/workload/zipf_exponent_milli")?;
        let hot_distribution = HotDistribution {
            one_key: u64_value(profile, "/workload/hot_distribution_permille/one_key")?,
            nine_keys: u64_value(profile, "/workload/hot_distribution_permille/nine_keys")?,
            uniform_remainder: u64_value(
                profile,
                "/workload/hot_distribution_permille/uniform_remainder",
            )?,
        };
        let single_vnode_distinct_keys_permille =
            u64_value(profile, "/workload/single_vnode_distinct_keys_permille")?;
        let timer_mix = TimerMix {
            state_or_timer_mutation: u64_value(
                profile,
                "/workload/timer_mix_permille/state_or_timer_mutation",
            )?,
            bounded_due_scan: u64_value(profile, "/workload/timer_mix_permille/bounded_due_scan")?,
            atomic_fire_delete: u64_value(
                profile,
                "/workload/timer_mix_permille/atomic_fire_delete",
            )?,
        };
        let timer_scan_max_rows = u32_value(profile, "/workload/timer_scan_max_rows")?;
        let timer_scan_max_bytes = u64_value(profile, "/workload/timer_scan_max_bytes")?;
        let join_match_counts = u32_vector(profile, "/workload/join_match_counts")?;
        let join_match_weights_permille =
            u64_vector(profile, "/workload/join_match_weights_permille")?;
        let fixed_seeds = u64_vector(profile, "/measurement/fixed_seeds")?;
        let encoded_key_bytes_max = u32_value(profile, "/restore_limits/encoded_key_bytes_max")?;
        let stored_state_bytes_max = u32_value(profile, "/restore_limits/stored_state_bytes_max")?;

        validate_model_profile_bounds(
            &batch_rows,
            hard_batch_bytes,
            compact_key_bytes,
            &variable_key_bytes,
            compact_state_bytes,
            &variable_state_bytes,
        )?;

        let mut model_input = Sha256::new();
        model_input.update(MODEL_INPUT_DOMAIN);
        hash_u64_vector(&mut model_input, &logical_state_bytes)?;
        hash_u32_vector(&mut model_input, &batch_rows)?;
        hash_u64(&mut model_input, target_batch_bytes);
        hash_u64(&mut model_input, hard_batch_bytes);
        hash_u64(&mut model_input, compact_key_bytes.into());
        hash_u64(&mut model_input, compact_state_bytes.into());
        hash_u32_vector(&mut model_input, &variable_key_bytes)?;
        hash_u32_vector(&mut model_input, &variable_state_bytes)?;
        hash_u64(&mut model_input, primary_vnode_count.into());
        hash_u32_vector(&mut model_input, &metadata_edge_vnode_counts)?;
        hash_u64(&mut model_input, zipf_exponent_milli);
        hash_u64(&mut model_input, hot_distribution.one_key);
        hash_u64(&mut model_input, hot_distribution.nine_keys);
        hash_u64(&mut model_input, hot_distribution.uniform_remainder);
        hash_u64(&mut model_input, single_vnode_distinct_keys_permille);
        hash_u64(&mut model_input, timer_mix.state_or_timer_mutation);
        hash_u64(&mut model_input, timer_mix.bounded_due_scan);
        hash_u64(&mut model_input, timer_mix.atomic_fire_delete);
        hash_u64(&mut model_input, timer_scan_max_rows.into());
        hash_u64(&mut model_input, timer_scan_max_bytes);
        hash_u32_vector(&mut model_input, &join_match_counts)?;
        hash_u64_vector(&mut model_input, &join_match_weights_permille)?;
        hash_u64_vector(&mut model_input, &fixed_seeds)?;

        Ok(Self {
            profile_id,
            profile_sha256: Sha256::digest(exact_profile_bytes).into(),
            model_input_sha256: model_input.finalize().into(),
            logical_state_bytes,
            batch_rows,
            target_batch_bytes,
            hard_batch_bytes,
            compact_key_bytes,
            compact_state_bytes,
            variable_key_bytes,
            variable_state_bytes,
            primary_vnode_count,
            metadata_edge_vnode_counts,
            zipf_exponent_milli,
            hot_distribution,
            single_vnode_distinct_keys_permille,
            timer_mix,
            timer_scan_max_rows,
            timer_scan_max_bytes,
            join_match_counts,
            join_match_weights_permille,
            fixed_seeds,
            encoded_key_bytes_max,
            stored_state_bytes_max,
        })
    }

    pub fn validate_case(&self, case: &ModelCase) -> Result<(), CheckErrors> {
        let mut errors = Vec::new();
        if !self.fixed_seeds.contains(&case.seed) {
            errors.push("case seed is not a measurement.fixed_seeds member".to_owned());
        }
        if !self.logical_state_bytes.contains(&case.logical_state_bytes) {
            errors.push("case logical_state_bytes is not a profile member".to_owned());
        }
        if !self.batch_rows.contains(&case.batch_rows) {
            errors.push("case batch_rows is not a profile member".to_owned());
        }
        if !(1..=MAX_REQUEST_COUNT).contains(&case.request_count) {
            errors.push(format!(
                "case request_count must be in 1..={MAX_REQUEST_COUNT}"
            ));
        }
        if case.key_bytes != self.compact_key_bytes
            && !self.variable_key_bytes.contains(&case.key_bytes)
        {
            errors.push("case key_bytes is not a compact or variable profile width".to_owned());
        }
        if case.value_bytes != self.compact_state_bytes
            && !self.variable_state_bytes.contains(&case.value_bytes)
        {
            errors.push("case value_bytes is not a compact or variable profile width".to_owned());
        }
        if case.key_bytes > self.encoded_key_bytes_max {
            errors.push("case key_bytes exceeds the active restore key limit".to_owned());
        }
        if case.value_bytes > self.stored_state_bytes_max {
            errors.push("case value_bytes exceeds the active restore state limit".to_owned());
        }

        let minimum_key_bytes = match case.scenario {
            Scenario::Aggregate => 8,
            Scenario::TimerWindow | Scenario::Join => 16,
        };
        if case.key_bytes < minimum_key_bytes {
            errors.push(format!(
                "case key_bytes is below the {}-byte {:?} minimum",
                minimum_key_bytes, case.scenario
            ));
        }

        match (case.scenario, case.join_match_count) {
            (Scenario::Join, Some(count)) if self.join_match_counts.contains(&count) => {}
            (Scenario::Join, Some(_)) => {
                errors.push("case join_match_count is not a profile member".to_owned())
            }
            (Scenario::Join, None) => {
                errors.push("join case requires a non-null join_match_count".to_owned())
            }
            (_, Some(_)) => errors.push("non-join case requires null join_match_count".to_owned()),
            (_, None) => {}
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(CheckErrors::many(errors))
        }
    }

    pub fn generate_request(
        &self,
        case: &ModelCase,
        ordinal: u64,
    ) -> Result<LogicalBatch, CheckErrors> {
        generate_request(self, case, ordinal)
    }

    pub fn preflight_case(&self, case: &ModelCase) -> Result<ReplayPreflight, CheckErrors> {
        preflight_replay(self, case)
    }

    pub fn requests<'a>(&'a self, case: &'a ModelCase) -> Result<RequestSequence<'a>, CheckErrors> {
        preflight_replay(self, case)?;
        Ok(RequestSequence {
            profile: self,
            case,
            next_ordinal: 0,
        })
    }
}

fn validate_model_profile_bounds(
    batch_rows: &[u32],
    hard_batch_bytes: u64,
    compact_key_bytes: u32,
    variable_key_bytes: &[u32],
    compact_state_bytes: u32,
    variable_state_bytes: &[u32],
) -> Result<(), CheckErrors> {
    let mut errors = Vec::new();
    if batch_rows.iter().any(|rows| *rows > MAX_MODEL_BATCH_ROWS) {
        errors.push(format!(
            "model batch_rows exceeds the C1 safety maximum {MAX_MODEL_BATCH_ROWS}"
        ));
    }
    if hard_batch_bytes > MAX_MODEL_HARD_BATCH_BYTES {
        errors.push(format!(
            "model hard_batch_bytes exceeds the C1 safety maximum {MAX_MODEL_HARD_BATCH_BYTES}"
        ));
    }
    if compact_key_bytes > MAX_MODEL_KEY_BYTES
        || variable_key_bytes
            .iter()
            .any(|width| *width > MAX_MODEL_KEY_BYTES)
    {
        errors.push(format!(
            "model key width exceeds the C1 safety maximum {MAX_MODEL_KEY_BYTES}"
        ));
    }
    if compact_state_bytes > MAX_MODEL_VALUE_BYTES
        || variable_state_bytes
            .iter()
            .any(|width| *width > MAX_MODEL_VALUE_BYTES)
    {
        errors.push(format!(
            "model value width exceeds the C1 safety maximum {MAX_MODEL_VALUE_BYTES}"
        ));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CheckErrors::many(errors))
    }
}

pub fn lowercase_sha256(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn u64_value(value: &Value, pointer: &str) -> Result<u64, CheckErrors> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| CheckErrors::one(format!("validated profile has no u64 at {pointer}")))
}

fn u32_value(value: &Value, pointer: &str) -> Result<u32, CheckErrors> {
    u32::try_from(u64_value(value, pointer)?).map_err(|_| {
        CheckErrors::one(format!(
            "validated profile value at {pointer} does not fit u32"
        ))
    })
}

fn text<'a>(value: &'a Value, pointer: &str) -> Result<&'a str, CheckErrors> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| CheckErrors::one(format!("validated profile has no string at {pointer}")))
}

fn u64_vector(value: &Value, pointer: &str) -> Result<Vec<u64>, CheckErrors> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .ok_or_else(|| CheckErrors::one(format!("validated profile has no array at {pointer}")))?
        .iter()
        .enumerate()
        .map(|(index, item)| {
            item.as_u64().ok_or_else(|| {
                CheckErrors::one(format!("validated profile has no u64 at {pointer}/{index}"))
            })
        })
        .collect()
}

fn u32_vector(value: &Value, pointer: &str) -> Result<Vec<u32>, CheckErrors> {
    u64_vector(value, pointer)?
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            u32::try_from(item).map_err(|_| {
                CheckErrors::one(format!(
                    "validated profile value at {pointer}/{index} does not fit u32"
                ))
            })
        })
        .collect()
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn hash_u64_vector(hasher: &mut Sha256, values: &[u64]) -> Result<(), CheckErrors> {
    let count = u32::try_from(values.len())
        .map_err(|_| CheckErrors::one("model-input vector count exceeds u32"))?;
    hasher.update(count.to_be_bytes());
    for value in values {
        hash_u64(hasher, *value);
    }
    Ok(())
}

fn hash_u32_vector(hasher: &mut Sha256, values: &[u32]) -> Result<(), CheckErrors> {
    let count = u32::try_from(values.len())
        .map_err(|_| CheckErrors::one("model-input vector count exceeds u32"))?;
    hasher.update(count.to_be_bytes());
    for value in values {
        hash_u64(hasher, u64::from(*value));
    }
    Ok(())
}

#[derive(Clone)]
struct Counter<'a> {
    profile: &'a ModelProfile,
    case: &'a ModelCase,
    request_ordinal: u64,
}

impl Counter<'_> {
    fn block(&self, row_ordinal: u32, lane: u32) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(COUNTER_DOMAIN);
        hasher.update(self.profile.model_input_sha256);
        hasher.update(self.case.seed.to_be_bytes());
        hasher.update([scenario_tag(self.case.scenario)]);
        hasher.update(self.request_ordinal.to_be_bytes());
        hasher.update(row_ordinal.to_be_bytes());
        hasher.update(lane.to_be_bytes());
        hasher.finalize().into()
    }

    fn word(&self, row_ordinal: u32, lane: u32) -> u64 {
        let block = self.block(row_ordinal, lane);
        let mut prefix = [0_u8; 8];
        prefix.copy_from_slice(&block[..8]);
        u64::from_be_bytes(prefix)
    }
}

fn entity_key(
    profile: &ModelProfile,
    case: &ModelCase,
    table: u8,
    vnode: u32,
    components: &[u64],
    mut base: Vec<u8>,
) -> Result<Vec<u8>, CheckErrors> {
    let target = usize::try_from(case.key_bytes)
        .map_err(|_| CheckErrors::one("case key width does not fit usize"))?;
    if base.len() > target {
        return Err(CheckErrors::one(format!(
            "{}-byte key base exceeds selected {target}-byte width",
            base.len()
        )));
    }
    let component_count = u8::try_from(components.len())
        .map_err(|_| CheckErrors::one("entity component count exceeds u8"))?;
    let mut block_index = 0_u32;
    while base.len() < target {
        let mut hasher = Sha256::new();
        hasher.update(ENTITY_DOMAIN);
        hasher.update(profile.model_input_sha256);
        hasher.update(case.seed.to_be_bytes());
        hasher.update([table]);
        hasher.update(vnode.to_be_bytes());
        hasher.update([component_count]);
        for component in components {
            hasher.update(component.to_be_bytes());
        }
        hasher.update(block_index.to_be_bytes());
        let block: [u8; 32] = hasher.finalize().into();
        let remaining = target - base.len();
        base.extend_from_slice(&block[..remaining.min(block.len())]);
        block_index = block_index
            .checked_add(1)
            .ok_or_else(|| CheckErrors::one("entity suffix block index overflow"))?;
    }
    Ok(base)
}

fn expanded_value(counter: &Counter<'_>, table: u8, row: u32) -> Result<Vec<u8>, CheckErrors> {
    let target = usize::try_from(counter.case.value_bytes)
        .map_err(|_| CheckErrors::one("case value width does not fit usize"))?;
    let mut value = Vec::with_capacity(target);
    let mut block = 0_u32;
    while value.len() < target {
        let lane = 256_u32
            .checked_add(u32::from(table) << 16)
            .and_then(|lane| lane.checked_add(block))
            .ok_or_else(|| CheckErrors::one("value expansion lane overflow"))?;
        let bytes = counter.block(row, lane);
        let remaining = target - value.len();
        value.extend_from_slice(&bytes[..remaining.min(bytes.len())]);
        block = block
            .checked_add(1)
            .ok_or_else(|| CheckErrors::one("value expansion block index overflow"))?;
    }
    Ok(value)
}

fn uniform_domain(case: &ModelCase) -> Result<u64, CheckErrors> {
    let width = u64::from(case.key_bytes)
        .checked_add(u64::from(case.value_bytes))
        .filter(|width| *width != 0)
        .ok_or_else(|| CheckErrors::one("case key/value width sum overflowed or is zero"))?;
    Ok((case.logical_state_bytes / width).saturating_sub(10).max(1))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayPreflight {
    pub request_count: u32,
    pub canonical_request_bytes: u64,
    pub declared_read_capacity_bytes: u64,
    pub mutation_bytes: u64,
    pub accounted_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequestCharge {
    canonical_request_bytes: u64,
    declared_read_capacity_bytes: u64,
    mutation_bytes: u64,
}

#[derive(Default)]
struct ReplayAccounting {
    canonical_request_bytes: u64,
    declared_read_capacity_bytes: u64,
    mutation_bytes: u64,
    accounted_bytes: u64,
}

impl ReplayAccounting {
    fn add(&mut self, charge: RequestCharge) -> Result<(), CheckErrors> {
        self.canonical_request_bytes = checked_add(
            self.canonical_request_bytes,
            charge.canonical_request_bytes,
            "replay canonical request bytes",
        )?;
        self.declared_read_capacity_bytes = checked_add(
            self.declared_read_capacity_bytes,
            charge.declared_read_capacity_bytes,
            "replay declared read capacity",
        )?;
        self.mutation_bytes = checked_add(
            self.mutation_bytes,
            charge.mutation_bytes,
            "replay mutation bytes",
        )?;
        self.accounted_bytes = checked_add(
            self.accounted_bytes,
            charge.canonical_request_bytes,
            "replay byte account",
        )?;
        self.accounted_bytes = checked_add(
            self.accounted_bytes,
            charge.declared_read_capacity_bytes,
            "replay byte account",
        )?;
        self.accounted_bytes = checked_add(
            self.accounted_bytes,
            charge.mutation_bytes,
            "replay byte account",
        )?;
        if self.accounted_bytes > MAX_REPLAY_ACCOUNTED_BYTES {
            return Err(CheckErrors::one(format!(
                "replay accounts for {} bytes; maximum is {MAX_REPLAY_ACCOUNTED_BYTES}",
                self.accounted_bytes
            )));
        }
        Ok(())
    }
}

pub struct RequestSequence<'a> {
    profile: &'a ModelProfile,
    case: &'a ModelCase,
    next_ordinal: u64,
}

impl Iterator for RequestSequence<'_> {
    type Item = Result<LogicalBatch, CheckErrors>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_ordinal >= u64::from(self.case.request_count) {
            return None;
        }
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        Some(generate_validated_request(self.profile, self.case, ordinal))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = u64::from(self.case.request_count).saturating_sub(self.next_ordinal);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RequestSequence<'_> {}

pub fn generate_request(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
) -> Result<LogicalBatch, CheckErrors> {
    profile.validate_case(case)?;
    validate_generation_work(case)?;
    if ordinal >= u64::from(case.request_count) {
        return Err(CheckErrors::one(format!(
            "request ordinal {ordinal} is outside case request_count {}",
            case.request_count
        )));
    }
    validate_request_charge(profile, request_charge(profile, case, ordinal)?)?;
    generate_validated_request(profile, case, ordinal)
}

pub fn preflight_replay(
    profile: &ModelProfile,
    case: &ModelCase,
) -> Result<ReplayPreflight, CheckErrors> {
    profile.validate_case(case)?;
    validate_generation_work(case)?;
    let mut accounting = ReplayAccounting::default();

    for ordinal in 0..u64::from(case.request_count) {
        let charge = request_charge(profile, case, ordinal)?;
        validate_request_charge(profile, charge)?;
        accounting.add(charge)?;
    }

    Ok(ReplayPreflight {
        request_count: case.request_count,
        canonical_request_bytes: accounting.canonical_request_bytes,
        declared_read_capacity_bytes: accounting.declared_read_capacity_bytes,
        mutation_bytes: accounting.mutation_bytes,
        accounted_bytes: accounting.accounted_bytes,
    })
}

fn validate_generation_work(case: &ModelCase) -> Result<(), CheckErrors> {
    let logical_rows = u64::from(case.request_count)
        .checked_mul(u64::from(case.batch_rows))
        .ok_or_else(|| CheckErrors::one("model logical-row work overflow"))?;
    if logical_rows > MAX_MODEL_LOGICAL_ROWS {
        return Err(CheckErrors::one(format!(
            "model replay has {logical_rows} logical rows; C1 safety maximum is {MAX_MODEL_LOGICAL_ROWS}"
        )));
    }
    Ok(())
}

fn validate_request_charge(
    profile: &ModelProfile,
    charge: RequestCharge,
) -> Result<(), CheckErrors> {
    if charge.canonical_request_bytes > profile.hard_batch_bytes {
        return Err(CheckErrors::one(format!(
            "canonical request {} exceeds hard batch limit {}",
            charge.canonical_request_bytes, profile.hard_batch_bytes
        )));
    }
    if charge.declared_read_capacity_bytes > profile.hard_batch_bytes {
        return Err(CheckErrors::one(format!(
            "declared read capacity {} exceeds hard batch limit {}",
            charge.declared_read_capacity_bytes, profile.hard_batch_bytes
        )));
    }
    if charge.mutation_bytes > profile.hard_batch_bytes {
        return Err(CheckErrors::one(format!(
            "mutation charge {} exceeds hard batch limit {}",
            charge.mutation_bytes, profile.hard_batch_bytes
        )));
    }
    Ok(())
}

fn request_charge(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
) -> Result<RequestCharge, CheckErrors> {
    let key_bytes = u64::from(case.key_bytes);
    let value_bytes = u64::from(case.value_bytes);
    let row_bytes = checked_add(key_bytes, value_bytes, "selected logical row width")?;
    let logical_key_bytes = checked_add(9, key_bytes, "encoded logical key bytes")?;
    let range_bytes = checked_add(
        25,
        key_bytes
            .checked_mul(2)
            .ok_or_else(|| CheckErrors::one("encoded range key bytes overflow"))?,
        "encoded range bytes",
    )?;
    let put_bytes = checked_add(
        14,
        checked_add(key_bytes, value_bytes, "encoded put payload bytes")?,
        "encoded put bytes",
    )?;
    let delete_bytes = checked_add(10, key_bytes, "encoded delete bytes")?;
    let counter = Counter {
        profile,
        case,
        request_ordinal: ordinal,
    };

    match case.scenario {
        Scenario::Aggregate => {
            let groups = u64::try_from(aggregate_group_rows(profile, case, &counter)?.len())
                .map_err(|_| CheckErrors::one("aggregate group count exceeds u64"))?;
            Ok(RequestCharge {
                canonical_request_bytes: checked_add(
                    77,
                    checked_mul(
                        groups,
                        checked_add(logical_key_bytes, put_bytes, "aggregate encoded row")?,
                        "aggregate canonical request bytes",
                    )?,
                    "aggregate canonical request bytes",
                )?,
                declared_read_capacity_bytes: checked_mul(
                    groups,
                    row_bytes,
                    "aggregate read capacity",
                )?,
                mutation_bytes: checked_mul(groups, row_bytes, "aggregate mutation bytes")?,
            })
        }
        Scenario::TimerWindow => match timer_request_kind(profile, &counter)? {
            TimerRequestKind::Mutation => {
                let rows = u64::from(case.batch_rows);
                Ok(RequestCharge {
                    canonical_request_bytes: checked_add(
                        77,
                        checked_add(
                            checked_mul(rows, logical_key_bytes, "timer point-read encoding")?,
                            checked_mul(
                                rows,
                                checked_mul(2, put_bytes, "timer put encoding")?,
                                "timer mutation encoding",
                            )?,
                            "timer mutation canonical payload",
                        )?,
                        "timer mutation canonical request bytes",
                    )?,
                    declared_read_capacity_bytes: checked_mul(
                        rows,
                        row_bytes,
                        "timer mutation read capacity",
                    )?,
                    mutation_bytes: checked_mul(
                        rows,
                        checked_mul(2, row_bytes, "timer mutation row bytes")?,
                        "timer mutation bytes",
                    )?,
                })
            }
            TimerRequestKind::DueScan => Ok(RequestCharge {
                canonical_request_bytes: checked_add(
                    77,
                    range_bytes,
                    "timer due-scan canonical request bytes",
                )?,
                declared_read_capacity_bytes: profile.timer_scan_max_bytes,
                mutation_bytes: 0,
            }),
            TimerRequestKind::FireDelete => {
                let rows = u64::from(case.batch_rows);
                let canonical_per_row = checked_add(
                    checked_mul(2, logical_key_bytes, "timer fire point-read encoding")?,
                    checked_add(put_bytes, delete_bytes, "timer fire mutation encoding")?,
                    "timer fire canonical row",
                )?;
                let read_per_row = checked_mul(2, row_bytes, "timer fire read row")?;
                let mutation_per_row = checked_add(
                    checked_mul(2, key_bytes, "timer fire mutation keys")?,
                    value_bytes,
                    "timer fire mutation row",
                )?;
                Ok(RequestCharge {
                    canonical_request_bytes: checked_add(
                        77,
                        checked_mul(rows, canonical_per_row, "timer fire encoding")?,
                        "timer fire canonical request bytes",
                    )?,
                    declared_read_capacity_bytes: checked_mul(
                        rows,
                        read_per_row,
                        "timer fire read capacity",
                    )?,
                    mutation_bytes: checked_mul(
                        rows,
                        mutation_per_row,
                        "timer fire mutation bytes",
                    )?,
                })
            }
        },
        Scenario::Join => {
            let rows = u64::from(case.batch_rows);
            let distinct_ranges = join_distinct_range_count(profile, case, ordinal, &counter)?;
            let range_rows = u64::from(
                case.join_match_count
                    .ok_or_else(|| CheckErrors::one("validated join case has null match count"))?
                    .max(1),
            );
            let range_capacity = checked_mul(range_rows, row_bytes, "join range capacity")?;
            Ok(RequestCharge {
                canonical_request_bytes: checked_add(
                    77,
                    checked_add(
                        checked_mul(distinct_ranges, range_bytes, "join range encoding")?,
                        checked_mul(rows, put_bytes, "join mutation encoding")?,
                        "join canonical payload",
                    )?,
                    "join canonical request bytes",
                )?,
                declared_read_capacity_bytes: checked_mul(
                    distinct_ranges,
                    range_capacity,
                    "join read capacity",
                )?,
                mutation_bytes: checked_mul(rows, row_bytes, "join mutation bytes")?,
            })
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimerRequestKind {
    Mutation,
    DueScan,
    FireDelete,
}

fn timer_request_kind(
    profile: &ModelProfile,
    counter: &Counter<'_>,
) -> Result<TimerRequestKind, CheckErrors> {
    let selector = counter.word(REQUEST_ROW, 0) % PERMILLE;
    let mutation_end = profile.timer_mix.state_or_timer_mutation;
    let scan_end = checked_add(
        mutation_end,
        profile.timer_mix.bounded_due_scan,
        "timer mode threshold",
    )?;
    if selector < mutation_end {
        Ok(TimerRequestKind::Mutation)
    } else if selector < scan_end {
        Ok(TimerRequestKind::DueScan)
    } else {
        Ok(TimerRequestKind::FireDelete)
    }
}

fn generate_validated_request(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
) -> Result<LogicalBatch, CheckErrors> {
    let counter = Counter {
        profile,
        case,
        request_ordinal: ordinal,
    };
    match case.scenario {
        Scenario::Aggregate => generate_aggregate(profile, case, ordinal, &counter),
        Scenario::TimerWindow => generate_timer_window(profile, case, ordinal, &counter),
        Scenario::Join => generate_join(profile, case, ordinal, &counter),
    }
}

fn generate_aggregate(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    counter: &Counter<'_>,
) -> Result<LogicalBatch, CheckErrors> {
    let groups = aggregate_group_rows(profile, case, counter)?;
    let mut point_reads = Vec::with_capacity(groups.len());
    let mut mutations = Vec::with_capacity(groups.len());
    for ((vnode, identity), lowest_row) in groups {
        let key = aggregate_or_window_key(profile, case, Table::AggregateState, vnode, identity)?;
        point_reads.push(key.clone());
        mutations.push(Mutation::Put {
            key,
            value: expanded_value(counter, table_tag(Table::AggregateState), lowest_row)?,
        });
    }

    finish_batch(profile, case, ordinal, point_reads, Vec::new(), mutations)
}

fn aggregate_group_rows(
    profile: &ModelProfile,
    case: &ModelCase,
    counter: &Counter<'_>,
) -> Result<BTreeMap<(u32, u64), u32>, CheckErrors> {
    let domain = uniform_domain(case)?;
    let one_key_end = profile.hot_distribution.one_key;
    let nine_keys_end = checked_add(
        one_key_end,
        profile.hot_distribution.nine_keys,
        "aggregate hot threshold",
    )?;
    let single_vnode =
        counter.word(REQUEST_ROW, 0) % PERMILLE < profile.single_vnode_distinct_keys_permille;
    let shared_vnode = vnode_from_word(counter.word(REQUEST_ROW, 1), profile)?;
    let mut groups = BTreeMap::<(u32, u64), u32>::new();

    for row in 0..case.batch_rows {
        let selector = counter.word(row, 0) % PERMILLE;
        let identity = if selector < one_key_end {
            0
        } else if selector < nine_keys_end {
            1 + counter.word(row, 1) % 9
        } else {
            10_u64
                .checked_add(counter.word(row, 1) % domain)
                .ok_or_else(|| CheckErrors::one("aggregate identity overflow"))?
        };
        let vnode = if single_vnode {
            shared_vnode
        } else {
            vnode_from_word(identity, profile)?
        };
        groups
            .entry((vnode, identity))
            .and_modify(|lowest| *lowest = (*lowest).min(row))
            .or_insert(row);
    }
    Ok(groups)
}

fn generate_timer_window(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    counter: &Counter<'_>,
) -> Result<LogicalBatch, CheckErrors> {
    match timer_request_kind(profile, counter)? {
        TimerRequestKind::Mutation => generate_timer_mutation(profile, case, ordinal, counter),
        TimerRequestKind::DueScan => generate_timer_due_scan(profile, case, ordinal, counter),
        TimerRequestKind::FireDelete => generate_timer_fire_delete(profile, case, ordinal, counter),
    }
}

fn generate_timer_mutation(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    counter: &Counter<'_>,
) -> Result<LogicalBatch, CheckErrors> {
    let row_width = selected_row_width(case)?;
    ensure_within_hard_batch(
        u64::from(case.batch_rows),
        row_width,
        profile.hard_batch_bytes,
        "timer mutation read capacity",
    )?;
    let mutation_rows = u64::from(case.batch_rows)
        .checked_mul(2)
        .ok_or_else(|| CheckErrors::one("timer mutation row count overflow"))?;
    ensure_within_hard_batch(
        mutation_rows,
        row_width,
        profile.hard_batch_bytes,
        "timer mutation write capacity",
    )?;
    let mut point_reads = Vec::with_capacity(usize_from_u32(case.batch_rows)?);
    let mutation_capacity = usize_from_u32(case.batch_rows)?
        .checked_mul(2)
        .ok_or_else(|| CheckErrors::one("timer mutation capacity overflow"))?;
    let mut mutations = Vec::with_capacity(mutation_capacity);

    for row in 0..case.batch_rows {
        let stable_row = stable_row_id(ordinal, row)?;
        let vnode = vnode_from_word(stable_row, profile)?;
        let timer_time = ordinal
            .checked_add(1)
            .and_then(|value| value.checked_add(counter.word(row, 1) % 1_024))
            .ok_or_else(|| CheckErrors::one("timer logical time overflow"))?;
        let window_key =
            aggregate_or_window_key(profile, case, Table::WindowState, vnode, stable_row)?;
        let timer_key = timer_key(profile, case, vnode, timer_time, stable_row)?;
        point_reads.push(window_key.clone());
        mutations.push(Mutation::Put {
            key: window_key,
            value: expanded_value(counter, table_tag(Table::WindowState), row)?,
        });
        mutations.push(Mutation::Put {
            key: timer_key,
            value: expanded_value(counter, table_tag(Table::TimerIndex), row)?,
        });
    }

    finish_batch(profile, case, ordinal, point_reads, Vec::new(), mutations)
}

fn generate_timer_due_scan(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    counter: &Counter<'_>,
) -> Result<LogicalBatch, CheckErrors> {
    let vnode = vnode_from_word(counter.word(REQUEST_ROW, 1), profile)?;
    let end_time = ordinal
        .checked_add(1)
        .ok_or_else(|| CheckErrors::one("timer due-scan boundary overflow"))?;
    let range = RangeRead {
        table: Table::TimerIndex,
        vnode,
        start_inclusive: timer_boundary(case, 0, 0)?,
        end_exclusive: timer_boundary(case, end_time, 0)?,
        max_rows: profile.timer_scan_max_rows,
        max_bytes: profile.timer_scan_max_bytes,
    };
    finish_batch(profile, case, ordinal, Vec::new(), vec![range], Vec::new())
}

fn generate_timer_fire_delete(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    counter: &Counter<'_>,
) -> Result<LogicalBatch, CheckErrors> {
    let key_width = u64::from(case.key_bytes);
    let value_width = u64::from(case.value_bytes);
    let read_width = key_width
        .checked_mul(2)
        .and_then(|keys| keys.checked_add(value_width.checked_mul(2)?))
        .ok_or_else(|| CheckErrors::one("timer fire read width overflow"))?;
    ensure_within_hard_batch(
        u64::from(case.batch_rows),
        read_width,
        profile.hard_batch_bytes,
        "timer fire read capacity",
    )?;
    let mutation_width = key_width
        .checked_mul(2)
        .and_then(|keys| keys.checked_add(value_width))
        .ok_or_else(|| CheckErrors::one("timer fire mutation width overflow"))?;
    ensure_within_hard_batch(
        u64::from(case.batch_rows),
        mutation_width,
        profile.hard_batch_bytes,
        "timer fire mutation capacity",
    )?;
    let source_ordinal = ordinal.saturating_sub(1);
    let source_counter = Counter {
        profile,
        case,
        request_ordinal: source_ordinal,
    };
    let point_capacity = usize_from_u32(case.batch_rows)?
        .checked_mul(2)
        .ok_or_else(|| CheckErrors::one("timer fire point-read capacity overflow"))?;
    let mut point_reads = Vec::with_capacity(point_capacity);
    let mut mutations = Vec::with_capacity(point_capacity);

    for row in 0..case.batch_rows {
        let stable_row = stable_row_id(source_ordinal, row)?;
        let vnode = vnode_from_word(stable_row, profile)?;
        let timer_time = source_ordinal
            .checked_add(1)
            .and_then(|value| value.checked_add(source_counter.word(row, 1) % 1_024))
            .ok_or_else(|| CheckErrors::one("timer fire logical time overflow"))?;
        let window_key =
            aggregate_or_window_key(profile, case, Table::WindowState, vnode, stable_row)?;
        let timer_key = timer_key(profile, case, vnode, timer_time, stable_row)?;
        point_reads.push(window_key.clone());
        point_reads.push(timer_key.clone());
        mutations.push(Mutation::Put {
            key: window_key,
            value: expanded_value(counter, table_tag(Table::WindowState), row)?,
        });
        mutations.push(Mutation::Delete { key: timer_key });
    }

    finish_batch(profile, case, ordinal, point_reads, Vec::new(), mutations)
}

fn generate_join(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    counter: &Counter<'_>,
) -> Result<LogicalBatch, CheckErrors> {
    let join_match_count = case
        .join_match_count
        .ok_or_else(|| CheckErrors::one("validated join case has null join_match_count"))?;
    let max_rows = join_match_count.max(1);
    let row_width = selected_row_width(case)?;
    let max_bytes = u64::from(max_rows)
        .checked_mul(row_width)
        .ok_or_else(|| CheckErrors::one("join range byte capacity overflow"))?;
    if max_bytes > profile.hard_batch_bytes {
        return Err(CheckErrors::one(format!(
            "join range capacity {max_bytes} exceeds hard batch limit {}",
            profile.hard_batch_bytes
        )));
    }
    ensure_within_hard_batch(
        u64::from(case.batch_rows),
        row_width,
        profile.hard_batch_bytes,
        "join mutation capacity",
    )?;

    let (arriving_table, opposite_table) = if counter.word(REQUEST_ROW, 0).is_multiple_of(2) {
        (Table::JoinLeftRows, Table::JoinRightRows)
    } else {
        (Table::JoinRightRows, Table::JoinLeftRows)
    };
    let domain = uniform_domain(case)?;
    let mut ranges = Vec::with_capacity(usize_from_u32(case.batch_rows)?);
    let mut mutations = Vec::with_capacity(usize_from_u32(case.batch_rows)?);

    for row in 0..case.batch_rows {
        let (stable_row, identity, event_time, vnode) =
            join_row_coordinates(profile, ordinal, counter, domain, row)?;
        let start_time = event_time.saturating_sub(1_024);
        let end_time = event_time
            .checked_add(1_025)
            .ok_or_else(|| CheckErrors::one("join exclusive event-time boundary overflow"))?;

        ranges.push(RangeRead {
            table: opposite_table,
            vnode,
            start_inclusive: join_boundary(case, identity, start_time)?,
            end_exclusive: join_boundary(case, identity, end_time)?,
            max_rows,
            max_bytes,
        });
        let key = join_key(
            profile,
            case,
            arriving_table,
            vnode,
            identity,
            event_time,
            stable_row,
        )?;
        mutations.push(Mutation::Put {
            key,
            value: expanded_value(counter, table_tag(arriving_table), row)?,
        });
    }

    finish_batch(profile, case, ordinal, Vec::new(), ranges, mutations)
}

fn join_distinct_range_count(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    counter: &Counter<'_>,
) -> Result<u64, CheckErrors> {
    let domain = uniform_domain(case)?;
    let mut ranges = BTreeSet::new();
    for row in 0..case.batch_rows {
        let (_, identity, event_time, vnode) =
            join_row_coordinates(profile, ordinal, counter, domain, row)?;
        ranges.insert((vnode, identity, event_time));
    }
    u64::try_from(ranges.len()).map_err(|_| CheckErrors::one("join range count exceeds u64"))
}

fn join_row_coordinates(
    profile: &ModelProfile,
    ordinal: u64,
    counter: &Counter<'_>,
    domain: u64,
    row: u32,
) -> Result<(u64, u64, u32, u32), CheckErrors> {
    let stable_row = stable_row_id(ordinal, row)?;
    let identity = counter.word(row, 0) % domain;
    let event_time_u64 = ordinal
        .checked_shl(16)
        .and_then(|base| base.checked_add(counter.word(row, 1) & 0xffff))
        .ok_or_else(|| CheckErrors::one("join event-time overflow"))?;
    let event_time = u32::try_from(event_time_u64)
        .map_err(|_| CheckErrors::one("join event-time does not fit u32"))?;
    let vnode = vnode_from_word(identity, profile)?;
    Ok((stable_row, identity, event_time, vnode))
}

fn finish_batch(
    profile: &ModelProfile,
    case: &ModelCase,
    ordinal: u64,
    mut point_reads: Vec<LogicalKey>,
    mut ranges: Vec<RangeRead>,
    mut mutations: Vec<Mutation>,
) -> Result<LogicalBatch, CheckErrors> {
    point_reads.sort_by(logical_key_cmp);
    point_reads.dedup_by(|left, right| logical_key_cmp(left, right).is_eq());
    ranges.sort_by(range_cmp);
    ranges.dedup_by(|left, right| range_cmp(left, right).is_eq());
    mutations.sort_by(|left, right| logical_key_cmp(mutation_key(left), mutation_key(right)));
    if mutations
        .windows(2)
        .any(|pair| logical_key_cmp(mutation_key(&pair[0]), mutation_key(&pair[1])).is_eq())
    {
        return Err(CheckErrors::one(
            "generated request contains duplicate mutation keys",
        ));
    }

    let point_count = u64::try_from(point_reads.len())
        .map_err(|_| CheckErrors::one("point-read count exceeds u64"))?;
    let selected_row_width = u64::from(case.key_bytes)
        .checked_add(u64::from(case.value_bytes))
        .ok_or_else(|| CheckErrors::one("selected point-read width overflow"))?;
    let point_bytes = point_count
        .checked_mul(selected_row_width)
        .ok_or_else(|| CheckErrors::one("point-read byte capacity overflow"))?;
    let read_rows_max_u64 = ranges.iter().try_fold(point_count, |total, range| {
        total
            .checked_add(u64::from(range.max_rows))
            .ok_or_else(|| CheckErrors::one("read-row capacity overflow"))
    })?;
    let read_bytes_max_u64 = ranges.iter().try_fold(point_bytes, |total, range| {
        total
            .checked_add(range.max_bytes)
            .ok_or_else(|| CheckErrors::one("read-byte capacity overflow"))
    })?;
    if read_bytes_max_u64 > profile.hard_batch_bytes {
        return Err(CheckErrors::one(format!(
            "declared read capacity {read_bytes_max_u64} exceeds hard batch limit {}",
            profile.hard_batch_bytes
        )));
    }

    let mutation_bytes = mutation_charge(&mutations)?;
    if mutation_bytes > profile.hard_batch_bytes {
        return Err(CheckErrors::one(format!(
            "mutation charge {mutation_bytes} exceeds hard batch limit {}",
            profile.hard_batch_bytes
        )));
    }

    let request = LogicalBatch {
        kind: BatchKind::Measured,
        scenario: case.scenario,
        ordinal,
        logical_rows: case.batch_rows,
        limits: BatchLimits {
            request_bytes_max_u64: profile.hard_batch_bytes,
            read_rows_max_u64,
            read_bytes_max_u64,
            mutation_bytes_max_u64: profile.hard_batch_bytes,
        },
        point_reads,
        ranges,
        mutations,
    };
    let request_bytes = encoded_request_len(&request)?;
    if request_bytes > request.limits.request_bytes_max_u64 {
        return Err(CheckErrors::one(format!(
            "canonical request {request_bytes} exceeds hard batch limit {}",
            request.limits.request_bytes_max_u64
        )));
    }
    Ok(request)
}

fn aggregate_or_window_key(
    profile: &ModelProfile,
    case: &ModelCase,
    table: Table,
    vnode: u32,
    identity: u64,
) -> Result<LogicalKey, CheckErrors> {
    let key = entity_key(
        profile,
        case,
        table_tag(table),
        vnode,
        &[identity],
        identity.to_be_bytes().to_vec(),
    )?;
    Ok(LogicalKey { table, vnode, key })
}

fn timer_key(
    profile: &ModelProfile,
    case: &ModelCase,
    vnode: u32,
    logical_time: u64,
    stable_row: u64,
) -> Result<LogicalKey, CheckErrors> {
    let mut base = Vec::with_capacity(16);
    base.extend_from_slice(&logical_time.to_be_bytes());
    base.extend_from_slice(&stable_row.to_be_bytes());
    let key = entity_key(
        profile,
        case,
        table_tag(Table::TimerIndex),
        vnode,
        &[logical_time, stable_row],
        base,
    )?;
    Ok(LogicalKey {
        table: Table::TimerIndex,
        vnode,
        key,
    })
}

fn join_key(
    profile: &ModelProfile,
    case: &ModelCase,
    table: Table,
    vnode: u32,
    identity: u64,
    event_time: u32,
    stable_row: u64,
) -> Result<LogicalKey, CheckErrors> {
    let mut base = Vec::with_capacity(16);
    base.extend_from_slice(&identity.to_be_bytes());
    base.extend_from_slice(&event_time.to_be_bytes());
    base.extend_from_slice(&stable_row.to_be_bytes()[4..]);
    let key = entity_key(
        profile,
        case,
        table_tag(table),
        vnode,
        &[identity, u64::from(event_time), stable_row],
        base,
    )?;
    Ok(LogicalKey { table, vnode, key })
}

fn timer_boundary(
    case: &ModelCase,
    logical_time: u64,
    stable_row: u64,
) -> Result<Vec<u8>, CheckErrors> {
    let mut key = Vec::with_capacity(usize_from_u32(case.key_bytes)?);
    key.extend_from_slice(&logical_time.to_be_bytes());
    key.extend_from_slice(&stable_row.to_be_bytes());
    pad_boundary(case, key)
}

fn join_boundary(case: &ModelCase, identity: u64, event_time: u32) -> Result<Vec<u8>, CheckErrors> {
    let mut key = Vec::with_capacity(usize_from_u32(case.key_bytes)?);
    key.extend_from_slice(&identity.to_be_bytes());
    key.extend_from_slice(&event_time.to_be_bytes());
    key.extend_from_slice(&0_u32.to_be_bytes());
    pad_boundary(case, key)
}

fn pad_boundary(case: &ModelCase, mut key: Vec<u8>) -> Result<Vec<u8>, CheckErrors> {
    let target = usize_from_u32(case.key_bytes)?;
    if key.len() > target {
        return Err(CheckErrors::one(
            "range boundary base exceeds selected key width",
        ));
    }
    key.resize(target, 0);
    Ok(key)
}

fn stable_row_id(ordinal: u64, row: u32) -> Result<u64, CheckErrors> {
    ordinal
        .checked_shl(32)
        .and_then(|value| value.checked_add(u64::from(row)))
        .ok_or_else(|| CheckErrors::one("stable row identifier overflow"))
}

fn selected_row_width(case: &ModelCase) -> Result<u64, CheckErrors> {
    u64::from(case.key_bytes)
        .checked_add(u64::from(case.value_bytes))
        .ok_or_else(|| CheckErrors::one("selected logical row width overflow"))
}

fn ensure_within_hard_batch(
    rows: u64,
    bytes_per_row: u64,
    hard_batch_bytes: u64,
    label: &str,
) -> Result<(), CheckErrors> {
    let bytes = rows
        .checked_mul(bytes_per_row)
        .ok_or_else(|| CheckErrors::one(format!("{label} overflow")))?;
    if bytes > hard_batch_bytes {
        return Err(CheckErrors::one(format!(
            "{label} {bytes} exceeds hard batch limit {hard_batch_bytes}"
        )));
    }
    Ok(())
}

fn vnode_from_word(word: u64, profile: &ModelProfile) -> Result<u32, CheckErrors> {
    if profile.primary_vnode_count == 0 {
        return Err(CheckErrors::one("active vnode count is zero"));
    }
    u32::try_from(word % u64::from(profile.primary_vnode_count))
        .map_err(|_| CheckErrors::one("vnode remainder does not fit u32"))
}

fn mutation_key(mutation: &Mutation) -> &LogicalKey {
    match mutation {
        Mutation::Put { key, .. } | Mutation::Delete { key } => key,
    }
}

fn mutation_charge(mutations: &[Mutation]) -> Result<u64, CheckErrors> {
    mutations.iter().try_fold(0_u64, |total, mutation| {
        let key_bytes = u64::try_from(mutation_key(mutation).key.len())
            .map_err(|_| CheckErrors::one("mutation key length exceeds u64"))?;
        let charge = match mutation {
            Mutation::Put { value, .. } => key_bytes
                .checked_add(
                    u64::try_from(value.len())
                        .map_err(|_| CheckErrors::one("mutation value length exceeds u64"))?,
                )
                .ok_or_else(|| CheckErrors::one("mutation byte charge overflow"))?,
            Mutation::Delete { .. } => key_bytes,
        };
        total
            .checked_add(charge)
            .ok_or_else(|| CheckErrors::one("mutation byte total overflow"))
    })
}

fn encoded_request_len(request: &LogicalBatch) -> Result<u64, CheckErrors> {
    model_encoded_request_len(request)
        .map_err(|error| CheckErrors::one(format!("measure generated request: {error}")))
}

fn logical_key_cmp(left: &LogicalKey, right: &LogicalKey) -> std::cmp::Ordering {
    table_tag(left.table)
        .cmp(&table_tag(right.table))
        .then_with(|| left.vnode.cmp(&right.vnode))
        .then_with(|| left.key.cmp(&right.key))
}

fn range_cmp(left: &RangeRead, right: &RangeRead) -> std::cmp::Ordering {
    table_tag(left.table)
        .cmp(&table_tag(right.table))
        .then_with(|| left.vnode.cmp(&right.vnode))
        .then_with(|| left.start_inclusive.cmp(&right.start_inclusive))
        .then_with(|| left.end_exclusive.cmp(&right.end_exclusive))
        .then_with(|| left.max_rows.cmp(&right.max_rows))
        .then_with(|| left.max_bytes.cmp(&right.max_bytes))
}

const fn table_tag(table: Table) -> u8 {
    table.tag()
}

const fn scenario_tag(scenario: Scenario) -> u8 {
    scenario.tag()
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64, CheckErrors> {
    left.checked_add(right)
        .ok_or_else(|| CheckErrors::one(format!("{label} overflow")))
}

fn checked_mul(left: u64, right: u64, label: &str) -> Result<u64, CheckErrors> {
    left.checked_mul(right)
        .ok_or_else(|| CheckErrors::one(format!("{label} overflow")))
}

fn usize_from_u32(value: u32) -> Result<usize, CheckErrors> {
    usize::try_from(value).map_err(|_| CheckErrors::one("u32 value does not fit usize"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE: &[u8] = include_bytes!("../profiles/linux-nvme-v1.candidate.json");

    fn profile() -> ModelProfile {
        ModelProfile::from_profile_bytes(PROFILE).unwrap()
    }

    fn aggregate_case() -> ModelCase {
        ModelCase {
            scenario: Scenario::Aggregate,
            seed: 2_026_072_201,
            logical_state_bytes: 4_294_967_296,
            batch_rows: 128,
            request_count: 16,
            key_bytes: 32,
            value_bytes: 208,
            join_match_count: None,
        }
    }

    #[test]
    fn profile_hashes_exact_bytes_but_model_hashes_only_ordered_inputs() {
        let original = profile();
        assert_eq!(
            lowercase_sha256(&original.profile_sha256),
            "8a7788c104e87257b3f853096ca998f1d846f120c3bd97025a59ec33888786ca"
        );
        assert_eq!(
            lowercase_sha256(&original.model_input_sha256),
            "a68277a4f4b83bc72f3ba4d46fb1ad0fc34236cf90607e22634348945be4dc61"
        );
        let mut pretty_value: Value = serde_json::from_slice(PROFILE).unwrap();
        pretty_value["environment"]["provider"] = "different-but-schema-valid".into();
        let changed_bytes = serde_json::to_vec_pretty(&pretty_value).unwrap();
        let changed = ModelProfile::from_profile_bytes(&changed_bytes).unwrap();

        assert_ne!(original.profile_sha256, changed.profile_sha256);
        assert_eq!(original.model_input_sha256, changed.model_input_sha256);
        assert_eq!(lowercase_sha256(&original.profile_sha256).len(), 64);
        assert!(lowercase_sha256(&original.profile_sha256)
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    }

    #[test]
    fn strict_case_requires_all_fields_and_rejects_duplicates_and_unknowns() {
        let valid = br#"{
            "scenario":"aggregate","seed":2026072201,
            "logical_state_bytes":4294967296,"batch_rows":128,
            "request_count":16,"key_bytes":32,"value_bytes":208,
            "join_match_count":null
        }"#;
        assert_eq!(
            serde_json::from_slice::<ModelCase>(valid).unwrap(),
            aggregate_case()
        );

        let missing = br#"{
            "scenario":"aggregate","seed":2026072201,
            "logical_state_bytes":4294967296,"batch_rows":128,
            "request_count":16,"key_bytes":32,"value_bytes":208
        }"#;
        assert!(serde_json::from_slice::<ModelCase>(missing).is_err());

        let duplicate = br#"{
            "scenario":"aggregate","scenario":"join","seed":2026072201,
            "logical_state_bytes":4294967296,"batch_rows":128,
            "request_count":16,"key_bytes":32,"value_bytes":208,
            "join_match_count":null
        }"#;
        assert!(serde_json::from_slice::<ModelCase>(duplicate).is_err());

        let unknown = br#"{
            "scenario":"aggregate","seed":2026072201,
            "logical_state_bytes":4294967296,"batch_rows":128,
            "request_count":16,"key_bytes":32,"value_bytes":208,
            "join_match_count":null,"backend":"fjall"
        }"#;
        assert!(serde_json::from_slice::<ModelCase>(unknown).is_err());
    }

    #[test]
    fn case_validation_enforces_membership_shape_and_minimum_key_width() {
        let profile = profile();
        profile.validate_case(&aggregate_case()).unwrap();

        let mut invalid = aggregate_case();
        invalid.seed = 7;
        invalid.request_count = 0;
        invalid.join_match_count = Some(1);
        let error = profile.validate_case(&invalid).unwrap_err().to_string();
        assert!(error.contains("fixed_seeds"));
        assert!(error.contains("request_count"));
        assert!(error.contains("non-join"));

        let mut join = aggregate_case();
        join.scenario = Scenario::Join;
        join.key_bytes = 16;
        join.join_match_count = Some(64);
        profile.validate_case(&join).unwrap();
        join.join_match_count = None;
        assert!(profile.validate_case(&join).is_err());
    }

    #[test]
    fn counter_entity_and_value_expansion_are_repeatable_and_domain_separated() {
        let profile = profile();
        let case = aggregate_case();
        let counter = Counter {
            profile: &profile,
            case: &case,
            request_ordinal: 3,
        };
        assert_eq!(
            lowercase_sha256(&counter.block(7, 9)),
            "623e44c62da9effd5047ac25b8b4373d9d3998da032d06ee5a45fee5950e9362"
        );
        assert_eq!(counter.block(7, 9), counter.block(7, 9));
        assert_ne!(counter.block(7, 9), counter.block(7, 10));

        let a = entity_key(&profile, &case, 1, 4, &[99], 99_u64.to_be_bytes().to_vec()).unwrap();
        let b = entity_key(&profile, &case, 2, 4, &[99], 99_u64.to_be_bytes().to_vec()).unwrap();
        assert_eq!(a.len(), 32);
        assert_ne!(a, b);

        let value = expanded_value(&counter, 1, 7).unwrap();
        assert_eq!(value.len(), 208);
        assert_ne!(value, expanded_value(&counter, 2, 7).unwrap());
    }

    #[test]
    fn uniform_domain_is_checked_and_never_zero() {
        let mut case = aggregate_case();
        case.logical_state_bytes = 1;
        assert_eq!(uniform_domain(&case).unwrap(), 1);
    }

    #[test]
    fn aggregate_generation_is_directly_addressable_sorted_and_deduplicated() {
        let profile = profile();
        let case = aggregate_case();
        let direct = profile.generate_request(&case, 7).unwrap();
        let sequential = profile.requests(&case).unwrap().nth(7).unwrap().unwrap();

        assert_eq!(direct, sequential);
        assert_eq!(direct.kind, BatchKind::Measured);
        assert_eq!(direct.scenario, Scenario::Aggregate);
        assert_eq!(direct.ordinal, 7);
        assert_eq!(direct.logical_rows, case.batch_rows);
        assert!(!direct.point_reads.is_empty());
        assert!(direct.point_reads.len() <= case.batch_rows as usize);
        assert_eq!(direct.point_reads.len(), direct.mutations.len());
        assert!(direct
            .point_reads
            .windows(2)
            .all(|pair| logical_key_cmp(&pair[0], &pair[1]).is_lt()));
        assert!(direct.mutations.windows(2).all(|pair| logical_key_cmp(
            mutation_key(&pair[0]),
            mutation_key(&pair[1])
        )
        .is_lt()));
    }

    #[test]
    fn timer_generator_reaches_all_modes_and_always_reports_case_rows() {
        let profile = profile();
        let mut case = aggregate_case();
        case.scenario = Scenario::TimerWindow;
        case.key_bytes = 16;
        case.request_count = 256;

        let mut saw_mutation = false;
        let mut saw_scan = false;
        let mut saw_fire = false;
        for ordinal in 0..u64::from(case.request_count) {
            let request = profile.generate_request(&case, ordinal).unwrap();
            assert_eq!(request.logical_rows, case.batch_rows);
            saw_scan |= !request.ranges.is_empty();
            saw_fire |= request
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, Mutation::Delete { .. }));
            saw_mutation |= request
                .mutations
                .iter()
                .filter(|mutation| matches!(mutation, Mutation::Put { .. }))
                .count()
                == usize_from_u32(case.batch_rows).unwrap() * 2;
            if saw_mutation && saw_scan && saw_fire {
                break;
            }
        }
        assert!(saw_mutation && saw_scan && saw_fire);
    }

    #[test]
    fn join_generator_reaches_both_arriving_sides_with_bounded_opposite_scans() {
        let profile = profile();
        let mut case = aggregate_case();
        case.scenario = Scenario::Join;
        case.key_bytes = 16;
        case.join_match_count = Some(8);
        case.request_count = 256;

        let mut saw_left = false;
        let mut saw_right = false;
        for ordinal in 0..u64::from(case.request_count) {
            let request = profile.generate_request(&case, ordinal).unwrap();
            let mutation_table = mutation_key(&request.mutations[0]).table;
            saw_left |= mutation_table == Table::JoinLeftRows;
            saw_right |= mutation_table == Table::JoinRightRows;
            let expected_scan_table = if mutation_table == Table::JoinLeftRows {
                Table::JoinRightRows
            } else {
                Table::JoinLeftRows
            };
            assert!(request.ranges.iter().all(|range| {
                range.table == expected_scan_table
                    && range.max_rows == 8
                    && range.start_inclusive < range.end_exclusive
            }));
            if saw_left && saw_right {
                break;
            }
        }
        assert!(saw_left && saw_right);
    }

    #[test]
    fn preflight_accounts_exact_request_read_and_mutation_bytes_and_caps_replay() {
        let profile = profile();
        let case = aggregate_case();
        let preflight = profile.preflight_case(&case).unwrap();
        assert_eq!(preflight.request_count, case.request_count);
        assert_eq!(
            preflight.accounted_bytes,
            preflight.canonical_request_bytes
                + preflight.declared_read_capacity_bytes
                + preflight.mutation_bytes
        );
        assert!(preflight.accounted_bytes <= MAX_REPLAY_ACCOUNTED_BYTES);

        let mut timer = case.clone();
        timer.scenario = Scenario::TimerWindow;
        timer.key_bytes = 16;
        timer.request_count = 1;
        assert!(profile.preflight_case(&timer).is_ok());

        let mut join = case.clone();
        join.scenario = Scenario::Join;
        join.key_bytes = 16;
        join.join_match_count = Some(1);
        join.request_count = 1;
        assert!(profile.preflight_case(&join).is_ok());

        let mut too_large = case;
        too_large.scenario = Scenario::TimerWindow;
        too_large.key_bytes = 16;
        too_large.request_count = 256;
        let error = profile.preflight_case(&too_large).unwrap_err().to_string();
        assert!(error.contains("maximum is 67108864"));
        assert!(profile.requests(&too_large).is_err());
    }

    #[test]
    fn replay_and_model_safety_bounds_accept_exact_and_reject_max_plus_one() {
        assert!(validate_model_profile_bounds(
            &[MAX_MODEL_BATCH_ROWS],
            MAX_MODEL_HARD_BATCH_BYTES,
            MAX_MODEL_KEY_BYTES,
            &[MAX_MODEL_KEY_BYTES],
            MAX_MODEL_VALUE_BYTES,
            &[MAX_MODEL_VALUE_BYTES],
        )
        .is_ok());
        assert!(validate_model_profile_bounds(
            &[MAX_MODEL_BATCH_ROWS + 1],
            MAX_MODEL_HARD_BATCH_BYTES + 1,
            MAX_MODEL_KEY_BYTES + 1,
            &[],
            MAX_MODEL_VALUE_BYTES + 1,
            &[],
        )
        .unwrap_err()
        .to_string()
        .contains("batch_rows"));

        let mut work = aggregate_case();
        work.batch_rows = 8_192;
        work.request_count = 512;
        assert!(validate_generation_work(&work).is_ok());
        work.request_count = 513;
        assert!(validate_generation_work(&work).is_err());

        let mut accounting = ReplayAccounting::default();
        accounting
            .add(RequestCharge {
                canonical_request_bytes: MAX_REPLAY_ACCOUNTED_BYTES,
                declared_read_capacity_bytes: 0,
                mutation_bytes: 0,
            })
            .unwrap();
        assert_eq!(accounting.accounted_bytes, MAX_REPLAY_ACCOUNTED_BYTES);
        assert!(accounting
            .add(RequestCharge {
                canonical_request_bytes: 1,
                declared_read_capacity_bytes: 0,
                mutation_bytes: 0,
            })
            .is_err());

        let mut overflow = ReplayAccounting {
            accounted_bytes: u64::MAX,
            ..ReplayAccounting::default()
        };
        assert!(overflow
            .add(RequestCharge {
                canonical_request_bytes: 1,
                declared_read_capacity_bytes: 0,
                mutation_bytes: 0,
            })
            .unwrap_err()
            .to_string()
            .contains("overflow"));
    }

    #[test]
    fn preflight_charge_matches_generated_requests_for_every_shape() {
        fn assert_charge(profile: &ModelProfile, case: &ModelCase, ordinal: u64) {
            let expected = request_charge(profile, case, ordinal).unwrap();
            let request = generate_validated_request(profile, case, ordinal).unwrap();
            assert_eq!(
                expected.canonical_request_bytes,
                model_encoded_request_len(&request).unwrap()
            );
            assert_eq!(
                expected.declared_read_capacity_bytes,
                request.limits.read_bytes_max_u64
            );
            assert_eq!(
                expected.mutation_bytes,
                mutation_charge(&request.mutations).unwrap()
            );
        }

        let profile = profile();
        let aggregate = aggregate_case();
        assert_charge(&profile, &aggregate, 0);

        let mut timer = aggregate.clone();
        timer.scenario = Scenario::TimerWindow;
        timer.key_bytes = 16;
        timer.request_count = 256;
        let mut mutation = None;
        let mut scan = None;
        let mut fire = None;
        for ordinal in 0..u64::from(timer.request_count) {
            let counter = Counter {
                profile: &profile,
                case: &timer,
                request_ordinal: ordinal,
            };
            match timer_request_kind(&profile, &counter).unwrap() {
                TimerRequestKind::Mutation => mutation.get_or_insert(ordinal),
                TimerRequestKind::DueScan => scan.get_or_insert(ordinal),
                TimerRequestKind::FireDelete => fire.get_or_insert(ordinal),
            };
            if mutation.is_some() && scan.is_some() && fire.is_some() {
                break;
            }
        }
        for ordinal in [mutation.unwrap(), scan.unwrap(), fire.unwrap()] {
            assert_charge(&profile, &timer, ordinal);
        }

        let mut join = aggregate;
        join.scenario = Scenario::Join;
        join.key_bytes = 16;
        join.join_match_count = Some(8);
        assert_charge(&profile, &join, 0);
    }

    #[test]
    fn aggregate_capacity_is_charged_after_deduplication() {
        let profile = profile();
        let mut case = aggregate_case();
        case.batch_rows = 8_192;
        case.request_count = 1;
        case.key_bytes = 256;
        case.value_bytes = 1_024;

        let request = profile.generate_request(&case, 0).unwrap();
        assert!(request.mutations.len() < usize_from_u32(case.batch_rows).unwrap());
        assert!(model_encoded_request_len(&request).unwrap() <= profile.hard_batch_bytes);
        assert!(request.limits.read_bytes_max_u64 <= profile.hard_batch_bytes);
        assert!(mutation_charge(&request.mutations).unwrap() <= profile.hard_batch_bytes);
    }

    #[test]
    fn case_level_hard_batch_caps_reject_oversized_join_fanout() {
        let profile = profile();
        let mut case = aggregate_case();
        case.scenario = Scenario::Join;
        case.batch_rows = 8_192;
        case.key_bytes = 4_096;
        case.value_bytes = 65_536;
        case.join_match_count = Some(64);
        let error = profile.generate_request(&case, 0).unwrap_err().to_string();
        assert!(error.contains("hard batch limit"));
    }
}
