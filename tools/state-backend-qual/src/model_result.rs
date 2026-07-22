use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::model::{
    encode_observation, encode_request, ModelError, Mutation, ReferenceModel, MODEL_VERSION,
    OBSERVATION_ENCODING_VERSION, REQUEST_ENCODING_VERSION, STATE_ENCODING_VERSION,
};
use crate::workload::{lowercase_sha256, ModelCase, ModelProfile, GENERATOR_VERSION};
use crate::{decode_unique_json, CheckErrors, MAX_MODEL_RESULT_BYTES, NOTICE};

pub const MODEL_RESULT_SCHEMA_VERSION: &str = "state-backend-model-result/v1";

const MODEL_RESULT_SCHEMA: &str = include_str!("../schema/model-result-v1.schema.json");
const REQUEST_STREAM_DOMAIN: &[u8] = b"LDB-SBQ-REQUEST-STREAM-V1\0";
const OBSERVATION_STREAM_DOMAIN: &[u8] = b"LDB-SBQ-OBSERVATION-STREAM-V1\0";
const TRACE_DOMAIN: &[u8] = b"LDB-SBQ-TRACE-V1\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResult {
    pub schema_version: String,
    pub notice: String,
    pub qualification_eligible: bool,
    pub versions: ResultVersions,
    pub profile: ResultProfile,
    pub case: ModelCase,
    pub counters: ResultCounters,
    pub digests: ResultDigests,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultVersions {
    pub model: String,
    pub generator: String,
    pub request_encoding: String,
    pub observation_encoding: String,
    pub state_encoding: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultProfile {
    pub id: String,
    pub sha256: String,
    pub model_input_sha256: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultCounters {
    pub requests: u64,
    pub logical_input_rows: u64,
    pub point_reads: u64,
    pub range_reads: u64,
    pub puts: u64,
    pub deletes: u64,
    pub returned_point_values: u64,
    pub returned_point_bytes: u64,
    pub returned_range_rows: u64,
    pub returned_range_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultDigests {
    pub requests_sha256: String,
    pub observations_sha256: String,
    pub trace_sha256: String,
    pub live_state_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelResultSummary {
    pub profile_id: String,
    pub scenario: crate::model::Scenario,
    pub requests: u64,
}

pub fn generate_model_result(
    profile_bytes: &[u8],
    case: &ModelCase,
) -> Result<ModelResult, CheckErrors> {
    let profile = ModelProfile::from_profile_bytes(profile_bytes)?;
    let mut model = ReferenceModel::new(
        profile.primary_vnode_count,
        profile.encoded_key_bytes_max,
        profile.stored_state_bytes_max,
    )
    .map_err(model_error)?;

    let request_count = u64::from(case.request_count);
    let mut requests_hasher = stream_hasher(REQUEST_STREAM_DOMAIN, request_count);
    let mut observations_hasher = stream_hasher(OBSERVATION_STREAM_DOMAIN, request_count);
    let mut trace_hasher = Sha256::new();
    trace_hasher.update(TRACE_DOMAIN);
    trace_hasher.update(request_count.to_be_bytes());
    let mut counters = ResultCounters::default();

    for generated in profile.requests(case)? {
        let request = generated?;
        let request_bytes = encode_request(&request).map_err(model_error)?;
        update_stream(&mut requests_hasher, &request_bytes)?;

        counters.requests = checked_add(counters.requests, 1, "request counter")?;
        counters.logical_input_rows = checked_add(
            counters.logical_input_rows,
            u64::from(request.logical_rows),
            "logical input row counter",
        )?;
        counters.point_reads = checked_add(
            counters.point_reads,
            usize_to_u64(request.point_reads.len())?,
            "point read counter",
        )?;
        counters.range_reads = checked_add(
            counters.range_reads,
            usize_to_u64(request.ranges.len())?,
            "range read counter",
        )?;
        for mutation in &request.mutations {
            match mutation {
                Mutation::Put { .. } => {
                    counters.puts = checked_add(counters.puts, 1, "put counter")?;
                }
                Mutation::Delete { .. } => {
                    counters.deletes = checked_add(counters.deletes, 1, "delete counter")?;
                }
            }
        }

        let observation = model.execute(&request).map_err(model_error)?;
        for point in &observation.point_results {
            let mut bytes = usize_to_u64(point.key.key.len())?;
            if let Some(value) = &point.value {
                counters.returned_point_values = checked_add(
                    counters.returned_point_values,
                    1,
                    "returned point value counter",
                )?;
                bytes = checked_add(bytes, usize_to_u64(value.len())?, "point result bytes")?;
            }
            counters.returned_point_bytes = checked_add(
                counters.returned_point_bytes,
                bytes,
                "returned point byte counter",
            )?;
        }
        for range in &observation.range_results {
            counters.returned_range_rows = checked_add(
                counters.returned_range_rows,
                usize_to_u64(range.rows.len())?,
                "returned range row counter",
            )?;
            for row in &range.rows {
                let bytes = checked_add(
                    usize_to_u64(row.key.key.len())?,
                    usize_to_u64(row.value.len())?,
                    "range result bytes",
                )?;
                counters.returned_range_bytes = checked_add(
                    counters.returned_range_bytes,
                    bytes,
                    "returned range byte counter",
                )?;
            }
        }

        let observation_bytes = encode_observation(&observation).map_err(model_error)?;
        update_stream(&mut observations_hasher, &observation_bytes)?;
        update_trace(&mut trace_hasher, &request_bytes, &observation_bytes)?;
    }

    let live_state = model.live_digest().map_err(model_error)?;
    Ok(ModelResult {
        schema_version: MODEL_RESULT_SCHEMA_VERSION.to_owned(),
        notice: NOTICE.to_owned(),
        qualification_eligible: false,
        versions: ResultVersions {
            model: MODEL_VERSION.to_owned(),
            generator: GENERATOR_VERSION.to_owned(),
            request_encoding: REQUEST_ENCODING_VERSION.to_owned(),
            observation_encoding: OBSERVATION_ENCODING_VERSION.to_owned(),
            state_encoding: STATE_ENCODING_VERSION.to_owned(),
        },
        profile: ResultProfile {
            id: profile.profile_id,
            sha256: lowercase_sha256(&profile.profile_sha256),
            model_input_sha256: lowercase_sha256(&profile.model_input_sha256),
        },
        case: case.clone(),
        counters,
        digests: ResultDigests {
            requests_sha256: lowercase_sha256(&requests_hasher.finalize().into()),
            observations_sha256: lowercase_sha256(&observations_hasher.finalize().into()),
            trace_sha256: lowercase_sha256(&trace_hasher.finalize().into()),
            live_state_sha256: lowercase_sha256(&live_state),
        },
    })
}

pub fn validate_model_result(
    profile_bytes: &[u8],
    result_bytes: &[u8],
) -> Result<ModelResultSummary, CheckErrors> {
    let result_value = decode_unique_json(result_bytes, MAX_MODEL_RESULT_BYTES, "model result")?;
    validate_result_schema(&result_value)?;
    let submitted: ModelResult = serde_json::from_value(result_value)
        .map_err(|error| CheckErrors::one(format!("decode typed model result: {error}")))?;
    let expected = generate_model_result(profile_bytes, &submitted.case)?;
    if submitted != expected {
        return Err(CheckErrors::one(
            "model result does not match deterministic replay",
        ));
    }
    Ok(ModelResultSummary {
        profile_id: submitted.profile.id,
        scenario: submitted.case.scenario,
        requests: submitted.counters.requests,
    })
}

pub fn serialize_model_result(result: &ModelResult) -> Result<Vec<u8>, CheckErrors> {
    serde_json::to_vec_pretty(result)
        .map_err(|error| CheckErrors::one(format!("encode model result: {error}")))
}

fn validate_result_schema(result: &Value) -> Result<(), CheckErrors> {
    let schema: Value = serde_json::from_str(MODEL_RESULT_SCHEMA).map_err(|error| {
        CheckErrors::one(format!("decode embedded model-result schema: {error}"))
    })?;
    let validator = jsonschema::validator_for(&schema).map_err(|error| {
        CheckErrors::one(format!("compile embedded model-result schema: {error}"))
    })?;
    let errors = validator
        .iter_errors(result)
        .map(|error| format!("schema {}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CheckErrors::many(errors))
    }
}

fn stream_hasher(domain: &[u8], count: u64) -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(count.to_be_bytes());
    hasher
}

fn update_stream(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), CheckErrors> {
    hasher.update(usize_to_u64(bytes.len())?.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn update_trace(
    hasher: &mut Sha256,
    request: &[u8],
    observation: &[u8],
) -> Result<(), CheckErrors> {
    update_stream(hasher, request)?;
    update_stream(hasher, observation)
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64, CheckErrors> {
    left.checked_add(right)
        .ok_or_else(|| CheckErrors::one(format!("{label} overflow")))
}

fn usize_to_u64(value: usize) -> Result<u64, CheckErrors> {
    u64::try_from(value).map_err(|_| CheckErrors::one("usize does not fit u64"))
}

fn model_error(error: ModelError) -> CheckErrors {
    CheckErrors::one(format!("reference model: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Scenario;

    const PROFILE: &[u8] = include_bytes!("../profiles/linux-nvme-v1.candidate.json");
    const AGGREGATE_RESULT_FIXTURE: &[u8] =
        include_bytes!("../tests/fixtures/model-result-aggregate-v1.json");

    fn case() -> ModelCase {
        ModelCase {
            scenario: Scenario::Aggregate,
            seed: 2_026_072_201,
            logical_state_bytes: 4_294_967_296,
            batch_rows: 128,
            request_count: 2,
            key_bytes: 32,
            value_bytes: 208,
            join_match_count: None,
        }
    }

    #[test]
    fn generated_result_round_trips_through_schema_and_replay() {
        let result = generate_model_result(PROFILE, &case()).unwrap();
        let fixture: ModelResult = serde_json::from_slice(AGGREGATE_RESULT_FIXTURE).unwrap();
        assert_eq!(result, fixture);
        let bytes = serialize_model_result(&result).unwrap();
        let summary = validate_model_result(PROFILE, &bytes).unwrap();
        assert!(validate_model_result(PROFILE, AGGREGATE_RESULT_FIXTURE).is_ok());
        assert_eq!(summary.profile_id, "linux-nvme-v1");
        assert_eq!(summary.scenario, Scenario::Aggregate);
        assert_eq!(summary.requests, 2);
        assert!(!result.qualification_eligible);
        assert_eq!(result.notice, NOTICE);
    }

    #[test]
    fn schema_and_replay_reject_unknown_uppercase_and_inconsistent_fields() {
        let result = generate_model_result(PROFILE, &case()).unwrap();
        let mut value = serde_json::to_value(result).unwrap();
        value["backend"] = Value::String("fjall".to_owned());
        assert!(validate_model_result(PROFILE, &serde_json::to_vec(&value).unwrap()).is_err());

        value.as_object_mut().unwrap().remove("backend");
        value["digests"]["trace_sha256"] = "A".repeat(64).into();
        assert!(validate_model_result(PROFILE, &serde_json::to_vec(&value).unwrap()).is_err());

        value["digests"]["trace_sha256"] = "0".repeat(64).into();
        assert!(validate_model_result(PROFILE, &serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn duplicate_keys_and_oversized_results_are_rejected_before_replay() {
        let duplicate = br#"{"schema_version":"a","schema_version":"b"}"#;
        let error = validate_model_result(PROFILE, duplicate)
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicate object key"));

        let oversized = vec![b' '; MAX_MODEL_RESULT_BYTES + 1];
        let error = validate_model_result(PROFILE, &oversized)
            .unwrap_err()
            .to_string();
        assert!(error.contains("maximum is 1048576"));
    }

    #[test]
    fn stream_framing_matches_frozen_v1_golden() {
        let finish = |hasher: Sha256| {
            let digest: [u8; 32] = hasher.finalize().into();
            lowercase_sha256(&digest)
        };

        let mut requests = stream_hasher(REQUEST_STREAM_DOMAIN, 1);
        update_stream(&mut requests, b"r").unwrap();
        assert_eq!(
            finish(requests),
            "f4a6d86c65a614f38b59cabfdd719ee0869c2015ba1fdcd99daa85d5c8846368"
        );

        let mut observations = stream_hasher(OBSERVATION_STREAM_DOMAIN, 1);
        update_stream(&mut observations, b"o").unwrap();
        assert_eq!(
            finish(observations),
            "82ad3153f8885bef8bf744054001a225c61fd4482b8299b3a865f39ef6830118"
        );

        let mut trace = stream_hasher(TRACE_DOMAIN, 1);
        update_trace(&mut trace, b"r", b"o").unwrap();
        assert_eq!(
            finish(trace),
            "7580fa0c032a61997728f9127e8fcb4ffb9e087c970cfa9189bda93499ea3f71"
        );
    }

    #[test]
    fn two_request_timer_mutation_and_join_results_match_frozen_digests() {
        let cases = [
            (
                Scenario::TimerWindow,
                None,
                256,
                0,
                512,
                "c1f496c244dacb77f82ebe9ee0a563c5fdb5161bf71f04102049508a894c53d1",
                "aab4890b53a30d1edbd6dc91bef3194d5f4dc12405aebee052041f45e495ab29",
                "40f6fd151c80353e48a8df710d714e8ef3fa0046d9871350e93ef4bd21502ea5",
                "f1d924bbd423716d02245fac93695354960a7703b1b22644ddb9052613aa4cca",
            ),
            (
                Scenario::Join,
                Some(8),
                0,
                256,
                256,
                "5f7f4e4181b0f679ca5d3501ec0f3a1382a8e104e043933d83cc49dda063d888",
                "7e99bee3a9c4424048b220eefc1ddff3f4b98d5c4638a8e30b1a443cd3d16314",
                "233bd2fd8387606d4c16123b268b8132bc6c5e07044a162cad410d6c93a32108",
                "4ce0bf71dc178c3b489f6b6cff8c8fe6e0fb1755a485fbff97589c4b6a7d39df",
            ),
        ];
        for (
            scenario,
            join_match_count,
            point_reads,
            range_reads,
            puts,
            requests_sha256,
            observations_sha256,
            trace_sha256,
            live_state_sha256,
        ) in cases
        {
            let mut selected = case();
            selected.scenario = scenario;
            selected.key_bytes = 16;
            selected.join_match_count = join_match_count;
            let result = generate_model_result(PROFILE, &selected).unwrap();
            assert_eq!(result.counters.point_reads, point_reads);
            assert_eq!(result.counters.range_reads, range_reads);
            assert_eq!(result.counters.puts, puts);
            assert_eq!(result.digests.requests_sha256, requests_sha256);
            assert_eq!(result.digests.observations_sha256, observations_sha256);
            assert_eq!(result.digests.trace_sha256, trace_sha256);
            assert_eq!(result.digests.live_state_sha256, live_state_sha256);
            let bytes = serialize_model_result(&result).unwrap();
            assert!(validate_model_result(PROFILE, &bytes).is_ok());
        }
    }
}
