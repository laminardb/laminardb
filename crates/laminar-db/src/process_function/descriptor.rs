use laminar_core::state::PARTITIONING_ABI_VERSION;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::schema::{schema_from_canonical_fields, CanonicalField};
use super::{
    canonical_fields, ProcessDeterminism, ProcessFunctionDescriptor, ProcessFunctionLimits,
    ProcessRuntime, PythonEnvironmentBinding, STATE_CODEC_VERSION,
};
use crate::error::DbError;

pub(crate) const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const PROTOCOL_VERSION: u32 = 1;
pub(crate) const MAX_PYTHON_IMPORT_ROOTS: usize = 16;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    protocol_version: u32,
    runtime: String,
    function_id: String,
    pipeline_state_id: String,
    implementation_digest: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "decode_python_environment"
    )]
    python_environment: Option<PythonEnvironmentBinding>,
    input_schema: Vec<CanonicalField>,
    output_schema: Vec<CanonicalField>,
    key_columns: Vec<String>,
    partitioning_abi: u16,
    event_time_column: String,
    output_event_time_column: String,
    late_event_policy: String,
    input_changelog: String,
    output_changelog: String,
    value_state_name: String,
    state_codec_version: u32,
    timer_names: Vec<String>,
    determinism: ProcessDeterminism,
    limits: ProcessFunctionLimits,
}

impl Manifest {
    fn from_descriptor(descriptor: &ProcessFunctionDescriptor) -> Result<Self, DbError> {
        Ok(Self {
            version: descriptor.version,
            protocol_version: PROTOCOL_VERSION,
            runtime: match descriptor.runtime {
                ProcessRuntime::NativeRust => "trusted_native_rust",
                ProcessRuntime::RemoteRust => "remote_rust",
                ProcessRuntime::RemotePython => "remote_python",
            }
            .into(),
            function_id: descriptor.function_id.clone(),
            pipeline_state_id: descriptor.pipeline_state_id.clone(),
            implementation_digest: descriptor.implementation_digest.clone(),
            python_environment: descriptor.python_environment.clone(),
            input_schema: canonical_fields(&descriptor.input_schema)?,
            output_schema: canonical_fields(&descriptor.output_schema)?,
            key_columns: descriptor.key_columns.clone(),
            partitioning_abi: PARTITIONING_ABI_VERSION,
            event_time_column: descriptor.event_time_column.clone(),
            output_event_time_column: descriptor.output_event_time_column.clone(),
            late_event_policy: "reject".into(),
            input_changelog: "append_only".into(),
            output_changelog: "append_only".into(),
            value_state_name: descriptor.value_state_name.clone(),
            state_codec_version: STATE_CODEC_VERSION,
            timer_names: descriptor.timer_names.clone(),
            determinism: descriptor.determinism,
            limits: descriptor.limits,
        })
    }

    fn into_descriptor(self) -> Result<ProcessFunctionDescriptor, DbError> {
        if self.version != 1
            || self.protocol_version != PROTOCOL_VERSION
            || self.partitioning_abi != PARTITIONING_ABI_VERSION
            || self.late_event_policy != "reject"
            || self.input_changelog != "append_only"
            || self.output_changelog != "append_only"
            || self.state_codec_version != STATE_CODEC_VERSION
        {
            return Err(DbError::Unsupported(
                "unsupported process function manifest contract".into(),
            ));
        }
        let runtime = match self.runtime.as_str() {
            "trusted_native_rust" => ProcessRuntime::NativeRust,
            "remote_rust" => ProcessRuntime::RemoteRust,
            "remote_python" => ProcessRuntime::RemotePython,
            _ => {
                return Err(DbError::Unsupported(
                    "unsupported process function runtime".into(),
                ))
            }
        };
        let descriptor = ProcessFunctionDescriptor {
            version: self.version,
            runtime,
            function_id: self.function_id,
            pipeline_state_id: self.pipeline_state_id,
            implementation_digest: self.implementation_digest,
            determinism: self.determinism,
            python_environment: self.python_environment,
            input_schema: schema_from_canonical_fields(self.input_schema)?,
            output_schema: schema_from_canonical_fields(self.output_schema)?,
            key_columns: self.key_columns,
            event_time_column: self.event_time_column,
            output_event_time_column: self.output_event_time_column,
            value_state_name: self.value_state_name,
            timer_names: self.timer_names,
            limits: self.limits,
        };
        super::operator::validate_descriptor(&descriptor)?;
        Ok(descriptor)
    }
}

impl PythonEnvironmentBinding {
    pub(crate) fn validate(&self) -> Result<(), DbError> {
        if self.version != 1
            || !valid_relative_python_path(&self.executable)
            || !valid_python_handler(&self.handler)
            || !valid_sha256(&self.runtime_sha256)
            || self.import_roots_sha256.is_empty()
            || self.import_roots_sha256.len() > MAX_PYTHON_IMPORT_ROOTS
            || !self
                .import_roots_sha256
                .iter()
                .all(|digest| valid_sha256(digest))
        {
            return Err(DbError::InvalidOperation(
                "invalid Python environment binding".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn valid_relative_python_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 1024
        && !path.bytes().any(|byte| matches!(byte, b'\\' | b':' | 0))
        && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
}

pub(crate) fn valid_python_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(crate) fn valid_python_handler(value: &str) -> bool {
    value.split_once(':').is_some_and(|(module, function)| {
        valid_python_identifier(module) && valid_python_identifier(function)
    })
}

fn valid_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn decode_python_environment<'de, D>(
    deserializer: D,
) -> Result<Option<PythonEnvironmentBinding>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    PythonEnvironmentBinding::deserialize(deserializer).map(Some)
}

impl ProcessFunctionDescriptor {
    /// Encode the validated v1 function contract as canonical UTF-8 JSON. This is a portable
    /// package and negotiation manifest; it does not package native code or certify replay.
    ///
    /// # Errors
    /// Rejects unsupported schemas or invalid descriptor fields.
    pub fn to_manifest_json(&self) -> Result<Vec<u8>, DbError> {
        super::operator::validate_descriptor(self)?;
        let bytes = serde_json::to_vec(&Manifest::from_descriptor(self)?).map_err(|error| {
            DbError::InvalidOperation(format!("encode process manifest: {error}"))
        })?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(DbError::InvalidOperation(
                "process manifest exceeds 64 KiB".into(),
            ));
        }
        Ok(bytes)
    }

    /// Decode and validate a v1 function manifest. Unknown fields and unsupported semantics
    /// fail closed; the caller must separately bind the implementation digest to trusted code.
    ///
    /// # Errors
    /// Rejects malformed, oversized or incompatible manifests.
    pub fn from_manifest_json(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(DbError::InvalidOperation(
                "process manifest exceeds 64 KiB".into(),
            ));
        }
        let manifest: Manifest = serde_json::from_slice(bytes).map_err(|error| {
            DbError::InvalidOperation(format!("decode process manifest: {error}"))
        })?;
        manifest.into_descriptor()
    }

    pub(crate) fn binding_sha256(&self) -> Result<String, DbError> {
        Ok(format!("{:x}", Sha256::digest(self.to_manifest_json()?)))
    }
}
