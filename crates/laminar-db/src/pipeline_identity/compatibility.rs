//! Object identities derived from the same canonical payload as strict checkpoint identity.
//!
//! These control-path hashes describe definitions, not authority to restore across pipelines.
//! Full pipeline identity remains unchanged and mandatory for ordinary restore.

use std::collections::BTreeMap;

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{canonical_pipeline, identity_for_payload, PipelineIdentityContext};
use crate::error::DbError;
use laminar_core::checkpoint::PipelineIdentity;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PipelineCompatibilityIdentities {
    pub(crate) pipeline: PipelineIdentity,
    pub(crate) environment_sha256: String,
    pub(crate) objects: BTreeMap<String, String>,
}

pub(crate) fn compatibility_identities(
    context: &PipelineIdentityContext<'_>,
) -> Result<PipelineCompatibilityIdentities, DbError> {
    let mut payload = canonical_pipeline(context)?;
    let pipeline = identity_for_payload(&payload)?;
    let mut objects = BTreeMap::new();
    for source in payload.sources.drain(..) {
        insert(&mut objects, &source.name, "source", &source)?;
    }
    for stream in payload.streams.drain(..) {
        insert(&mut objects, &stream.name, "stream", &stream)?;
    }
    for sink in payload.sinks.drain(..) {
        insert(&mut objects, &sink.name, "sink", &sink)?;
    }
    for table in payload.tables.drain(..) {
        insert(&mut objects, &table.name, "table", &table)?;
    }
    // Keeping the canonical payload with empty object lists automatically includes every global
    // ABI/config field. Adding an unrelated object cannot alter a preserved object's definition.
    Ok(PipelineCompatibilityIdentities {
        pipeline,
        environment_sha256: digest(&("laminardb-topology-environment-v1", payload))?,
        objects,
    })
}

fn insert(
    objects: &mut BTreeMap<String, String>,
    name: &str,
    kind: &str,
    definition: &impl Serialize,
) -> Result<(), DbError> {
    let identity = digest(&("laminardb-topology-definition-v1", kind, definition))?;
    if objects.insert(name.to_owned(), identity).is_some() {
        return Err(
            laminar_core::cluster::control::TopologyError::Invalid(format!(
                "canonical pipeline repeats object '{name}'"
            ))
            .into(),
        );
    }
    Ok(())
}

pub(crate) fn digest(value: &impl Serialize) -> Result<String, DbError> {
    let encoded = serde_json::to_vec(value).map_err(|error| {
        laminar_core::cluster::control::TopologyError::Invalid(format!(
            "topology compatibility encoding failed: {error}"
        ))
    })?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}
