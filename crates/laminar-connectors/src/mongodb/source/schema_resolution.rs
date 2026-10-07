//! Fixed CDC envelope with pre-consumption collection and deployment identity.

use arrow_schema::SchemaRef;
use std::collections::BTreeMap;

use super::{checkpoint, MongoDbSourceConfig, MongoDeploymentIdentity};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::resolution::{fixed_binding, NativeSchema, SchemaBinding};

pub(super) async fn resolve(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = MongoDbSourceConfig::from_config(config)?;
    let mut binding = fixed_binding(config, explicit, &super::mongodb_cdc_envelope_schema())?;
    #[cfg(feature = "mongodb-cdc")]
    {
        let database =
            super::admission::source_database(&parsed.connection_uri, &parsed.database).await?;
        let observation = super::admission::observe_mongodb_admission(
            &database,
            &parsed.database,
            &parsed.collection,
        )
        .await?;
        binding.value = Some(NativeSchema {
            format: "mongodb_change_stream".into(),
            identity: BTreeMap::from([
                (
                    "collection_uuid".into(),
                    observation
                        .collection
                        .collection_uuid
                        .hyphenated()
                        .to_string(),
                ),
                (
                    "deployment".into(),
                    observation.deployment_identity.encode(),
                ),
                ("database".into(), parsed.database),
                ("collection".into(), parsed.collection),
            ]),
            definition: serde_json::json!({"envelope": "expanded-change-stream-json-v1", "post_images_enabled": observation.collection.post_images_enabled}),
            references: Vec::new(),
        });
        Ok(binding)
    }
    #[cfg(not(feature = "mongodb-cdc"))]
    {
        let _ = (parsed, binding);
        Err(ConnectorError::FeatureUnsupported(
            "MongoDB collection schema identity requires the mongodb-cdc feature".into(),
        ))
    }
}

pub(super) fn committed_identity(
    binding: Option<&SchemaBinding>,
) -> Result<Option<(uuid::Uuid, MongoDeploymentIdentity)>, ConnectorError> {
    let Some(binding) = binding else {
        return Ok(None);
    };
    let native = binding.value.as_ref().ok_or_else(|| {
        ConnectorError::SchemaMismatch(
            "committed MongoDB CDC binding lacks collection identity; migrate the legacy catalog before activation".into(),
        )
    })?;
    if native.format != "mongodb_change_stream" {
        return Err(ConnectorError::SchemaMismatch(
            "MongoDB reader has a different native protocol".into(),
        ));
    }
    let collection = native.identity.get("collection_uuid").ok_or_else(|| {
        ConnectorError::SchemaMismatch("MongoDB contract has no collection UUID".into())
    })?;
    let deployment = native.identity.get("deployment").ok_or_else(|| {
        ConnectorError::SchemaMismatch("MongoDB contract has no deployment identity".into())
    })?;
    Ok(Some((
        checkpoint::parse_collection_uuid(collection)?,
        checkpoint::parse_deployment_identity(deployment)?,
    )))
}
