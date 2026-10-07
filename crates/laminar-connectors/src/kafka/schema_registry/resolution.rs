//! Bounded transitive reference resolution for the existing Avro codec.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    schema_to_arrow, CachedSchema, SchemaRegistryClient, SchemaType, SchemaVersionResponse,
};
use crate::error::ConnectorError;
use crate::schema::resolution::NativeSchemaArtifact;

pub(super) const MAX_SCHEMA_BYTES: usize = 1024 * 1024;
const MAX_REFERENCE_DEPTH: usize = 16;
const MAX_REFERENCE_BYTES: usize = 3 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

pub(super) fn encoded_subject(subject: &str) -> String {
    url::form_urlencoded::byte_serialize(subject.as_bytes()).collect()
}

impl SchemaRegistryClient {
    pub(super) async fn complete_schema(
        &self,
        id: i32,
        version: i32,
        schema: String,
        schema_type: &str,
        references: Vec<SchemaReference>,
    ) -> Result<CachedSchema, ConnectorError> {
        let schema_type: SchemaType = schema_type.parse()?;
        if id <= 0 || schema.len() > MAX_SCHEMA_BYTES {
            return Err(ConnectorError::SchemaMismatch(
                "invalid registry ID or oversized schema".into(),
            ));
        }
        if schema_type == SchemaType::Avro {
            validate_avro_json(&schema)?;
        }
        let artifacts = self.fetch_references(references).await?;
        let resolved_schema_str = if artifacts.is_empty() {
            schema.clone()
        } else {
            if schema_type != SchemaType::Avro {
                return Err(ConnectorError::FeatureUnsupported(
                    "registry references are implemented for Avro only".into(),
                ));
            }
            inline_avro_references(&schema, &artifacts)?
        };
        let arrow_schema = schema_to_arrow(schema_type, &resolved_schema_str)?;
        Ok(CachedSchema {
            id,
            version,
            schema_type,
            schema_str: schema,
            resolved_schema_str,
            references: artifacts,
            arrow_schema,
            inserted_at: std::time::Instant::now(),
        })
    }

    async fn fetch_references(
        &self,
        references: Vec<SchemaReference>,
    ) -> Result<Vec<NativeSchemaArtifact>, ConnectorError> {
        let mut pending = references
            .into_iter()
            .map(|reference| (reference, 1, BTreeSet::new()))
            .collect::<Vec<_>>();
        let mut artifacts = BTreeMap::new();
        let mut bytes = 0;
        while let Some((reference, depth, mut ancestors)) = pending.pop() {
            let key = (reference.subject.clone(), reference.version);
            if reference.version <= 0
                || depth > MAX_REFERENCE_DEPTH
                || !ancestors.insert(key.clone())
            {
                return Err(ConnectorError::SchemaMismatch(
                    "cyclic, too-deep or unversioned schema reference".into(),
                ));
            }
            if artifacts.contains_key(&(reference.name.clone(), key.clone())) {
                continue;
            }
            if artifacts.len() + pending.len()
                >= laminar_core::schema_binding::MAX_SCHEMA_REFERENCES
            {
                return Err(ConnectorError::SchemaMismatch(
                    "too many transitive schema references".into(),
                ));
            }
            let url = format!(
                "{}/subjects/{}/versions/{}",
                self.base_url,
                encoded_subject(&reference.subject),
                reference.version
            );
            let response: SchemaVersionResponse = self
                .get_json(&url, "fetch concrete schema reference")
                .await?;
            if response.version != reference.version
                || response.id <= 0
                || response.schema_type != "AVRO"
                || response.schema.len() > MAX_SCHEMA_BYTES
            {
                return Err(ConnectorError::SchemaMismatch(
                    "schema reference changed version or exceeds size limit".into(),
                ));
            }
            validate_avro_json(&response.schema)?;
            bytes += response.schema.len();
            if bytes > MAX_REFERENCE_BYTES {
                return Err(ConnectorError::SchemaMismatch(
                    "transitive schema references exceed byte limit".into(),
                ));
            }
            let definition = serde_json::json!({
                "schema": serde_json::from_str::<serde_json::Value>(&response.schema).map_err(|_| ConnectorError::SchemaMismatch("malformed referenced Avro schema".into()))?,
                "references": response.references,
            });
            let artifact = NativeSchemaArtifact {
                name: reference.name.clone(),
                identity: BTreeMap::from([
                    ("subject".into(), reference.subject),
                    ("version".into(), reference.version.to_string()),
                    ("id".into(), response.id.to_string()),
                ]),
                definition,
            };
            artifacts.insert((reference.name, key), artifact);
            for child in response.references {
                pending.push((child, depth + 1, ancestors.clone()));
            }
        }
        Ok(artifacts.into_values().collect())
    }
}

fn validate_avro_json(schema: &str) -> Result<(), ConnectorError> {
    let root: serde_json::Value = serde_json::from_str(schema)
        .map_err(|_| ConnectorError::SchemaMismatch("malformed Avro schema".into()))?;
    let mut pending = vec![(&root, 0)];
    let mut nodes = 0;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if depth > 64 || nodes > 65536 {
            return Err(ConnectorError::SchemaMismatch(
                "Avro schema exceeds nesting/node limits".into(),
            ));
        }
        match value {
            serde_json::Value::Array(values) => {
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(values) => {
                pending.extend(values.values().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}

fn inline_avro_references(
    schema: &str,
    artifacts: &[NativeSchemaArtifact],
) -> Result<String, ConnectorError> {
    let definitions = artifacts
        .iter()
        .map(|artifact| (artifact.name.as_str(), &artifact.definition["schema"]))
        .collect::<BTreeMap<_, _>>();
    if definitions.len() != artifacts.len() {
        return Err(ConnectorError::SchemaMismatch(
            "ambiguous Avro reference names".into(),
        ));
    }
    let mut schema: serde_json::Value = serde_json::from_str(schema)
        .map_err(|_| ConnectorError::SchemaMismatch("malformed Avro schema".into()))?;
    inline_type(&mut schema, &definitions, &mut BTreeSet::new(), 0)?;
    let encoded = serde_json::to_string(&schema)
        .map_err(|_| ConnectorError::SchemaMismatch("Avro reference encoding failed".into()))?;
    if encoded.len() > MAX_SCHEMA_BYTES {
        return Err(ConnectorError::SchemaMismatch(
            "expanded Avro schema exceeds byte limit".into(),
        ));
    }
    Ok(encoded)
}

fn inline_type(
    schema: &mut serde_json::Value,
    definitions: &BTreeMap<&str, &serde_json::Value>,
    defined: &mut BTreeSet<String>,
    depth: usize,
) -> Result<(), ConnectorError> {
    if depth > 32 {
        return Err(ConnectorError::SchemaMismatch(
            "Avro type nesting exceeds 32".into(),
        ));
    }
    match schema {
        serde_json::Value::String(name) => {
            if let Some(definition) = definitions.get(name.as_str()) {
                if defined.insert(name.clone()) {
                    *schema = (*definition).clone();
                    inline_type(schema, definitions, defined, depth + 1)?;
                }
            }
        }
        serde_json::Value::Array(branches) => {
            for branch in branches {
                inline_type(branch, definitions, defined, depth + 1)?;
            }
        }
        serde_json::Value::Object(object) => {
            if let Some(serde_json::Value::Array(fields)) = object.get_mut("fields") {
                for field in fields {
                    let field_type = field.get_mut("type").ok_or_else(|| {
                        ConnectorError::SchemaMismatch("Avro field has no type".into())
                    })?;
                    inline_type(field_type, definitions, defined, depth + 1)?;
                }
            }
            for key in ["type", "items", "values"] {
                if let Some(child) = object.get_mut(key) {
                    inline_type(child, definitions, defined, depth + 1)?;
                }
            }
        }
        _ => return Err(ConnectorError::SchemaMismatch("malformed Avro type".into())),
    }
    Ok(())
}
