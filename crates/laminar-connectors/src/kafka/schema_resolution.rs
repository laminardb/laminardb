//! Creation-time Kafka reader/writer contracts. No consumer or producer is constructed here.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;

use super::config::{
    resolve_value_subject, KafkaSourceConfig, SrAuth, SubjectNameStrategy, TopicSubscription,
};
use super::schema_registry::{CachedSchema, SchemaRegistryClient, SchemaType};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::resolution::{
    bind_external, logical_binding, NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};
use crate::serde::Format;

pub(super) fn registry_client(
    config: &ConnectorConfig,
) -> Result<SchemaRegistryClient, ConnectorError> {
    let url = config.require("schema.registry.url")?;
    let parsed = url::Url::parse(url).map_err(|_| {
        ConnectorError::ConfigurationError("invalid Schema Registry service URL".into())
    })?;
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ConnectorError::ConfigurationError("Schema Registry service URL cannot embed credentials, query parameters or fragments; use the authentication settings".into()));
    }
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.path().contains("/subjects/")
        || parsed.path().contains("/schemas/")
    {
        return Err(ConnectorError::ConfigurationError("schema.registry.url must name the registry service; select the subject/version with schema.registry.value.subject and schema.registry.value.version".into()));
    }
    let auth = match (
        config.get("schema.registry.username"),
        config.get("schema.registry.password"),
    ) {
        (Some(username), Some(password)) => Some(SrAuth {
            username: username.into(),
            password: password.into(),
        }),
        (None, None) => None,
        _ => {
            return Err(ConnectorError::ConfigurationError(
                "Schema Registry username and password must both be supplied".into(),
            ))
        }
    };
    if let Some(ca) = config.get("schema.registry.ssl.ca.location") {
        SchemaRegistryClient::with_tls_mtls(
            url,
            auth,
            ca,
            config.get("schema.registry.ssl.certificate.location"),
            config.get("schema.registry.ssl.key.location"),
        )
    } else {
        SchemaRegistryClient::new(url, auth)
    }
}

pub(super) async fn resolve_source(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    resolve_source_with_registry(config, explicit, None).await
}

pub(super) async fn resolve_source_with_registry(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
    existing: Option<&SchemaRegistryClient>,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = KafkaSourceConfig::from_config(config)?;
    if parsed.format != Format::Avro {
        reject_registry_for_plain_format(config)?;
        let reader =
            crate::serde::schema_contract::reader_binding(config, parsed.format, explicit)?;
        let schema = Arc::new(reader.logical);
        super::source::validate_kafka_output_schema(
            &schema,
            parsed.include_metadata,
            parsed.include_headers,
        )?;
        let output = super::source::kafka_output_schema(
            &schema,
            parsed.include_metadata,
            parsed.include_headers,
        );
        return logical_binding(config, SchemaDirection::Source, reader.origin, &output);
    }
    reject_unsupported_key_codec(config)?;
    let topic = match &parsed.subscription {
        TopicSubscription::Topics(topics) if topics.len() == 1 => Some(topics[0].as_str()),
        _ => None,
    };
    let subject = selected_subject(
        config,
        topic,
        parsed.schema_registry_subject_strategy,
        parsed.schema_registry_record_name.as_deref(),
    )?;
    let owned;
    let registry = if let Some(client) = existing {
        client
    } else {
        owned = registry_client(config)?;
        &owned
    };
    let cached = fetch_selected(registry, config, &subject).await?;
    let origin = if explicit.is_some() {
        SchemaOrigin::Explicit
    } else {
        SchemaOrigin::Metadata
    };
    let payload = explicit.unwrap_or_else(|| Arc::clone(&cached.arrow_schema));
    super::source::validate_kafka_output_schema(
        &payload,
        parsed.include_metadata,
        parsed.include_headers,
    )?;
    let logical = super::source::kafka_output_schema(
        &payload,
        parsed.include_metadata,
        parsed.include_headers,
    );
    let mut binding = logical_binding(config, SchemaDirection::Source, origin, &logical)?;
    bind_external(
        &mut binding,
        &super::source::kafka_output_schema(
            &cached.arrow_schema,
            parsed.include_metadata,
            parsed.include_headers,
        ),
    )?;
    binding.value = Some(native_contract(config, &subject, cached)?);
    Ok(binding)
}

pub(super) async fn resolve_sink(
    config: &ConnectorConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    resolve_sink_with_registry(config, input, None).await
}

pub(super) async fn resolve_sink_with_registry(
    config: &ConnectorConfig,
    input: SchemaRef,
    existing: Option<&SchemaRegistryClient>,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = super::sink_config::KafkaSinkConfig::from_config(config)?;
    let business = if parsed.envelope == super::sink_config::SinkEnvelope::Upsert {
        let indices = input
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !matches!(field.name().as_str(), "_op" | "_ts_ms" | "__weight"))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        Arc::new(
            input
                .project(&indices)
                .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?,
        )
    } else {
        Arc::clone(&input)
    };
    let mut binding = logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, &input)?;
    binding.control_fields = input
        .fields()
        .iter()
        .filter(|field| business.index_of(field.name()).is_err())
        .map(|field| field.name().clone())
        .collect();
    bind_external(&mut binding, &business)?;
    if parsed.format != Format::Avro {
        reject_registry_for_plain_format(config)?;
        crate::serde::schema_contract::validate_writer(parsed.format, &business)?;
        return Ok(binding);
    }
    reject_unsupported_key_codec(config)?;
    let strategy = config
        .get("schema.registry.subject.name.strategy")
        .map(str::parse)
        .transpose()?
        .unwrap_or(SubjectNameStrategy::TopicName);
    let subject = selected_subject(
        config,
        Some(&parsed.topic),
        strategy,
        config.get("schema.registry.record.name"),
    )?;
    let registration = config
        .get_parsed::<bool>("schema.registry.auto.register")?
        .unwrap_or(false);
    if parsed.schema_compatibility.is_some() && !registration {
        return Err(ConnectorError::ConfigurationError("schema.compatibility changes registry configuration and requires schema.registry.auto.register=true".into()));
    }
    let owned;
    let registry = if let Some(client) = existing {
        client
    } else {
        owned = registry_client(config)?;
        &owned
    };
    if registration {
        let schema = super::schema_registry::arrow_to_avro_schema(&business, &parsed.topic)
            .map_err(ConnectorError::Serde)?;
        let external = super::schema_registry::avro_to_arrow_schema(&schema)?;
        bind_external(&mut binding, &external)?;
        let compatible = registry.check_compatibility(&subject, &schema).await?;
        if !compatible.is_compatible {
            return Err(ConnectorError::SchemaMismatch(
                "query writer schema is incompatible with the registry subject".into(),
            ));
        }
        binding.value = Some(NativeSchema {
            format: "avro".into(),
            identity: registry_identity(config, &subject),
            definition: serde_json::json!({"schema": serde_json::from_str::<serde_json::Value>(&schema).map_err(|_| ConnectorError::SchemaMismatch("invalid generated Avro schema".into()))?, "resolved": serde_json::from_str::<serde_json::Value>(&schema).map_err(|_| ConnectorError::SchemaMismatch("invalid generated Avro schema".into()))?}),
            references: Vec::new(),
        });
    } else {
        let cached = fetch_selected(registry, config, &subject).await?;
        bind_external(&mut binding, &cached.arrow_schema)?;
        binding.value = Some(native_contract(config, &subject, cached)?);
    }
    Ok(binding)
}

pub(super) async fn prepare_sink(
    config: &ConnectorConfig,
    binding: &mut SchemaBinding,
) -> Result<(), ConnectorError> {
    let Some(native) = binding.value.as_mut() else {
        return Ok(());
    };
    if native.identity.contains_key("id") {
        return Ok(());
    }
    if !config
        .get_parsed::<bool>("schema.registry.auto.register")?
        .unwrap_or(false)
    {
        return Err(ConnectorError::ConfigurationError(
            "writer has no concrete schema ID; schema registration is disabled".into(),
        ));
    }
    let subject = native
        .identity
        .get("subject")
        .ok_or_else(|| ConnectorError::SchemaMismatch("writer subject is missing".into()))?;
    let registry = registry_client(config)?;
    let schema = serde_json::to_string(&native.definition["schema"])
        .map_err(|_| ConnectorError::SchemaMismatch("writer schema cannot be encoded".into()))?;
    if let Some(level) = config.get("schema.compatibility") {
        registry
            .set_compatibility_level(subject, level.parse()?)
            .await?;
    }
    let id = registry
        .register_schema(subject, &schema, SchemaType::Avro)
        .await?;
    // Resolve the exact registered definition, never a later latest version.
    let cached = registry.fetch_registered_schema(id).await?;
    native.identity.insert("id".into(), id.to_string());
    let registered: serde_json::Value = serde_json::from_str(&cached.schema_str).map_err(|_| {
        ConnectorError::SchemaMismatch("registered writer definition is malformed".into())
    })?;
    if cached.schema_type != SchemaType::Avro || registered != native.definition["schema"] {
        return Err(ConnectorError::SchemaMismatch(
            "registered writer identity does not match the prepared Avro definition".into(),
        ));
    }
    Ok(())
}

fn selected_subject(
    config: &ConnectorConfig,
    topic: Option<&str>,
    strategy: SubjectNameStrategy,
    record_name: Option<&str>,
) -> Result<String, ConnectorError> {
    if let Some(subject) = config.get("schema.registry.value.subject") {
        if !subject.is_empty() {
            return Ok(subject.into());
        }
    }
    if strategy != SubjectNameStrategy::TopicName && record_name.is_none() {
        return Err(ConnectorError::ConfigurationError(
            "record naming requires schema.registry.record.name or schema.registry.value.subject"
                .into(),
        ));
    }
    if strategy == SubjectNameStrategy::RecordName {
        return Ok(record_name.unwrap_or_default().to_owned());
    }
    let topic = topic.ok_or_else(|| ConnectorError::ConfigurationError(
        "ambiguous Kafka schema selection for multiple topics or topic.pattern; set schema.registry.value.subject to the committed reader contract".into()))?;
    Ok(resolve_value_subject(strategy, record_name, topic))
}

async fn fetch_selected(
    registry: &SchemaRegistryClient,
    config: &ConnectorConfig,
    subject: &str,
) -> Result<CachedSchema, ConnectorError> {
    let id = config.get_parsed::<i32>("schema.registry.value.id")?;
    let version = config.get_parsed::<i32>("schema.registry.value.version")?;
    if id.is_some() && version.is_some() {
        return Err(ConnectorError::ConfigurationError(
            "select schema.registry.value.id or schema.registry.value.version, not both".into(),
        ));
    }
    let cached = match (id, version) {
        (Some(id), None) => registry.get_schema_by_id(id).await?,
        (None, Some(version)) if version > 0 => {
            registry.get_schema_version(subject, version).await?
        }
        (None, Some(_)) => {
            return Err(ConnectorError::ConfigurationError(
                "schema version must be positive; omit it to resolve latest during creation".into(),
            ))
        }
        (None, None) => registry.get_latest_schema(subject).await?,
        (Some(_), Some(_)) => {
            return Err(ConnectorError::ConfigurationError(
                "conflicting schema selectors".into(),
            ))
        }
    };
    if cached.schema_type != SchemaType::Avro {
        return Err(ConnectorError::FeatureUnsupported("Kafka registry decoding/encoding supports Avro only; JSON Schema and Protobuf codecs are unavailable".into()));
    }
    Ok(cached)
}

fn registry_identity(config: &ConnectorConfig, subject: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "registry".into(),
            crate::security::sanitize_identity_value(
                "schema.registry.url",
                config.get("schema.registry.url").unwrap_or_default(),
            ),
        ),
        ("subject".into(), subject.into()),
    ])
}

fn native_contract(
    config: &ConnectorConfig,
    subject: &str,
    cached: CachedSchema,
) -> Result<NativeSchema, ConnectorError> {
    let mut identity = registry_identity(config, subject);
    identity.insert("id".into(), cached.id.to_string());
    if cached.version > 0 {
        identity.insert("version".into(), cached.version.to_string());
    }
    Ok(NativeSchema {
        format: "avro".into(),
        identity,
        definition: serde_json::json!({"schema": serde_json::from_str::<serde_json::Value>(&cached.schema_str).map_err(|_| ConnectorError::SchemaMismatch("invalid Avro schema".into()))?,
            "resolved": serde_json::from_str::<serde_json::Value>(&cached.resolved_schema_str).map_err(|_| ConnectorError::SchemaMismatch("invalid resolved Avro schema".into()))?}),
        references: cached.references,
    })
}

fn reject_registry_for_plain_format(config: &ConnectorConfig) -> Result<(), ConnectorError> {
    if config.get("schema.registry.url").is_some() {
        return Err(ConnectorError::ConfigurationError("schema.registry.url requires FORMAT AVRO; plain JSON/CSV/raw output uses the declared or query schema without a registry".into()));
    }
    Ok(())
}

fn reject_unsupported_key_codec(config: &ConnectorConfig) -> Result<(), ConnectorError> {
    if config.get("schema.registry.key.subject").is_some() || config.get("key.format").is_some() {
        return Err(ConnectorError::FeatureUnsupported("Kafka keys use the existing raw key-column encoding; registry-backed key codecs are not implemented".into()));
    }
    Ok(())
}

pub(super) fn writer_schema(binding: &SchemaBinding) -> Result<SchemaRef, ConnectorError> {
    let native = binding.value.as_ref().ok_or_else(|| {
        ConnectorError::SchemaMismatch("Avro writer has no committed native schema".into())
    })?;
    let mut schema = binding
        .external
        .clone()
        .ok_or_else(|| ConnectorError::SchemaMismatch("Avro writer fields are missing".into()))?;
    let mut metadata = schema.metadata().clone();
    metadata.insert(
        arrow_avro::schema::SCHEMA_METADATA_KEY.into(),
        native.definition["resolved"].to_string(),
    );
    schema = schema.with_metadata(metadata);
    Ok(Arc::new(schema))
}

pub(super) fn contract_schema_id(binding: &SchemaBinding) -> Result<u32, ConnectorError> {
    binding.value.as_ref().and_then(|native| native.identity.get("id"))
        .and_then(|id| id.parse::<u32>().ok()).filter(|id| *id > 0 && i32::try_from(*id).is_ok())
        .ok_or_else(|| ConnectorError::SchemaMismatch("Avro writer schema ID is missing; resolve and commit the writer contract before activation".into()))
}

pub(crate) fn payload_schema(
    binding: &SchemaBinding,
    metadata: bool,
    headers: bool,
) -> Result<SchemaRef, ConnectorError> {
    let appended = usize::from(metadata) * 3 + usize::from(headers);
    let length = binding
        .logical
        .fields()
        .len()
        .checked_sub(appended)
        .ok_or_else(|| {
            ConnectorError::SchemaMismatch("reader metadata fields are missing".into())
        })?;
    let payload = Arc::new(arrow_schema::Schema::new_with_metadata(
        binding.logical.fields()[..length].to_vec(),
        binding.logical.metadata().clone(),
    ));
    if super::source::kafka_output_schema(&payload, metadata, headers).as_ref() != &binding.logical
    {
        return Err(ConnectorError::SchemaMismatch(
            "committed Kafka metadata fields differ from the configured protocol".into(),
        ));
    }
    Ok(payload)
}

pub(super) fn sink_value_schema(binding: &SchemaBinding) -> Result<SchemaRef, ConnectorError> {
    let indices = binding
        .logical
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !binding.control_fields.contains(field.name()))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    binding
        .logical
        .project(&indices)
        .map(Arc::new)
        .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))
}
