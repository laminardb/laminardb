//! Read-only native table contracts. Snapshot/log positions remain connector cursors.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::resolution::{
    bind_external, logical_binding, NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};

#[cfg(feature = "delta-lake")]
pub(super) async fn delta_source(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = super::DeltaSourceConfig::from_config(config)?;
    let options = parsed.stable_storage_options();
    let (path, options) = super::delta_io::resolve_catalog_options(
        &parsed.catalog_type,
        parsed.catalog_database.as_deref(),
        parsed.catalog_name.as_deref(),
        parsed.catalog_schema.as_deref(),
        &parsed.table_path,
        &options,
    )
    .await?;
    let options = crate::storage::StorageCredentialResolver::resolve(&path, &options).options;
    let table = super::delta_io::open_or_create_table(&path, options, None).await?;
    let external = delta_read_schema(&super::delta_io::get_table_schema(&table)?);
    let origin = if explicit.is_some() {
        SchemaOrigin::Explicit
    } else {
        SchemaOrigin::Metadata
    };
    let mut binding = logical_binding(
        config,
        SchemaDirection::Source,
        origin,
        &explicit.unwrap_or_else(|| Arc::clone(&external)),
    )?;
    bind_external(&mut binding, &external)?;
    binding.value = Some(delta_native(&table)?);
    Ok(binding)
}

#[cfg(feature = "delta-lake")]
pub(super) fn delta_read_schema(payload: &SchemaRef) -> SchemaRef {
    if payload.index_of("__weight").is_ok() {
        return Arc::clone(payload);
    }
    let mut fields = payload.fields().to_vec();
    fields.push(Arc::new(arrow_schema::Field::new(
        "__weight",
        arrow_schema::DataType::Int64,
        false,
    )));
    Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        payload.metadata().clone(),
    ))
}

#[cfg(feature = "delta-lake")]
pub(super) async fn delta_reference(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    let mut binding = delta_source(config, explicit).await?;
    if binding.origin == SchemaOrigin::Metadata {
        let indices = binding
            .logical
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() != "__weight")
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        binding.logical = binding
            .logical
            .project(&indices)
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
    }
    if let Some(external) = &binding.external {
        let indices = external
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() != "__weight")
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let external = Arc::new(
            external
                .project(&indices)
                .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?,
        );
        bind_external(&mut binding, &external)?;
    }
    Ok(binding)
}

#[cfg(feature = "delta-lake")]
pub(super) async fn delta_sink(
    config: &ConnectorConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = super::DeltaLakeSinkConfig::from_config(config)?;
    delta_sink_with_config(config, &parsed, input).await
}

#[cfg(feature = "delta-lake")]
pub(super) async fn delta_sink_with_config(
    config: &ConnectorConfig,
    parsed: &super::DeltaLakeSinkConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    if parsed.schema_evolution {
        return Err(ConnectorError::FeatureUnsupported(
        "durable schema bindings require schema.evolution=false; evolve the target through a controlled migration".into()));
    }
    let mut binding = logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, &input)?;
    let business = super::DeltaLakeSink::target_schema(&input, parsed.write_mode);
    validate_delta_creation_schema(&business)?;
    binding.control_fields = input
        .fields()
        .iter()
        .filter(|field| business.index_of(field.name()).is_err())
        .map(|field| field.name().clone())
        .collect();
    bind_external(&mut binding, &business)?;
    #[cfg(feature = "delta-lake-unity")]
    if parsed.auto_create {
        if let super::delta_config::DeltaCatalogType::Unity {
            workspace_url,
            access_token,
        } = &parsed.catalog_type
        {
            let name = parsed
                .table_path
                .strip_prefix("uc://")
                .unwrap_or(&parsed.table_path);
            if super::unity_catalog::find_table_storage_location(workspace_url, access_token, name)
                .await?
                .is_none()
            {
                return Ok(binding);
            }
        }
    }
    let (path, options) = super::delta_io::resolve_catalog_options(
        &parsed.catalog_type,
        parsed.catalog_database.as_deref(),
        parsed.catalog_name.as_deref(),
        parsed.catalog_schema.as_deref(),
        &parsed.table_path,
        &parsed.storage_options,
    )
    .await?;
    let options = crate::storage::StorageCredentialResolver::resolve(&path, &options).options;
    if missing_local_delta_directory(&path)?.is_some() {
        if parsed.auto_create {
            return Ok(binding);
        }
        return Err(ConnectorError::ConfigurationError("Delta target directory does not exist; create the table separately or enable auto.create=true".into()));
    }
    let table = super::delta_io::open_or_create_table(&path, options, None).await?;
    if table.version().is_none() {
        if !parsed.auto_create {
            return Err(ConnectorError::ConfigurationError("Delta target does not exist; create it separately or explicitly enable auto.create=true".into()));
        }
        return Ok(binding);
    }
    delta_bind_writer(&mut binding, &table)?;
    Ok(binding)
}

#[cfg(feature = "delta-lake")]
pub(super) async fn prepare_delta(
    config: &ConnectorConfig,
    binding: &mut SchemaBinding,
) -> Result<(), ConnectorError> {
    if binding.value.is_some() {
        return Ok(());
    }
    let parsed = super::DeltaLakeSinkConfig::from_config(config)?;
    prepare_delta_with_config(&parsed, binding).await
}

#[cfg(feature = "delta-lake")]
pub(super) async fn prepare_delta_with_config(
    parsed: &super::DeltaLakeSinkConfig,
    binding: &mut SchemaBinding,
) -> Result<(), ConnectorError> {
    if binding.value.is_some() {
        return Ok(());
    }
    if !parsed.auto_create {
        return Err(ConnectorError::ConfigurationError(
            "Delta table creation is disabled".into(),
        ));
    }
    #[cfg(feature = "delta-lake-unity")]
    {
        let schema = Arc::new(
            binding
                .external
                .as_ref()
                .unwrap_or(&binding.logical)
                .clone(),
        );
        super::delta::ensure_uc_table_exists(parsed, Some(&schema)).await?;
    }
    let (path, options) = super::delta_io::resolve_catalog_options(
        &parsed.catalog_type,
        parsed.catalog_database.as_deref(),
        parsed.catalog_name.as_deref(),
        parsed.catalog_schema.as_deref(),
        &parsed.table_path,
        &parsed.storage_options,
    )
    .await?;
    let options = crate::storage::StorageCredentialResolver::resolve(&path, &options).options;
    if let Some(directory) = missing_local_delta_directory(&path)? {
        tokio::task::spawn_blocking(move || std::fs::create_dir_all(directory))
            .await
            .map_err(|_| {
                ConnectorError::Internal("Delta directory preparation worker failed".into())
            })?
            .map_err(|_| {
                ConnectorError::ConnectionFailed(
                    "authorized Delta directory creation failed".into(),
                )
            })?;
    }
    let schema = Arc::new(
        binding
            .external
            .as_ref()
            .unwrap_or(&binding.logical)
            .clone(),
    );
    let table = super::delta_io::open_or_create_table(&path, options, Some(&schema)).await?;
    delta_bind_writer(binding, &table)?;
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn missing_local_delta_directory(path: &str) -> Result<Option<std::path::PathBuf>, ConnectorError> {
    let directory = if path.contains("://") {
        let url = url::Url::parse(path)
            .map_err(|_| ConnectorError::ConfigurationError("invalid Delta target URL".into()))?;
        if url.scheme() != "file" {
            return Ok(None);
        }
        url.to_file_path().map_err(|()| {
            ConnectorError::ConfigurationError("invalid local Delta file URL".into())
        })?
    } else {
        std::path::PathBuf::from(path)
    };
    match std::fs::metadata(&directory) {
        Ok(metadata) if metadata.is_dir() => Ok(None),
        Ok(_) => Err(ConnectorError::ConfigurationError(
            "Delta target must be a directory".into(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Some(directory)),
        Err(_) => Err(ConnectorError::ConnectionFailed(
            "local Delta target metadata is unavailable".into(),
        )),
    }
}

#[cfg(feature = "delta-lake")]
fn validate_delta_creation_schema(schema: &SchemaRef) -> Result<(), ConnectorError> {
    use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow as _, TryIntoKernel as _};
    let normalized = super::delta_io::widen_millisecond_timestamps(schema);
    let kernel: deltalake::kernel::StructType =
        normalized.as_ref().try_into_kernel().map_err(|error| {
            ConnectorError::SchemaMismatch(format!(
                "Delta cannot represent the query schema: {error}; cast in CREATE STREAM"
            ))
        })?;
    let restored: arrow_schema::Schema = (&kernel).try_into_arrow().map_err(|error| {
        ConnectorError::SchemaMismatch(format!("Delta native schema conversion failed: {error}"))
    })?;
    if normalized.fields().len() != restored.fields().len()
        || normalized
            .fields()
            .iter()
            .zip(restored.fields())
            .any(|(input, native)| {
                input.name() != native.name()
                    || input.data_type() != native.data_type()
                    || input.is_nullable() != native.is_nullable()
            })
    {
        return Err(ConnectorError::SchemaMismatch(
            "Delta native schema changes query types or nullability; use explicit casts in CREATE STREAM before enabling table creation".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn delta_bind_writer(
    binding: &mut SchemaBinding,
    table: &deltalake::DeltaTable,
) -> Result<(), ConnectorError> {
    let native = super::delta_io::get_table_schema(table)?;
    let mut normalized = binding.clone();
    normalized.logical =
        super::delta_io::widen_millisecond_timestamps(&Arc::new(binding.logical.clone()))
            .as_ref()
            .clone();
    bind_external(&mut normalized, &native)?;
    // The established Delta write path performs this lossless top-level widening. Persist
    // both its input encoding and the native table types; no general SQL cast is inferred.
    let encoding = arrow_schema::Schema::new_with_metadata(
        native
            .fields()
            .iter()
            .map(|field| {
                let logical = binding
                    .logical
                    .field_with_name(field.name())
                    .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
                Ok(Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(logical.data_type().clone()),
                ))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?,
        native.metadata().clone(),
    );
    bind_external(binding, &Arc::new(encoding))?;
    let mut contract = delta_native(table)?;
    contract.definition["writer_encoding"] = serde_json::json!("delta-kernel-ms-to-us-v1");
    binding.value = Some(contract);
    Ok(())
}

#[cfg(feature = "delta-lake")]
pub(super) fn delta_native(table: &deltalake::DeltaTable) -> Result<NativeSchema, ConnectorError> {
    let snapshot = table.snapshot().map_err(|_| {
        ConnectorError::SchemaMismatch("Delta table has no committed metadata".into())
    })?;
    let metadata = snapshot.metadata();
    let schema = metadata
        .parse_schema()
        .map_err(|_| ConnectorError::SchemaMismatch("invalid Delta native schema".into()))?;
    let protocol = snapshot.protocol();
    let configuration: BTreeMap<_, _> = metadata
        .configuration()
        .iter()
        .filter(|(key, _)| {
            key.starts_with("delta.columnMapping.") || key.starts_with("delta.feature.")
        })
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    Ok(NativeSchema {
        format: "delta".into(),
        identity: BTreeMap::from([("table_id".into(), metadata.id().to_owned())]),
        definition: serde_json::json!({"schema": schema, "partition_columns": metadata.partition_columns(),
            "configuration": configuration, "protocol": {"min_reader_version": protocol.min_reader_version(),
                "min_writer_version": protocol.min_writer_version(), "reader_features": super::delta_io::sorted_protocol_features(protocol.reader_features()),
                "writer_features": super::delta_io::sorted_protocol_features(protocol.writer_features())}}),
        references: Vec::new(),
    })
}

#[cfg(feature = "iceberg-core")]
pub(super) async fn iceberg_source(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = super::IcebergSourceConfig::from_config(config)?;
    super::iceberg::capabilities::validate_source(&parsed)?;
    let built = super::iceberg_io::build_catalog_for_access(
        &parsed.catalog,
        &parsed.storage,
        super::iceberg_io::CatalogAccess::Read,
    )
    .await?;
    let table = super::iceberg_io::load_table_with_timeout(
        built.catalog.as_ref(),
        &parsed.catalog.namespace,
        &parsed.catalog.table_name,
        parsed.catalog.request_timeout,
    )
    .await?;
    let native = table.current_schema_ref();
    let external = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(&native).map_err(|error| {
            ConnectorError::SchemaMismatch(format!("Iceberg native schema: {error}"))
        })?,
    );
    let origin = if explicit.is_some() {
        SchemaOrigin::Explicit
    } else {
        SchemaOrigin::Metadata
    };
    let logical = match explicit {
        Some(schema) => schema,
        None if parsed.select_columns.is_empty() => Arc::clone(&external),
        None => Arc::new(
            external
                .project(
                    &parsed
                        .select_columns
                        .iter()
                        .map(|name| external.index_of(name))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?,
                )
                .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?,
        ),
    };
    let mut binding = logical_binding(config, SchemaDirection::Source, origin, &logical)?;
    bind_external(&mut binding, &external)?;
    binding.value = Some(iceberg_native(&table));
    Ok(binding)
}

#[cfg(feature = "iceberg-core")]
pub(super) async fn iceberg_sink(
    config: &ConnectorConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = super::IcebergSinkConfig::from_config(config)?;
    super::iceberg::capabilities::validate_sink(&parsed)?;
    let mut binding = logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, &input)?;
    let built = super::iceberg_io::build_catalog_for_access(
        &parsed.catalog,
        &parsed.storage,
        super::iceberg_io::CatalogAccess::Read,
    )
    .await?;
    if parsed.auto_create {
        let namespace = iceberg::NamespaceIdent::from_strs(parsed.catalog.namespace.split('.'))
            .map_err(|_| ConnectorError::ConfigurationError("invalid Iceberg namespace".into()))?;
        let identifier = iceberg::TableIdent::new(namespace, parsed.catalog.table_name.clone());
        let exists = tokio::time::timeout(
            parsed.catalog.request_timeout,
            built.catalog.table_exists(&identifier),
        )
        .await
        .map_err(|_| {
            ConnectorError::Timeout(
                parsed
                    .catalog
                    .request_timeout
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
            )
        })?
        .map_err(|error| {
            ConnectorError::ReadError(format!(
                "Iceberg target existence check failed ({})",
                error.kind()
            ))
        })?;
        if !exists {
            return Ok(binding);
        }
    }
    let table = super::iceberg_io::load_table_with_timeout(
        built.catalog.as_ref(),
        &parsed.catalog.namespace,
        &parsed.catalog.table_name,
        parsed.catalog.request_timeout,
    )
    .await?;
    let external = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(&table.current_schema_ref()).map_err(|error| {
            ConnectorError::SchemaMismatch(format!("Iceberg native schema: {error}"))
        })?,
    );
    bind_external(&mut binding, &external)?;
    binding.value = Some(iceberg_native(&table));
    Ok(binding)
}

#[cfg(feature = "iceberg-core")]
pub(super) async fn prepare_iceberg(
    config: &ConnectorConfig,
    binding: &mut SchemaBinding,
) -> Result<(), ConnectorError> {
    if binding.value.is_some() {
        return Ok(());
    }
    let parsed = super::IcebergSinkConfig::from_config(config)?;
    if !parsed.auto_create {
        return Err(ConnectorError::ConfigurationError(
            "Iceberg table creation is disabled".into(),
        ));
    }
    let built = super::iceberg_io::build_catalog_for_access(
        &parsed.catalog,
        &parsed.storage,
        super::iceberg_io::CatalogAccess::Write { auto_create: true },
    )
    .await?;
    super::iceberg_io::ensure_table_exists(
        built.catalog.as_ref(),
        &parsed,
        &Arc::new(binding.logical.clone()),
    )
    .await?;
    *binding = iceberg_sink(config, Arc::new(binding.logical.clone())).await?;
    Ok(())
}

#[cfg(feature = "iceberg-core")]
pub(super) fn iceberg_native(table: &iceberg::table::Table) -> NativeSchema {
    let metadata = table.metadata();
    NativeSchema {
        format: "iceberg".into(),
        identity: BTreeMap::from([
            ("table_uuid".into(), metadata.uuid().to_string()),
            ("schema_id".into(), metadata.current_schema_id().to_string()),
        ]),
        definition: serde_json::json!({"schema": metadata.current_schema(), "partition_spec": metadata.default_partition_spec(),
            "sort_order": metadata.default_sort_order(), "format_version": metadata.format_version()}),
        references: Vec::new(),
    }
}

pub(super) fn verify_identity(
    binding: Option<&SchemaBinding>,
    current: &NativeSchema,
) -> Result<(), ConnectorError> {
    let Some(binding) = binding else {
        return Ok(());
    };
    let expected = binding.value.as_ref().ok_or_else(|| {
        ConnectorError::SchemaMismatch(format!(
            "committed {} binding lacks native table identity; migrate the legacy catalog before activation",
            current.format
        ))
    })?;
    let identity_key = if current.format == "delta" {
        "table_id"
    } else {
        "table_uuid"
    };
    if expected.format != current.format
        || expected.identity.get(identity_key) != current.identity.get(identity_key)
    {
        return Err(ConnectorError::SchemaMismatch(
            "external table was replaced under its configured name; migrate the catalog binding"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn legacy_table_contract_cannot_select_a_native_identity_during_activation() {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let binding = SchemaBinding::logical(
            "delta-lake",
            SchemaDirection::Source,
            SchemaOrigin::Explicit,
            schema,
        )
        .unwrap();
        let current = NativeSchema {
            format: "delta".into(),
            identity: BTreeMap::from([("table_id".into(), "replacement".into())]),
            definition: serde_json::json!({}),
            references: Vec::new(),
        };
        let error = verify_identity(Some(&binding), &current).unwrap_err();
        assert!(error.to_string().contains("migrate the legacy catalog"));
    }
}
