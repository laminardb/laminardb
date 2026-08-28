use arrow_schema::SchemaRef;
use iceberg::spec::{
    FormatVersion, NullOrder, PartitionSpec, Schema, SortDirection, SortField, SortOrder, Transform,
};
use iceberg::TableCreation;

use super::super::iceberg_config::{
    IcebergNullOrder, IcebergSinkConfig, IcebergSortDirection, IcebergTransform,
    PARQUET_COMPRESSION_PROPERTY, PARQUET_ROW_GROUP_SIZE_PROPERTY, TARGET_FILE_SIZE_PROPERTY,
};
use crate::error::ConnectorError;

pub(super) fn build_table_creation(
    config: &IcebergSinkConfig,
    arrow_schema: &SchemaRef,
) -> Result<TableCreation, ConnectorError> {
    config.validate_table_creation()?;
    let schema = table_schema(config, arrow_schema)?;
    let partition_spec = partition_spec(config, &schema)?;
    let sort_order = sort_order(config, &schema)?;
    let properties = table_properties(config)?;

    Ok(TableCreation::builder()
        .name(config.catalog.table_name.clone())
        .schema(schema)
        .partition_spec_opt(partition_spec)
        .sort_order_opt(sort_order)
        .format_version(format_version(config.format_version)?)
        .properties(properties)
        .build())
}

fn table_schema(
    config: &IcebergSinkConfig,
    arrow_schema: &SchemaRef,
) -> Result<Schema, ConnectorError> {
    let schema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(arrow_schema).map_err(|error| {
            ConnectorError::SchemaMismatch(format!("arrow to Iceberg schema: {error}"))
        })?;
    if config.identifier_fields.is_empty() {
        return Ok(schema);
    }

    let identifier_ids = config
        .identifier_fields
        .iter()
        .map(|name| {
            schema.field_id_by_name(name).ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "identifier field '{name}' is absent from the input schema"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Schema::builder()
        .with_schema_id(schema.schema_id())
        .with_fields(schema.as_struct().fields().iter().cloned())
        .with_identifier_field_ids(identifier_ids)
        .build()
        .map_err(|error| {
            ConnectorError::SchemaMismatch(format!("invalid Iceberg identifier fields: {error}"))
        })
}

fn partition_spec(
    config: &IcebergSinkConfig,
    schema: &Schema,
) -> Result<Option<iceberg::spec::UnboundPartitionSpec>, ConnectorError> {
    if config.partition_spec.is_empty() {
        return Ok(None);
    }

    let mut builder = PartitionSpec::builder(schema.clone());
    for field in &config.partition_spec {
        builder = builder
            .add_partition_field(&field.source, &field.name, transform(field.transform))
            .map_err(|error| {
                ConnectorError::SchemaMismatch(format!(
                    "invalid Iceberg partition field '{}': {error}",
                    field.name
                ))
            })?;
    }
    builder.build().map(Into::into).map(Some).map_err(|error| {
        ConnectorError::SchemaMismatch(format!("invalid Iceberg partition spec: {error}"))
    })
}

fn sort_order(
    config: &IcebergSinkConfig,
    schema: &Schema,
) -> Result<Option<SortOrder>, ConnectorError> {
    if config.sort_order.is_empty() {
        return Ok(None);
    }

    let mut builder = SortOrder::builder();
    for field in &config.sort_order {
        let source_id = schema.field_id_by_name(&field.source).ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!(
                "sort field '{}' is absent from the input schema",
                field.source
            ))
        })?;
        builder.with_sort_field(
            SortField::builder()
                .source_id(source_id)
                .transform(transform(field.transform))
                .direction(sort_direction(field.direction))
                .null_order(null_order(field.null_order))
                .build(),
        );
    }
    builder.build(schema).map(Some).map_err(|error| {
        ConnectorError::SchemaMismatch(format!("invalid Iceberg sort order: {error}"))
    })
}

fn table_properties(
    config: &IcebergSinkConfig,
) -> Result<std::collections::HashMap<String, String>, ConnectorError> {
    let mut properties = config.initial_table_properties.clone();
    properties.insert(
        TARGET_FILE_SIZE_PROPERTY.into(),
        config.target_file_size_bytes.to_string(),
    );
    properties.insert(
        PARQUET_ROW_GROUP_SIZE_PROPERTY.into(),
        config.parquet_row_group_size_bytes.to_string(),
    );
    properties.insert(
        PARQUET_COMPRESSION_PROPERTY.into(),
        super::super::iceberg_config::parse_parquet_compression(&config.compression)?,
    );
    Ok(properties)
}

fn format_version(version: u8) -> Result<FormatVersion, ConnectorError> {
    match version {
        1 => Ok(FormatVersion::V1),
        2 => Ok(FormatVersion::V2),
        3 => Ok(FormatVersion::V3),
        _ => Err(ConnectorError::ConfigurationError(
            "format.version must be 1, 2, or 3".into(),
        )),
    }
}

fn transform(value: IcebergTransform) -> Transform {
    match value {
        IcebergTransform::Identity => Transform::Identity,
        IcebergTransform::Bucket(count) => Transform::Bucket(count),
        IcebergTransform::Truncate(width) => Transform::Truncate(width),
        IcebergTransform::Year => Transform::Year,
        IcebergTransform::Month => Transform::Month,
        IcebergTransform::Day => Transform::Day,
        IcebergTransform::Hour => Transform::Hour,
        IcebergTransform::Void => Transform::Void,
    }
}

fn sort_direction(value: IcebergSortDirection) -> SortDirection {
    match value {
        IcebergSortDirection::Asc => SortDirection::Ascending,
        IcebergSortDirection::Desc => SortDirection::Descending,
    }
}

fn null_order(value: IcebergNullOrder) -> NullOrder {
    match value {
        IcebergNullOrder::NullsFirst => NullOrder::First,
        IcebergNullOrder::NullsLast => NullOrder::Last,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    use super::*;
    use crate::config::ConnectorConfig;

    fn advanced_config() -> IcebergSinkConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://catalog.test");
        config.set("catalog.warehouse", "file:///warehouse");
        config.set("namespace", "prod");
        config.set("table.name", "events");
        config.set("auto.create", "true");
        config.set("format.version", "2");
        config.set("identifier.fields", "id");
        config.set(
            "partition.spec",
            r#"[{"source":"event_time","name":"event_day","transform":"day"}]"#,
        );
        config.set(
            "sort.order",
            r#"[{"source":"id","transform":"identity","direction":"asc","null_order":"nulls-last"}]"#,
        );
        config.set("table.property.owner", "streaming");
        config.set("target.file.size.bytes", "4096");
        config.set("parquet.row.group.size.bytes", "2048");
        config.set("parquet.compression", "snappy");
        IcebergSinkConfig::from_config(&config).unwrap()
    }

    #[test]
    fn table_creation_applies_declared_schema_layout_and_properties() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let creation = build_table_creation(&advanced_config(), &schema).unwrap();

        assert_eq!(creation.format_version, FormatVersion::V2);
        assert_eq!(
            creation.schema.identifier_field_ids().collect::<Vec<_>>(),
            [1]
        );
        let partition = creation.partition_spec.as_ref().unwrap().fields();
        assert_eq!(partition.len(), 1);
        assert_eq!(partition[0].source_id, 2);
        assert_eq!(partition[0].name, "event_day");
        assert_eq!(partition[0].transform, Transform::Day);
        let sort = creation.sort_order.as_ref().unwrap();
        assert_eq!(sort.fields.len(), 1);
        assert_eq!(sort.fields[0].source_id, 1);
        assert_eq!(
            creation
                .properties
                .get(TARGET_FILE_SIZE_PROPERTY)
                .map(String::as_str),
            Some("4096")
        );
        assert_eq!(
            creation
                .properties
                .get(PARQUET_ROW_GROUP_SIZE_PROPERTY)
                .map(String::as_str),
            Some("2048")
        );
        assert_eq!(
            creation
                .properties
                .get(PARQUET_COMPRESSION_PROPERTY)
                .map(String::as_str),
            Some("snappy")
        );
        assert_eq!(
            creation.properties.get("owner").map(String::as_str),
            Some("streaming")
        );
    }

    #[test]
    fn invalid_partition_transform_fails_before_catalog_io() {
        let mut config = advanced_config();
        config.partition_spec[0].source = "payload".into();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]));

        assert!(matches!(
            build_table_creation(&config, &schema).unwrap_err(),
            ConnectorError::SchemaMismatch(_)
        ));
    }

    #[test]
    fn programmatic_config_cannot_bypass_persisted_secret_validation() {
        let mut config = advanced_config();
        config
            .initial_table_properties
            .insert("password".into(), "do-not-persist".into());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let error = build_table_creation(&config, &schema)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("do-not-persist"));
    }
}
