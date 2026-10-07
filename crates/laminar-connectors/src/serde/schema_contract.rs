//! Cold-path validation against the codecs used by schemaless transports.

use arrow_schema::{DataType, SchemaRef};

use super::Format;
#[cfg(any(test, feature = "kafka", feature = "nats", feature = "files"))]
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
#[cfg(any(test, feature = "kafka", feature = "nats", feature = "files"))]
use crate::schema::resolution::fixed_binding;
#[cfg(any(test, feature = "kafka", feature = "nats"))]
use crate::schema::resolution::{logical_binding, SchemaBinding, SchemaDirection, SchemaOrigin};

#[cfg(any(test, feature = "kafka", feature = "nats"))]
pub(crate) fn reader_binding(
    config: &ConnectorConfig,
    format: Format,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    if format == Format::Raw {
        return fixed_binding(config, explicit, &super::raw::raw_schema());
    }
    let schema = explicit.ok_or_else(|| ConnectorError::FeatureUnsupported(format!(
        "{} {} source has no authoritative field schema; declare columns or use a metadata-capable format",
        config.connector_type(), format
    )))?;
    validate_reader(format, &schema)?;
    logical_binding(
        config,
        SchemaDirection::Source,
        SchemaOrigin::Explicit,
        &schema,
    )
}

#[cfg(any(test, feature = "kafka", feature = "nats", feature = "files"))]
pub(crate) fn validate_reader(format: Format, schema: &SchemaRef) -> Result<(), ConnectorError> {
    super::create_deserializer(format).map_err(ConnectorError::Serde)?;
    match format {
        Format::Json | Format::Debezium => crate::schema::JsonDecoder::validate_schema(schema)?,
        Format::Csv => crate::schema::CsvDecoder::validate_schema(schema)?,
        Format::Raw => {
            fixed_binding(
                &ConnectorConfig::new("raw"),
                Some(schema.clone()),
                &super::raw::raw_schema(),
            )?;
        }
        Format::Avro => {
            return Err(ConnectorError::FeatureUnsupported(
                "Avro requires a connector-provided reader contract".into(),
            ))
        }
    }
    Ok(())
}

pub(crate) fn validate_writer(format: Format, schema: &SchemaRef) -> Result<(), ConnectorError> {
    crate::schema::resolution::logical_binding(
        &crate::config::ConnectorConfig::new("codec"),
        crate::schema::resolution::SchemaDirection::Sink,
        crate::schema::resolution::SchemaOrigin::Query,
        schema,
    )?;
    super::create_serializer(format).map_err(ConnectorError::Serde)?;
    match format {
        Format::Json => crate::schema::JsonEncoder::validate_schema(schema)?,
        Format::Csv => {
            let batch = arrow_array::RecordBatch::new_empty(schema.clone());
            arrow_csv::writer::WriterBuilder::new()
                .with_header(false)
                .build(std::io::sink())
                .write(&batch)
                .map_err(|error| {
                    ConnectorError::SchemaMismatch(format!(
                        "CSV writer cannot encode the bound query: {error}"
                    ))
                })?;
        }
        Format::Raw => {
            if schema.fields().len() != 1 || schema.field(0).data_type() != &DataType::Utf8 {
                return Err(ConnectorError::SchemaMismatch(
                    "raw writer requires exactly one Utf8 query field; project it in CREATE STREAM"
                        .into(),
                ));
            }
        }
        Format::Avro | Format::Debezium => {
            return Err(ConnectorError::FeatureUnsupported(
                "format requires a connector-provided writer contract".into(),
            ))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn reader_and_writer_validation_matches_actual_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["answer"])),
            ],
        )
        .unwrap();
        for (format, input) in [
            (Format::Json, br#"{"id":42,"label":"answer"}"#.as_slice()),
            (Format::Csv, b"42,answer".as_slice()),
        ] {
            validate_reader(format, &schema).unwrap();
            validate_writer(format, &schema).unwrap();
            let decoded = super::super::create_deserializer(format)
                .unwrap()
                .deserialize(input, &schema)
                .unwrap();
            assert_eq!(
                decoded
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                42
            );
            assert_eq!(
                decoded
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "answer"
            );
            assert_eq!(
                super::super::create_serializer(format)
                    .unwrap()
                    .serialize(&batch)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(decoded.column(0).null_count(), 0);
        }
    }

    #[test]
    fn unsupported_reader_and_lossy_raw_writer_fail_before_activation() {
        let decimal = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Decimal128(10, 2),
            true,
        )]));
        assert!(validate_reader(Format::Json, &decimal).is_err());
        assert!(validate_reader(Format::Csv, &decimal).is_err());
        assert!(validate_writer(Format::Raw, &decimal).is_err());
        let nested = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));
        assert!(validate_writer(Format::Csv, &nested).is_err());
        validate_reader(Format::Json, &nested).unwrap();
        let binding = reader_binding(&ConnectorConfig::new("nats"), Format::Raw, None).unwrap();
        assert_eq!(binding.origin, SchemaOrigin::BuiltIn);
        assert_eq!(binding.logical, *super::super::raw::raw_schema());
    }
}
