use super::*;

#[test]
fn test_format_from_str() {
    assert_eq!(Format::parse("json").unwrap(), Format::Json);
    assert_eq!(Format::parse("JSON").unwrap(), Format::Json);
    assert_eq!(Format::parse("csv").unwrap(), Format::Csv);
    assert_eq!(Format::parse("raw").unwrap(), Format::Raw);
    assert_eq!(Format::parse("bytes").unwrap(), Format::Raw);
    assert_eq!(Format::parse("debezium").unwrap(), Format::Debezium);
    assert_eq!(Format::parse("debezium-json").unwrap(), Format::Debezium);
    assert_eq!(Format::parse("avro").unwrap(), Format::Avro);
    assert_eq!(Format::parse("confluent-avro").unwrap(), Format::Avro);
}

#[test]
fn test_format_display() {
    assert_eq!(Format::Json.to_string(), "json");
    assert_eq!(Format::Csv.to_string(), "csv");
    assert_eq!(Format::Raw.to_string(), "raw");
    assert_eq!(Format::Debezium.to_string(), "debezium");
    assert_eq!(Format::Avro.to_string(), "avro");
}

#[test]
fn test_create_deserializer() {
    assert!(create_deserializer(Format::Json).is_ok());
    assert!(create_deserializer(Format::Csv).is_ok());
    assert!(create_deserializer(Format::Raw).is_ok());
    assert!(create_deserializer(Format::Debezium).is_ok());
}

#[test]
fn test_create_serializer() {
    assert!(create_serializer(Format::Json).is_ok());
    assert!(create_serializer(Format::Csv).is_ok());
    assert!(create_serializer(Format::Raw).is_ok());
    assert!(create_serializer(Format::Debezium).is_err()); // deser-only
}

#[test]
fn generic_avro_errors_are_connector_neutral() {
    let deserialize_error = create_deserializer(Format::Avro)
        .err()
        .expect("generic Avro deserialization must require a connector codec")
        .to_string();
    let serialize_error = create_serializer(Format::Avro)
        .err()
        .expect("generic Avro serialization must require a connector codec")
        .to_string();

    assert_eq!(
        deserialize_error,
        "unsupported format: Avro requires a connector-provided deserializer"
    );
    assert_eq!(
        serialize_error,
        "unsupported format: Avro requires a connector-provided serializer"
    );
}
