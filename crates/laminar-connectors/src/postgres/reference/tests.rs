use arrow_schema::Field;

use super::*;

fn declared_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

#[test]
fn query_uses_declared_field_order_and_quotes_identifiers() {
    let mut config = ConnectorConfig::new("postgres");
    config.set("table", "public.order");
    let source = PostgresReferenceTableSource::new(config, declared_schema());

    assert_eq!(
        source.snapshot_query().unwrap(),
        "SELECT \"id\", \"name\" FROM \"public\".\"order\""
    );
    assert_eq!(source.declared_schema.field(0).name(), "id");
    assert!(!source.declared_schema.field(0).is_nullable());
}

#[test]
fn tls_is_verified_by_default_and_plaintext_is_explicit() {
    let source =
        PostgresReferenceTableSource::new(ConnectorConfig::new("postgres"), declared_schema());
    assert_eq!(
        source.ssl_mode().unwrap(),
        crate::postgres::SslMode::VerifyFull
    );
    assert_eq!(
        source.postgres_config().unwrap().get_ssl_mode(),
        tokio_postgres::config::SslMode::Require
    );

    let mut config = ConnectorConfig::new("postgres");
    config.set(
        "connection",
        "postgresql://user@localhost/db?sslmode=disable",
    );
    let source = PostgresReferenceTableSource::new(config, declared_schema());
    assert_eq!(
        source.postgres_config().unwrap().get_ssl_mode(),
        tokio_postgres::config::SslMode::Require
    );

    let mut config = ConnectorConfig::new("postgres");
    config.set("ssl.mode", "disable");
    let source = PostgresReferenceTableSource::new(config, declared_schema());
    assert_eq!(
        source.ssl_mode().unwrap(),
        crate::postgres::SslMode::Disable
    );
    assert_eq!(
        source.postgres_config().unwrap().get_ssl_mode(),
        tokio_postgres::config::SslMode::Disable
    );

    let mut config = ConnectorConfig::new("postgres");
    config.set("ssl.mode", "require");
    let source = PostgresReferenceTableSource::new(config, declared_schema());
    assert!(source.ssl_mode().is_err());
}

#[test]
fn connection_options_fail_closed() {
    for (key, value) in [
        ("port", "not-a-port"),
        ("port", "0"),
        ("options", "bad\0option"),
        ("host", " "),
        ("connection", ""),
    ] {
        let mut config = ConnectorConfig::new("postgres");
        config.set(key, value);
        let source = PostgresReferenceTableSource::new(config, declared_schema());
        assert!(source.postgres_config().is_err());
    }

    let mut config = ConnectorConfig::new("postgres");
    config.set("connection", "host=localhost dbname=db user=user");
    config.set("host", "other");
    let source = PostgresReferenceTableSource::new(config, declared_schema());
    assert!(source.postgres_config().is_err());

    let mut config = ConnectorConfig::new("postgres");
    config.set("connection", "host=localhost port=0 dbname=db user=user");
    let source = PostgresReferenceTableSource::new(config, declared_schema());
    assert!(source.postgres_config().is_err());

    let mut config = ConnectorConfig::new("postgres");
    config.set("ssl.mode", "disable");
    config.set("ssl.ca.cert.path", "/certs/ca.pem");
    let source = PostgresReferenceTableSource::new(config, declared_schema());
    assert!(source.postgres_config().is_err());
}

#[test]
fn snapshot_batch_memory_is_bounded() {
    let column: Arc<dyn Array> = Arc::new(arrow_array::StringArray::from(vec!["payload"]));
    let bytes = column.get_array_memory_size();
    enforce_snapshot_batch_bytes_with_limit(&[Arc::clone(&column)], bytes).unwrap();
    assert!(enforce_snapshot_batch_bytes_with_limit(&[column], bytes - 1).is_err());
}

#[test]
fn supported_postgres_types_have_explicit_arrow_mappings() {
    use tokio_postgres::types::Type;

    assert_eq!(postgres_type_to_arrow(&Type::BYTEA), Some(DataType::Binary));
    assert_eq!(postgres_type_to_arrow(&Type::DATE), Some(DataType::Date32));
    assert_eq!(postgres_type_to_arrow(&Type::NUMERIC), None);
    assert_eq!(
        reference_select_expression("amount", &Type::NUMERIC, &DataType::Utf8).unwrap(),
        "CAST(\"amount\" AS TEXT) AS \"amount\""
    );
    assert!(reference_select_expression("amount", &Type::NUMERIC, &DataType::Float64).is_err());
    assert_eq!(
        reference_select_expression("id", &Type::INT8, &DataType::Int64).unwrap(),
        "\"id\""
    );
}

#[tokio::test]
async fn close_is_idempotent_and_prevents_reads() {
    let mut source =
        PostgresReferenceTableSource::new(ConnectorConfig::new("postgres"), declared_schema());
    source.close().await.unwrap();
    source.close().await.unwrap();
    assert!(source.poll_snapshot().await.is_err());
}
