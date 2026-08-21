use super::*;

#[test]
fn test_integer_type_mapping() {
    assert_eq!(pg_type_to_arrow(INT2_OID), DataType::Int16);
    assert_eq!(pg_type_to_arrow(INT4_OID), DataType::Int32);
    assert_eq!(pg_type_to_arrow(INT8_OID), DataType::Int64);
}

#[test]
fn test_float_type_mapping() {
    assert_eq!(pg_type_to_arrow(FLOAT4_OID), DataType::Float32);
    assert_eq!(pg_type_to_arrow(FLOAT8_OID), DataType::Float64);
}

#[test]
fn test_text_type_mapping() {
    assert_eq!(pg_type_to_arrow(TEXT_OID), DataType::Utf8);
    assert_eq!(pg_type_to_arrow(VARCHAR_OID), DataType::Utf8);
    assert_eq!(pg_type_to_arrow(BPCHAR_OID), DataType::Utf8);
    assert_eq!(pg_type_to_arrow(NAME_OID), DataType::Utf8);
}

#[test]
fn test_bool_type_mapping() {
    assert_eq!(pg_type_to_arrow(BOOL_OID), DataType::Boolean);
}

#[test]
fn test_timestamp_type_mapping() {
    assert!(matches!(
        pg_type_to_arrow(TIMESTAMP_OID),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    ));
    assert!(matches!(
        pg_type_to_arrow(TIMESTAMPTZ_OID),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(_))
    ));
}

#[test]
fn test_date_time_mapping() {
    assert_eq!(pg_type_to_arrow(DATE_OID), DataType::Date32);
    assert!(matches!(
        pg_type_to_arrow(TIME_OID),
        DataType::Time64(arrow_schema::TimeUnit::Microsecond)
    ));
}

#[test]
fn test_binary_type_mapping() {
    assert_eq!(pg_type_to_arrow(BYTEA_OID), DataType::Binary);
}

#[test]
fn test_json_type_mapping() {
    assert_eq!(pg_type_to_arrow(JSON_OID), DataType::Utf8);
    assert_eq!(pg_type_to_arrow(JSONB_OID), DataType::Utf8);
}

#[test]
fn test_unknown_type_fallback() {
    assert_eq!(pg_type_to_arrow(99999), DataType::Utf8);
}

#[test]
fn test_pg_column() {
    let col = PgColumn::new("id".to_string(), INT8_OID, -1, true);
    assert_eq!(col.name, "id");
    assert_eq!(col.type_oid, INT8_OID);
    assert!(col.is_key);
    assert_eq!(col.arrow_type(), DataType::Int64);
}

#[test]
fn test_pg_type_name() {
    assert_eq!(pg_type_name(INT4_OID), "int4");
    assert_eq!(pg_type_name(TEXT_OID), "text");
    assert_eq!(pg_type_name(BOOL_OID), "bool");
    assert_eq!(pg_type_name(99999), "unknown");
}

#[test]
fn test_numeric_maps_to_utf8() {
    // Numeric must be Utf8 to preserve arbitrary precision
    assert_eq!(pg_type_to_arrow(NUMERIC_OID), DataType::Utf8);
}

#[test]
fn test_array_types_map_to_utf8() {
    assert_eq!(pg_type_to_arrow(INT4_ARRAY_OID), DataType::Utf8);
    assert_eq!(pg_type_to_arrow(TEXT_ARRAY_OID), DataType::Utf8);
}
