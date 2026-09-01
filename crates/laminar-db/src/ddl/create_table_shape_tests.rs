use arrow::datatypes::DataType;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::{build_table_fields_and_primary_key, validate_create_table_envelope};

fn parse_create_table(sql: &str) -> sqlparser::ast::CreateTable {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    assert_eq!(statements.len(), 1);
    match statements.remove(0) {
        Statement::CreateTable(create) => create,
        statement => panic!("expected CREATE TABLE, got {statement}"),
    }
}

#[test]
fn primary_key_is_single_column_and_non_nullable() {
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, value VARCHAR NULL)",
        "CREATE TABLE t (id INT, value VARCHAR, PRIMARY KEY (id))",
    ] {
        let create = parse_create_table(sql);
        validate_create_table_envelope(&create).unwrap();
        let (fields, primary_key) = build_table_fields_and_primary_key(&create).unwrap();
        assert_eq!(primary_key, "id");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert!(!fields[0].is_nullable());
        assert!(fields[1].is_nullable());
    }
}

#[test]
fn duplicate_columns_and_ambiguous_nullability_are_rejected() {
    for (sql, expected) in [
        (
            "CREATE TABLE t (id INT PRIMARY KEY, id INT)",
            "duplicate CREATE TABLE column 'id'",
        ),
        (
            "CREATE TABLE t (id INT PRIMARY KEY, ID INT)",
            "duplicate CREATE TABLE column 'ID'",
        ),
        (
            "CREATE TABLE t (id INT NULL PRIMARY KEY)",
            "cannot be declared NULL",
        ),
        (
            "CREATE TABLE t (id INT PRIMARY KEY, value INT NULL NOT NULL)",
            "repeated or conflicting NULL/NOT NULL",
        ),
        (
            "CREATE TABLE t (id INT PRIMARY KEY, value INT NOT NULL NOT NULL)",
            "repeated or conflicting NULL/NOT NULL",
        ),
    ] {
        let create = parse_create_table(sql);
        let error = build_table_fields_and_primary_key(&create).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected rejection for {sql}: {error}"
        );
    }
}

#[test]
fn unquoted_primary_key_reference_is_case_insensitive() {
    let create = parse_create_table("CREATE TABLE t (id INT, PRIMARY KEY (ID))");
    let (fields, primary_key) = build_table_fields_and_primary_key(&create).unwrap();
    assert_eq!(primary_key, "id");
    assert!(!fields[0].is_nullable());

    let quoted = parse_create_table("CREATE TABLE t (id INT, \"ID\" INT, PRIMARY KEY (\"ID\"))");
    let (fields, primary_key) = build_table_fields_and_primary_key(&quoted).unwrap();
    assert_eq!(primary_key, "ID");
    assert!(fields[0].is_nullable());
    assert!(!fields[1].is_nullable());
}

#[test]
fn unsupported_column_and_table_constraints_are_rejected() {
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, value INT DEFAULT 1)",
        "CREATE TABLE t (id INT PRIMARY KEY, value INT UNIQUE)",
        "CREATE TABLE t (id INT PRIMARY KEY, value INT CHECK (value > 0))",
        "CREATE TABLE t (id INT, value INT, PRIMARY KEY (id), UNIQUE (value))",
        "CREATE TABLE t (id INT, CONSTRAINT named_pk PRIMARY KEY (id))",
    ] {
        let create = parse_create_table(sql);
        let error = build_table_fields_and_primary_key(&create).unwrap_err();
        assert!(
            error.to_string().contains("unsupported"),
            "unexpected rejection for {sql}: {error}"
        );
    }
}

#[test]
fn unsupported_top_level_shape_is_rejected() {
    let base = parse_create_table("CREATE TABLE t (id INT PRIMARY KEY)");

    let mut create = base.clone();
    create.temporary = true;
    assert!(validate_create_table_envelope(&create)
        .unwrap_err()
        .to_string()
        .contains("TEMPORARY"));

    let mut create = base.clone();
    create.without_rowid = true;
    assert!(validate_create_table_envelope(&create)
        .unwrap_err()
        .to_string()
        .contains("WITHOUT ROWID"));

    let mut create = base;
    create.query = Some(Box::new(
        match Parser::parse_sql(&GenericDialect {}, "SELECT 1")
            .unwrap()
            .remove(0)
        {
            Statement::Query(query) => *query,
            statement => panic!("expected query, got {statement}"),
        },
    ));
    assert!(validate_create_table_envelope(&create)
        .unwrap_err()
        .to_string()
        .contains("AS query"));
}
