use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use mongodb::options::{Collation, IndexOptions};

#[test]
fn cell_to_bson_types_and_null() {
    assert_eq!(
        MongoLookupSource::cell_to_bson(&Int64Array::from(vec![7i64]), 0).unwrap(),
        Some(Bson::Int64(7))
    );
    assert_eq!(
        MongoLookupSource::cell_to_bson(&StringArray::from(vec!["k"]), 0).unwrap(),
        Some(Bson::String("k".into()))
    );
    let nullable = Int64Array::from(vec![None, Some(1)]);
    assert!(MongoLookupSource::cell_to_bson(&nullable, 0)
        .unwrap()
        .is_none());
}

#[test]
fn cell_to_bson_rejects_unsupported_type() {
    assert!(MongoLookupSource::cell_to_bson(&arrow_array::Date32Array::from(vec![1]), 0).is_err());
}

#[test]
fn bson_numeric_coercion() {
    assert_eq!(bson_as_i64(Some(&Bson::Double(3.9))), Some(3));
    assert_eq!(bson_as_f64(Some(&Bson::Int64(5))), Some(5.0));
    assert_eq!(bson_as_i64(Some(&Bson::String("x".into()))), None);
    assert_eq!(bson_as_i64(None), None);
}

#[test]
fn lookup_key_limits_are_enforced_before_decode() {
    assert!(validate_lookup_keys(&[&b"a"[..], &b"bc"[..]]).is_ok());

    let too_many = vec![&b""[..]; MAX_LOOKUP_KEYS + 1];
    assert!(validate_lookup_keys(&too_many).is_err());

    let oversized = vec![0_u8; MAX_LOOKUP_KEY_BYTES + 1];
    assert!(validate_lookup_keys(&[oversized.as_slice()]).is_err());
}

#[test]
fn lookup_command_limit_leaves_bson_headroom() {
    let small = doc! { "id": doc! { "$in": [1_i64, 2_i64] } };
    assert!(validate_lookup_command("db", "items", &small, None, 2).is_ok());

    let oversized = doc! {
        "id": doc! { "$in": ["x".repeat(MAX_LOOKUP_COMMAND_BYTES)] }
    };
    let error = validate_lookup_command("db", "items", &oversized, None, 1)
        .expect_err("oversized command must be rejected before I/O");
    assert!(error.to_string().contains("lookup command"));
}

#[test]
fn lookup_index_must_uniquely_cover_the_key() {
    let unique = IndexModel::builder()
        .keys(doc! { "id": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();
    assert!(has_usable_unique_lookup_index(&unique, "id"));

    let non_unique = IndexModel::builder().keys(doc! { "id": 1 }).build();
    assert!(!has_usable_unique_lookup_index(&non_unique, "id"));

    let compound = IndexModel::builder()
        .keys(doc! { "id": 1, "tenant": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();
    assert!(!has_usable_unique_lookup_index(&compound, "id"));

    let partial = IndexModel::builder()
        .keys(doc! { "id": 1 })
        .options(
            IndexOptions::builder()
                .unique(true)
                .partial_filter_expression(doc! { "active": true })
                .build(),
        )
        .build();
    assert!(!has_usable_unique_lookup_index(&partial, "id"));

    let hidden = IndexModel::builder()
        .keys(doc! { "id": 1 })
        .options(IndexOptions::builder().unique(true).hidden(true).build())
        .build();
    assert!(!has_usable_unique_lookup_index(&hidden, "id"));

    let collated = IndexModel::builder()
        .keys(doc! { "id": 1 })
        .options(
            IndexOptions::builder()
                .unique(true)
                .collation(Collation::builder().locale("en").build())
                .build(),
        )
        .build();
    assert!(!has_usable_unique_lookup_index(&collated, "id"));

    let hashed = IndexModel::builder()
        .keys(doc! { "id": "hashed" })
        .options(IndexOptions::builder().unique(true).build())
        .build();
    assert!(!has_usable_unique_lookup_index(&hashed, "id"));

    let implicit_id = IndexModel::builder()
        .keys(doc! { "_id": 1 })
        .options(
            IndexOptions::builder()
                .name(Some("_id_".to_owned()))
                .build(),
        )
        .build();
    assert!(has_usable_unique_lookup_index(&implicit_id, "_id"));
}

#[test]
fn mongos_topology_is_rejected_before_index_admission() {
    let error = validate_lookup_server_topology(&doc! {
        "msg": "isdbgrid",
        "isWritablePrimary": true
    })
    .expect_err("mongos cannot prove cluster-global key uniqueness");
    assert!(error.to_string().contains("shard-key routing"));
    assert!(error.to_string().contains("cluster-global uniqueness"));

    assert!(validate_lookup_server_topology(&doc! {
        "setName": "rs0",
        "isWritablePrimary": true
    })
    .is_ok());
    assert!(validate_lookup_server_topology(&doc! {
        "isWritablePrimary": true
    })
    .is_ok());
}

#[tokio::test]
async fn load_balanced_topology_is_rejected_before_client_construction() {
    let config = MongoLookupSourceConfig {
        connection_uri: "mongodb://localhost:27017/?loadBalanced=true&tls=false".into(),
        database: "db".into(),
        collection: "items".into(),
        primary_key_columns: vec!["id".into()],
        schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
    };
    let error = MongoLookupSource::open(config)
        .await
        .err()
        .expect("load-balanced lookup must fail before server selection");
    assert!(error.to_string().contains("load-balanced topology"));
    assert!(error.to_string().contains("cluster-global uniqueness"));
}

#[tokio::test]
async fn wildcard_collection_is_rejected_before_network_io() {
    let config = MongoLookupSourceConfig {
        connection_uri: "mongodb://localhost:27017".into(),
        database: "db".into(),
        collection: "*".into(),
        primary_key_columns: vec!["id".into()],
        schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
    };
    let error = MongoLookupSource::open(config)
        .await
        .err()
        .expect("wildcard lookup must be rejected");
    assert!(error.to_string().contains("collection='*'"));
}
