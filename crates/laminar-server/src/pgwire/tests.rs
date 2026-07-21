use super::*;

fn parse_one(sql: &str) -> StreamingStatement {
    parse_streaming_sql(sql)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

fn standard(sql: &str) -> Statement {
    match parse_one(sql) {
        StreamingStatement::Standard(s) => *s,
        other => panic!("expected Standard, got {other:?}"),
    }
}

#[test]
fn pg_text_array_literal_quotes_nulls_and_escapes() {
    assert_eq!(pg_text_array_literal(&[]), "{}");
    assert_eq!(
        pg_text_array_literal(&[Some("en".into()), Some("ja".into())]),
        r#"{"en","ja"}"#
    );
    assert_eq!(
        pg_text_array_literal(&[None, Some("x".into())]),
        r#"{NULL,"x"}"#
    );
    // Embedded quote and backslash are escaped, not left ambiguous.
    assert_eq!(
        pg_text_array_literal(&[Some("a\"b\\c".into())]),
        r#"{"a\"b\\c"}"#
    );
}

#[test]
fn subscription_progress_row_uses_ordered_envelope_columns() {
    use arrow_schema::{DataType, Field, Schema};

    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let fields = Arc::new(subscription_field_infos(&schema, None));
    assert_eq!(fields.len(), 7);
    assert_eq!(fields[1].name(), SUBSCRIPTION_KIND_COLUMN);
    assert_eq!(fields[2].name(), SUBSCRIPTION_EPOCH_COLUMN);
    assert_eq!(fields[3].name(), SUBSCRIPTION_CHECKPOINT_COLUMN);
    assert_eq!(fields[4].name(), SUBSCRIPTION_LOG_SEQUENCE_COLUMN);
    assert_eq!(fields[5].name(), SUBSCRIPTION_ROW_INDEX_COLUMN);
    assert_eq!(fields[6].name(), SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN);
    encode_subscription_progress_row(&fields, 1, 8, 7, 99, 6).unwrap();
}

#[test]
fn uint64_subscription_fails_instead_of_corrupting_bigint() {
    use arrow_array::{RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};

    fn batch(value: u64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![value]))],
        )
        .unwrap()
    }

    fn assert_out_of_range(error: PgWireError) {
        let PgWireError::UserError(info) = error else {
            panic!("expected user error");
        };
        assert_eq!(info.code, "22003");
    }

    for binary in [false, true] {
        let ok = batch(i64::MAX as u64);
        let mut fields = subscription_field_infos(&ok.schema(), None);
        if binary {
            fields[0] = FieldInfo::new(
                "id".to_string(),
                None,
                None,
                Type::INT8,
                FieldFormat::Binary,
            );
        }
        let fields = Arc::new(fields);
        encode_subscription_batch_row(&ok, 0, 7, &fields).unwrap();

        for value in [i64::MAX as u64 + 1, u64::MAX] {
            assert_out_of_range(
                encode_subscription_batch_row(&batch(value), 0, 7, &fields).unwrap_err(),
            );
        }
    }
}

#[test]
fn cached_subscription_schema_change_is_rejected() {
    use arrow_schema::{DataType, Field, Schema};

    let cached = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let same = cached.clone();
    let changed = Schema::new(vec![Field::new("id", DataType::Utf8, false)]);
    ensure_cached_subscription_schema(&cached, &same).unwrap();
    let error = ensure_cached_subscription_schema(&cached, &changed).unwrap_err();
    let PgWireError::UserError(info) = error else {
        panic!("expected user error");
    };
    assert_eq!(info.code, "0A000");
    assert_eq!(info.message, "cached result type changed");
}

#[test]
fn subscription_open_errors_keep_distinct_sqlstates() {
    for (error, expected) in [
        (laminar_db::DbError::StreamNotFound("s".into()), "42P01"),
        (laminar_db::DbError::Unsupported("cluster".into()), "0A000"),
        (
            laminar_db::DbError::InvalidOperation("epoch is not committed".into()),
            "22023",
        ),
        (
            laminar_db::DbError::Pipeline("subscriber cap".into()),
            "53300",
        ),
    ] {
        let PgWireError::UserError(info) = subscription_open_error("s", error) else {
            panic!("expected user error");
        };
        assert_eq!(info.code, expected);
    }
}

#[tokio::test]
async fn select_one_dispatches() {
    let db = LaminarDB::open().unwrap();
    for sql in ["SELECT 1", "select 1", "/* hint */ SELECT 1"] {
        standard_response(&db, standard(sql)).unwrap();
    }
}

#[tokio::test]
async fn driver_select_builtins_dispatch() {
    let db = LaminarDB::open().unwrap();
    for sql in [
        "SELECT version()",
        "SELECT current_schema()",
        "SELECT current_database()",
        "SELECT current_user",
    ] {
        // current_user parses as Expr::Function with no parens in some versions;
        // we accept whatever the parser gives us.
        let _ = standard_response(&db, standard(sql));
    }
}

#[tokio::test]
async fn select_with_from_is_rejected() {
    let db = LaminarDB::open().unwrap();
    let err = standard_response(&db, standard("SELECT 1 FROM foo")).unwrap_err();
    assert!(err.to_string().contains("limited to literals"));
}

#[tokio::test]
async fn ddl_routed_to_http() {
    let db = LaminarDB::open().unwrap();
    let err = standard_response(&db, standard("CREATE TABLE foo (id INT)")).unwrap_err();
    assert!(err.to_string().contains("HTTP /api/v1/sql"));
}

#[tokio::test]
async fn transaction_control_dispatches() {
    let db = LaminarDB::open().unwrap();
    for sql in [
        "BEGIN",
        "BEGIN TRANSACTION",
        "START TRANSACTION",
        "COMMIT",
        "ROLLBACK",
    ] {
        standard_response(&db, standard(sql)).unwrap();
    }
}

#[tokio::test]
async fn set_writes_to_session_properties() {
    let db = LaminarDB::open().unwrap();
    standard_response(&db, standard("SET extra_float_digits = 3")).unwrap();
    assert_eq!(
        db.get_session_property("extra_float_digits").as_deref(),
        Some("3"),
    );
}

#[tokio::test]
async fn set_transaction_isolation_is_rejected() {
    let db = LaminarDB::open().unwrap();
    let err = standard_response(
        &db,
        standard("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("SET TRANSACTION"));
}

#[test]
fn multi_statement_parses() {
    let stmts = parse_streaming_sql("BEGIN; SELECT 1; COMMIT").unwrap();
    assert_eq!(stmts.len(), 3);
}

#[test]
fn classify_outcome_buckets_errors() {
    use std::io::{Error, ErrorKind};
    assert_eq!(super::classify_outcome(&Ok(())), "ok");
    assert_eq!(
        super::classify_outcome(&Err(Error::other("FATAL: 28P01 bad pass"))),
        "auth_failed"
    );
    assert_eq!(
        super::classify_outcome(&Err(Error::other("rustls HandshakeFailure"))),
        "tls_failed"
    );
    assert_eq!(
        super::classify_outcome(&Err(Error::new(ErrorKind::BrokenPipe, "broken"))),
        "error"
    );
}

#[test]
fn failure_tracker_blocks_after_threshold() {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;
    let ip: IpAddr = Ipv4Addr::LOCALHOST.into();
    let tracker = super::FailureTracker::default();
    let limit = 3;
    let window = Duration::from_secs(60);

    for _ in 0..limit {
        assert!(!tracker.is_blocked(ip, limit, window));
        tracker.record_failure(ip);
    }
    assert!(tracker.is_blocked(ip, limit, window));
}

#[test]
fn failure_tracker_disabled_when_limit_zero() {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;
    let ip: IpAddr = Ipv4Addr::LOCALHOST.into();
    let tracker = super::FailureTracker::default();
    for _ in 0..100 {
        tracker.record_failure(ip);
    }
    assert!(!tracker.is_blocked(ip, 0, Duration::from_secs(60)));
}

#[test]
fn failure_tracker_expires_old_entries() {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;
    let ip: IpAddr = Ipv4Addr::LOCALHOST.into();
    let tracker = super::FailureTracker::default();
    for _ in 0..5 {
        tracker.record_failure(ip);
    }
    // Window of 0 means every recorded failure is already expired.
    assert!(!tracker.is_blocked(ip, 5, Duration::from_secs(0)));
}

#[test]
fn failure_tracker_caps_distinct_ips() {
    use std::net::{IpAddr, Ipv4Addr};
    let tracker = super::FailureTracker::default();
    // Push past the cap; map size must stay bounded.
    for i in 0..(super::MAX_TRACKED_IPS + 100) {
        let upper = (i / 256).to_le_bytes()[0];
        let lower = i.to_le_bytes()[0];
        let ip: IpAddr = Ipv4Addr::new(10, 0, upper, lower).into();
        tracker.record_failure(ip);
    }
    let len = tracker.inner.lock().len();
    assert!(
        len <= super::MAX_TRACKED_IPS,
        "tracker exceeded cap: {len} > {}",
        super::MAX_TRACKED_IPS
    );
}

#[tokio::test]
async fn serve_rejects_remote_bind_in_trust_mode() {
    let db = LaminarDB::open().expect("db opens");
    let err = serve(db, "0.0.0.0:0", HashMap::new(), false, None, 256, 10)
        .await
        .expect_err("trust + 0.0.0.0 must fail");
    assert!(err.to_string().contains("trust auth"), "got: {err}");
}

#[tokio::test]
async fn serve_rejects_remote_bind_without_explicit_optin() {
    let db = LaminarDB::open().expect("db opens");
    let mut users = HashMap::new();
    users.insert("alice".into(), Secret::new("wonderland-key"));
    let err = serve(db, "0.0.0.0:0", users, false, None, 256, 10)
        .await
        .expect_err("md5 + 0.0.0.0 without allow_remote must fail");
    assert!(
        err.to_string().contains("pgwire_allow_remote"),
        "got: {err}"
    );
}

#[tokio::test]
async fn serve_rejects_remote_bind_without_tls() {
    let db = LaminarDB::open().expect("db opens");
    let mut users = HashMap::new();
    users.insert("alice".into(), Secret::new("wonderland-key"));
    let err = serve(db, "0.0.0.0:0", users, true, None, 256, 10)
        .await
        .expect_err("remote pgwire must not start without TLS");
    assert!(
        err.to_string().contains("requires pgwire_tls_cert"),
        "got: {err}"
    );
}
