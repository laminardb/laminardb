//! End-to-end pgwire driven by `tokio_postgres` against an in-process
//! `LaminarDB`. Verifies the wire-protocol surface — handshake, SimpleQuery
//! dispatch, error reporting — that unit tests can't reach. Engine-level
//! row flow is covered in `laminar-db`'s `db::tests`.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use laminar_db::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};
use laminar_db::LaminarDB;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use super::{
    Secret, SUBSCRIPTION_CHECKPOINT_COLUMN, SUBSCRIPTION_EPOCH_COLUMN, SUBSCRIPTION_FETCH_WAIT,
    SUBSCRIPTION_KIND_COLUMN, SUBSCRIPTION_LOG_SEQUENCE_COLUMN, SUBSCRIPTION_ROW_INDEX_COLUMN,
    SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN,
};

async fn spawn_server_with(
    users: HashMap<String, Secret>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let db = LaminarDB::open().expect("db opens");
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .expect("create source");
    db.execute(
        "CREATE MATERIALIZED VIEW prices AS \
         SELECT symbol, price FROM trades",
    )
    .await
    .expect("create mv");
    db.start().await.expect("db starts");

    let (addr, handle) = super::serve(Arc::clone(&db), "127.0.0.1:0", users, false, None, 256, 10)
        .await
        .expect("pgwire serve");
    (addr, handle)
}

async fn spawn_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    spawn_server_with(HashMap::new()).await
}

async fn connect(addr: std::net::SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=any dbname=laminardb",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("pgwire connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn raw_read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let message_type = stream.read_u8().await.expect("backend message type");
    let length = stream.read_i32().await.expect("backend message length");
    assert!(length >= 4, "invalid backend message length {length}");
    let mut body = vec![0; (length - 4) as usize];
    stream
        .read_exact(&mut body)
        .await
        .expect("backend message body");
    (message_type, body)
}

async fn raw_read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    loop {
        let message = raw_read_message(stream).await;
        let ready = message.0 == b'Z';
        messages.push(message);
        if ready {
            return messages;
        }
    }
}

fn raw_frame(message_type: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = BytesMut::with_capacity(body.len() + 5);
    frame.put_u8(message_type);
    frame.put_i32(i32::try_from(body.len() + 4).expect("test frame length"));
    frame.extend_from_slice(body);
    frame.to_vec()
}

async fn raw_connect(addr: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("raw connect");
    let mut body = BytesMut::new();
    body.put_i32(196_608);
    body.extend_from_slice(b"user\0any\0database\0laminardb\0\0");
    let mut startup = BytesMut::new();
    startup.put_i32(i32::try_from(body.len() + 4).unwrap());
    startup.extend_from_slice(&body);
    stream.write_all(&startup).await.expect("write startup");
    let messages = raw_read_until_ready(&mut stream).await;
    assert!(messages.iter().all(|message| message.0 != b'E'));
    stream
}

async fn raw_query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut body = BytesMut::new();
    body.extend_from_slice(sql.as_bytes());
    body.put_u8(0);
    stream
        .write_all(&raw_frame(b'Q', &body))
        .await
        .expect("write Query");
    raw_read_until_ready(stream).await
}

async fn raw_parse_bind_sync(
    stream: &mut TcpStream,
    statement: &str,
    portal: &str,
    sql: &str,
) -> Vec<(u8, Vec<u8>)> {
    let mut parse = BytesMut::new();
    parse.extend_from_slice(statement.as_bytes());
    parse.put_u8(0);
    parse.extend_from_slice(sql.as_bytes());
    parse.put_u8(0);
    parse.put_u16(0);

    let mut bind = BytesMut::new();
    bind.extend_from_slice(portal.as_bytes());
    bind.put_u8(0);
    bind.extend_from_slice(statement.as_bytes());
    bind.put_u8(0);
    bind.put_u16(0);
    bind.put_u16(0);
    bind.put_u16(0);

    let mut frames = raw_frame(b'P', &parse);
    frames.extend_from_slice(&raw_frame(b'B', &bind));
    frames.extend_from_slice(&raw_frame(b'S', &[]));
    stream
        .write_all(&frames)
        .await
        .expect("write Parse/Bind/Sync");
    raw_read_until_ready(stream).await
}

async fn raw_execute_sync(
    stream: &mut TcpStream,
    portal: &str,
    max_rows: i32,
) -> Vec<(u8, Vec<u8>)> {
    let mut execute = BytesMut::new();
    execute.extend_from_slice(portal.as_bytes());
    execute.put_u8(0);
    execute.put_i32(max_rows);

    let mut frames = raw_frame(b'E', &execute);
    frames.extend_from_slice(&raw_frame(b'S', &[]));
    stream.write_all(&frames).await.expect("write Execute/Sync");
    raw_read_until_ready(stream).await
}

fn first_row_value(messages: &[SimpleQueryMessage], col: usize) -> Option<&str> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(col),
        _ => None,
    })
}

#[tokio::test]
async fn handshake_and_builtins() {
    let (addr, handle) = spawn_server().await;
    let client = connect(addr).await;

    let messages = client
        .simple_query("SELECT version()")
        .await
        .expect("version");
    let v = first_row_value(&messages, 0).expect("row");
    assert!(v.contains("LaminarDB"), "version: {v}");

    let messages = client
        .simple_query("SELECT current_database()")
        .await
        .expect("current_database");
    assert_eq!(first_row_value(&messages, 0), Some("laminar"));

    handle.abort();
}

#[tokio::test]
async fn show_streams_runs() {
    let (addr, handle) = spawn_server().await;
    let client = connect(addr).await;

    // No assertion on contents — just that the dispatch path returns rows
    // without error. Engine-level SHOW behavior is covered in laminar-db.
    client
        .simple_query("SHOW STREAMS")
        .await
        .expect("SHOW STREAMS");

    handle.abort();
}

#[tokio::test]
async fn simple_subscribe_is_rejected_as_unbounded() {
    let (addr, handle) = spawn_server().await;
    let client = connect(addr).await;

    let err = client
        .simple_query("SUBSCRIBE prices")
        .await
        .expect_err("must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "0A000");
    assert!(
        db_err.message().contains("WebSocket"),
        "message: {}",
        db_err.message()
    );

    handle.abort();
}

#[tokio::test]
async fn bounded_subscribe_with_unknown_filter_column_returns_pg_error() {
    let (addr, handle) = spawn_server().await;
    let mut client = connect(addr).await;
    let tx = client.transaction().await.expect("BEGIN");
    let stmt = tx
        .prepare("SUBSCRIBE prices WHERE no_such_col > 1")
        .await
        .expect("parse resolves the stream schema");
    let portal = tx.bind(&stmt, &[]).await.expect("bind");

    let err = tx.query_portal(&portal, 1).await.expect_err("must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert!(
        db_err.message().contains("no_such_col"),
        "filter error must name the bad column, got: {}",
        db_err.message()
    );

    handle.abort();
}

#[tokio::test]
async fn subscribe_as_of_uncommitted_returns_pg_error() {
    // No checkpoint has committed on `prices`, so a future AS OF cut must
    // be distinguished from pruned history.
    let (addr, handle) = spawn_server().await;
    let mut client = connect(addr).await;
    let tx = client.transaction().await.expect("BEGIN");
    let stmt = tx
        .prepare("SUBSCRIBE prices AS OF EPOCH 1")
        .await
        .expect("prepare");
    let portal = tx.bind(&stmt, &[]).await.expect("bind");

    let err = tx.query_portal(&portal, 1).await.expect_err("must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "22023");
    assert!(
        db_err.message().contains("not committed"),
        "message: {}",
        db_err.message()
    );

    handle.abort();
}

/// SUBSCRIBE must actually stream emitted MV rows over the socket: bind a
/// portal, push rows into the source, and read them back via the
/// extended-query portal (the chunked path JDBC/asyncpg use).
#[tokio::test]
async fn subscribe_streams_emitted_rows_over_the_wire() {
    use std::time::Duration;

    use arrow_array::{Float64Array, RecordBatch, StringArray};

    let db = LaminarDB::open().expect("db opens");
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .expect("create source");
    db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
        .await
        .expect("create mv");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        None,
        256,
        10,
    )
    .await
    .expect("serve");
    let mut client = connect(addr).await;
    let txn = client.transaction().await.expect("begin");

    // The subscription opens when the first Execute runs, so push once the
    // read is in flight (Tail would otherwise miss earlier rows).
    let stmt = txn.prepare("SUBSCRIBE prices").await.expect("prepare");
    let portal = txn.bind(&stmt, &[]).await.expect("bind");

    let pusher = tokio::spawn({
        let db = Arc::clone(&db);
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let src = db.source_untyped("trades").expect("source handle");
            let batch = RecordBatch::try_new(
                src.schema().clone(),
                vec![
                    Arc::new(StringArray::from(vec!["AAPL", "MSFT"])),
                    Arc::new(Float64Array::from(vec![100.0, 200.0])),
                ],
            )
            .expect("batch");
            src.push_arrow(batch).expect("push");
        }
    });

    let rows = tokio::time::timeout(Duration::from_secs(10), txn.query_portal(&portal, 2))
        .await
        .expect("read did not time out")
        .expect("query_portal");
    pusher.await.expect("pusher");

    let mut symbols: Vec<String> = rows
        .iter()
        .map(|r| r.get::<_, &str>(0).to_string())
        .collect();
    symbols.sort();
    assert_eq!(
        symbols,
        ["AAPL", "MSFT"],
        "both emitted rows arrive over pgwire"
    );

    handle.abort();
}

/// A TEXT[] column must round-trip over the binary wire (asyncpg/JDBC
/// request binary): the column advertises the _text OID and encodes as a
/// Postgres array, so tokio_postgres decodes it into a Vec<String>.
#[tokio::test]
async fn subscribe_decodes_text_array_in_binary_format() {
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};

    let db = LaminarDB::open().expect("db opens");
    db.execute("CREATE SOURCE feed (id BIGINT)")
        .await
        .expect("create source");
    db.execute(
        "CREATE MATERIALIZED VIEW tagged AS SELECT id, make_array('en','ja') AS tags FROM feed",
    )
    .await
    .expect("create mv");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        None,
        256,
        10,
    )
    .await
    .expect("serve");
    let mut client = connect(addr).await;
    let txn = client.transaction().await.expect("begin");
    let stmt = txn.prepare("SUBSCRIBE tagged").await.expect("prepare");
    let portal = txn.bind(&stmt, &[]).await.expect("bind");

    let pusher = tokio::spawn({
        let db = Arc::clone(&db);
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let src = db.source_untyped("feed").expect("source handle");
            let batch = RecordBatch::try_new(
                src.schema().clone(),
                vec![Arc::new(Int64Array::from(vec![1_i64]))],
            )
            .expect("batch");
            src.push_arrow(batch).expect("push");
        }
    });

    let rows = tokio::time::timeout(Duration::from_secs(10), txn.query_portal(&portal, 1))
        .await
        .expect("read did not time out")
        .expect("query_portal");
    pusher.await.expect("pusher");

    assert_eq!(rows.len(), 1);
    let tags: Vec<String> = rows[0].get(1);
    assert_eq!(
        tags,
        vec!["en".to_string(), "ja".to_string()],
        "TEXT[] decoded over the binary wire"
    );

    handle.abort();
}

#[tokio::test]
async fn ddl_returns_pg_error_pointing_at_http() {
    let (addr, handle) = spawn_server().await;
    let client = connect(addr).await;

    let err = client
        .simple_query("CREATE SOURCE more_trades (sym VARCHAR)")
        .await
        .expect_err("DDL must be rejected");
    let db_err = err.as_db_error().expect("typed PG error");
    assert!(
        db_err.message().contains("/api/v1/sql"),
        "message: {}",
        db_err.message()
    );

    handle.abort();
}

fn md5_users() -> HashMap<String, Secret> {
    let mut u = HashMap::new();
    u.insert("alice".to_string(), Secret::new(test_password("alice")));
    u
}

fn test_password(user: &str) -> String {
    format!("{user}-test-{}", std::process::id())
}

async fn connect_with_password(
    addr: std::net::SocketAddr,
    user: &str,
    password: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let conn_str = format!(
        "host={} port={} user={user} password={password} dbname=laminardb",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

#[tokio::test]
async fn md5_auth_accepts_correct_password() {
    let (addr, handle) = spawn_server_with(md5_users()).await;
    let password = test_password("alice");

    let client = connect_with_password(addr, "alice", &password)
        .await
        .expect("auth must succeed");

    let messages = client
        .simple_query("SELECT version()")
        .await
        .expect("query after auth");
    let v = first_row_value(&messages, 0).expect("row");
    assert!(v.contains("LaminarDB"), "version: {v}");

    handle.abort();
}

#[tokio::test]
async fn concurrent_md5_challenges_are_session_isolated() {
    let alice_password = test_password("alice");
    let bob_password = test_password("bob");
    let mut users = HashMap::new();
    users.insert("alice".to_owned(), Secret::new(alice_password.clone()));
    users.insert("bob".to_owned(), Secret::new(bob_password.clone()));
    let (addr, handle) = spawn_server_with(users).await;

    let attempts = (0..64).map(|index| {
        let (user, password) = if index % 2 == 0 {
            ("alice", alice_password.as_str())
        } else {
            ("bob", bob_password.as_str())
        };
        async move {
            let client = connect_with_password(addr, user, password)
                .await
                .expect("concurrent authentication must succeed");
            client
                .simple_query("SELECT 1")
                .await
                .expect("authenticated session remains usable");
        }
    });
    futures::future::join_all(attempts).await;

    handle.abort();
}

#[tokio::test]
async fn md5_auth_rejects_wrong_password() {
    let (addr, handle) = spawn_server_with(md5_users()).await;
    let wrong_password = test_password("wrong");

    let err = connect_with_password(addr, "alice", &wrong_password)
        .await
        .expect_err("auth must fail");

    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");

    handle.abort();
}

/// Pre-hashed pgwire_users entry: stored value is `md5{hex(md5(pw||user))}`,
/// matching pg_authid. Plaintext never touches disk yet auth still succeeds.
fn md5_users_prehashed(user: &str, password: &str) -> HashMap<String, Secret> {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(password.as_bytes());
    h.update(user.as_bytes());
    let inner = format!("{:x}", h.finalize());
    let mut u = HashMap::new();
    u.insert(user.to_string(), Secret::new(format!("md5{inner}")));
    u
}

#[tokio::test]
async fn md5_auth_accepts_correct_password_against_prehash() {
    let password = test_password("alice");
    let (addr, handle) = spawn_server_with(md5_users_prehashed("alice", &password)).await;
    let client = connect_with_password(addr, "alice", &password)
        .await
        .expect("auth must succeed against pre-hashed entry");
    let messages = client
        .simple_query("SELECT version()")
        .await
        .expect("query after auth");
    let v = first_row_value(&messages, 0).expect("row");
    assert!(v.contains("LaminarDB"), "version: {v}");
    handle.abort();
}

#[tokio::test]
async fn md5_auth_rejects_wrong_password_against_prehash() {
    let password = test_password("alice");
    let wrong_password = test_password("wrong");
    let (addr, handle) = spawn_server_with(md5_users_prehashed("alice", &password)).await;
    let err = connect_with_password(addr, "alice", &wrong_password)
        .await
        .expect_err("auth must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");
    handle.abort();
}

#[test]
fn parse_pre_hashed_md5_strict_format() {
    // 32 lowercase hex after the tag → accepted.
    let inner = "5d41402abc4b2a76b9719d911017c592";
    assert_eq!(
        super::parse_pre_hashed_md5(&format!("md5{inner}")),
        Some(inner),
    );
    // Wrong length, uppercase hex, missing prefix, or non-hex → rejected.
    assert_eq!(super::parse_pre_hashed_md5("md5short"), None);
    assert_eq!(
        super::parse_pre_hashed_md5("md55D41402ABC4B2A76B9719D911017C592"),
        None,
    );
    assert_eq!(super::parse_pre_hashed_md5(inner), None);
    assert_eq!(
        super::parse_pre_hashed_md5("md5zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
        None,
    );
}

#[tokio::test]
async fn md5_auth_rejects_unknown_user() {
    let (addr, handle) = spawn_server_with(md5_users()).await;
    let password = test_password("mallory");

    let err = connect_with_password(addr, "mallory", &password)
        .await
        .expect_err("auth must fail");

    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");

    handle.abort();
}

#[tokio::test]
async fn connection_cap_drops_excess_clients() {
    // Cap of 1; first client occupies the slot, second receives a startup
    // FATAL without displacing the active session.
    let db = LaminarDB::open().expect("db opens");
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .expect("create source");
    db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
        .await
        .expect("create mv");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        None,
        1,
        10,
    )
    .await
    .expect("pgwire serve");

    // An authenticated connection occupies the only session slot.
    let _first = connect(addr).await;
    let conn_str = format!(
        "host={} port={} user=any dbname=laminardb",
        addr.ip(),
        addr.port()
    );
    let error = match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok(_) => panic!("second connect must be refused"),
        Err(error) => error,
    };
    let db_error = error.as_db_error().expect("typed startup FATAL");
    assert_eq!(db_error.code().code(), "53300");

    handle.abort();
}

#[tokio::test]
async fn cancel_request_bypasses_full_session_cap() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let db = LaminarDB::open().expect("db opens");
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .expect("create source");
    db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
        .await
        .expect("create mv");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "0.0.0.0:0",
        md5_users(),
        true,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: None,
        }),
        1,
        10,
    )
    .await
    .expect("pgwire serve");

    // Prefer negotiates TLS for the normal session but lets NoTls below
    // send the protocol-defined plaintext CancelRequest on a fresh socket.
    let conn_str = format!(
        "host=localhost hostaddr=127.0.0.1 port={} user=alice password={} \
         dbname=laminardb sslmode=prefer",
        addr.port(),
        test_password("alice"),
    );
    let tls = make_client_tls(&cert_path, None);
    let (client, connection) = tokio_postgres::connect(&conn_str, tls)
        .await
        .expect("TLS pgwire connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let cancel = client.cancel_token();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let query = tokio::spawn(async move {
        let mut client = client;
        let transaction = client.transaction().await.expect("BEGIN");
        let statement = transaction
            .prepare("SUBSCRIBE prices")
            .await
            .expect("prepare");
        let portal = transaction.bind(&statement, &[]).await.expect("bind");
        ready_tx.send(()).expect("query ready");
        transaction
            .query_portal(&portal, 1)
            .await
            .expect_err("quiet fetch must be cancelled")
            .as_db_error()
            .map(|error| error.code().code().to_owned())
    });

    ready_rx.await.expect("query ready");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    cancel
        .cancel_query(NoTls)
        .await
        .expect("plaintext CancelRequest bypasses TLS and the full session semaphore");
    let code = tokio::time::timeout(std::time::Duration::from_secs(3), query)
        .await
        .expect("cancel response")
        .expect("query task");
    assert_eq!(code.as_deref(), Some("57014"));

    handle.abort();
}

/// Self-signed cert+key written to a tempdir for the duration of the
/// test. `rcgen` is the well-maintained option for ad-hoc certs.
fn self_signed_pem() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen issue cert");
    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
    (dir, cert_path, key_path)
}

/// CA + client-leaf bundle for mTLS tests. The CA PEM is written to a
/// tempfile so the server can be pointed at it via `pgwire_tls_client_ca`;
/// the leaf cert+key are returned in DER form for direct use by a rustls
/// `ClientConfig`.
struct MintedClientPki {
    _dir: tempfile::TempDir,
    ca_pem_path: std::path::PathBuf,
    leaf_chain: Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    leaf_key: tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
}

fn mint_ca_and_client_leaf(common_name: &str) -> MintedClientPki {
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    let mut ca_params = rcgen::CertificateParams::new(vec!["mtls-test-ca".into()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut leaf_params = rcgen::CertificateParams::new(vec![common_name.into()]).unwrap();
    leaf_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let ca_pem_path = dir.path().join("ca.pem");
    std::fs::write(&ca_pem_path, ca_cert.pem()).unwrap();

    let leaf_chain = vec![CertificateDer::from(leaf_cert.der().to_vec())];
    let leaf_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()));

    MintedClientPki {
        _dir: dir,
        ca_pem_path,
        leaf_chain,
        leaf_key,
    }
}

/// Builds a tokio_postgres TLS connector that trusts `server_cert_path`
/// for the server hello and (optionally) presents a client cert for mTLS.
fn make_client_tls(
    server_cert_path: &std::path::Path,
    client_auth: Option<(
        Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
        tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
    )>,
) -> tokio_postgres_rustls::MakeRustlsConnect {
    super::ensure_tls_provider();
    let cert_bytes = std::fs::read(server_cert_path).unwrap();
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for c in rustls_pemfile::certs(&mut std::io::Cursor::new(cert_bytes))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    {
        roots.add(c).unwrap();
    }
    let builder = tokio_rustls::rustls::ClientConfig::builder().with_root_certificates(roots);
    let client_cfg = match client_auth {
        Some((chain, key)) => builder.with_client_auth_cert(chain, key).unwrap(),
        None => builder.with_no_client_auth(),
    };
    tokio_postgres_rustls::MakeRustlsConnect::new(client_cfg)
}

async fn assert_plaintext_startup_is_fatal(addr: std::net::SocketAddr) {
    let mut stream = TcpStream::connect(addr).await.expect("raw NoTls connect");
    let mut body = BytesMut::new();
    body.put_i32(196_608);
    body.extend_from_slice(b"user\0alice\0database\0laminardb\0\0");
    let mut startup = BytesMut::new();
    startup.put_i32(i32::try_from(body.len() + 4).expect("startup length"));
    startup.extend_from_slice(&body);
    stream
        .write_all(&startup)
        .await
        .expect("write plaintext StartupMessage");

    let (message_type, body) = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        raw_read_message(&mut stream),
    )
    .await
    .expect("startup FATAL response");
    assert_eq!(
        message_type, b'E',
        "authentication must not begin on plaintext"
    );
    assert!(
        body.windows(b"TLS is required".len())
            .any(|window| window == b"TLS is required"),
        "unexpected ErrorResponse: {body:?}"
    );
}

#[tokio::test]
async fn remote_listener_rejects_raw_notls_startup_before_auth() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let (bound, handle) = super::serve(
        Arc::clone(&db),
        "0.0.0.0:0",
        md5_users(),
        true,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: None,
        }),
        256,
        10,
    )
    .await
    .expect("remote TLS listener");

    assert_plaintext_startup_is_fatal(std::net::SocketAddr::from((
        std::net::Ipv4Addr::LOCALHOST,
        bound.port(),
    )))
    .await;
    handle.abort();
}

#[tokio::test]
async fn client_ca_requires_tls_on_loopback() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let pki = mint_ca_and_client_leaf("alice");
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: Some(&pki.ca_pem_path),
        }),
        256,
        10,
    )
    .await
    .expect("mTLS listener");

    assert_plaintext_startup_is_fatal(addr).await;
    handle.abort();
}

/// Self-signed cert with notAfter in the past, for the expiry test.
fn expired_self_signed_pem() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
    let one_year_ago = time::OffsetDateTime::now_utc() - time::Duration::days(365);
    params.not_before = one_year_ago - time::Duration::days(2);
    params.not_after = one_year_ago;
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (dir, cert_path, key_path)
}

#[tokio::test]
async fn tls_load_rejects_expired_cert() {
    let (_dir, cert_path, key_path) = expired_self_signed_pem();
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let err = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: None,
        }),
        256,
        10,
    )
    .await
    .expect_err("expired cert must be rejected");
    assert!(err.to_string().contains("expired"), "got: {err}");
}

#[tokio::test]
async fn tls_min_1_3_rejects_tls_1_2_client() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_3,
            client_ca: None,
        }),
        256,
        10,
    )
    .await
    .expect("pgwire serve");

    let cert_bytes = std::fs::read(&cert_path).unwrap();
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for c in rustls_pemfile::certs(&mut std::io::Cursor::new(cert_bytes))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    {
        roots.add(c).unwrap();
    }
    super::ensure_tls_provider();
    // Client pinned to TLS 1.2 only — must be refused by a 1.3-min server.
    let client_cfg = tokio_rustls::rustls::ClientConfig::builder_with_protocol_versions(&[
        &tokio_rustls::rustls::version::TLS12,
    ])
    .with_root_certificates(roots)
    .with_no_client_auth();

    let conn_str = format!(
        "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
        addr.ip(),
        addr.port(),
    );
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(client_cfg);
    let err = match tokio_postgres::connect(&conn_str, tls).await {
        Ok(_) => panic!("TLS 1.2 client must be refused by a 1.3-min server"),
        Err(e) => e,
    };
    // tokio_postgres wraps the rustls error; flatten the chain so we can
    // assert against the version-mismatch token rustls emits.
    let chain = std::iter::successors(Some(&err as &dyn std::error::Error), |e| e.source())
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        chain.contains("ProtocolVersion") || chain.contains("incompatible"),
        "expected a TLS version-mismatch error, got: {chain}"
    );

    handle.abort();
}

#[tokio::test]
async fn tls_handshake_succeeds() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: None,
        }),
        256,
        10,
    )
    .await
    .expect("pgwire serve");

    // Build a client TLS config that trusts the same self-signed cert.
    let cert_bytes = std::fs::read(&cert_path).unwrap();
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for c in rustls_pemfile::certs(&mut std::io::Cursor::new(cert_bytes))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    {
        roots.add(c).unwrap();
    }
    super::ensure_tls_provider();
    let client_cfg = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let conn_str = format!(
        "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
        addr.ip(),
        addr.port(),
    );
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(client_cfg);
    let (client, conn) = tokio_postgres::connect(&conn_str, tls)
        .await
        .expect("TLS handshake + connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let messages = client
        .simple_query("SELECT version()")
        .await
        .expect("query over TLS");
    let v = first_row_value(&messages, 0).expect("row");
    assert!(v.contains("LaminarDB"), "version: {v}");

    handle.abort();
}

/// mTLS: with a client_ca configured, a client that presents no cert
/// must be refused at handshake time.
#[tokio::test]
async fn mtls_rejects_client_without_cert() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let pki = mint_ca_and_client_leaf("alice");
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: Some(&pki.ca_pem_path),
        }),
        256,
        10,
    )
    .await
    .expect("pgwire serve");

    let tls = make_client_tls(&cert_path, None);
    let conn_str = format!(
        "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
        addr.ip(),
        addr.port(),
    );
    let err = match tokio_postgres::connect(&conn_str, tls).await {
        Ok(_) => panic!("client without a cert must be refused under mTLS"),
        Err(e) => e,
    };
    assert!(
        err_chain(&err).contains("CertificateRequired")
            || err_chain(&err).contains("HandshakeFailure")
            || err_chain(&err).contains("certificate required"),
        "expected a missing-client-cert error, got: {}",
        err_chain(&err),
    );
    handle.abort();
}

/// mTLS: a client cert signed by an unknown CA must be refused.
#[tokio::test]
async fn mtls_rejects_untrusted_client_cert() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let trusted = mint_ca_and_client_leaf("trusted");
    let stranger = mint_ca_and_client_leaf("stranger");
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: Some(&trusted.ca_pem_path),
        }),
        256,
        10,
    )
    .await
    .expect("pgwire serve");

    // Client presents a leaf signed by a CA the server doesn't know.
    let tls = make_client_tls(
        &cert_path,
        Some((stranger.leaf_chain.clone(), stranger.leaf_key.clone_key())),
    );
    let conn_str = format!(
        "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
        addr.ip(),
        addr.port(),
    );
    let err = match tokio_postgres::connect(&conn_str, tls).await {
        Ok(_) => panic!("untrusted client cert must be refused"),
        Err(e) => e,
    };
    // rustls maps a verifier-rejected client cert to a fatal alert; the
    // exact variant depends on the protocol version and verifier path
    // (UnknownCA / BadCertificate on 1.2, DecryptError or
    // CertificateUnknown on 1.3). We assert it failed at the TLS layer.
    let chain = err_chain(&err);
    assert!(
        chain.contains("UnknownCA")
            || chain.contains("BadCertificate")
            || chain.contains("CertificateUnknown")
            || chain.contains("DecryptError")
            || chain.contains("HandshakeFailure"),
        "expected a cert-rejection alert, got: {chain}",
    );
    handle.abort();
}

/// mTLS: a client cert signed by the configured CA is accepted, and a
/// SimpleQuery completes over the encrypted+authenticated session.
#[tokio::test]
async fn mtls_accepts_trusted_client_cert() {
    let (_dir, cert_path, key_path) = self_signed_pem();
    let pki = mint_ca_and_client_leaf("alice");
    let db = LaminarDB::open().expect("db opens");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        Some(super::TlsPaths {
            cert: &cert_path,
            key: &key_path,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: Some(&pki.ca_pem_path),
        }),
        256,
        10,
    )
    .await
    .expect("pgwire serve");

    let tls = make_client_tls(
        &cert_path,
        Some((pki.leaf_chain.clone(), pki.leaf_key.clone_key())),
    );
    let conn_str = format!(
        "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
        addr.ip(),
        addr.port(),
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, tls)
        .await
        .expect("mTLS handshake + connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let messages = client
        .simple_query("SELECT version()")
        .await
        .expect("query over mTLS");
    let v = first_row_value(&messages, 0).expect("row");
    assert!(v.contains("LaminarDB"), "version: {v}");
    handle.abort();
}

/// Build a `TlsReloadState` directly for unit-testing the reload path
/// without standing up a listener.
fn build_reload_state(cert: &std::path::Path, key: &std::path::Path) -> super::TlsReloadState {
    let paths = super::TlsPaths {
        cert,
        key,
        min_version: super::TlsMinVersion::V1_2,
        client_ca: None,
    };
    let acceptor = super::load_tls_acceptor(super::TlsPaths {
        cert: paths.cert,
        key: paths.key,
        min_version: paths.min_version,
        client_ca: paths.client_ca,
    })
    .expect("initial acceptor loads");
    super::TlsReloadState {
        paths: super::TlsConfigPaths::from_paths(&paths),
        acceptor: parking_lot::Mutex::new(Arc::new(acceptor)),
    }
}

/// Hot-reload: writing a fresh cert+key over the configured paths and
/// calling `try_reload_tls` swaps the acceptor under the mutex.
#[test]
fn tls_reload_swaps_acceptor_on_valid_pair() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    // Initial cert.
    let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(&cert_path, first.cert.pem()).unwrap();
    std::fs::write(&key_path, first.key_pair.serialize_pem()).unwrap();

    let state = build_reload_state(&cert_path, &key_path);
    let before = state.snapshot();

    // Rotate to a brand-new pair.
    let second = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(&cert_path, second.cert.pem()).unwrap();
    std::fs::write(&key_path, second.key_pair.serialize_pem()).unwrap();

    super::try_reload_tls(&state).expect("reload succeeds");
    let after = state.snapshot();
    assert!(
        !Arc::ptr_eq(&before, &after),
        "acceptor pointer must change after a successful reload",
    );
}

/// Hot-reload: a corrupt cert file leaves the previous acceptor in
/// place — TLS doesn't go down on a bad rotation.
#[test]
fn tls_reload_keeps_old_acceptor_on_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(&cert_path, first.cert.pem()).unwrap();
    std::fs::write(&key_path, first.key_pair.serialize_pem()).unwrap();

    let state = build_reload_state(&cert_path, &key_path);
    let before = state.snapshot();

    // Truncate cert.pem to non-PEM garbage.
    std::fs::write(&cert_path, b"this is not a certificate").unwrap();
    let err = super::try_reload_tls(&state).expect_err("reload must fail");
    let after = state.snapshot();
    assert!(
        Arc::ptr_eq(&before, &after),
        "acceptor must be unchanged on reload failure",
    );
    assert!(
        err.to_string().contains("pgwire_tls_cert"),
        "error should mention pgwire_tls_cert, got: {err}",
    );
}

/// Flatten an error and its `source()` chain to a single string for
/// substring assertions.
fn err_chain(err: &(dyn std::error::Error + 'static)) -> String {
    std::iter::successors(Some(err), |e| e.source())
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Push one row into the `trades` source so subsequent SUBSCRIBE
/// reads have something to drain. Returns the schema for tests
/// that want to build their own batches.
async fn push_one_trade(db: &Arc<LaminarDB>, symbol: &str, price: f64) -> arrow_schema::SchemaRef {
    let handle = db.source_untyped("trades").expect("source handle");
    let schema = handle.schema().clone();
    let batch = arrow_array::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::StringArray::from(vec![symbol])),
            Arc::new(arrow_array::Float64Array::from(vec![price])),
        ],
    )
    .expect("batch");
    handle.push_arrow(batch).expect("push");
    schema
}

/// Wait until the coordinator has published rows to every attached subscriber.
/// The probe and pgwire cursor must both be opened before ingestion starts.
async fn wait_for_published_rows(portal: &mut SubscriptionPortal, expected_rows: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut observed_rows = 0;
        while observed_rows < expected_rows {
            match portal
                .next_frame()
                .await
                .expect("publication probe remains open")
            {
                PortalFrame::Batch { batch, .. } => observed_rows += batch.num_rows(),
                PortalFrame::Barrier { .. } => {}
                PortalFrame::Lagged(skipped) => {
                    panic!("publication probe lagged by {skipped} frames")
                }
                PortalFrame::Error { message } => {
                    panic!("publication probe failed: {message}")
                }
            }
        }
    })
    .await
    .expect("coordinator publishes rows before the test deadline");
}

/// Ingest a row and return both the running server and the underlying db
/// so tests can keep pushing rows after the listener is up.
async fn spawn_with_data() -> (
    Arc<LaminarDB>,
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let db = LaminarDB::open().expect("db opens");
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .expect("create source");
    db.execute(
        "CREATE MATERIALIZED VIEW prices AS \
         SELECT symbol, price FROM trades",
    )
    .await
    .expect("create mv");
    db.start().await.expect("db starts");

    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        None,
        256,
        10,
    )
    .await
    .expect("pgwire serve");
    (db, addr, handle)
}

/// `prepare()` triggers `Parse` + `Describe(Statement)`. Verifies the
/// extended-query parser resolves stream schemas at parse time and
/// returns column metadata to the client.
#[tokio::test]
async fn extended_query_describe_subscribe_returns_columns() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    let stmt = client
        .prepare("SUBSCRIBE prices")
        .await
        .expect("prepare SUBSCRIBE prices");

    let cols = stmt.columns();
    assert_eq!(cols.len(), 8, "expected 8 columns, got {}", cols.len());
    assert_eq!(cols[0].name(), "symbol");
    assert_eq!(cols[1].name(), "price");
    assert_eq!(cols[0].type_(), &tokio_postgres::types::Type::VARCHAR);
    assert_eq!(cols[1].type_(), &tokio_postgres::types::Type::FLOAT8);
    assert_eq!(cols[2].name(), SUBSCRIPTION_KIND_COLUMN);
    assert_eq!(cols[3].name(), SUBSCRIPTION_EPOCH_COLUMN);
    assert_eq!(cols[4].name(), SUBSCRIPTION_CHECKPOINT_COLUMN);
    assert_eq!(cols[5].name(), SUBSCRIPTION_LOG_SEQUENCE_COLUMN);
    assert_eq!(cols[6].name(), SUBSCRIPTION_ROW_INDEX_COLUMN);
    assert_eq!(cols[7].name(), SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN);

    handle.abort();
}

#[tokio::test]
async fn execute_zero_rejects_before_acquiring_subscription_slot() {
    let (db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;
    let stmt = client.prepare("SUBSCRIBE prices").await.expect("prepare");

    let error = client
        .query(&stmt, &[])
        .await
        .expect_err("Execute(0) must be rejected");
    let db_error = error.as_db_error().expect("typed PG error");
    assert_eq!(db_error.code().code(), "0A000");

    let mut portals = Vec::new();
    for _ in 0..64 {
        portals.push(
            db.open_subscription("prices", None, SubscribeStart::Tail)
                .await
                .expect("rejected Execute must not consume a slot"),
        );
    }
    assert!(
        db.open_subscription("prices", None, SubscribeStart::Tail)
            .await
            .is_err(),
        "the configured 64-slot limit must still be enforced"
    );

    handle.abort();
}

#[tokio::test]
async fn sync_obeys_transaction_scoped_portal_lifetime() {
    let (_db, addr, handle) = spawn_with_data().await;

    let mut outside = raw_connect(addr).await;
    let bind = raw_parse_bind_sync(
        &mut outside,
        "outside_statement",
        "outside_portal",
        "SUBSCRIBE prices",
    )
    .await;
    assert!(bind.iter().all(|message| message.0 != b'E'));
    let execute = raw_execute_sync(&mut outside, "outside_portal", 1).await;
    assert!(
        execute.iter().any(|message| message.0 == b'E'),
        "Sync outside BEGIN must end the implicit transaction and destroy portals"
    );

    let mut inside = raw_connect(addr).await;
    let begin = raw_query(&mut inside, "BEGIN").await;
    assert_eq!(
        begin.last().and_then(|message| message.1.first()).copied(),
        Some(b'T')
    );
    for (statement, portal) in [("named_statement", "named_portal"), ("", "")] {
        let bind = raw_parse_bind_sync(&mut inside, statement, portal, "SUBSCRIBE prices").await;
        assert!(bind.iter().all(|message| message.0 != b'E'));
        assert_eq!(
            bind.last().and_then(|message| message.1.first()).copied(),
            Some(b'T')
        );

        let execute = raw_execute_sync(&mut inside, portal, i32::MAX).await;
        assert!(execute.iter().all(|message| message.0 != b'E'));
        assert!(
            execute.iter().any(|message| message.0 == b's'),
            "bounded fetch must suspend without allocating from i32::MAX"
        );
    }
    let rollback = raw_query(&mut inside, "ROLLBACK").await;
    assert_eq!(
        rollback
            .last()
            .and_then(|message| message.1.first())
            .copied(),
        Some(b'I')
    );

    handle.abort();
}

#[tokio::test]
async fn cancel_interrupts_subscription_fetch_and_releases_slot() {
    let (db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;
    let cancel = client.cancel_token();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    let query = tokio::spawn(async move {
        let mut client = client;
        let tx = client.transaction().await.expect("BEGIN");
        let statement = tx.prepare("SUBSCRIBE prices").await.expect("prepare");
        let portal = tx.bind(&statement, &[]).await.expect("bind");
        ready_tx.send(()).expect("signal query readiness");
        let error = tx
            .query_portal(&portal, 1)
            .await
            .expect_err("cancel must interrupt the fetch");
        error
            .as_db_error()
            .map(|error| error.code().code().to_owned())
    });

    ready_rx.await.expect("query ready");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    cancel
        .cancel_query(NoTls)
        .await
        .expect("send CancelRequest");
    let code = tokio::time::timeout(std::time::Duration::from_secs(3), query)
        .await
        .expect("cancel response")
        .expect("query task");
    assert_eq!(code.as_deref(), Some("57014"));

    let mut portals = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while portals.len() < 64 {
        match db
            .open_subscription("prices", None, SubscribeStart::Tail)
            .await
        {
            Ok(portal) => portals.push(portal),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Err(error) => panic!("cancel did not release subscription slot: {error}"),
        }
    }

    handle.abort();
}

#[tokio::test]
async fn extended_query_emits_committed_checkpoint_progress() {
    let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
    let db = LaminarDB::open_with_config(laminar_db::LaminarConfig {
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            data_dir: Some(checkpoint_dir.path().to_path_buf()),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("db opens");
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .expect("create source");
    db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
        .await
        .expect("create mv");
    db.start().await.expect("db starts");
    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        None,
        256,
        10,
    )
    .await
    .expect("pgwire serve");
    let mut client = connect(addr).await;
    let tx = client.transaction().await.expect("BEGIN");
    let stmt = tx.prepare("SUBSCRIBE prices").await.expect("prepare");
    let portal = tx.bind(&stmt, &[]).await.expect("bind portal");

    let pusher = tokio::spawn({
        let db = Arc::clone(&db);
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            push_one_trade(&db, "AAPL", 150.0).await;
        }
    });
    let mut rows = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tx.query_portal(&portal, 1),
    )
    .await
    .expect("data row arrives")
    .expect("query portal");
    pusher.await.expect("pusher");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let committed = db.checkpoint().await.expect("checkpoint");
    rows.extend(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tx.query_portal(&portal, 1),
        )
        .await
        .expect("progress row arrives")
        .expect("query portal"),
    );

    assert!(committed.success, "checkpoint must commit");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "AAPL");
    assert_eq!(rows[0].get::<_, &str>(2), "data");
    assert!(rows[0].get::<_, Option<&str>>(3).is_none());
    assert!(rows[0].get::<_, Option<&str>>(4).is_none());
    assert_eq!(rows[0].get::<_, &str>(5), "0");
    assert_eq!(rows[0].get::<_, &str>(6), "0");
    assert!(rows[0].get::<_, Option<&str>>(7).is_none());
    assert!(rows[1].get::<_, Option<&str>>(0).is_none());
    assert!(rows[1].get::<_, Option<f64>>(1).is_none());
    assert_eq!(rows[1].get::<_, &str>(2), "progress");
    assert_eq!(rows[1].get::<_, &str>(3), committed.epoch.to_string());
    assert_eq!(
        rows[1].get::<_, &str>(4),
        committed.checkpoint_id.to_string()
    );
    assert_eq!(rows[1].get::<_, &str>(5), "1");
    assert!(rows[1].get::<_, Option<&str>>(6).is_none());
    assert_eq!(rows[1].get::<_, &str>(7), "1");

    handle.abort();
}

#[tokio::test]
async fn prepared_subscribe_rejects_drop_recreate_schema_change() {
    let (db, addr, handle) = spawn_with_data().await;
    let mut client = connect(addr).await;

    let stmt = client
        .prepare("SUBSCRIBE prices")
        .await
        .expect("prepare old result type");
    db.execute("DROP MATERIALIZED VIEW prices")
        .await
        .expect("drop old view");
    db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol FROM trades")
        .await
        .expect("create changed view");

    let tx = client.transaction().await.expect("BEGIN");
    let portal = tx.bind(&stmt, &[]).await.expect("bind cached statement");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tx.query_portal(&portal, 1),
    )
    .await
    .expect("schema fence responds")
    .expect_err("cached result type must not execute");
    let db_error = error.as_db_error().expect("typed PG error");
    assert_eq!(db_error.code().code(), "0A000");
    assert_eq!(db_error.message(), "cached result type changed");

    handle.abort();
}

#[tokio::test]
async fn abrupt_disconnect_releases_named_cursor_subscription_slot() {
    let (db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE abandoned CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");
    drop(client);

    let mut portals = Vec::new();
    for _ in 0..63 {
        portals.push(
            db.open_subscription("prices", None, SubscribeStart::Tail)
                .await
                .expect("63 direct slots remain"),
        );
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match db
            .open_subscription("prices", None, SubscribeStart::Tail)
            .await
        {
            Ok(portal) => {
                portals.push(portal);
                break;
            }
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Err(error) => panic!("disconnect did not release cursor slot: {error}"),
        }
    }
    assert_eq!(portals.len(), 64);
    assert!(
        db.open_subscription("prices", None, SubscribeStart::Tail)
            .await
            .is_err(),
        "the 64-slot limit remains enforced"
    );

    handle.abort();
}

/// Unknown stream → typed PG error at `Parse` time, before any rows
/// are pulled.
#[tokio::test]
async fn extended_query_prepare_unknown_stream_errors() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    let err = client
        .prepare("SUBSCRIBE no_such_view")
        .await
        .expect_err("must fail at Parse");
    let db_err = err.as_db_error().expect("typed PG error");
    assert!(db_err.message().contains("no_such_view"));

    handle.abort();
}

/// Bind + Execute with `max_rows=1` against a portal returns one row at a
/// time and `PortalSuspended`. Drives the binary-format encoders for
/// VARCHAR + FLOAT8.
#[tokio::test]
async fn extended_query_binary_chunked_subscribe() {
    let (db, addr, handle) = spawn_with_data().await;
    let mut client = connect(addr).await;

    // tokio_postgres' `bind` + `query_portal` uses the extended-query
    // protocol with binary format for known column types — the path
    // JDBC and asyncpg take with prepared statements.
    let tx = client.transaction().await.expect("BEGIN");
    let stmt = tx.prepare("SUBSCRIBE prices").await.expect("prepare");
    let portal = tx.bind(&stmt, &[]).await.expect("bind portal");

    // The MV broadcast has no receiver until `Execute` reaches the
    // server and runs `do_query` → `open_subscription`. We can't push
    // from this task before query_portal because query_portal blocks
    // waiting for a row, so spawn the pushes from a sibling task with
    // a short head start for the receiver to attach. With cap=0
    // retention, a push that lands before the receiver is dropped.
    let pusher = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            push_one_trade(&db, "AAPL", 150.5).await;
            push_one_trade(&db, "GOOG", 2700.25).await;
        })
    };

    let first = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tx.query_portal(&portal, 1),
    )
    .await
    .expect("first chunk arrives within 3s")
    .expect("query_portal #1");
    assert_eq!(first.len(), 1);
    let symbol: &str = first[0].get(0);
    let price: f64 = first[0].get(1);
    assert_eq!(symbol, "AAPL");
    assert!((price - 150.5).abs() < 1e-9);

    let second = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tx.query_portal(&portal, 1),
    )
    .await
    .expect("second chunk arrives within 3s")
    .expect("query_portal #2");
    assert_eq!(second.len(), 1);
    let symbol: &str = second[0].get(0);
    let price: f64 = second[0].get(1);
    assert_eq!(symbol, "GOOG");
    assert!((price - 2700.25).abs() < 1e-9);

    pusher.await.expect("push task");
    handle.abort();
}

/// Regression: binary encoding of `TIMESTAMP` columns must downcast
/// the Arrow array as its unit-specific primitive type
/// (`PrimitiveArray<TimestampMicrosecondType>`, not
/// `PrimitiveArray<Int64Type>`). A bug in this branch would panic on
/// the first row.
#[tokio::test]
async fn extended_query_binary_timestamp() {
    let db = LaminarDB::open().expect("db opens");
    // `WATERMARK FOR ts AS ts - INTERVAL '0' SECOND` declares event time
    // so the streaming pipeline drives progress on the timestamp
    // column — without it, the MV stays empty.
    db.execute(
        "CREATE SOURCE events (ts TIMESTAMP, sym VARCHAR, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .expect("create source");
    db.execute("CREATE MATERIALIZED VIEW ev AS SELECT ts, sym FROM events")
        .await
        .expect("create mv");
    db.start().await.expect("db starts");

    let (addr, handle) = super::serve(
        Arc::clone(&db),
        "127.0.0.1:0",
        HashMap::new(),
        false,
        None,
        256,
        10,
    )
    .await
    .expect("pgwire serve");

    let mut client = connect(addr).await;
    let tx = client.transaction().await.expect("BEGIN");
    let stmt = tx.prepare("SUBSCRIBE ev").await.expect("prepare");
    let portal = tx.bind(&stmt, &[]).await.expect("bind");

    let expected = chrono::NaiveDate::from_ymd_opt(2026, 5, 9)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let ts_us = expected.and_utc().timestamp_micros();

    // Push from a sibling task after a short delay so the MV
    // broadcast receiver (created inside `Execute`) is attached
    // before send_batch fires. See the matching note in
    // `extended_query_binary_chunked_subscribe`.
    let pusher = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let src = db.source_untyped("events").expect("source");
            let batch = arrow_array::RecordBatch::try_new(
                src.schema().clone(),
                vec![
                    Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![ts_us])),
                    Arc::new(arrow_array::StringArray::from(vec!["AAPL"])),
                ],
            )
            .expect("batch");
            src.push_arrow(batch).expect("push");
        })
    };

    let rows = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tx.query_portal(&portal, 1),
    )
    .await
    .expect("row arrives within 3s")
    .expect("query_portal");
    assert_eq!(rows.len(), 1);

    let ts: chrono::NaiveDateTime = rows[0].get(0);
    let sym: &str = rows[0].get(1);
    assert_eq!(ts, expected);
    assert_eq!(sym, "AAPL");

    pusher.await.expect("push task");
    handle.abort();
}

/// DDL on the extended-query path is refused at `Parse` with a typed
/// 0A000 error pointing at the HTTP endpoint — same surface as the
/// SimpleQuery path.
#[tokio::test]
async fn extended_query_ddl_rejected() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    let err = client
        .prepare("CREATE SOURCE more_trades (sym VARCHAR)")
        .await
        .expect_err("DDL must be rejected at Parse");
    let db_err = err.as_db_error().expect("typed PG error");
    assert!(
        db_err.message().contains("/api/v1/sql"),
        "message: {}",
        db_err.message()
    );

    handle.abort();
}

/// `\set FETCH_COUNT N` flow: BEGIN; DECLARE …; FETCH N FROM …; CLOSE; COMMIT.
/// All over SimpleQuery — the path psql uses when `FETCH_COUNT` is set.
#[tokio::test]
async fn cursor_declare_fetch_close_happy_path() {
    let (db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;
    let mut publication_probe = db
        .open_subscription("prices", None, SubscribeStart::Tail)
        .await
        .expect("open publication probe");

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");

    for i in 0..4 {
        push_one_trade(&db, &format!("S{i}"), i as f64).await;
    }
    wait_for_published_rows(&mut publication_probe, 4).await;

    let messages = client
        .simple_query("FETCH 2 FROM c")
        .await
        .expect("FETCH 2");
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 2, "expected exactly 2 rows from FETCH 2");

    client.simple_query("CLOSE c").await.expect("CLOSE");
    client.simple_query("COMMIT").await.expect("COMMIT");

    handle.abort();
}

#[tokio::test]
async fn cursor_requires_explicit_transaction() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    let error = client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect_err("DECLARE outside BEGIN must fail");
    let db_error = error.as_db_error().expect("typed PG error");
    assert_eq!(db_error.code().code(), "25001");

    handle.abort();
}

#[tokio::test]
async fn cursor_rejects_unbounded_and_oversized_fetches() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    for (sql, code) in [
        ("FETCH ALL FROM c", "0A000"),
        ("FETCH 1025 FROM c", "22023"),
    ] {
        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");
        let error = client.simple_query(sql).await.expect_err("FETCH must fail");
        let db_error = error.as_db_error().expect("typed PG error");
        assert_eq!(db_error.code().code(), code, "{sql}: {db_error:?}");
        client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    }

    handle.abort();
}

#[tokio::test]
async fn quiet_cursor_fetch_returns_a_bounded_empty_poll() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");

    let started = tokio::time::Instant::now();
    let messages = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        client.simple_query("FETCH 1 FROM c"),
    )
    .await
    .expect("bounded poll must return")
    .expect("FETCH");
    assert!(messages
        .iter()
        .all(|message| !matches!(message, SimpleQueryMessage::Row(_))));
    assert!(started.elapsed() >= SUBSCRIPTION_FETCH_WAIT);
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");

    handle.abort();
}

/// COMMIT must close any open cursors. After COMMIT, FETCH against the
/// same name returns "cursor does not exist".
#[tokio::test]
async fn cursor_commit_closes_cursors() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");
    client.simple_query("COMMIT").await.expect("COMMIT");
    client.simple_query("BEGIN").await.expect("BEGIN again");

    let err = client
        .simple_query("FETCH 1 FROM c")
        .await
        .expect_err("FETCH after COMMIT must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

    handle.abort();
}

/// ROLLBACK closes cursors too — same reaper as COMMIT.
#[tokio::test]
async fn cursor_rollback_closes_cursors() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    client.simple_query("BEGIN").await.expect("BEGIN again");

    let err = client
        .simple_query("FETCH 1 FROM c")
        .await
        .expect_err("FETCH after ROLLBACK must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

    handle.abort();
}

/// Explicit CLOSE destroys the cursor while its transaction remains open.
#[tokio::test]
async fn cursor_close_explicit() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");
    client.simple_query("CLOSE c").await.expect("CLOSE");

    let err = client
        .simple_query("FETCH 1 FROM c")
        .await
        .expect_err("FETCH after CLOSE must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

    handle.abort();
}

/// `SCROLL`, `BINARY`, `WITH HOLD` all rejected at parse time.
#[tokio::test]
async fn cursor_unsupported_modifiers_rejected() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    for sql in [
        "DECLARE c SCROLL CURSOR FOR SUBSCRIBE prices",
        "DECLARE c BINARY CURSOR FOR SUBSCRIBE prices",
        "DECLARE c CURSOR WITH HOLD FOR SUBSCRIBE prices",
        "DECLARE c INSENSITIVE CURSOR FOR SUBSCRIBE prices",
    ] {
        let err = client
            .simple_query(sql)
            .await
            .expect_err(&format!("{sql} must fail"));
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(
            db_err.code().code(),
            "42601",
            "{sql}: expected parse error, got {db_err:?}"
        );
    }

    handle.abort();
}

/// `FETCH BACKWARD` and other reverse / absolute directions are rejected
/// because SUBSCRIBE is forward-only.
#[tokio::test]
async fn cursor_backward_directions_rejected() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    for sql in [
        "FETCH PRIOR FROM c",
        "FETCH BACKWARD 1 FROM c",
        "FETCH FIRST FROM c",
        "FETCH LAST FROM c",
        "FETCH ABSOLUTE 1 FROM c",
        "FETCH RELATIVE 1 FROM c",
    ] {
        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");
        let err = client
            .simple_query(sql)
            .await
            .expect_err(&format!("{sql} must fail"));
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "0A000", "{sql}: got {db_err:?}");
        client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    }
    handle.abort();
}

/// `DECLARE … CURSOR FOR <SELECT …>` (regular query, not SUBSCRIBE) is
/// not supported on pgwire.
#[tokio::test]
async fn cursor_for_non_subscribe_rejected() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    let err = client
        .simple_query("DECLARE c CURSOR FOR SELECT 1")
        .await
        .expect_err("DECLARE FOR SELECT must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "0A000", "got {db_err:?}");

    handle.abort();
}

/// FETCH against a name we never declared returns 34000 (invalid_cursor_name).
#[tokio::test]
async fn cursor_fetch_unknown_name_errors() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    let err = client
        .simple_query("FETCH 1 FROM nope")
        .await
        .expect_err("must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

    handle.abort();
}

/// A multi-row batch with `FETCH 1` repeated must return each row in
/// order — leftover rows persist on the cursor instead of being dropped
/// when the response stream ends. With the bug, `FETCH 1` would consume
/// the batch internally, return row[0], and discard row[1].
#[tokio::test]
async fn cursor_fetch_preserves_leftover_rows_in_one_batch() {
    let (db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;
    let mut publication_probe = db
        .open_subscription("prices", None, SubscribeStart::Tail)
        .await
        .expect("open publication probe");

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");

    let src = db.source_untyped("trades").expect("source");
    let batch = arrow_array::RecordBatch::try_new(
        src.schema().clone(),
        vec![
            Arc::new(arrow_array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow_array::Float64Array::from(vec![1.0, 2.0])),
        ],
    )
    .expect("batch");
    src.push_arrow(batch).expect("push");
    wait_for_published_rows(&mut publication_probe, 2).await;

    let first = client
        .simple_query("FETCH 1 FROM c")
        .await
        .expect("FETCH 1");
    let r1: Vec<&str> = first
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0),
            _ => None,
        })
        .collect();
    assert_eq!(r1, vec!["AAPL"]);

    let second = client
        .simple_query("FETCH 1 FROM c")
        .await
        .expect("FETCH 1");
    let r2: Vec<&str> = second
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0),
            _ => None,
        })
        .collect();
    assert_eq!(r2, vec!["GOOG"]);

    client.simple_query("CLOSE c").await.expect("CLOSE");
    client.simple_query("COMMIT").await.expect("COMMIT");
    handle.abort();
}

/// Re-DECLAREing an open cursor name returns 42P03; user must CLOSE first.
#[tokio::test]
async fn cursor_duplicate_declare_rejected() {
    let (_db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("first DECLARE");

    let err = client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect_err("duplicate DECLARE must fail");
    let db_err = err.as_db_error().expect("typed PG error");
    assert_eq!(db_err.code().code(), "42P03", "got {db_err:?}");

    client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("re-DECLARE after transaction rollback");
    client.simple_query("CLOSE c").await.expect("CLOSE");
    client.simple_query("COMMIT").await.expect("COMMIT");

    handle.abort();
}

/// Cursor name lookup is case-insensitive (PG identifier folding rules).
#[tokio::test]
async fn cursor_name_case_insensitive() {
    let (db, addr, handle) = spawn_with_data().await;
    let client = connect(addr).await;
    let mut publication_probe = db
        .open_subscription("prices", None, SubscribeStart::Tail)
        .await
        .expect("open publication probe");

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DECLARE MyCursor CURSOR FOR SUBSCRIBE prices")
        .await
        .expect("DECLARE");

    push_one_trade(&db, "AAPL", 1.0).await;
    wait_for_published_rows(&mut publication_probe, 1).await;

    let messages = client
        .simple_query("FETCH 1 FROM mycursor")
        .await
        .expect("FETCH from lowercased name");
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 1);

    client.simple_query("CLOSE MYCURSOR").await.expect("CLOSE");
    client.simple_query("COMMIT").await.expect("COMMIT");
    handle.abort();
}
