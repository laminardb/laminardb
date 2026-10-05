use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn peer(
    status: &str,
    body: &str,
    length: Option<usize>,
) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", length.unwrap_or(body.len()));
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count != 0);
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() < 8192);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&request[..end]).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        socket.write_all(response.as_bytes()).await.unwrap();
        request
    });
    (address, task)
}

#[tokio::test]
async fn topology_forwarding_preserves_authorization_identity_payload_and_remaining_budget() {
    let db = LaminarDB::open_with_config(crate::LaminarConfig {
        http_auth_token: Some(crate::config::SecretString::new("forward-test-token")),
        ..Default::default()
    })
    .unwrap();
    let request = crate::ClusterTopologyRequest {
        operation_id: uuid::Uuid::from_u128(1306).try_into().unwrap(),
        expected_parent_version: laminar_core::cluster::control::TopologyVersion::new(7).unwrap(),
        statements: vec!["CREATE STREAM future AS SELECT * FROM trades".into()],
    };
    let receipt = serde_json::json!({"operation_id": request.operation_id});
    let (address, task) = peer("202 Accepted", &receipt.to_string(), None).await;
    let actual: serde_json::Value = db
        .forward_topology_request(
            &address,
            "/api/v1/cluster/topology/operations",
            &request,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(actual, receipt);
    let captured = task.await.unwrap();
    let end = captured
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(&captured[..end])
        .unwrap()
        .to_ascii_lowercase();
    assert!(headers.starts_with("post /api/v1/cluster/topology/operations http/1.1\r\n"));
    assert!(headers.contains("authorization: bearer forward-test-token\r\n"));
    let budget: u64 = headers
        .lines()
        .find_map(|line| line.strip_prefix("x-laminar-topology-budget-nanos: "))
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=5_000_000_000).contains(&budget));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured[end + 4..]).unwrap(),
        serde_json::to_value(request).unwrap()
    );
}

#[tokio::test]
async fn topology_forwarding_keeps_typed_fences_and_rejects_response_bounds_and_redirects() {
    let db = LaminarDB::open().unwrap();
    for (status, body, length, fenced) in [
        ("503 Service Unavailable", serde_json::json!({"error":"stale receiver", "code":laminar_core::error_codes::TOPOLOGY_FENCED}).to_string(), None, true),
        ("202 Accepted", "{}".to_owned(), Some(MAX_FORWARDED_RESPONSE_BYTES + 1), false),
        ("302 Found", "{}".to_owned(), None, false),
    ] {
        let (address, task) = peer(status, &body, length).await;
        let result = db.forward_topology_request::<_, serde_json::Value>(&address,
            "/api/v1/cluster/topology/operations", &serde_json::json!({}),
            tokio::time::Instant::now() + std::time::Duration::from_secs(5)).await;
        assert!(result.is_err());
        if fenced { assert!(matches!(result, Err(DbError::Topology(TopologyError::Fenced)))); }
        else if length.is_some() { assert!(matches!(result, Err(DbError::Topology(TopologyError::Invalid(_))))); }
        task.await.unwrap();
    }
    for address in ["user@127.0.0.1:1", "127.0.0.1:1/path", "127.0.0.1:1?query"] {
        assert!(matches!(
            db.forward_topology_request::<_, serde_json::Value>(
                address,
                "/api/v1/cluster/topology/operations",
                &serde_json::json!({}),
                tokio::time::Instant::now() + std::time::Duration::from_secs(5)
            )
            .await,
            Err(DbError::Topology(TopologyError::Protocol(_)))
        ));
    }
}
