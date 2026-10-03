//! Public write boundaries; runtime admission is covered by DB actors and the real-process soak.

use super::*;

fn post(path: &str, token: Option<&str>, body: String) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    request.body(Body::from(body)).unwrap()
}

#[tokio::test]
async fn topology_writes_require_console_authorization_and_report_noncluster() {
    for path in [
        "/api/v1/cluster/topology/operations",
        "/api/v1/cluster/topology/adopt",
    ] {
        let app = build_router(test_state_with_token("topology-console"));
        for token in [None, Some("wrong-token")] {
            let response = app
                .clone()
                .oneshot(post(path, token, "{}".into()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        let response = app
            .oneshot(post(path, Some("topology-console"), "{}".into()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn topology_writes_reject_malformed_identity_unknown_fields_and_oversized_bodies() {
    let console = canonical_auth_token(53);
    let diagnostic = canonical_auth_token(54);
    let fixture = local_evidence_fixture_with_auth(
        Some(&console),
        Some(&diagnostic),
        ready_serving_gate(),
        false,
    )
    .await;
    let app = build_router(fixture.state);
    let path = "/api/v1/cluster/topology/operations";
    let valid = serde_json::json!({
        "operation_id": uuid::Uuid::from_u128(1301),
        "expected_parent_version": 1,
        "statements": ["CREATE STREAM future AS SELECT * FROM trades"]
    });
    assert_eq!(
        app.clone()
            .oneshot(post(path, Some(&diagnostic), valid.to_string()))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let mut invalid = Vec::new();
    let mut nil = valid.clone();
    nil["operation_id"] = serde_json::json!(uuid::Uuid::nil());
    invalid.push(nil);
    let mut zero = valid.clone();
    zero["expected_parent_version"] = serde_json::json!(0);
    invalid.push(zero);
    let mut unknown = valid.clone();
    unknown["bootstrap"] = serde_json::json!(true);
    invalid.push(unknown);
    for body in invalid {
        let response = app
            .clone()
            .oneshot(post(path, Some(&console), body.to_string()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    for (path, limit) in [
        (path, 512 * 1024),
        ("/api/v1/cluster/topology/adopt", 16 * 1024),
    ] {
        let response = app
            .clone()
            .oneshot(post(path, Some(&console), " ".repeat(limit + 1)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
    for value in ["0", "45000000001", "invalid"] {
        let mut request = post(path, Some(&console), valid.to_string());
        request
            .headers_mut()
            .insert("x-laminar-topology-budget-nanos", value.parse().unwrap());
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn topology_uncertain_sql_errors_preserve_the_queryable_operation_identity() {
    use laminar_core::cluster::control::TopologyError;
    let operation_id = uuid::Uuid::from_u128(1305).try_into().unwrap();
    let response = super::super::topology_submission::topology_write_error(
        laminar_db::DbError::TopologySubmission {
            operation_id,
            source: Box::new(TopologyError::Contended.into()),
        },
    );
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        response.headers()["x-laminar-topology-operation-id"],
        uuid::Uuid::from_u128(1305).to_string()
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["retry-after"], "1");
    let bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body["code"],
        laminar_core::error_codes::TOPOLOGY_AUTHORITY_CONTENDED
    );
}
