use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use delta_reqwest::header::{HeaderName, HeaderValue};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{IcebergCatalogConfig, IcebergStorageConfig};

use super::{
    rest_properties, storage_factory, validate_storage_options, BuiltCatalog, CatalogAccess,
    CatalogCapabilities,
};

mod facade;
use facade::{RestCatalogFacade, RestCommitTransport};
mod oauth;
pub(super) use oauth::RestAuthentication;
use oauth::{OAuthCatalogState, RestCatalogTemplate};

const MAX_CONFIG_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_CONFIG_ENDPOINTS: usize = 512;
const MAX_CONFIG_ENDPOINT_BYTES: usize = 1024;
const LOAD_TABLE_ENDPOINT: &str = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}";
const COMMIT_TABLE_ENDPOINT: &str = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}";
const CREATE_NAMESPACE_ENDPOINT: &str = "POST /v1/{prefix}/namespaces";
const CREATE_TABLE_ENDPOINT: &str = "POST /v1/{prefix}/namespaces/{namespace}/tables";
const NAMESPACE_EXISTS_ENDPOINT: &str = "HEAD /v1/{prefix}/namespaces/{namespace}";
const TABLE_EXISTS_ENDPOINT: &str = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}";

#[derive(Deserialize)]
struct RestConfigResponse {
    #[serde(default)]
    defaults: HashMap<String, String>,
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    endpoints: Option<Vec<String>>,
    #[serde(default, rename = "idempotency-key-lifetime")]
    idempotency_key_lifetime: Option<String>,
}

struct RestDiscovery {
    capabilities: CatalogCapabilities,
    effective_uri: String,
    effective_properties: HashMap<String, String>,
}

pub(super) async fn build(
    config: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
    access: CatalogAccess,
    credential_refresh_failures: Option<prometheus::IntCounter>,
) -> Result<BuiltCatalog, ConnectorError> {
    super::super::iceberg::capabilities::validate_catalog_session(config)?;
    validate_storage_options(&config.warehouse, storage)?;
    let factory = storage_factory(&config.warehouse, storage)?;
    let properties = rest_properties(config, storage)?;
    validate_rest_properties(&properties)?;
    let client = http_client(config)?;
    let authentication = RestAuthentication::initialize(
        config,
        client.clone(),
        &properties,
        credential_refresh_failures,
    )
    .await?;
    let discovered = discover(config, &client, &properties, access, &authentication).await?;
    let catalog = match authentication.oauth_session() {
        Some(session) => {
            let template = RestCatalogTemplate::new(config, factory, client, properties);
            Arc::new(RestCatalogFacade::oauth(
                OAuthCatalogState::new(session, template).await?,
            )) as Arc<dyn Catalog>
        }
        None => build_inner(config, factory, client, properties).await?,
    };
    Ok(BuiltCatalog {
        catalog,
        capabilities: discovered.capabilities,
        session: super::CatalogSession {
            rest_authentication: Some(authentication),
        },
    })
}

pub(super) async fn build_publication(
    catalog: Arc<dyn Catalog>,
    config: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
    session: &super::CatalogSession,
    idempotency_key: uuid::Uuid,
) -> Result<Arc<dyn Catalog>, ConnectorError> {
    if idempotency_key.get_version_num() != 7 {
        return Err(ConnectorError::Internal(
            "Iceberg REST idempotency key is not UUIDv7".into(),
        ));
    }
    super::super::iceberg::capabilities::validate_catalog_session(config)?;
    validate_storage_options(&config.warehouse, storage)?;
    let properties = rest_properties(config, storage)?;
    validate_rest_properties(&properties)?;
    let client = http_client(config)?;
    let authentication =
        session
            .rest_authentication
            .clone()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg REST catalog session".into(),
                actual: "REST authentication session is absent".into(),
            })?;
    if !authentication.matches(config.auth_type) {
        return Err(ConnectorError::InvalidState {
            expected: "Iceberg REST authentication bound during open".into(),
            actual: "publication authentication mode changed".into(),
        });
    }
    let discovered = discover(
        config,
        &client,
        &properties,
        CatalogAccess::Write { auto_create: false },
        &authentication,
    )
    .await?;
    if discovered.capabilities.idempotency_key_lifetime.is_none() {
        return Err(ConnectorError::FeatureUnsupported(
            "iceberg.catalog.rest.idempotency: server no longer advertises idempotency-key-lifetime"
                .into(),
        ));
    }
    Ok(Arc::new(RestCatalogFacade::publication(
        catalog,
        RestCommitTransport::new(
            client,
            authentication,
            discovered.effective_uri,
            discovered.effective_properties,
            config.request_timeout,
            idempotency_key,
        ),
    )))
}

fn http_client(config: &IcebergCatalogConfig) -> Result<delta_reqwest::Client, ConnectorError> {
    delta_reqwest::Client::builder()
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout)
        .build()
        .map_err(|_| {
            ConnectorError::ConfigurationError(
                "Iceberg REST HTTP client configuration is invalid".into(),
            )
        })
}

async fn build_inner(
    config: &IcebergCatalogConfig,
    factory: Arc<dyn iceberg::io::StorageFactory>,
    client: delta_reqwest::Client,
    mut properties: HashMap<String, String>,
) -> Result<Arc<dyn Catalog>, ConnectorError> {
    properties.insert("uri".into(), config.catalog_uri.clone());
    properties.insert("warehouse".into(), config.warehouse.clone());
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(factory)
        .with_client(client)
        .load("laminardb", properties)
        .await
        .map_err(|error| {
            ConnectorError::ConnectionFailed(format!(
                "Iceberg catalog initialization failed ({})",
                super::external_error_summary(&error)
            ))
        })?;
    Ok(Arc::new(RestCatalogFacade::read(Arc::new(catalog))))
}

async fn discover(
    config: &IcebergCatalogConfig,
    client: &delta_reqwest::Client,
    properties: &HashMap<String, String>,
    access: CatalogAccess,
    authentication: &RestAuthentication,
) -> Result<RestDiscovery, ConnectorError> {
    let endpoint = format!("{}/v1/config", config.catalog_uri.trim_end_matches('/'));
    let mut request = client
        .get(endpoint)
        .query(&[("warehouse", config.warehouse.as_str())]);
    request = apply_request_properties(request, authentication, properties, config.request_timeout)
        .await?;
    let response = request.send().await.map_err(|_| {
        ConnectorError::ConnectionFailed(
            "Iceberg REST /v1/config request failed before catalog initialization".into(),
        )
    })?;
    if response.status() != delta_reqwest::StatusCode::OK {
        return Err(ConnectorError::ConnectionFailed(format!(
            "Iceberg REST /v1/config returned HTTP {}",
            response.status().as_u16()
        )));
    }
    let response: RestConfigResponse = read_bounded_json(response).await?;
    validate_server_properties(&response.defaults)?;
    validate_server_properties(&response.overrides)?;
    validate_endpoints(response.endpoints.as_deref(), access)?;
    let idempotency_key_lifetime = response
        .idempotency_key_lifetime
        .as_deref()
        .map(parse_idempotency_lifetime)
        .transpose()?;
    let effective_uri = response
        .overrides
        .get("uri")
        .cloned()
        .unwrap_or_else(|| config.catalog_uri.clone());
    if crate::security::value_contains_uri_secret(&effective_uri, false) {
        return Err(ConnectorError::FeatureUnsupported(
            "iceberg.catalog.rest.uri: server override embeds credential material".into(),
        ));
    }
    let mut effective_properties = response.defaults;
    effective_properties.extend(properties.clone());
    effective_properties.extend(response.overrides);
    effective_properties.remove("uri");
    validate_request_properties(&effective_properties)?;
    authentication.update_from_discovery(&effective_uri, &effective_properties)?;
    effective_properties.remove("credential");
    effective_properties.remove("token");
    Ok(RestDiscovery {
        capabilities: CatalogCapabilities {
            idempotency_key_lifetime,
        },
        effective_uri,
        effective_properties,
    })
}

async fn apply_request_properties(
    mut request: delta_reqwest::RequestBuilder,
    authentication: &RestAuthentication,
    properties: &HashMap<String, String>,
    required_validity: Duration,
) -> Result<delta_reqwest::RequestBuilder, ConnectorError> {
    for (name, value) in configured_headers(properties)? {
        request = request.header(name, value);
    }
    authentication.apply(request, required_validity).await
}

fn validate_request_properties(properties: &HashMap<String, String>) -> Result<(), ConnectorError> {
    let _ = configured_headers(properties)?;
    Ok(())
}

fn configured_headers(
    properties: &HashMap<String, String>,
) -> Result<Vec<(HeaderName, HeaderValue)>, ConnectorError> {
    let mut headers = Vec::new();
    for (key, value) in properties.iter().filter_map(|(key, value)| {
        key.strip_prefix("header.")
            .map(|name| (name, value.as_str()))
    }) {
        let name = HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
            ConnectorError::ConfigurationError(
                "Iceberg REST catalog contains an invalid custom header name".into(),
            )
        })?;
        let value = HeaderValue::from_str(value).map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "Iceberg REST catalog header '{key}' has an invalid value"
            ))
        })?;
        headers.push((name, value));
    }
    Ok(headers)
}

pub(super) fn bearer_token(properties: &HashMap<String, String>) -> Result<&str, ConnectorError> {
    properties.get("token").map(String::as_str).ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "catalog.auth.type=bearer requires a resolved catalog.property.token".into(),
        )
    })
}

async fn read_bounded_json<T: DeserializeOwned>(
    mut response: delta_reqwest::Response,
) -> Result<T, ConnectorError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        ConnectorError::ConnectionFailed(
            "Iceberg REST /v1/config response body could not be read".into(),
        )
    })? {
        let next_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
            ConnectorError::ConnectionFailed(
                "Iceberg REST /v1/config response size overflow".into(),
            )
        })?;
        if next_len > MAX_CONFIG_RESPONSE_BYTES {
            return Err(ConnectorError::ConnectionFailed(format!(
                "Iceberg REST /v1/config exceeds the {MAX_CONFIG_RESPONSE_BYTES}-byte limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| {
        ConnectorError::ConnectionFailed(
            "Iceberg REST /v1/config returned an invalid configuration document".into(),
        )
    })
}

fn validate_rest_properties(properties: &HashMap<String, String>) -> Result<(), ConnectorError> {
    for (key, value) in properties {
        let normalized = key.to_ascii_lowercase();
        if normalized == "disable-header-redaction" && value.eq_ignore_ascii_case("true") {
            return Err(ConnectorError::ConfigurationError(
                "catalog.property.disable-header-redaction=true is prohibited".into(),
            ));
        }
        if normalized == "header.idempotency-key" {
            return Err(ConnectorError::ConfigurationError(
                "catalog.property.header.Idempotency-Key is managed by coordinated publication"
                    .into(),
            ));
        }
        if normalized == "header.authorization" {
            return Err(ConnectorError::ConfigurationError(
                "catalog.property.header.Authorization must use catalog.auth.type instead".into(),
            ));
        }
        if normalized == "header.x-iceberg-access-delegation" {
            return Err(ConnectorError::FeatureUnsupported(
                "iceberg.catalog.rest.access-delegation: custom delegation headers are unsupported"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn validate_server_properties(properties: &HashMap<String, String>) -> Result<(), ConnectorError> {
    for (key, value) in properties {
        let normalized = key.to_ascii_lowercase();
        if normalized == "disable-header-redaction" && value.eq_ignore_ascii_case("true") {
            return Err(ConnectorError::FeatureUnsupported(
                "iceberg.catalog.rest.header-redaction: server configuration attempts to disable credential redaction"
                    .into(),
            ));
        }
        if normalized == "header.idempotency-key" {
            return Err(ConnectorError::FeatureUnsupported(
                "iceberg.catalog.rest.idempotency: server configuration supplies a catalog-wide idempotency key"
                    .into(),
            ));
        }
        if normalized == "header.authorization" {
            return Err(ConnectorError::FeatureUnsupported(
                "iceberg.catalog.rest.authentication: server configuration supplies an Authorization header"
                    .into(),
            ));
        }
        if matches!(normalized.as_str(), "credential" | "token") {
            return Err(ConnectorError::FeatureUnsupported(
                "iceberg.catalog.rest.authentication: server configuration supplies catalog credentials"
                    .into(),
            ));
        }
        if normalized == "header.x-iceberg-access-delegation" {
            return Err(ConnectorError::FeatureUnsupported(
                "iceberg.catalog.rest.access-delegation: server configuration enables unsupported credential delegation"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn validate_endpoints(
    endpoints: Option<&[String]>,
    access: CatalogAccess,
) -> Result<(), ConnectorError> {
    let Some(endpoints) = endpoints else {
        return Ok(());
    };
    if endpoints.len() > MAX_CONFIG_ENDPOINTS {
        return Err(ConnectorError::ConnectionFailed(format!(
            "Iceberg REST /v1/config exceeds the {MAX_CONFIG_ENDPOINTS}-endpoint limit"
        )));
    }
    let mut normalized = HashSet::with_capacity(endpoints.len());
    for endpoint in endpoints {
        if endpoint.len() > MAX_CONFIG_ENDPOINT_BYTES {
            return Err(ConnectorError::ConnectionFailed(format!(
                "Iceberg REST /v1/config endpoint exceeds the {MAX_CONFIG_ENDPOINT_BYTES}-byte limit"
            )));
        }
        normalized.insert(endpoint.split_whitespace().collect::<Vec<_>>().join(" "));
    }
    for required in access.required_endpoints() {
        if !normalized.contains(*required) {
            return Err(ConnectorError::FeatureUnsupported(format!(
                "iceberg.catalog.rest.endpoint: server does not advertise '{required}'"
            )));
        }
    }
    Ok(())
}

fn parse_idempotency_lifetime(value: &str) -> Result<Duration, ConnectorError> {
    if value.len() > 64 || !value.starts_with('P') {
        return Err(invalid_idempotency_lifetime());
    }
    let span: jiff::Span = value.parse().map_err(|_| invalid_idempotency_lifetime())?;
    if span.get_years() != 0 || span.get_months() != 0 {
        return Err(invalid_idempotency_lifetime());
    }
    let fields = [
        i128::from(span.get_weeks()),
        i128::from(span.get_days()),
        i128::from(span.get_hours()),
        i128::from(span.get_minutes()),
        i128::from(span.get_seconds()),
        i128::from(span.get_milliseconds()),
        i128::from(span.get_microseconds()),
        i128::from(span.get_nanoseconds()),
    ];
    if fields.iter().any(|value| *value < 0) {
        return Err(invalid_idempotency_lifetime());
    }
    let multipliers = [
        7 * 24 * 60 * 60 * 1_000_000_000,
        24 * 60 * 60 * 1_000_000_000,
        60 * 60 * 1_000_000_000,
        60 * 1_000_000_000,
        1_000_000_000,
        1_000_000,
        1_000,
        1,
    ];
    let mut nanoseconds = 0_i128;
    for (field, multiplier) in fields.into_iter().zip(multipliers) {
        nanoseconds = nanoseconds
            .checked_add(
                field
                    .checked_mul(multiplier)
                    .ok_or_else(invalid_idempotency_lifetime)?,
            )
            .ok_or_else(invalid_idempotency_lifetime)?;
    }
    if nanoseconds == 0 {
        return Err(invalid_idempotency_lifetime());
    }
    let seconds =
        u64::try_from(nanoseconds / 1_000_000_000).map_err(|_| invalid_idempotency_lifetime())?;
    let subsecond =
        u32::try_from(nanoseconds % 1_000_000_000).map_err(|_| invalid_idempotency_lifetime())?;
    Ok(Duration::new(seconds, subsecond))
}

fn invalid_idempotency_lifetime() -> ConnectorError {
    ConnectorError::ConnectionFailed(
        "Iceberg REST /v1/config contains an invalid idempotency-key-lifetime".into(),
    )
}

impl CatalogAccess {
    fn required_endpoints(self) -> &'static [&'static str] {
        match self {
            Self::Read => &[LOAD_TABLE_ENDPOINT],
            Self::Write { auto_create: false } => &[LOAD_TABLE_ENDPOINT, COMMIT_TABLE_ENDPOINT],
            Self::Write { auto_create: true } => &[
                LOAD_TABLE_ENDPOINT,
                COMMIT_TABLE_ENDPOINT,
                NAMESPACE_EXISTS_ENDPOINT,
                CREATE_NAMESPACE_ENDPOINT,
                TABLE_EXISTS_ENDPOINT,
                CREATE_TABLE_ENDPOINT,
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::ErrorKind;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::config::ConnectorConfig;
    use crate::lakehouse::iceberg_config::IcebergCatalogConfig;

    use super::*;

    fn catalog_config(uri: &str) -> IcebergCatalogConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", uri);
        config.set("catalog.warehouse", "warehouse");
        config.set("namespace", "test");
        config.set("table.name", "events");
        config.set("catalog.auth.type", "bearer");
        config.set("catalog.property.token", "catalog-secret");
        IcebergCatalogConfig::from_config(&config).unwrap()
    }

    fn oauth_catalog_config(uri: &str) -> IcebergCatalogConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", uri);
        config.set("catalog.warehouse", "warehouse");
        config.set("namespace", "test");
        config.set("table.name", "events");
        config.set("catalog.auth.type", "oauth2");
        config.set("catalog.oauth2.server_uri", format!("{uri}/tokens"));
        config.set("catalog.oauth2.client_id", "laminar-client");
        config.set("catalog.property.credential", "catalog-client-secret");
        config.set("catalog.request_timeout", "1s");
        IcebergCatalogConfig::from_config(&config).unwrap()
    }

    #[test]
    fn parses_fixed_iso_idempotency_lifetimes() {
        assert_eq!(
            parse_idempotency_lifetime("PT30M").unwrap(),
            Duration::from_secs(30 * 60)
        );
        assert_eq!(
            parse_idempotency_lifetime("P1DT2H3M4.5S").unwrap(),
            Duration::from_millis(((26 * 60 + 3) * 60 + 4) * 1000 + 500)
        );
        for invalid in ["", "30m", "P0D", "-PT1M", "P1M", "P1Y"] {
            assert!(parse_idempotency_lifetime(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn explicit_endpoint_lists_fail_closed() {
        let endpoints = vec![LOAD_TABLE_ENDPOINT.to_string()];
        validate_endpoints(Some(&endpoints), CatalogAccess::Read).unwrap();
        let error = validate_endpoints(
            Some(&endpoints),
            CatalogAccess::Write { auto_create: false },
        )
        .unwrap_err();
        assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
        assert!(error.to_string().contains(COMMIT_TABLE_ENDPOINT));
    }

    #[test]
    fn server_configuration_cannot_replace_catalog_credentials() {
        for property in ["token", "credential", "header.Authorization"] {
            let properties = HashMap::from([(property.into(), "server-secret".into())]);
            let error = validate_server_properties(&properties).unwrap_err();
            assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
            assert!(!error.to_string().contains("server-secret"));
        }
    }

    #[tokio::test]
    async fn discovery_authenticates_and_reads_capabilities() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(query_param("warehouse", "warehouse"))
            .and(header("authorization", "Bearer catalog-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "defaults": {},
                "overrides": {},
                "idempotency-key-lifetime": "PT30M",
                "endpoints": [LOAD_TABLE_ENDPOINT]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let config = catalog_config(&server.uri());
        let client = http_client(&config).unwrap();
        let properties = rest_properties(
            &config,
            &IcebergStorageConfig::from_config(&ConnectorConfig::new("iceberg")).unwrap(),
        )
        .unwrap();
        let authentication =
            RestAuthentication::initialize(&config, client.clone(), &properties, None)
                .await
                .unwrap();
        let discovered = discover(
            &config,
            &client,
            &properties,
            CatalogAccess::Read,
            &authentication,
        )
        .await
        .unwrap();
        assert_eq!(
            discovered.capabilities.idempotency_key_lifetime,
            Some(Duration::from_secs(30 * 60))
        );
    }

    #[tokio::test]
    async fn idempotency_key_is_scoped_to_the_commit_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(header("authorization", "Bearer catalog-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "defaults": {},
                "overrides": {"prefix": "tenant"},
                "idempotency-key-lifetime": "PT30M",
                "endpoints": [LOAD_TABLE_ENDPOINT, COMMIT_TABLE_ENDPOINT]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let idempotency_key = uuid::Uuid::now_v7();
        Mock::given(method("POST"))
            .and(path("/v1/tenant/namespaces/test/tables/events"))
            .and(header("authorization", "Bearer catalog-secret"))
            .and(header("idempotency-key", idempotency_key.to_string()))
            .respond_with(ResponseTemplate::new(409))
            .expect(1)
            .mount(&server)
            .await;

        let config = catalog_config(&server.uri());
        let storage = IcebergStorageConfig::from_config(&ConnectorConfig::new("iceberg")).unwrap();
        let properties = rest_properties(&config, &storage).unwrap();
        let client = http_client(&config).unwrap();
        let authentication =
            RestAuthentication::initialize(&config, client.clone(), &properties, None)
                .await
                .unwrap();
        let discovered = discover(
            &config,
            &client,
            &properties,
            CatalogAccess::Write { auto_create: false },
            &authentication,
        )
        .await
        .unwrap();
        let fixture = super::super::super::iceberg::test_support::create_test_table(false).await;
        let catalog = RestCatalogFacade::publication(
            fixture.catalog,
            RestCommitTransport::new(
                client,
                authentication,
                discovered.effective_uri,
                discovered.effective_properties,
                config.request_timeout,
                idempotency_key,
            ),
        );
        let transaction = Transaction::new(&fixture.table);
        let transaction = transaction
            .update_table_properties()
            .set("test".into(), "value".into())
            .apply(transaction)
            .unwrap();
        let update_started = AtomicBool::new(false);
        let single_dispatch =
            super::super::SingleDispatchCatalog::new(&catalog, &fixture.table, &update_started);
        let error = transaction.commit(&single_dispatch).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);

        let requests = server.received_requests().await.unwrap();
        let config_requests = requests
            .iter()
            .filter(|request| request.url.path() == "/v1/config")
            .collect::<Vec<_>>();
        assert_eq!(config_requests.len(), 1);
        assert!(!config_requests[0].headers.contains_key("idempotency-key"));
    }

    #[tokio::test]
    async fn oauth_rejection_is_not_retried_after_commit_dispatch() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tokens"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "short-lived-catalog-token",
                "token_type": "Bearer",
                "expires_in": 60
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(header("authorization", "Bearer short-lived-catalog-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "defaults": {},
                "overrides": {},
                "idempotency-key-lifetime": "PT30M",
                "endpoints": [LOAD_TABLE_ENDPOINT, COMMIT_TABLE_ENDPOINT]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let idempotency_key = uuid::Uuid::now_v7();
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/test/tables/events"))
            .and(header("authorization", "Bearer short-lived-catalog-token"))
            .and(header("idempotency-key", idempotency_key.to_string()))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        let config = oauth_catalog_config(&server.uri());
        let storage = IcebergStorageConfig::from_config(&ConnectorConfig::new("iceberg")).unwrap();
        let properties = rest_properties(&config, &storage).unwrap();
        let client = http_client(&config).unwrap();
        let authentication =
            RestAuthentication::initialize(&config, client.clone(), &properties, None)
                .await
                .unwrap();
        let discovered = discover(
            &config,
            &client,
            &properties,
            CatalogAccess::Write { auto_create: false },
            &authentication,
        )
        .await
        .unwrap();
        let fixture = super::super::super::iceberg::test_support::create_test_table(false).await;
        let catalog = RestCatalogFacade::publication(
            fixture.catalog,
            RestCommitTransport::new(
                client,
                authentication,
                discovered.effective_uri,
                discovered.effective_properties,
                config.request_timeout,
                idempotency_key,
            ),
        );
        let transaction = Transaction::new(&fixture.table);
        let transaction = transaction
            .update_table_properties()
            .set("test".into(), "value".into())
            .apply(transaction)
            .unwrap();
        let update_started = AtomicBool::new(false);
        let single_dispatch =
            super::super::SingleDispatchCatalog::new(&catalog, &fixture.table, &update_started);
        let error = transaction.commit(&single_dispatch).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        let error = error.to_string();
        assert!(!error.contains("short-lived-catalog-token"));
        assert!(!error.contains("catalog-client-secret"));
    }

    #[tokio::test]
    async fn catalog_debug_never_contains_bearer_material() {
        let config = catalog_config("https://catalog.test");
        let mut properties = HashMap::from([
            ("uri".into(), config.catalog_uri.clone()),
            ("warehouse".into(), config.warehouse.clone()),
            ("token".into(), "catalog-secret".into()),
        ]);
        let catalog = RestCatalogBuilder::default()
            .load("laminardb", std::mem::take(&mut properties))
            .await
            .unwrap();
        let wrapped = RestCatalogFacade::read(Arc::new(catalog));
        let debug = format!("{wrapped:?}");
        assert!(!debug.contains("catalog-secret"));
        assert_eq!(debug, "RestCatalog { .. }");
    }
}
