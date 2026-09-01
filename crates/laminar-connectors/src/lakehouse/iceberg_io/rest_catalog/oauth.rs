use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use delta_reqwest::header::{HeaderName, HeaderValue};
use iceberg::io::StorageFactory;
use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind};
use iceberg_catalog_rest::RestCatalogBuilder;
use parking_lot::RwLock;
use prometheus::IntCounter;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{IcebergCatalogAuthType, IcebergCatalogConfig};

const MAX_TOKEN_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ACCESS_TOKEN_BYTES: usize = 16 * 1024;
const MAX_OAUTH_PARAMETER_BYTES: usize = 16 * 1024;
const TOKEN_EXPIRY_SAFETY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) enum RestAuthentication {
    None,
    Bearer(Arc<str>),
    OAuth2(Arc<OAuthSession>),
}

impl RestAuthentication {
    pub(super) async fn initialize(
        config: &IcebergCatalogConfig,
        client: delta_reqwest::Client,
        properties: &HashMap<String, String>,
        refresh_failures: Option<IntCounter>,
    ) -> Result<Self, ConnectorError> {
        match config.auth_type {
            IcebergCatalogAuthType::None => Ok(Self::None),
            IcebergCatalogAuthType::Bearer => {
                let token = super::bearer_token(properties)?;
                validate_access_token(token)?;
                Ok(Self::Bearer(Arc::from(token)))
            }
            IcebergCatalogAuthType::OAuth2 => Ok(Self::OAuth2(Arc::new(
                OAuthSession::initialize(config, client, properties, refresh_failures).await?,
            ))),
        }
    }

    pub(super) async fn apply(
        &self,
        request: delta_reqwest::RequestBuilder,
        required_validity: Duration,
    ) -> Result<delta_reqwest::RequestBuilder, ConnectorError> {
        match self {
            Self::None => Ok(request),
            Self::Bearer(token) => Ok(request.bearer_auth(token.as_ref())),
            Self::OAuth2(session) => {
                let lease = session.token(required_validity).await?;
                Ok(request.bearer_auth(lease.token.as_ref()))
            }
        }
    }

    pub(super) fn update_from_discovery(
        &self,
        catalog_uri: &str,
        properties: &HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        if let Self::OAuth2(session) = self {
            session.update_request_config(catalog_uri, properties)?;
        }
        Ok(())
    }

    pub(super) fn oauth_session(&self) -> Option<Arc<OAuthSession>> {
        match self {
            Self::OAuth2(session) => Some(Arc::clone(session)),
            Self::None | Self::Bearer(_) => None,
        }
    }

    pub(super) fn matches(&self, auth_type: IcebergCatalogAuthType) -> bool {
        matches!(
            (self, auth_type),
            (Self::None, IcebergCatalogAuthType::None)
                | (Self::Bearer(_), IcebergCatalogAuthType::Bearer)
                | (Self::OAuth2(_), IcebergCatalogAuthType::OAuth2)
        )
    }
}

impl fmt::Debug for RestAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "RestAuthentication::None",
            Self::Bearer(_) => "RestAuthentication::Bearer(<redacted>)",
            Self::OAuth2(_) => "RestAuthentication::OAuth2(<redacted>)",
        })
    }
}

pub(crate) struct OAuthSession {
    client: delta_reqwest::Client,
    credentials: OAuthCredentials,
    request_config: RwLock<OAuthRequestConfig>,
    token: RwLock<TokenState>,
    refresh: Mutex<()>,
    refresh_failures: Option<IntCounter>,
}

impl OAuthSession {
    async fn initialize(
        config: &IcebergCatalogConfig,
        client: delta_reqwest::Client,
        properties: &HashMap<String, String>,
        refresh_failures: Option<IntCounter>,
    ) -> Result<Self, ConnectorError> {
        let credentials = OAuthCredentials::parse(config)?;
        let request_config = OAuthRequestConfig::parse(config, properties)?;
        let response = exchange_token(&client, &credentials, &request_config).await?;
        let token = TokenState::from_response(response, 1)?;
        token.ensure_valid_for(config.request_timeout)?;
        Ok(Self {
            client,
            credentials,
            request_config: RwLock::new(request_config),
            token: RwLock::new(token),
            refresh: Mutex::new(()),
            refresh_failures,
        })
    }

    fn update_request_config(
        &self,
        catalog_uri: &str,
        properties: &HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let config = OAuthRequestConfig::parse_for_uri(catalog_uri, properties)?;
        *self.request_config.write() = config;
        Ok(())
    }

    async fn token(&self, required_validity: Duration) -> Result<TokenLease, ConnectorError> {
        if let Some(lease) = self.current_lease(required_validity) {
            return Ok(lease);
        }

        // INVARIANT: one task exchanges credentials; no catalog request holds this guard.
        let _refresh = self.refresh.lock().await;
        if let Some(lease) = self.current_lease(required_validity) {
            return Ok(lease);
        }
        let request_config = self.request_config.read().clone();
        let response = match exchange_token(&self.client, &self.credentials, &request_config).await
        {
            Ok(response) => response,
            Err(error) => {
                self.record_refresh_failure();
                return Err(error);
            }
        };
        let generation =
            self.token.read().generation.checked_add(1).ok_or_else(|| {
                ConnectorError::Internal("OAuth2 token generation overflow".into())
            })?;
        let next = match TokenState::from_response(response, generation) {
            Ok(next) => next,
            Err(error) => {
                self.record_refresh_failure();
                return Err(error);
            }
        };
        if let Err(error) = next.ensure_valid_for(required_validity) {
            self.record_refresh_failure();
            return Err(error);
        }
        let lease = next.lease();
        *self.token.write() = next;
        Ok(lease)
    }

    fn current_lease(&self, required_validity: Duration) -> Option<TokenLease> {
        let token = self.token.read();
        token.is_valid_for(required_validity).then(|| token.lease())
    }

    fn record_refresh_failure(&self) {
        if let Some(counter) = &self.refresh_failures {
            counter.inc();
        }
    }
}

impl fmt::Debug for OAuthSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthSession")
            .field("credentials", &"<redacted>")
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct OAuthCredentials {
    client_id: Option<String>,
    client_secret: Arc<str>,
}

impl OAuthCredentials {
    fn parse(config: &IcebergCatalogConfig) -> Result<Self, ConnectorError> {
        let credential = config
            .properties
            .get("credential")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "catalog.auth.type=oauth2 requires a resolved catalog.property.credential"
                        .into(),
                )
            })?;
        let (client_id, client_secret) = match &config.oauth2_client_id {
            Some(client_id) => {
                let secret = credential
                    .strip_prefix(client_id)
                    .and_then(|value| value.strip_prefix(':'))
                    .unwrap_or(credential);
                (Some(client_id.clone()), secret)
            }
            None => match credential.split_once(':') {
                Some((client_id, secret)) => (Some(client_id.to_string()), secret),
                None => (None, credential.as_str()),
            },
        };
        if client_id.as_deref().is_some_and(str::is_empty) || client_secret.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg REST OAuth2 client credentials must not be empty".into(),
            ));
        }
        if client_id
            .as_deref()
            .is_some_and(|value| value.len() > MAX_OAUTH_PARAMETER_BYTES)
            || client_secret.len() > MAX_OAUTH_PARAMETER_BYTES
        {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg REST OAuth2 client credentials exceed the configured size limit".into(),
            ));
        }
        Ok(Self {
            client_id,
            client_secret: Arc::from(client_secret),
        })
    }
}

#[derive(Clone)]
struct OAuthRequestConfig {
    token_endpoint: String,
    scope: String,
    audience: Option<String>,
    resource: Option<String>,
    headers: Vec<(HeaderName, HeaderValue)>,
}

impl OAuthRequestConfig {
    fn parse(
        config: &IcebergCatalogConfig,
        properties: &HashMap<String, String>,
    ) -> Result<Self, ConnectorError> {
        Self::parse_for_uri(&config.catalog_uri, properties)
    }

    fn parse_for_uri(
        catalog_uri: &str,
        properties: &HashMap<String, String>,
    ) -> Result<Self, ConnectorError> {
        let token_endpoint = properties
            .get("oauth2-server-uri")
            .cloned()
            .unwrap_or_else(|| format!("{}/v1/oauth/tokens", catalog_uri.trim_end_matches('/')));
        validate_token_endpoint(&token_endpoint)?;
        let scope = properties
            .get("scope")
            .cloned()
            .unwrap_or_else(|| "catalog".into());
        let audience = properties.get("audience").cloned();
        let resource = properties.get("resource").cloned();
        for value in [Some(&scope), audience.as_ref(), resource.as_ref()]
            .into_iter()
            .flatten()
        {
            if value.is_empty() || value.len() > MAX_OAUTH_PARAMETER_BYTES {
                return Err(ConnectorError::ConfigurationError(
                    "Iceberg REST OAuth2 parameters must be non-empty and bounded".into(),
                ));
            }
        }
        Ok(Self {
            token_endpoint,
            scope,
            audience,
            resource,
            headers: super::configured_headers(properties)?,
        })
    }
}

struct TokenState {
    token: Arc<str>,
    expires_at: Instant,
    generation: u64,
}

impl TokenState {
    fn from_response(response: TokenResponse, generation: u64) -> Result<Self, ConnectorError> {
        if !response.token_type.eq_ignore_ascii_case("bearer") {
            return Err(ConnectorError::ConnectionFailed(
                "Iceberg REST OAuth2 token endpoint returned an unsupported token type".into(),
            ));
        }
        validate_access_token(&response.access_token)?;
        let expires_in = response
            .expires_in
            .filter(|seconds| *seconds > 0)
            .ok_or_else(|| {
                ConnectorError::ConnectionFailed(
                    "Iceberg REST OAuth2 token response requires a positive expires_in".into(),
                )
            })?;
        let expires_at = Instant::now()
            .checked_add(Duration::from_secs(expires_in))
            .ok_or_else(|| {
                ConnectorError::ConnectionFailed(
                    "Iceberg REST OAuth2 token expiry exceeds the supported range".into(),
                )
            })?;
        Ok(Self {
            token: Arc::from(response.access_token),
            expires_at,
            generation,
        })
    }

    fn ensure_valid_for(&self, required: Duration) -> Result<(), ConnectorError> {
        if self.is_valid_for(required) {
            Ok(())
        } else {
            Err(ConnectorError::ConnectionFailed(
                "Iceberg REST OAuth2 token lifetime is shorter than catalog.request_timeout".into(),
            ))
        }
    }

    fn is_valid_for(&self, required: Duration) -> bool {
        let Some(horizon) = required.checked_add(TOKEN_EXPIRY_SAFETY) else {
            return false;
        };
        Instant::now()
            .checked_add(horizon)
            .is_some_and(|required_until| required_until < self.expires_at)
    }

    fn lease(&self) -> TokenLease {
        TokenLease {
            token: Arc::clone(&self.token),
            generation: self.generation,
        }
    }
}

pub(super) struct TokenLease {
    pub(super) token: Arc<str>,
    pub(super) generation: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: Option<u64>,
}

async fn exchange_token(
    client: &delta_reqwest::Client,
    credentials: &OAuthCredentials,
    config: &OAuthRequestConfig,
) -> Result<TokenResponse, ConnectorError> {
    let mut form = HashMap::from([
        ("grant_type", "client_credentials"),
        ("client_secret", credentials.client_secret.as_ref()),
        ("scope", config.scope.as_str()),
    ]);
    if let Some(client_id) = &credentials.client_id {
        form.insert("client_id", client_id);
    }
    if let Some(audience) = &config.audience {
        form.insert("audience", audience);
    }
    if let Some(resource) = &config.resource {
        form.insert("resource", resource);
    }
    let mut request = client.post(&config.token_endpoint).form(&form);
    for (name, value) in &config.headers {
        request = request.header(name, value);
    }
    request = request.header(
        delta_reqwest::header::CONTENT_TYPE,
        "application/x-www-form-urlencoded",
    );
    let mut response = request.send().await.map_err(|_| {
        ConnectorError::ConnectionFailed("Iceberg REST OAuth2 token request failed".into())
    })?;
    if response.status() != delta_reqwest::StatusCode::OK {
        return Err(ConnectorError::ConnectionFailed(format!(
            "Iceberg REST OAuth2 token endpoint returned HTTP {}",
            response.status().as_u16()
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        ConnectorError::ConnectionFailed(
            "Iceberg REST OAuth2 token response body could not be read".into(),
        )
    })? {
        let next_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
            ConnectorError::ConnectionFailed(
                "Iceberg REST OAuth2 token response size overflow".into(),
            )
        })?;
        if next_len > MAX_TOKEN_RESPONSE_BYTES {
            return Err(ConnectorError::ConnectionFailed(format!(
                "Iceberg REST OAuth2 token response exceeds the {MAX_TOKEN_RESPONSE_BYTES}-byte limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| {
        ConnectorError::ConnectionFailed(
            "Iceberg REST OAuth2 token endpoint returned an invalid response".into(),
        )
    })
}

fn validate_access_token(token: &str) -> Result<(), ConnectorError> {
    if token.is_empty()
        || token.len() > MAX_ACCESS_TOKEN_BYTES
        || HeaderValue::from_str(&format!("Bearer {token}")).is_err()
    {
        return Err(ConnectorError::ConnectionFailed(
            "Iceberg REST authentication returned an invalid bearer token".into(),
        ));
    }
    Ok(())
}

fn validate_token_endpoint(endpoint: &str) -> Result<(), ConnectorError> {
    if crate::security::value_contains_uri_secret(endpoint, false) {
        return Err(ConnectorError::ConfigurationError(
            "catalog.oauth2.server_uri must not embed credentials".into(),
        ));
    }
    let url = delta_reqwest::Url::parse(endpoint).map_err(|_| {
        ConnectorError::ConfigurationError(
            "catalog.oauth2.server_uri must be an absolute HTTP(S) URL".into(),
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ConnectorError::ConfigurationError(
            "catalog.oauth2.server_uri must be an absolute credential-free HTTP(S) URL".into(),
        ));
    }
    Ok(())
}

pub(super) struct OAuthCatalogState {
    session: Arc<OAuthSession>,
    template: RestCatalogTemplate,
    current: RwLock<CachedCatalog>,
}

impl OAuthCatalogState {
    pub(super) async fn new(
        session: Arc<OAuthSession>,
        template: RestCatalogTemplate,
    ) -> Result<Self, ConnectorError> {
        let lease = session.token(template.request_timeout).await?;
        let catalog = template.build(lease.token.as_ref()).await?;
        Ok(Self {
            session,
            template,
            current: RwLock::new(CachedCatalog {
                generation: lease.generation,
                catalog,
            }),
        })
    }

    pub(super) async fn current(&self) -> iceberg::Result<Arc<dyn Catalog>> {
        let lease = self
            .session
            .token(self.template.request_timeout)
            .await
            .map_err(|_| oauth_catalog_error())?;
        {
            let current = self.current.read();
            if current.generation == lease.generation {
                return Ok(Arc::clone(&current.catalog));
            }
        }
        let catalog = self
            .template
            .build(lease.token.as_ref())
            .await
            .map_err(|_| oauth_catalog_error())?;
        let mut current = self.current.write();
        if current.generation < lease.generation {
            *current = CachedCatalog {
                generation: lease.generation,
                catalog: Arc::clone(&catalog),
            };
            return Ok(catalog);
        }
        Ok(Arc::clone(&current.catalog))
    }
}

impl fmt::Debug for OAuthCatalogState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthCatalogState")
            .finish_non_exhaustive()
    }
}

pub(super) struct RestCatalogTemplate {
    catalog_uri: String,
    warehouse: String,
    factory: Arc<dyn StorageFactory>,
    client: delta_reqwest::Client,
    properties: HashMap<String, String>,
    request_timeout: Duration,
}

impl RestCatalogTemplate {
    pub(super) fn new(
        config: &IcebergCatalogConfig,
        factory: Arc<dyn StorageFactory>,
        client: delta_reqwest::Client,
        mut properties: HashMap<String, String>,
    ) -> Self {
        properties.remove("credential");
        properties.remove("token");
        Self {
            catalog_uri: config.catalog_uri.clone(),
            warehouse: config.warehouse.clone(),
            factory,
            client,
            properties,
            request_timeout: config.request_timeout,
        }
    }

    async fn build(&self, token: &str) -> Result<Arc<dyn Catalog>, ConnectorError> {
        let mut properties = self.properties.clone();
        properties.insert("uri".into(), self.catalog_uri.clone());
        properties.insert("warehouse".into(), self.warehouse.clone());
        properties.insert("token".into(), token.into());
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::clone(&self.factory))
            .with_client(self.client.clone())
            .load("laminardb", properties)
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(
                    "Iceberg OAuth2 catalog session could not be initialized".into(),
                )
            })?;
        Ok(Arc::new(catalog))
    }
}

struct CachedCatalog {
    generation: u64,
    catalog: Arc<dyn Catalog>,
}

fn oauth_catalog_error() -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        "Iceberg REST OAuth2 session is unavailable before catalog dispatch",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::future::join_all;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use crate::config::ConnectorConfig;

    use super::*;

    fn oauth_config(server: &MockServer) -> IcebergCatalogConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", server.uri());
        config.set("catalog.warehouse", "warehouse");
        config.set("namespace", "test");
        config.set("table.name", "events");
        config.set("catalog.auth.type", "oauth2");
        config.set("catalog.oauth2.server_uri", server.uri() + "/tokens");
        config.set("catalog.oauth2.client_id", "laminar-client");
        config.set("catalog.oauth2.scope", "catalog:read catalog:write");
        config.set("catalog.property.credential", "refresh-secret");
        config.set("catalog.request_timeout", "5s");
        IcebergCatalogConfig::from_config(&config).unwrap()
    }

    fn oauth_properties(config: &IcebergCatalogConfig) -> HashMap<String, String> {
        let mut properties = config.properties.clone();
        properties.insert(
            "oauth2-server-uri".into(),
            config.oauth2_server_uri.clone().unwrap(),
        );
        properties.insert("scope".into(), config.oauth2_scope.clone().unwrap());
        properties
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_proactive_refresh() {
        let server = MockServer::start().await;
        let exchanges = Arc::new(AtomicUsize::new(0));
        let responder_exchanges = Arc::clone(&exchanges);
        Mock::given(method("POST"))
            .and(path("/tokens"))
            .respond_with(move |_: &Request| {
                let ordinal = responder_exchanges.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": format!("catalog-token-{ordinal}"),
                    "token_type": "Bearer",
                    "expires_in": 30
                }))
            })
            .expect(2)
            .mount(&server)
            .await;

        let config = oauth_config(&server);
        let client = super::super::http_client(&config).unwrap();
        let session = Arc::new(
            OAuthSession::initialize(&config, client, &oauth_properties(&config), None)
                .await
                .unwrap(),
        );
        session.token.write().expires_at = Instant::now();

        let leases = join_all((0..16).map(|_| session.token(config.request_timeout))).await;
        for lease in leases {
            let lease = lease.unwrap();
            assert_eq!(lease.generation, 2);
            assert_eq!(lease.token.as_ref(), "catalog-token-1");
        }
        assert_eq!(exchanges.load(Ordering::SeqCst), 2);

        let requests = server.received_requests().await.unwrap();
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(body.contains("grant_type=client_credentials"));
        assert!(body.contains("client_id=laminar-client"));
        assert!(body.contains("scope=catalog%3Aread+catalog%3Awrite"));
    }

    #[tokio::test]
    async fn catalog_operations_switch_to_the_refreshed_session() {
        let server = MockServer::start().await;
        let exchanges = Arc::new(AtomicUsize::new(0));
        let responder_exchanges = Arc::clone(&exchanges);
        Mock::given(method("POST"))
            .and(path("/tokens"))
            .respond_with(move |_: &Request| {
                let ordinal = responder_exchanges.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": format!("catalog-token-{ordinal}"),
                    "token_type": "Bearer",
                    "expires_in": 30
                }))
            })
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "defaults": {},
                "overrides": {}
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"namespaces": []})),
            )
            .expect(2)
            .mount(&server)
            .await;

        let config = oauth_config(&server);
        let client = super::super::http_client(&config).unwrap();
        let properties = oauth_properties(&config);
        let session = Arc::new(
            OAuthSession::initialize(&config, client.clone(), &properties, None)
                .await
                .unwrap(),
        );
        let template = RestCatalogTemplate::new(
            &config,
            Arc::new(iceberg::io::MemoryStorageFactory),
            client,
            properties,
        );
        let catalog = super::super::facade::RestCatalogFacade::oauth(
            OAuthCatalogState::new(Arc::clone(&session), template)
                .await
                .unwrap(),
        );
        assert!(catalog.list_namespaces(None).await.unwrap().is_empty());
        session.token.write().expires_at = Instant::now();
        assert!(catalog.list_namespaces(None).await.unwrap().is_empty());

        let requests = server.received_requests().await.unwrap();
        let catalog_requests = requests
            .iter()
            .filter(|request| request.url.path() != "/tokens")
            .collect::<Vec<_>>();
        assert_eq!(catalog_requests.len(), 4);
        for request in &catalog_requests[..2] {
            assert_eq!(
                request.headers["authorization"].to_str().unwrap(),
                "Bearer catalog-token-0"
            );
        }
        for request in &catalog_requests[2..] {
            assert_eq!(
                request.headers["authorization"].to_str().unwrap(),
                "Bearer catalog-token-1"
            );
        }
    }

    #[tokio::test]
    async fn refresh_failure_keeps_the_old_token_and_updates_the_metric() {
        let server = MockServer::start().await;
        let exchanges = Arc::new(AtomicUsize::new(0));
        let responder_exchanges = Arc::clone(&exchanges);
        Mock::given(method("POST"))
            .and(path("/tokens"))
            .respond_with(move |_: &Request| {
                if responder_exchanges.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "access_token": "old-catalog-token",
                        "token_type": "bearer",
                        "expires_in": 30
                    }))
                } else {
                    ResponseTemplate::new(503)
                }
            })
            .expect(2)
            .mount(&server)
            .await;
        let metric = IntCounter::new(
            "iceberg_oauth_refresh_failures_test",
            "test refresh failures",
        )
        .unwrap();
        let config = oauth_config(&server);
        let client = super::super::http_client(&config).unwrap();
        let session = OAuthSession::initialize(
            &config,
            client,
            &oauth_properties(&config),
            Some(metric.clone()),
        )
        .await
        .unwrap();
        session.token.write().expires_at = Instant::now();

        let error = match session.token(config.request_timeout).await {
            Ok(_) => panic!("expired OAuth2 token must not be reused after refresh failure"),
            Err(error) => error,
        };
        assert_eq!(metric.get(), 1);
        let error = error.to_string();
        let debug = format!("{session:?}");
        for secret in ["refresh-secret", "old-catalog-token"] {
            assert!(!error.contains(secret));
            assert!(!debug.contains(secret));
        }
    }

    #[tokio::test]
    async fn missing_expiry_and_oversized_responses_fail_closed() {
        for body in [
            serde_json::json!({
                "access_token": "catalog-token",
                "token_type": "Bearer"
            })
            .to_string(),
            "x".repeat(MAX_TOKEN_RESPONSE_BYTES + 1),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/tokens"))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .expect(1)
                .mount(&server)
                .await;
            let config = oauth_config(&server);
            let error = OAuthSession::initialize(
                &config,
                super::super::http_client(&config).unwrap(),
                &oauth_properties(&config),
                None,
            )
            .await
            .unwrap_err();
            assert!(matches!(error, ConnectorError::ConnectionFailed(_)));
        }
    }

    #[test]
    fn authentication_debug_is_always_redacted() {
        let authentication = RestAuthentication::Bearer(Arc::from("catalog-secret"));
        assert_eq!(
            format!("{authentication:?}"),
            "RestAuthentication::Bearer(<redacted>)"
        );
    }
}
