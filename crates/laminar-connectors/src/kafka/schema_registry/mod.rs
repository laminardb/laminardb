//! Confluent Schema Registry client.
//!
//! [`SchemaRegistryClient`] provides a lightweight async REST client for
//! the Confluent Schema Registry API, with in-memory caching, arrow
//! schema conversion, and compatibility checking.

use std::time::{Duration, Instant};

use quick_cache::sync::Cache;

use arrow_schema::SchemaRef;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::{ConnectorError, SerdeError};
use crate::kafka::config::{CompatibilityLevel, SrAuth};

mod resolution;

const SCHEMA_REGISTRY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const SCHEMA_REGISTRY_READ_TIMEOUT: Duration = Duration::from_secs(2);
const SCHEMA_REGISTRY_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy)]
struct SchemaRegistryHttpTimeouts {
    connect: Duration,
    read: Duration,
    request: Duration,
}

const SCHEMA_REGISTRY_HTTP_TIMEOUTS: SchemaRegistryHttpTimeouts = SchemaRegistryHttpTimeouts {
    connect: SCHEMA_REGISTRY_CONNECT_TIMEOUT,
    read: SCHEMA_REGISTRY_READ_TIMEOUT,
    request: SCHEMA_REGISTRY_REQUEST_TIMEOUT,
};

fn http_client_builder(timeouts: SchemaRegistryHttpTimeouts) -> reqwest::ClientBuilder {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(timeouts.connect)
        .read_timeout(timeouts.read)
        .timeout(timeouts.request)
}

fn build_http_client(timeouts: SchemaRegistryHttpTimeouts) -> Result<Client, ConnectorError> {
    http_client_builder(timeouts).build().map_err(|e| {
        ConnectorError::ConfigurationError(format!(
            "failed to build Schema Registry HTTP client: {e}"
        ))
    })
}

fn schema_registry_http_error(
    operation: &str,
    status: reqwest::StatusCode,
    detail: &str,
) -> ConnectorError {
    let _ = detail;
    let message = format!("schema registry {operation} failed: {status}");
    if status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        ConnectorError::ConnectionFailed(message)
    } else {
        ConnectorError::ConfigurationError(message)
    }
}

fn schema_registry_request_error(operation: &str, error: reqwest::Error) -> ConnectorError {
    let builder_error = error.is_builder();
    let message = format!(
        "schema registry {operation} failed: {}",
        error.without_url()
    );
    if builder_error {
        ConnectorError::ConfigurationError(message)
    } else {
        ConnectorError::ConnectionFailed(message)
    }
}

/// Schema type as reported by the Schema Registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    /// Apache Avro schema.
    Avro,
    /// Protocol Buffers schema.
    Protobuf,
    /// JSON Schema.
    Json,
}

impl std::str::FromStr for SchemaType {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "AVRO" => Ok(SchemaType::Avro),
            "PROTOBUF" => Ok(SchemaType::Protobuf),
            "JSON" => Ok(SchemaType::Json),
            other => Err(ConnectorError::ConfigurationError(format!(
                "unknown schema type: '{other}'"
            ))),
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Avro => write!(f, "AVRO"),
            SchemaType::Protobuf => write!(f, "PROTOBUF"),
            SchemaType::Json => write!(f, "JSON"),
        }
    }
}

fn require_avro_mutation(schema_type: SchemaType) -> Result<(), ConnectorError> {
    if schema_type == SchemaType::Avro {
        Ok(())
    } else {
        Err(ConnectorError::ConfigurationError(format!(
            "Kafka Schema Registry registration supports AVRO only, got {schema_type}"
        )))
    }
}

/// Configuration for the Schema Registry cache.
#[derive(Debug, Clone)]
pub struct SchemaRegistryCacheConfig {
    /// Maximum number of cached schemas. Default: 1000.
    pub max_entries: usize,
    /// TTL for cache entries. `None` means no expiry. Default: 1 hour.
    pub ttl: Option<Duration>,
}

impl Default for SchemaRegistryCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl: Some(Duration::from_secs(3600)),
        }
    }
}

/// A cached schema entry from the Schema Registry.
#[derive(Debug, Clone)]
pub struct CachedSchema {
    /// Schema with transitive named references resolved for the selected Arrow codec.
    pub resolved_schema_str: String,
    /// Complete, concrete transitive reference definitions.
    pub references: Vec<crate::schema::resolution::NativeSchemaArtifact>,
    /// Schema Registry schema ID.
    pub id: i32,
    /// Schema version within its subject.
    pub version: i32,
    /// The schema type.
    pub schema_type: SchemaType,
    /// Raw schema string (e.g., Avro JSON).
    pub schema_str: String,
    /// Derived Arrow schema for `RecordBatch` construction.
    pub arrow_schema: SchemaRef,
    /// When this entry was inserted or last accessed.
    inserted_at: Instant,
}

/// Result of a compatibility check.
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Whether the schema is compatible.
    pub is_compatible: bool,
    /// Incompatibility reasons (if any).
    pub messages: Vec<String>,
}

/// Async client for the Confluent Schema Registry REST API.
///
/// Provides schema lookup by ID and subject, caching with LRU eviction
/// and TTL, compatibility checking, and Avro-to-Arrow schema conversion.
pub struct SchemaRegistryClient {
    client: Client,
    base_url: String,
    auth: Option<SrAuth>,
    /// Cache by schema ID (`quick_cache`, S3-FIFO-style eviction).
    cache: Cache<i32, CachedSchema>,
    /// Cache by subject name (latest version).
    subject_cache: Cache<String, CachedSchema>,
    /// Cache configuration.
    cache_config: SchemaRegistryCacheConfig,
    // INVARIANT: serialize cache misses per authenticated registry client. Reference fetches
    // use get_json directly, so this guard never recursively acquires itself.
    lookup_gate: tokio::sync::Mutex<()>,
}

// -- Schema Registry REST API response types --

#[derive(Deserialize)]
struct SchemaByIdResponse {
    #[serde(default)]
    references: Vec<resolution::SchemaReference>,
    schema: String,
    #[serde(default = "default_schema_type")]
    #[serde(rename = "schemaType")]
    schema_type: String,
}

#[derive(Deserialize)]
struct SchemaVersionResponse {
    #[serde(default)]
    references: Vec<resolution::SchemaReference>,
    id: i32,
    version: i32,
    schema: String,
    #[serde(default = "default_schema_type")]
    #[serde(rename = "schemaType")]
    schema_type: String,
}

#[derive(Deserialize)]
struct CompatibilityResponse {
    is_compatible: bool,
    #[serde(default)]
    messages: Vec<String>,
}

#[derive(Deserialize)]
struct ConfigResponse {
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: String,
}

#[derive(Serialize)]
struct CompatibilityRequest {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: String,
}

#[derive(Serialize)]
struct ConfigUpdateRequest {
    compatibility: String,
}

#[derive(Serialize)]
struct RegisterSchemaRequest {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: String,
}

#[derive(Deserialize)]
struct RegisterSchemaResponse {
    id: i32,
}

fn default_schema_type() -> String {
    "AVRO".to_string()
}

impl SchemaRegistryClient {
    /// Creates a new Schema Registry client with default cache config.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the HTTP client cannot be built.
    pub fn new(base_url: impl Into<String>, auth: Option<SrAuth>) -> Result<Self, ConnectorError> {
        Self::with_cache_config(base_url, auth, SchemaRegistryCacheConfig::default())
    }

    /// Creates a TLS client (CA cert only). Delegates to [`Self::with_tls_mtls`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the CA cert cannot be read.
    pub fn with_tls(
        base_url: impl Into<String>,
        auth: Option<SrAuth>,
        ca_cert_path: &str,
    ) -> Result<Self, ConnectorError> {
        Self::with_tls_mtls(base_url, auth, ca_cert_path, None, None)
    }

    /// Creates a client with full TLS/mTLS support.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if any cert/key file
    /// cannot be read or parsed.
    pub fn with_tls_mtls(
        base_url: impl Into<String>,
        auth: Option<SrAuth>,
        ca_cert_path: &str,
        client_cert_path: Option<&str>,
        client_key_path: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        let pem = std::fs::read(ca_cert_path).map_err(|e| {
            ConnectorError::ConfigurationError(format!(
                "failed to read SR CA cert at '{ca_cert_path}': {e}"
            ))
        })?;
        let cert = reqwest::tls::Certificate::from_pem(&pem).map_err(|e| {
            ConnectorError::ConfigurationError(format!(
                "invalid PEM CA cert at '{ca_cert_path}': {e}"
            ))
        })?;

        let mut builder =
            http_client_builder(SCHEMA_REGISTRY_HTTP_TIMEOUTS).add_root_certificate(cert);

        if client_cert_path.is_some() != client_key_path.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "mTLS requires both client cert and key — only one was provided".into(),
            ));
        }
        if let (Some(cert_path), Some(key_path)) = (client_cert_path, client_key_path) {
            let mut identity_pem = std::fs::read(cert_path).map_err(|e| {
                ConnectorError::ConfigurationError(format!(
                    "failed to read SR client cert at '{cert_path}': {e}"
                ))
            })?;
            let key_pem = std::fs::read(key_path).map_err(|e| {
                ConnectorError::ConfigurationError(format!(
                    "failed to read SR client key at '{key_path}': {e}"
                ))
            })?;
            // reqwest Identity expects cert + key concatenated in PEM format.
            identity_pem.extend_from_slice(&key_pem);
            let identity = reqwest::tls::Identity::from_pem(&identity_pem).map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid client cert/key PEM: {e}"))
            })?;
            builder = builder.identity(identity);
        }

        let client = builder.build().map_err(|e| {
            ConnectorError::ConfigurationError(format!("failed to build TLS client: {e}"))
        })?;

        Ok(Self::from_http_client(
            base_url,
            auth,
            SchemaRegistryCacheConfig::default(),
            client,
        ))
    }

    /// Creates a new Schema Registry client with custom cache config.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the HTTP client cannot be built.
    pub fn with_cache_config(
        base_url: impl Into<String>,
        auth: Option<SrAuth>,
        cache_config: SchemaRegistryCacheConfig,
    ) -> Result<Self, ConnectorError> {
        Self::with_cache_config_and_timeouts(
            base_url,
            auth,
            cache_config,
            SCHEMA_REGISTRY_HTTP_TIMEOUTS,
        )
    }

    fn with_cache_config_and_timeouts(
        base_url: impl Into<String>,
        auth: Option<SrAuth>,
        cache_config: SchemaRegistryCacheConfig,
        timeouts: SchemaRegistryHttpTimeouts,
    ) -> Result<Self, ConnectorError> {
        let client = build_http_client(timeouts)?;
        Ok(Self::from_http_client(base_url, auth, cache_config, client))
    }

    fn from_http_client(
        base_url: impl Into<String>,
        auth: Option<SrAuth>,
        cache_config: SchemaRegistryCacheConfig,
        client: Client,
    ) -> Self {
        let cache = Cache::new(cache_config.max_entries.clamp(1, 64));
        // Subject cache is small — one entry per subject
        let subject_cache = Cache::new(16);
        Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            auth,
            cache,
            subject_cache,
            cache_config,
            lookup_gate: tokio::sync::Mutex::new(()),
        }
    }

    /// Returns the base URL of the Schema Registry.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns `true` if authentication is configured.
    #[must_use]
    pub fn has_auth(&self) -> bool {
        self.auth.is_some()
    }

    /// Returns the cache configuration.
    #[must_use]
    pub fn cache_config(&self) -> &SchemaRegistryCacheConfig {
        &self.cache_config
    }

    /// Inserts a schema into the cache.
    ///
    /// `quick_cache` handles eviction internally (S3-FIFO-style).
    fn cache_insert(&self, id: i32, mut schema: CachedSchema) {
        schema.inserted_at = Instant::now();
        self.cache.insert(id, schema);
    }

    /// Gets from cache, returning `None` if expired.
    ///
    /// TTL is checked lazily on access — expired entries are removed
    /// and treated as cache misses.
    fn cache_get(&self, id: i32) -> Option<CachedSchema> {
        let schema = self.cache.get(&id)?;
        if let Some(ttl) = self.cache_config.ttl {
            if schema.inserted_at.elapsed() > ttl {
                self.cache.remove(&id);
                return None;
            }
        }
        // quick_cache's get() already promotes the entry in the eviction policy
        Some(schema)
    }

    /// Fetches a schema by its global ID.
    ///
    /// Results are cached for subsequent lookups.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails or the schema
    /// cannot be parsed.
    pub async fn get_schema_by_id(&self, id: i32) -> Result<CachedSchema, ConnectorError> {
        if let Some(cached) = self.cache_get(id) {
            return Ok(cached);
        }

        if id <= 0 {
            return Err(ConnectorError::SchemaMismatch(
                "registry schema ID must be positive".into(),
            ));
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let _lookup = self.lookup_gate.lock().await;
            if let Some(cached) = self.cache_get(id) {
                return Ok(cached);
            }
            let url = format!("{}/schemas/ids/{}", self.base_url, id);
            let operation = format!("fetch schema ID {id}");
            let resp: SchemaByIdResponse = self.get_json(&url, &operation).await?;
            let cached = self
                .complete_schema(id, 0, resp.schema, &resp.schema_type, resp.references)
                .await?;
            self.cache_insert(id, cached.clone());
            Ok(cached)
        })
        .await
        .map_err(|_| ConnectorError::Timeout(30_000))?
    }

    pub(super) async fn fetch_registered_schema(
        &self,
        id: i32,
    ) -> Result<CachedSchema, ConnectorError> {
        // Registration echoes only an ID. Verify its definition instead of trusting
        // the optimistic cache populated from our request body.
        self.cache.remove(&id);
        self.get_schema_by_id(id).await
    }

    /// Fetches the latest schema version for a subject.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails.
    pub async fn get_latest_schema(&self, subject: &str) -> Result<CachedSchema, ConnectorError> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let _lookup = self.lookup_gate.lock().await;
            let url = format!(
                "{}/subjects/{}/versions/latest",
                self.base_url,
                resolution::encoded_subject(subject)
            );
            let operation = format!("fetch latest schema for subject '{subject}'");
            let resp: SchemaVersionResponse = self.get_json(&url, &operation).await?;

            if resp.version <= 0 {
                return Err(ConnectorError::SchemaMismatch(
                    "registry returned a different or invalid concrete version".into(),
                ));
            }
            let cached = self
                .complete_schema(
                    resp.id,
                    resp.version,
                    resp.schema,
                    &resp.schema_type,
                    resp.references,
                )
                .await?;
            self.cache_insert(cached.id, cached.clone());
            self.subject_cache
                .insert(subject.to_string(), cached.clone());
            Ok(cached)
        })
        .await
        .map_err(|_| ConnectorError::Timeout(30_000))?
    }

    /// Fetches a specific schema version for a subject.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails.
    pub async fn get_schema_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<CachedSchema, ConnectorError> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let _lookup = self.lookup_gate.lock().await;
            let url = format!(
                "{}/subjects/{}/versions/{}",
                self.base_url,
                resolution::encoded_subject(subject),
                version
            );
            let operation = format!("fetch version {version} for subject '{subject}'");
            let resp: SchemaVersionResponse = self.get_json(&url, &operation).await?;

            if resp.version <= 0 || resp.version != version {
                return Err(ConnectorError::SchemaMismatch(
                    "registry returned a different or invalid concrete version".into(),
                ));
            }
            let cached = self
                .complete_schema(
                    resp.id,
                    resp.version,
                    resp.schema,
                    &resp.schema_type,
                    resp.references,
                )
                .await?;
            self.cache_insert(cached.id, cached.clone());
            Ok(cached)
        })
        .await
        .map_err(|_| ConnectorError::Timeout(30_000))?
    }

    /// Checks compatibility of an Avro schema against the latest version.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails.
    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema_str: &str,
    ) -> Result<CompatibilityResult, ConnectorError> {
        let url = format!(
            "{}/compatibility/subjects/{}/versions/latest",
            self.base_url,
            resolution::encoded_subject(subject)
        );

        let body = CompatibilityRequest {
            schema: schema_str.to_string(),
            schema_type: SchemaType::Avro.to_string(),
        };

        let mut req = self.client.post(&url).json(&body);
        if let Some(ref auth) = self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }

        let operation = format!("compatibility check for subject '{subject}'");
        let resp = req
            .send()
            .await
            .map_err(|error| schema_registry_request_error(&operation, error))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = String::new();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(CompatibilityResult {
                    is_compatible: true,
                    messages: Vec::new(),
                });
            }
            return Err(schema_registry_http_error(&operation, status, &text));
        }

        let result: CompatibilityResponse =
            Self::bounded_json(resp, &operation).await.map_err(|e| {
                ConnectorError::Internal(format!(
                    "schema registry {operation} returned an invalid response: {e}"
                ))
            })?;

        Ok(CompatibilityResult {
            is_compatible: result.is_compatible,
            messages: result.messages,
        })
    }

    /// Gets the compatibility level for a subject.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails.
    pub async fn get_compatibility_level(
        &self,
        subject: &str,
    ) -> Result<CompatibilityLevel, ConnectorError> {
        let url = format!(
            "{}/config/{}",
            self.base_url,
            resolution::encoded_subject(subject)
        );
        let operation = format!("fetch compatibility config for subject '{subject}'");
        let resp: ConfigResponse = self.get_json(&url, &operation).await?;
        resp.compatibility_level.parse()
    }

    /// Sets the compatibility level for a subject.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails.
    pub async fn set_compatibility_level(
        &self,
        subject: &str,
        level: CompatibilityLevel,
    ) -> Result<(), ConnectorError> {
        let url = format!(
            "{}/config/{}",
            self.base_url,
            resolution::encoded_subject(subject)
        );
        let body = ConfigUpdateRequest {
            compatibility: level.as_str().to_string(),
        };

        let mut req = self.client.put(&url).json(&body);
        if let Some(ref auth) = self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }

        let operation = format!("update compatibility config for subject '{subject}'");
        let resp = req
            .send()
            .await
            .map_err(|error| schema_registry_request_error(&operation, error))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = String::new();
            return Err(schema_registry_http_error(&operation, status, &text));
        }

        Ok(())
    }

    /// Resolves a Confluent schema ID, returning from cache if available.
    ///
    /// This is the hot-path method called during Avro deserialization to
    /// look up schemas by the 4-byte ID in the Confluent wire format.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the schema cannot be fetched.
    pub async fn resolve_confluent_id(&self, id: i32) -> Result<CachedSchema, ConnectorError> {
        self.get_schema_by_id(id).await
    }

    /// Registers an Avro schema with the Schema Registry under the given subject.
    ///
    /// Returns the schema ID assigned by the registry. Caches the result
    /// so subsequent calls with the same subject return immediately.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if `schema_type` is not Avro, the HTTP request
    /// fails, or the response is malformed.
    pub async fn register_schema(
        &self,
        subject: &str,
        schema_str: &str,
        schema_type: SchemaType,
    ) -> Result<i32, ConnectorError> {
        require_avro_mutation(schema_type)?;

        // Check subject cache — only return cached ID if schema hasn't changed.
        if let Some(cached) = self.subject_cache.get(subject) {
            if cached.schema_str == schema_str {
                return Ok(cached.id);
            }
        }

        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            resolution::encoded_subject(subject)
        );
        let body = RegisterSchemaRequest {
            schema: schema_str.to_string(),
            schema_type: schema_type.to_string(),
        };

        let mut req = self.client.post(&url).json(&body);
        if let Some(ref auth) = self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }

        let operation = format!("register schema for subject '{subject}'");
        let resp = req
            .send()
            .await
            .map_err(|error| schema_registry_request_error(&operation, error))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = String::new();
            return Err(schema_registry_http_error(&operation, status, &text));
        }

        let result: RegisterSchemaResponse =
            Self::bounded_json(resp, &operation).await.map_err(|e| {
                ConnectorError::Internal(format!(
                    "schema registry {operation} returned an invalid response: {e}"
                ))
            })?;

        if result.id <= 0 {
            return Err(ConnectorError::SchemaMismatch(
                "registry returned a non-positive registration ID".into(),
            ));
        }
        let arrow_schema = avro_to_arrow_schema(schema_str)?;
        let cached = CachedSchema {
            resolved_schema_str: schema_str.to_string(),
            references: Vec::new(),
            id: result.id,
            version: 0,
            schema_type,
            schema_str: schema_str.to_string(),
            arrow_schema,
            inserted_at: Instant::now(),
        };
        self.cache_insert(result.id, cached.clone());
        self.subject_cache.insert(subject.to_string(), cached);

        Ok(result.id)
    }

    /// Validates compatibility and registers a schema in one step.
    ///
    /// If the subject already has schemas registered, checks compatibility
    /// first. Returns `SerdeError::SchemaIncompatible` if the new schema
    /// is not compatible with the existing versions.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::Serde(SchemaIncompatible)` if incompatible,
    /// or `ConnectorError` for HTTP/network errors.
    pub async fn validate_and_register_schema(
        &self,
        subject: &str,
        schema_str: &str,
        schema_type: SchemaType,
    ) -> Result<i32, ConnectorError> {
        // Reject unsupported mutation types before the compatibility request.
        // `register_schema` repeats this guard because it is also public.
        require_avro_mutation(schema_type)?;

        let result = self.check_compatibility(subject, schema_str).await?;
        if !result.is_compatible {
            let message = if result.messages.is_empty() {
                "new schema is not compatible with existing version".to_string()
            } else {
                result.messages.join("; ")
            };
            return Err(ConnectorError::Serde(SerdeError::SchemaIncompatible {
                subject: subject.to_string(),
                message,
            }));
        }

        self.register_schema(subject, schema_str, schema_type).await
    }

    /// Returns `true` if the schema ID is in the local cache.
    #[must_use]
    pub fn is_cached(&self, id: i32) -> bool {
        self.cache.contains_key(&id)
    }

    /// Returns the number of cached schemas.
    #[must_use]
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Helper to perform a GET request and deserialize JSON.
    ///
    /// Retries transient failures (408, 429, 5xx, and transport errors) up to
    /// 3 attempts with exponential backoff (100ms, 500ms).
    async fn bounded_json<T: serde::de::DeserializeOwned>(
        mut response: reqwest::Response,
        operation: &str,
    ) -> Result<T, ConnectorError> {
        const LIMIT: usize = 2 * 1024 * 1024;
        if response
            .content_length()
            .is_some_and(|length| length > LIMIT as u64)
        {
            return Err(ConnectorError::SchemaMismatch(
                "registry response exceeds 2 MiB".into(),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| schema_registry_request_error(operation, error))?
        {
            if bytes.len().saturating_add(chunk.len()) > LIMIT {
                return Err(ConnectorError::SchemaMismatch(
                    "registry response exceeds 2 MiB".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| {
            ConnectorError::SchemaMismatch(format!(
                "registry {operation} returned malformed metadata"
            ))
        })
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        operation: &str,
    ) -> Result<T, ConnectorError> {
        let backoffs = [
            std::time::Duration::from_millis(100),
            std::time::Duration::from_millis(500),
        ];
        let mut last_err = None;

        for (attempt, backoff) in std::iter::once(&std::time::Duration::ZERO)
            .chain(backoffs.iter())
            .enumerate()
        {
            if attempt > 0 {
                tokio::time::sleep(*backoff).await;
            }

            let mut req = self.client.get(url);
            if let Some(ref auth) = self.auth {
                req = req.basic_auth(&auth.username, Some(&auth.password));
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let error = schema_registry_request_error(operation, e);
                    if !error.is_transient() {
                        return Err(error);
                    }
                    tracing::warn!(
                        attempt = attempt + 1,
                        error = %error,
                        "schema registry request failed, retrying"
                    );
                    last_err = Some(error);
                    continue;
                }
            };

            let status = resp.status();
            if status.is_success() {
                return Self::bounded_json(resp, operation).await;
            }

            let transient = status == reqwest::StatusCode::REQUEST_TIMEOUT
                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error();
            if !transient {
                let text = String::new();
                return Err(schema_registry_http_error(operation, status, &text));
            }

            let text = String::new();
            tracing::warn!(
                attempt = attempt + 1,
                status = %status,
                "schema registry server error, retrying"
            );
            last_err = Some(schema_registry_http_error(operation, status, &text));
        }

        Err(last_err.unwrap_or_else(|| {
            ConnectorError::ConnectionFailed(format!(
                "schema registry {operation} exhausted all retries"
            ))
        }))
    }
}

impl std::fmt::Debug for SchemaRegistryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaRegistryClient")
            .field("base_url", &self.base_url)
            .field("has_auth", &self.auth.is_some())
            .field("cached_schemas", &self.cache.len())
            .field("cached_subjects", &self.subject_cache.len())
            .finish_non_exhaustive()
    }
}

/// Dispatch a Schema Registry payload to the right Arrow converter.
/// Only Avro is wired today; JSON Schema and Protobuf return an
/// actionable error until a maintained conversion library lands.
fn schema_to_arrow(schema_type: SchemaType, schema_str: &str) -> Result<SchemaRef, ConnectorError> {
    let name = match schema_type {
        SchemaType::Avro => return avro_to_arrow_schema(schema_str),
        SchemaType::Json => "JSON Schema Registry",
        SchemaType::Protobuf => "Protobuf Schema Registry",
    };
    Err(ConnectorError::SchemaMismatch(format!(
        "{name} subjects are not yet supported for auto-discovery \
         — declare columns explicitly or use an Avro subject"
    )))
}

/// Converts an Avro JSON schema string to an Arrow [`SchemaRef`] via `arrow-avro`'s Decoder.
///
/// # Errors
///
/// Returns `ConnectorError::SchemaMismatch` if the JSON is invalid or conversion fails.
pub fn avro_to_arrow_schema(avro_schema_str: &str) -> Result<SchemaRef, ConnectorError> {
    use arrow_avro::reader::ReaderBuilder;
    use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};

    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    let avro_schema = AvroSchema::new(avro_schema_str.to_string());
    let fp = Fingerprint::Id(0);
    store
        .set(fp, avro_schema)
        .map_err(|e| ConnectorError::SchemaMismatch(format!("invalid Avro schema: {e}")))?;

    let decoder = ReaderBuilder::new()
        .with_writer_schema_store(store)
        .with_active_fingerprint(fp)
        .build_decoder()
        .map_err(|e| ConnectorError::SchemaMismatch(format!("Avro→Arrow conversion: {e}")))?;

    Ok(decoder.schema())
}

/// Converts an Arrow [`SchemaRef`] to an Avro JSON schema string.
///
/// Generates a record schema named `"record"` with fields mapped from
/// Arrow data types to Avro primitives.
///
/// # Errors
///
/// Returns `SerdeError` if an Arrow type has no Avro equivalent.
pub fn arrow_to_avro_schema(schema: &SchemaRef, record_name: &str) -> Result<String, SerdeError> {
    let mut metadata = schema.metadata().clone();
    metadata
        .entry("avro.name".into())
        .or_insert_with(|| record_name.replace('-', "_"));
    let native_schema = schema.as_ref().clone().with_metadata(metadata);
    arrow_avro::schema::AvroSchema::try_from(&native_schema)
        .map(|schema| schema.json_string)
        .map_err(|error| SerdeError::UnsupportedFormat(format!("Arrow to Avro: {error}")))
}

#[cfg(test)]
mod tests;
