//! Confluent Schema Registry client.
//!
//! [`SchemaRegistryClient`] provides a lightweight async REST client for
//! the Confluent Schema Registry API, with in-memory caching, arrow
//! schema conversion, and compatibility checking.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::{ConnectorError, SerdeError};
use crate::kafka::config::{CompatibilityLevel, SrAuth};

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

/// A cached schema entry from the Schema Registry.
#[derive(Debug, Clone)]
pub struct CachedSchema {
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
/// Provides schema lookup by ID and subject, caching, compatibility
/// checking, and Avro-to-Arrow schema conversion.
pub struct SchemaRegistryClient {
    client: Client,
    base_url: String,
    auth: Option<SrAuth>,
    /// Cache by schema ID.
    cache: HashMap<i32, CachedSchema>,
    /// Cache by subject name (latest version).
    subject_cache: HashMap<String, CachedSchema>,
}

// -- Schema Registry REST API response types --

#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(default = "default_schema_type")]
    #[serde(rename = "schemaType")]
    schema_type: String,
}

#[derive(Deserialize)]
struct SchemaVersionResponse {
    id: i32,
    version: i32,
    schema: String,
    #[serde(default = "default_schema_type")]
    #[serde(rename = "schemaType")]
    schema_type: String,
    #[allow(dead_code)]
    subject: Option<String>,
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
    /// Creates a new Schema Registry client.
    #[must_use]
    pub fn new(base_url: impl Into<String>, auth: Option<SrAuth>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            auth,
            cache: HashMap::new(),
            subject_cache: HashMap::new(),
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

    /// Fetches a schema by its global ID.
    ///
    /// Results are cached for subsequent lookups.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails or the schema
    /// cannot be parsed.
    pub async fn get_schema_by_id(&mut self, id: i32) -> Result<CachedSchema, ConnectorError> {
        if let Some(cached) = self.cache.get(&id) {
            return Ok(cached.clone());
        }

        let url = format!("{}/schemas/ids/{}", self.base_url, id);
        let resp: SchemaByIdResponse = self.get_json(&url).await?;

        let schema_type: SchemaType = resp.schema_type.parse()?;
        let arrow_schema = avro_to_arrow_schema(&resp.schema)?;

        let cached = CachedSchema {
            id,
            version: 0, // not available from this endpoint
            schema_type,
            schema_str: resp.schema,
            arrow_schema,
        };
        self.cache.insert(id, cached.clone());
        Ok(cached)
    }

    /// Fetches the latest schema version for a subject.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails.
    pub async fn get_latest_schema(
        &mut self,
        subject: &str,
    ) -> Result<CachedSchema, ConnectorError> {
        let url = format!("{}/subjects/{}/versions/latest", self.base_url, subject);
        let resp: SchemaVersionResponse = self.get_json(&url).await?;

        let schema_type: SchemaType = resp.schema_type.parse()?;
        let arrow_schema = avro_to_arrow_schema(&resp.schema)?;

        let cached = CachedSchema {
            id: resp.id,
            version: resp.version,
            schema_type,
            schema_str: resp.schema,
            arrow_schema,
        };

        self.cache.insert(resp.id, cached.clone());
        self.subject_cache
            .insert(subject.to_string(), cached.clone());
        Ok(cached)
    }

    /// Fetches a specific schema version for a subject.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails.
    pub async fn get_schema_version(
        &mut self,
        subject: &str,
        version: i32,
    ) -> Result<CachedSchema, ConnectorError> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, subject, version
        );
        let resp: SchemaVersionResponse = self.get_json(&url).await?;

        let schema_type: SchemaType = resp.schema_type.parse()?;
        let arrow_schema = avro_to_arrow_schema(&resp.schema)?;

        let cached = CachedSchema {
            id: resp.id,
            version: resp.version,
            schema_type,
            schema_str: resp.schema,
            arrow_schema,
        };
        self.cache.insert(resp.id, cached.clone());
        Ok(cached)
    }

    /// Checks compatibility of a schema against the latest version.
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
            self.base_url, subject
        );

        let body = CompatibilityRequest {
            schema: schema_str.to_string(),
            schema_type: "AVRO".to_string(),
        };

        let mut req = self.client.post(&url).json(&body);
        if let Some(ref auth) = self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("schema registry: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ConnectionFailed(format!(
                "schema registry compatibility check failed: {status} {text}"
            )));
        }

        let result: CompatibilityResponse = resp.json().await.map_err(|e| {
            ConnectorError::Internal(format!("failed to parse compatibility response: {e}"))
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
        let url = format!("{}/config/{}", self.base_url, subject);
        let resp: ConfigResponse = self.get_json(&url).await?;
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
        let url = format!("{}/config/{}", self.base_url, subject);
        let body = ConfigUpdateRequest {
            compatibility: level.as_str().to_string(),
        };

        let mut req = self.client.put(&url).json(&body);
        if let Some(ref auth) = self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("schema registry: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ConnectionFailed(format!(
                "schema registry config update failed: {status} {text}"
            )));
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
    pub async fn resolve_confluent_id(&mut self, id: i32) -> Result<CachedSchema, ConnectorError> {
        self.get_schema_by_id(id).await
    }

    /// Registers a schema with the Schema Registry under the given subject.
    ///
    /// Returns the schema ID assigned by the registry. Caches the result
    /// so subsequent calls with the same subject return immediately.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the HTTP request fails or the response
    /// is malformed.
    pub async fn register_schema(
        &mut self,
        subject: &str,
        schema_str: &str,
        schema_type: SchemaType,
    ) -> Result<i32, ConnectorError> {
        // Check subject cache first.
        if let Some(cached) = self.subject_cache.get(subject) {
            return Ok(cached.id);
        }

        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let body = RegisterSchemaRequest {
            schema: schema_str.to_string(),
            schema_type: schema_type.to_string(),
        };

        let mut req = self.client.post(&url).json(&body);
        if let Some(ref auth) = self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("schema registry: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ConnectionFailed(format!(
                "schema registry register failed: {status} {text}"
            )));
        }

        let result: RegisterSchemaResponse = resp.json().await.map_err(|e| {
            ConnectorError::Internal(format!("failed to parse register schema response: {e}"))
        })?;

        let arrow_schema = avro_to_arrow_schema(schema_str)?;
        let cached = CachedSchema {
            id: result.id,
            version: 0,
            schema_type,
            schema_str: schema_str.to_string(),
            arrow_schema,
        };
        self.cache.insert(result.id, cached.clone());
        self.subject_cache.insert(subject.to_string(), cached);

        Ok(result.id)
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
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<T, ConnectorError> {
        let mut req = self.client.get(url);
        if let Some(ref auth) = self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("schema registry: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ConnectionFailed(format!(
                "schema registry request failed: {status} {text}"
            )));
        }

        resp.json::<T>().await.map_err(|e| {
            ConnectorError::Internal(format!("failed to parse schema registry response: {e}"))
        })
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

/// Converts an Avro JSON schema string to an Arrow [`SchemaRef`].
///
/// Supports Avro record schemas with primitive field types.
///
/// # Errors
///
/// Returns `ConnectorError::SchemaMismatch` if the schema JSON is invalid
/// or contains unsupported types.
pub fn avro_to_arrow_schema(avro_schema_str: &str) -> Result<SchemaRef, ConnectorError> {
    let avro: serde_json::Value = serde_json::from_str(avro_schema_str)
        .map_err(|e| ConnectorError::SchemaMismatch(format!("invalid Avro schema JSON: {e}")))?;

    let fields_val = avro.get("fields").ok_or_else(|| {
        ConnectorError::SchemaMismatch("Avro schema missing 'fields' array".into())
    })?;

    let fields_arr = fields_val.as_array().ok_or_else(|| {
        ConnectorError::SchemaMismatch("Avro schema 'fields' is not an array".into())
    })?;

    let mut arrow_fields = Vec::with_capacity(fields_arr.len());
    for field in fields_arr {
        let name = field
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::SchemaMismatch("Avro field missing 'name'".into()))?;

        let (data_type, nullable) = parse_avro_type(field.get("type").ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!("Avro field '{name}' missing 'type'"))
        })?)?;

        arrow_fields.push(Field::new(name, data_type, nullable));
    }

    Ok(Arc::new(Schema::new(arrow_fields)))
}

/// Parses an Avro type definition to an Arrow `DataType` and nullable flag.
fn parse_avro_type(avro_type: &serde_json::Value) -> Result<(DataType, bool), ConnectorError> {
    match avro_type {
        serde_json::Value::String(s) => Ok((avro_primitive_to_arrow(s)?, false)),
        serde_json::Value::Array(union) => {
            // Union type — check for ["null", T] pattern
            let non_null: Vec<_> = union
                .iter()
                .filter(|v| v.as_str() != Some("null"))
                .collect();
            let nullable = union.iter().any(|v| v.as_str() == Some("null"));

            if non_null.len() == 1 {
                if let Some(s) = non_null[0].as_str() {
                    Ok((avro_primitive_to_arrow(s)?, nullable))
                } else {
                    // Complex type in union
                    Ok((DataType::Utf8, nullable))
                }
            } else {
                // Multi-type union — fall back to string
                Ok((DataType::Utf8, nullable))
            }
        }
        serde_json::Value::Object(obj) => {
            // Nested record or logical type
            if let Some(logical) = obj.get("logicalType").and_then(|v| v.as_str()) {
                match logical {
                    "timestamp-millis" | "timestamp-micros" => Ok((DataType::Int64, false)),
                    "date" => Ok((DataType::Int32, false)),
                    "decimal" => Ok((DataType::Float64, false)),
                    _ => Ok((DataType::Utf8, false)),
                }
            } else {
                Ok((DataType::Utf8, false))
            }
        }
        _ => Err(ConnectorError::SchemaMismatch(format!(
            "unsupported Avro type: {avro_type}"
        ))),
    }
}

/// Maps an Avro primitive type name to Arrow `DataType`.
fn avro_primitive_to_arrow(avro_type: &str) -> Result<DataType, ConnectorError> {
    match avro_type {
        "null" => Ok(DataType::Null),
        "boolean" => Ok(DataType::Boolean),
        "int" => Ok(DataType::Int32),
        "long" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "bytes" => Ok(DataType::Binary),
        "string" => Ok(DataType::Utf8),
        other => Err(ConnectorError::SchemaMismatch(format!(
            "unsupported Avro primitive type: '{other}'"
        ))),
    }
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
    let mut fields = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let avro_type = arrow_to_avro_type(field.data_type())?;

        let field_type = if field.is_nullable() {
            serde_json::json!(["null", avro_type])
        } else {
            serde_json::json!(avro_type)
        };

        fields.push(serde_json::json!({
            "name": field.name(),
            "type": field_type,
        }));
    }

    let schema = serde_json::json!({
        "type": "record",
        "name": record_name,
        "fields": fields,
    });

    serde_json::to_string(&schema)
        .map_err(|e| SerdeError::MalformedInput(format!("failed to serialize Avro schema: {e}")))
}

/// Maps an Arrow `DataType` to an Avro type string.
fn arrow_to_avro_type(data_type: &DataType) -> Result<&'static str, SerdeError> {
    match data_type {
        DataType::Null => Ok("null"),
        DataType::Boolean => Ok("boolean"),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Ok("int"),
        DataType::Int64 | DataType::UInt64 => Ok("long"),
        DataType::Float32 => Ok("float"),
        DataType::Float64 => Ok("double"),
        DataType::Utf8 | DataType::LargeUtf8 => Ok("string"),
        DataType::Binary | DataType::LargeBinary => Ok("bytes"),
        other => Err(SerdeError::UnsupportedFormat(format!(
            "no Avro equivalent for Arrow type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avro_to_arrow_simple_record() {
        let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
            ]
        }"#;

        let schema = avro_to_arrow_schema(avro).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "active");
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_avro_to_arrow_nullable_union() {
        let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }"#;

        let schema = avro_to_arrow_schema(avro).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_avro_to_arrow_all_primitives() {
        let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "b", "type": "boolean"},
                {"name": "i", "type": "int"},
                {"name": "l", "type": "long"},
                {"name": "f", "type": "float"},
                {"name": "d", "type": "double"},
                {"name": "s", "type": "string"},
                {"name": "raw", "type": "bytes"}
            ]
        }"#;

        let schema = avro_to_arrow_schema(avro).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(schema.field(3).data_type(), &DataType::Float32);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert_eq!(schema.field(5).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(6).data_type(), &DataType::Binary);
    }

    #[test]
    fn test_avro_to_arrow_invalid_json() {
        assert!(avro_to_arrow_schema("not json").is_err());
    }

    #[test]
    fn test_avro_to_arrow_missing_fields() {
        let avro = r#"{"type": "record", "name": "test"}"#;
        assert!(avro_to_arrow_schema(avro).is_err());
    }

    #[test]
    fn test_schema_type_parsing() {
        assert_eq!("AVRO".parse::<SchemaType>().unwrap(), SchemaType::Avro);
        assert_eq!(
            "PROTOBUF".parse::<SchemaType>().unwrap(),
            SchemaType::Protobuf
        );
        assert_eq!("JSON".parse::<SchemaType>().unwrap(), SchemaType::Json);
        assert!("UNKNOWN".parse::<SchemaType>().is_err());
    }

    #[test]
    fn test_schema_type_display() {
        assert_eq!(SchemaType::Avro.to_string(), "AVRO");
        assert_eq!(SchemaType::Protobuf.to_string(), "PROTOBUF");
        assert_eq!(SchemaType::Json.to_string(), "JSON");
    }

    #[test]
    fn test_client_creation() {
        let client = SchemaRegistryClient::new("http://localhost:8081", None);
        assert_eq!(client.base_url(), "http://localhost:8081");
        assert!(!client.has_auth());
        assert_eq!(client.cache_size(), 0);
    }

    #[test]
    fn test_client_with_auth() {
        let auth = SrAuth {
            username: "user".into(),
            password: "pass".into(),
        };
        let client = SchemaRegistryClient::new("http://localhost:8081", Some(auth));
        assert!(client.has_auth());
    }

    #[test]
    fn test_client_trailing_slash_stripped() {
        let client = SchemaRegistryClient::new("http://localhost:8081/", None);
        assert_eq!(client.base_url(), "http://localhost:8081");
    }

    #[test]
    fn test_arrow_to_avro_schema_simple() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let avro_str = arrow_to_avro_schema(&schema, "test_record").unwrap();
        let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();

        assert_eq!(avro["type"], "record");
        assert_eq!(avro["name"], "test_record");

        let fields = avro["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[0]["type"], "long");
        assert_eq!(fields[1]["name"], "name");
        assert_eq!(fields[1]["type"], "string");
    }

    #[test]
    fn test_arrow_to_avro_schema_nullable() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]));

        let avro_str = arrow_to_avro_schema(&schema, "record").unwrap();
        let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();

        let fields = avro["fields"].as_array().unwrap();
        // Non-nullable: plain type
        assert_eq!(fields[0]["type"], "long");
        // Nullable: union ["null", "string"]
        let union = fields[1]["type"].as_array().unwrap();
        assert_eq!(union.len(), 2);
        assert_eq!(union[0], "null");
        assert_eq!(union[1], "string");
    }

    #[test]
    fn test_arrow_to_avro_all_primitives() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("bin", DataType::Binary, false),
        ]));

        let avro_str = arrow_to_avro_schema(&schema, "all_types").unwrap();
        let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();
        let fields = avro["fields"].as_array().unwrap();

        assert_eq!(fields[0]["type"], "boolean");
        assert_eq!(fields[1]["type"], "int");
        assert_eq!(fields[2]["type"], "long");
        assert_eq!(fields[3]["type"], "float");
        assert_eq!(fields[4]["type"], "double");
        assert_eq!(fields[5]["type"], "string");
        assert_eq!(fields[6]["type"], "bytes");
    }

    #[test]
    fn test_arrow_to_avro_roundtrip() {
        let original = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]));

        let avro_str = arrow_to_avro_schema(&original, "roundtrip").unwrap();
        let recovered = avro_to_arrow_schema(&avro_str).unwrap();

        assert_eq!(recovered.fields().len(), 3);
        assert_eq!(recovered.field(0).data_type(), &DataType::Int64);
        assert!(!recovered.field(0).is_nullable());
        assert_eq!(recovered.field(1).data_type(), &DataType::Utf8);
        assert!(recovered.field(1).is_nullable());
        assert_eq!(recovered.field(2).data_type(), &DataType::Boolean);
    }
}
