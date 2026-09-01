//! Provider-neutral parsing for object-store locations.
//!
//! Parsing is deliberately separate from client construction. A location is
//! parsed once, then adapted to the URL and non-secret options expected by a
//! particular storage consumer.

use std::fmt;

/// Cloud or local storage provider selected by a location URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageProvider {
    /// Amazon S3, including separately classified S3-compatible endpoints.
    AwsS3,
    /// Azure Blob Storage or Azure Data Lake Storage Gen2.
    AzureAdls,
    /// Google Cloud Storage.
    Gcs,
    /// Local filesystem.
    Local,
}

impl StorageProvider {
    /// Detect a recognized provider URI.
    #[must_use]
    pub fn detect_uri(location: &str) -> Option<Self> {
        StorageLocation::parse(location)
            .ok()
            .map(|parsed| parsed.provider)
    }

    /// Detect a provider, retaining the historical local-path fallback.
    #[must_use]
    pub fn detect(location: &str) -> Self {
        Self::detect_uri(location).unwrap_or(Self::Local)
    }

    /// Whether a URL names a shared object store.
    #[must_use]
    pub fn is_shared_uri(location: &str) -> bool {
        Self::detect_uri(location).is_some_and(|provider| provider != Self::Local)
    }

    /// Whether a URL directly names the S3 log-store family admitted by cluster policy.
    ///
    /// Endpoint and conditional-write preflight remains a separate mandatory check.
    #[must_use]
    pub fn is_direct_s3_uri(location: &str) -> bool {
        StorageLocation::parse(location).is_ok_and(|parsed| {
            parsed.provider == Self::AwsS3
                && matches!(parsed.original_scheme.as_str(), "s3" | "s3a")
        })
    }

    /// Whether this provider may require cloud authentication.
    #[must_use]
    pub const fn requires_credentials(self) -> bool {
        !matches!(self, Self::Local)
    }

    /// Stable provider name suitable for diagnostics.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AwsS3 => "AWS S3",
            Self::AzureAdls => "Azure ADLS",
            Self::Gcs => "Google Cloud Storage",
            Self::Local => "Local Filesystem",
        }
    }
}

impl fmt::Display for StorageProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Storage client that will consume a parsed location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageConsumer {
    /// The `object_store` checkpoint client.
    ObjectStore,
    /// Delta Lake / delta-rs.
    Delta,
    /// Iceberg's `OpenDAL` storage implementation.
    Iceberg,
}

/// Redacted description of a configured endpoint override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointOverride {
    scheme: String,
    has_path: bool,
}

impl EndpointOverride {
    /// Endpoint transport scheme.
    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Whether the endpoint includes a non-root path.
    #[must_use]
    pub const fn has_path(&self) -> bool {
        self.has_path
    }

    /// Whether transport encryption is disabled.
    #[must_use]
    pub fn uses_http(&self) -> bool {
        self.scheme == "http"
    }
}

impl fmt::Display for EndpointOverride {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<custom-{}-endpoint>", self.scheme)
    }
}

/// Native or compatibility classification of a storage endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageEndpointClass {
    /// Provider-native endpoint selected by the downstream default.
    Native,
    /// Explicit S3-compatible endpoint such as `MinIO`.
    S3Compatible,
    /// Explicit Azure or GCS endpoint, including emulators.
    CustomOrEmulator,
    /// Local filesystem.
    Local,
}

impl StorageEndpointClass {
    /// Stable low-cardinality value suitable for tracing fields.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::S3Compatible => "s3-compatible",
            Self::CustomOrEmulator => "custom-or-emulator",
            Self::Local => "local",
        }
    }
}

impl fmt::Display for StorageEndpointClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// A validated storage location with the authority components consumers need.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageLocation {
    /// Parsed provider.
    pub provider: StorageProvider,
    /// Original, credential-free URL.
    pub original_url: String,
    /// Lowercase input scheme.
    pub original_scheme: String,
    /// Provider's canonical scheme (`s3`, `gs`, `az`, or `file`).
    pub canonical_scheme: String,
    /// Bucket, container, or Azure filesystem.
    pub bucket_or_container: String,
    /// Azure storage account from a fully qualified Hadoop-style URL.
    pub account: Option<String>,
    /// Azure filesystem/container from a fully qualified Hadoop-style URL.
    pub filesystem: Option<String>,
    /// Raw object prefix without the authority's first `/`.
    pub prefix: String,
    /// Redacted explicit endpoint information, when attached by configuration resolution.
    pub endpoint_override: Option<EndpointOverride>,
    authority: String,
    azure_service: Option<String>,
    azure_endpoint_suffix: Option<String>,
}

impl fmt::Debug for StorageLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageLocation")
            .field("provider", &self.provider)
            .field("original_url", &"<redacted-location>")
            .field("original_scheme", &self.original_scheme)
            .field("canonical_scheme", &self.canonical_scheme)
            .field("bucket_or_container", &"<configured>")
            .field("account", &self.account.as_ref().map(|_| "<configured>"))
            .field(
                "filesystem",
                &self.filesystem.as_ref().map(|_| "<configured>"),
            )
            .field("prefix", &"<configured>")
            .field("endpoint_override", &self.endpoint_override)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for StorageLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ({})", self.provider, self.original_scheme)
    }
}

/// Consumer-ready URL plus non-secret properties derived from its authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptedStorageLocation {
    /// URL accepted by the selected consumer.
    pub url: String,
    /// Non-secret options required to preserve provider authority semantics.
    pub derived_options: Vec<(String, String)>,
}

/// Errors from parsing or adapting a storage location.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StorageLocationError {
    /// The value is not a valid absolute URL.
    #[error("invalid storage URL: {0}")]
    InvalidUrl(String),
    /// The scheme is outside `LaminarDB`'s object-store support boundary.
    #[error("unsupported storage URL scheme '{0}'")]
    UnsupportedScheme(String),
    /// A bucket, container, or host is absent.
    #[error("{0} storage URL is missing its bucket or container authority")]
    MissingAuthority(&'static str),
    /// User information is forbidden in storage URLs.
    #[error("storage URLs must not embed credentials or user-info")]
    EmbeddedCredentials,
    /// Query parameters are forbidden because they commonly contain signed credentials.
    #[error("storage URLs must not contain query parameters; configure credentials separately")]
    QueryNotAllowed,
    /// Fragments have no object-store semantics.
    #[error("storage URLs must not contain fragments")]
    FragmentNotAllowed,
    /// A local URL does not identify an absolute local path.
    #[error("file URL must identify an absolute local path without a remote authority")]
    InvalidFileUrl,
    /// A Hadoop-style Azure authority is incomplete or contradictory.
    #[error("invalid Azure storage authority: {0}")]
    InvalidAzureAuthority(&'static str),
    /// The parsed provider cannot be represented safely for a particular consumer.
    #[error("{consumer} does not support {scheme} locations in this build: {reason}")]
    ConsumerUnsupported {
        /// Consumer name.
        consumer: &'static str,
        /// Input scheme.
        scheme: String,
        /// Non-secret corrective guidance.
        reason: &'static str,
    },
    /// An endpoint override is malformed or unsafe to retain in diagnostics.
    #[error("invalid storage endpoint override: {0}")]
    InvalidEndpoint(&'static str),
}

impl StorageLocation {
    /// Parse a supported, credential-free absolute storage URL.
    ///
    /// Paths are retained from the input rather than reserialized through
    /// `url::Url`, preserving repeated slashes and dot segments.
    ///
    /// # Errors
    ///
    /// Returns a typed error for unsupported schemes, malformed authorities,
    /// embedded credentials, signed queries, or non-absolute file URLs.
    pub fn parse(input: &str) -> Result<Self, StorageLocationError> {
        let parsed = url::Url::parse(input)
            .map_err(|error| StorageLocationError::InvalidUrl(error.to_string()))?;
        if parsed.query().is_some() {
            return Err(StorageLocationError::QueryNotAllowed);
        }
        if parsed.fragment().is_some() {
            return Err(StorageLocationError::FragmentNotAllowed);
        }
        let (raw_scheme, authority, prefix) = raw_url_components(input)?;
        let original_scheme = raw_scheme.to_ascii_lowercase();
        match original_scheme.as_str() {
            "s3" | "s3a" => Self::parse_bucket(
                input,
                &parsed,
                original_scheme,
                authority,
                prefix,
                StorageProvider::AwsS3,
                "s3",
                "S3",
            ),
            "gs" | "gcs" => Self::parse_bucket(
                input,
                &parsed,
                original_scheme,
                authority,
                prefix,
                StorageProvider::Gcs,
                "gs",
                "GCS",
            ),
            "az" | "abfs" | "abfss" | "wasb" | "wasbs" => {
                Self::parse_azure(input, &parsed, original_scheme, authority, prefix)
            }
            "file" => Self::parse_file(input, &parsed, original_scheme, authority, prefix),
            _ => Err(StorageLocationError::UnsupportedScheme(original_scheme)),
        }
    }

    /// Attach a redacted endpoint override classification.
    ///
    /// # Errors
    ///
    /// Returns an error for credentials, queries, fragments, or non-HTTP(S) endpoints.
    pub fn with_endpoint_override(mut self, endpoint: &str) -> Result<Self, StorageLocationError> {
        let parsed = url::Url::parse(endpoint)
            .map_err(|_| StorageLocationError::InvalidEndpoint("expected an absolute URL"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(StorageLocationError::InvalidEndpoint(
                "only http and https endpoints are supported",
            ));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(StorageLocationError::InvalidEndpoint(
                "credentials and user-info are forbidden",
            ));
        }
        if parsed.host_str().is_none() {
            return Err(StorageLocationError::InvalidEndpoint("host is required"));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(StorageLocationError::InvalidEndpoint(
                "query parameters and fragments are forbidden",
            ));
        }
        self.endpoint_override = Some(EndpointOverride {
            scheme: parsed.scheme().to_string(),
            has_path: !matches!(parsed.path(), "" | "/"),
        });
        Ok(self)
    }

    /// Classify the effective endpoint without exposing its hostname.
    #[must_use]
    pub fn endpoint_class(&self) -> StorageEndpointClass {
        let custom_azure_authority = self.provider == StorageProvider::AzureAdls
            && self
                .azure_endpoint_suffix
                .as_deref()
                .is_some_and(|suffix| !is_native_azure_endpoint_suffix(suffix));
        match (self.provider, self.endpoint_override.is_some()) {
            (StorageProvider::Local, _) => StorageEndpointClass::Local,
            (StorageProvider::AwsS3, true) => StorageEndpointClass::S3Compatible,
            (StorageProvider::AzureAdls | StorageProvider::Gcs, true) => {
                StorageEndpointClass::CustomOrEmulator
            }
            (StorageProvider::AzureAdls, false) if custom_azure_authority => {
                StorageEndpointClass::CustomOrEmulator
            }
            (StorageProvider::AwsS3 | StorageProvider::AzureAdls | StorageProvider::Gcs, false) => {
                StorageEndpointClass::Native
            }
        }
    }

    /// Adapt this location to a downstream storage consumer.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected consumer cannot safely represent the
    /// location, such as an unqualified Azure path for Iceberg/OpenDAL.
    pub fn adapt(
        &self,
        consumer: StorageConsumer,
    ) -> Result<AdaptedStorageLocation, StorageLocationError> {
        match self.provider {
            StorageProvider::AwsS3 => Ok(AdaptedStorageLocation {
                url: self.rebuild(&self.original_scheme, &self.authority),
                derived_options: Vec::new(),
            }),
            StorageProvider::Gcs => Ok(AdaptedStorageLocation {
                url: self.rebuild("gs", &self.authority),
                derived_options: Vec::new(),
            }),
            StorageProvider::AzureAdls => self.adapt_azure(consumer),
            StorageProvider::Local => Ok(AdaptedStorageLocation {
                url: format!("file://{}", raw_path(&self.prefix)),
                derived_options: Vec::new(),
            }),
        }
    }

    fn parse_bucket(
        input: &str,
        parsed: &url::Url,
        original_scheme: String,
        authority: String,
        prefix: String,
        provider: StorageProvider,
        canonical_scheme: &str,
        provider_name: &'static str,
    ) -> Result<Self, StorageLocationError> {
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(StorageLocationError::EmbeddedCredentials);
        }
        if parsed.port().is_some() {
            return Err(StorageLocationError::InvalidUrl(
                "storage URL authorities must not contain a port; use an endpoint override".into(),
            ));
        }
        let bucket = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(StorageLocationError::MissingAuthority(provider_name))?;
        Ok(Self {
            provider,
            original_url: input.to_string(),
            original_scheme,
            canonical_scheme: canonical_scheme.to_string(),
            bucket_or_container: bucket.to_string(),
            account: None,
            filesystem: None,
            prefix,
            endpoint_override: None,
            authority,
            azure_service: None,
            azure_endpoint_suffix: None,
        })
    }

    fn parse_azure(
        input: &str,
        parsed: &url::Url,
        original_scheme: String,
        authority: String,
        prefix: String,
    ) -> Result<Self, StorageLocationError> {
        if parsed.password().is_some() || authority.matches('@').count() > 1 {
            return Err(StorageLocationError::EmbeddedCredentials);
        }
        if original_scheme == "az" && !parsed.username().is_empty() {
            return Err(StorageLocationError::EmbeddedCredentials);
        }
        if parsed.port().is_some() {
            return Err(StorageLocationError::InvalidAzureAuthority(
                "ports belong in an explicit endpoint override",
            ));
        }
        let host = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(StorageLocationError::MissingAuthority("Azure"))?;
        let qualified = !parsed.username().is_empty();
        let (container, account, service, suffix, filesystem) = if qualified {
            let filesystem = parsed.username();
            if filesystem.contains(':') || filesystem.is_empty() {
                return Err(StorageLocationError::InvalidAzureAuthority(
                    "filesystem/container is missing",
                ));
            }
            let mut host_parts = host.splitn(3, '.');
            let account = host_parts.next().unwrap_or_default();
            let service = host_parts.next();
            let suffix = host_parts.next();
            if account.is_empty() || service.is_some() != suffix.is_some() {
                return Err(StorageLocationError::InvalidAzureAuthority(
                    "expected <filesystem>@<account>.<service>.<endpoint-suffix>",
                ));
            }
            if let Some(service) = service {
                let expected_service = match original_scheme.as_str() {
                    "abfs" | "abfss" => "dfs",
                    "wasb" | "wasbs" => "blob",
                    "az" => service,
                    _ => unreachable!("scheme matched before Azure parsing"),
                };
                if !matches!(service, "dfs" | "blob") || service != expected_service {
                    return Err(StorageLocationError::InvalidAzureAuthority(
                        "scheme and blob/dfs service are inconsistent",
                    ));
                }
            }
            (
                filesystem.to_string(),
                Some(account.to_string()),
                service.map(str::to_string),
                suffix.map(str::to_string),
                Some(filesystem.to_string()),
            )
        } else {
            if host.contains('.') && original_scheme != "az" {
                return Err(StorageLocationError::InvalidAzureAuthority(
                    "fully qualified Hadoop URLs require a filesystem/container before '@'",
                ));
            }
            (host.to_string(), None, None, None, None)
        };
        Ok(Self {
            provider: StorageProvider::AzureAdls,
            original_url: input.to_string(),
            original_scheme,
            canonical_scheme: "az".to_string(),
            bucket_or_container: container,
            account,
            filesystem,
            prefix,
            endpoint_override: None,
            authority,
            azure_service: service,
            azure_endpoint_suffix: suffix,
        })
    }

    fn parse_file(
        input: &str,
        parsed: &url::Url,
        original_scheme: String,
        authority: String,
        prefix: String,
    ) -> Result<Self, StorageLocationError> {
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(StorageLocationError::EmbeddedCredentials);
        }
        if parsed
            .host_str()
            .is_some_and(|host| !host.eq_ignore_ascii_case("localhost"))
            || !parsed.path().starts_with('/')
            || parsed.path() == "/"
        {
            return Err(StorageLocationError::InvalidFileUrl);
        }
        Ok(Self {
            provider: StorageProvider::Local,
            original_url: input.to_string(),
            original_scheme,
            canonical_scheme: "file".to_string(),
            bucket_or_container: String::new(),
            account: None,
            filesystem: None,
            prefix,
            endpoint_override: None,
            authority,
            azure_service: None,
            azure_endpoint_suffix: None,
        })
    }

    fn adapt_azure(
        &self,
        consumer: StorageConsumer,
    ) -> Result<AdaptedStorageLocation, StorageLocationError> {
        if consumer == StorageConsumer::Iceberg {
            if self.account.is_none()
                || self.azure_service.is_none()
                || self.azure_endpoint_suffix.is_none()
                || !matches!(
                    self.original_scheme.as_str(),
                    "abfs" | "abfss" | "wasb" | "wasbs"
                )
            {
                return Err(StorageLocationError::ConsumerUnsupported {
                    consumer: "Iceberg/OpenDAL",
                    scheme: self.original_scheme.clone(),
                    reason: "use a fully qualified abfs[s] or wasb[s] URL",
                });
            }
            return Ok(AdaptedStorageLocation {
                url: self.rebuild(&self.original_scheme, &self.authority),
                derived_options: Vec::new(),
            });
        }

        let Some(account) = &self.account else {
            return Ok(AdaptedStorageLocation {
                url: self.rebuild("az", &self.bucket_or_container),
                derived_options: Vec::new(),
            });
        };
        let Some(service) = self.azure_service.as_deref() else {
            return Ok(AdaptedStorageLocation {
                url: self.rebuild("az", &self.bucket_or_container),
                derived_options: vec![
                    ("azure_storage_account_name".into(), account.clone()),
                    (
                        "azure_container_name".into(),
                        self.bucket_or_container.clone(),
                    ),
                ],
            });
        };
        let suffix = self.azure_endpoint_suffix.as_deref().ok_or(
            StorageLocationError::InvalidAzureAuthority("endpoint suffix is missing"),
        )?;
        let transport = match self.original_scheme.as_str() {
            "abfs" | "wasb" => "http",
            "abfss" | "wasbs" | "az" => "https",
            _ => unreachable!("scheme matched before Azure adaptation"),
        };
        Ok(AdaptedStorageLocation {
            url: self.rebuild("az", &self.bucket_or_container),
            derived_options: vec![
                ("azure_storage_account_name".into(), account.clone()),
                (
                    "azure_container_name".into(),
                    self.bucket_or_container.clone(),
                ),
                (
                    "azure_endpoint".into(),
                    format!("{transport}://{account}.{service}.{suffix}"),
                ),
            ],
        })
    }

    fn rebuild(&self, scheme: &str, authority: &str) -> String {
        format!("{scheme}://{authority}{}", raw_path(&self.prefix))
    }
}

fn raw_url_components(input: &str) -> Result<(String, String, String), StorageLocationError> {
    let separator = input.find("://").ok_or_else(|| {
        StorageLocationError::InvalidUrl("absolute storage URL requires '://'".into())
    })?;
    let scheme = &input[..separator];
    if scheme.is_empty() {
        return Err(StorageLocationError::InvalidUrl("scheme is missing".into()));
    }
    let remainder = &input[separator + 3..];
    let authority_end = remainder.find('/').unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    let path = remainder.get(authority_end..).unwrap_or_default();
    let prefix = path.strip_prefix('/').unwrap_or(path);
    Ok((
        scheme.to_string(),
        authority.to_string(),
        prefix.to_string(),
    ))
}

fn raw_path(prefix: &str) -> String {
    format!("/{prefix}")
}

fn is_native_azure_endpoint_suffix(suffix: &str) -> bool {
    [
        "core.windows.net",
        "core.usgovcloudapi.net",
        "core.chinacloudapi.cn",
        "core.cloudapi.de",
        "fabric.microsoft.com",
    ]
    .iter()
    .any(|native| suffix.eq_ignore_ascii_case(native))
}

#[cfg(test)]
mod tests;
