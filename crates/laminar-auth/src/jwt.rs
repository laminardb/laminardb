//! JWT-based authentication provider.
//!
//! Validates JWT tokens using HMAC (HS256) or RSA (RS256) signatures,
//! extracts claims, and maps them to an [`Identity`] with roles.

use std::collections::HashSet;

use jsonwebtoken::{decode, Algorithm, DecodingKey, TokenData, Validation};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::identity::{AuthError, AuthProvider, Credentials, Identity};
use crate::rbac::Role;

/// Configuration for the JWT authentication provider.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// Algorithm: HS256 (symmetric) or RS256 (asymmetric).
    pub algorithm: Algorithm,

    /// Decoding key material.
    ///
    /// - For HS256: the shared secret (raw bytes).
    /// - For RS256: the PEM-encoded public key.
    pub key_material: Vec<u8>,

    /// Expected issuer (`iss` claim). `None` = don't validate issuer.
    pub expected_issuer: Option<String>,

    /// Expected audience (`aud` claim). `None` = don't validate audience.
    pub expected_audience: Option<String>,

    /// JWT claim name that contains the user's roles (default: `"roles"`).
    /// The claim value should be a JSON array of strings.
    pub roles_claim: String,
}

impl JwtConfig {
    /// Create a config for HS256 (HMAC-SHA256) with the given secret.
    pub fn hs256(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            algorithm: Algorithm::HS256,
            key_material: secret.into(),
            expected_issuer: None,
            expected_audience: None,
            roles_claim: "roles".to_string(),
        }
    }

    /// Create a config for RS256 (RSA-SHA256) with the given PEM public key.
    pub fn rs256(pem_public_key: impl Into<Vec<u8>>) -> Self {
        Self {
            algorithm: Algorithm::RS256,
            key_material: pem_public_key.into(),
            expected_issuer: None,
            expected_audience: None,
            roles_claim: "roles".to_string(),
        }
    }

    /// Set the expected issuer.
    #[must_use]
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.expected_issuer = Some(issuer.into());
        self
    }

    /// Set the expected audience.
    #[must_use]
    pub fn with_audience(mut self, audience: impl Into<String>) -> Self {
        self.expected_audience = Some(audience.into());
        self
    }

    /// Set the claim name used for roles.
    #[must_use]
    pub fn with_roles_claim(mut self, claim: impl Into<String>) -> Self {
        self.roles_claim = claim.into();
        self
    }
}

/// JWT claims structure expected in tokens.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    /// Subject (user ID).
    sub: String,

    /// Expiry (UTC epoch seconds).
    #[serde(default)]
    exp: Option<u64>,

    /// Issued at (UTC epoch seconds).
    #[serde(default)]
    iat: Option<u64>,

    /// Issuer.
    #[serde(default)]
    iss: Option<String>,

    /// Audience.
    #[serde(default)]
    aud: Option<Audience>,

    /// Roles (custom claim, configurable name).
    /// We capture all remaining claims and extract roles from them.
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// Audience can be a single string or an array of strings.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum Audience {
    /// Single audience string.
    Single(String),
    /// Multiple audience strings.
    Multiple(Vec<String>),
}

/// JWT authentication provider.
///
/// Validates JWT bearer tokens and produces [`Identity`] values.
pub struct JwtAuthProvider {
    config: JwtConfig,
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtAuthProvider {
    /// Create a new JWT auth provider from the given config.
    pub fn new(config: JwtConfig) -> Result<Self, AuthError> {
        let decoding_key = match config.algorithm {
            Algorithm::HS256 => DecodingKey::from_secret(&config.key_material),
            Algorithm::RS256 => DecodingKey::from_rsa_pem(&config.key_material)
                .map_err(|e| AuthError::Configuration(format!("invalid RSA PEM key: {e}")))?,
            other => {
                return Err(AuthError::Configuration(format!(
                    "unsupported algorithm: {other:?}"
                )));
            }
        };

        let mut validation = Validation::new(config.algorithm);

        if let Some(ref iss) = config.expected_issuer {
            validation.set_issuer(&[iss]);
        }

        if let Some(ref aud) = config.expected_audience {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        // We handle expiry in our own Identity logic as well, but let
        // jsonwebtoken enforce it too for defense in depth.
        validation.validate_exp = true;

        Ok(Self {
            config,
            decoding_key,
            validation,
        })
    }

    /// Extract roles from the JWT claims.
    fn extract_roles(&self, claims: &Claims) -> HashSet<Role> {
        let mut roles = HashSet::new();

        if let Some(value) = claims.extra.get(&self.config.roles_claim) {
            if let Some(arr) = value.as_array() {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        roles.insert(Role::parse(s));
                    }
                }
            } else if let Some(s) = value.as_str() {
                // Single role as string
                roles.insert(Role::parse(s));
            }
        }

        // Default to ReadOnly if no roles found
        if roles.is_empty() {
            roles.insert(Role::ReadOnly);
        }

        roles
    }
}

#[async_trait::async_trait]
impl AuthProvider for JwtAuthProvider {
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, AuthError> {
        let Credentials::Bearer(token) = credentials else {
            return Err(AuthError::InvalidCredentials(
                "JWT provider requires Bearer token".to_string(),
            ));
        };

        let token_data: TokenData<Claims> = decode(token, &self.decoding_key, &self.validation)
            .map_err(|e| {
                warn!(error = %e, "JWT validation failed");
                AuthError::InvalidCredentials(format!("JWT validation failed: {e}"))
            })?;

        let claims = token_data.claims;
        let roles = self.extract_roles(&claims);

        let mut identity = Identity::new(&claims.sub);
        identity.roles = roles;

        if let Some(exp) = claims.exp {
            #[allow(clippy::cast_possible_wrap)]
            {
                identity.expires_at = Some(exp as i64);
            }
        }

        // Copy extra claims as attributes (skip the roles claim)
        for (key, value) in &claims.extra {
            if key != &self.config.roles_claim {
                if let Some(s) = value.as_str() {
                    identity.attributes.insert(key.clone(), s.to_string());
                } else {
                    identity.attributes.insert(key.clone(), value.to_string());
                }
            }
        }

        Ok(identity)
    }

    fn name(&self) -> &'static str {
        "jwt"
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    const TEST_SECRET: &[u8] = b"super-secret-key-for-testing-only";

    fn make_token(claims: &serde_json::Value) -> String {
        encode(
            &Header::default(), // HS256
            claims,
            &EncodingKey::from_secret(TEST_SECRET),
        )
        .unwrap()
    }

    fn make_provider() -> JwtAuthProvider {
        JwtAuthProvider::new(JwtConfig::hs256(TEST_SECRET)).unwrap()
    }

    #[tokio::test]
    async fn test_valid_token() {
        let provider = make_provider();
        let exp = chrono::Utc::now().timestamp() + 3600;
        let token = make_token(&serde_json::json!({
            "sub": "alice",
            "exp": exp,
            "roles": ["admin", "read_write"],
        }));

        let identity = provider
            .authenticate(&Credentials::Bearer(token))
            .await
            .unwrap();

        assert_eq!(identity.user_id, "alice");
        assert!(identity.has_role(&Role::Admin));
        assert!(identity.has_role(&Role::ReadWrite));
    }

    #[tokio::test]
    async fn test_expired_token() {
        let provider = make_provider();
        let token = make_token(&serde_json::json!({
            "sub": "bob",
            "exp": 0, // expired at epoch
        }));

        let result = provider.authenticate(&Credentials::Bearer(token)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_signature() {
        let provider = make_provider();
        let token = make_token(&serde_json::json!({
            "sub": "charlie",
            "exp": chrono::Utc::now().timestamp() + 3600,
        }));

        // Tamper with the token
        let tampered = format!("{token}x");
        let result = provider.authenticate(&Credentials::Bearer(tampered)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_default_role_when_no_roles_claim() {
        let provider = make_provider();
        let exp = chrono::Utc::now().timestamp() + 3600;
        let token = make_token(&serde_json::json!({
            "sub": "dave",
            "exp": exp,
        }));

        let identity = provider
            .authenticate(&Credentials::Bearer(token))
            .await
            .unwrap();

        assert!(identity.has_role(&Role::ReadOnly));
    }

    #[tokio::test]
    async fn test_wrong_credential_type() {
        let provider = make_provider();
        let result = provider
            .authenticate(&Credentials::Basic {
                username: "user".to_string(),
                password: "pass".to_string(),
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_issuer_validation() {
        let config = JwtConfig::hs256(TEST_SECRET).with_issuer("laminardb");
        let provider = JwtAuthProvider::new(config).unwrap();
        let exp = chrono::Utc::now().timestamp() + 3600;

        // Token with correct issuer
        let good_token = make_token(&serde_json::json!({
            "sub": "eve",
            "exp": exp,
            "iss": "laminardb",
        }));
        assert!(provider
            .authenticate(&Credentials::Bearer(good_token))
            .await
            .is_ok());

        // Token with wrong issuer
        let bad_token = make_token(&serde_json::json!({
            "sub": "eve",
            "exp": exp,
            "iss": "wrong-issuer",
        }));
        assert!(provider
            .authenticate(&Credentials::Bearer(bad_token))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_extra_claims_as_attributes() {
        let provider = make_provider();
        let exp = chrono::Utc::now().timestamp() + 3600;
        let token = make_token(&serde_json::json!({
            "sub": "frank",
            "exp": exp,
            "tenant_id": "acme-corp",
            "department": "engineering",
        }));

        let identity = provider
            .authenticate(&Credentials::Bearer(token))
            .await
            .unwrap();

        assert_eq!(identity.attributes.get("tenant_id").unwrap(), "acme-corp");
        assert_eq!(
            identity.attributes.get("department").unwrap(),
            "engineering"
        );
    }
}
