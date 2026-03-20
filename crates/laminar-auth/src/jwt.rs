//! Minimal JWT verification (HMAC-SHA256 only).
//!
//! Validates the structure and signature of a JWT token against a shared
//! secret key. This is intentionally minimal — it covers the HS256
//! algorithm only and is meant for internal service-to-service auth.
//!
//! **Note:** This module is infrastructure only and is not yet wired into
//! the server authentication pipeline. Wiring is a follow-up task.
//!
//! # Known Limitations
//!
//! - **Static secret only.** The current implementation uses a caller-provided
//!   symmetric key. There is no JWKS endpoint fetching or key rotation support.
//!
//! # TODO: JWKS Support
//!
//! Future work should add a `JwksProvider` that:
//! 1. Fetches the JWKS document from a configurable URL on startup.
//! 2. Caches keys by `kid` (Key ID) with a configurable TTL (default: 1 hour).
//! 3. On encountering an unknown `kid`, performs a **rate-limited** re-fetch
//!    (at most once per N seconds, default: 30s) to handle key rotation.
//! 4. Supports RS256/RS384/RS512 in addition to HS256.
//! 5. Rejects tokens whose `kid` is not found even after re-fetch (fail-closed).

use base64::Engine;
use sha2::Digest;

use crate::identity::AuthError;

/// Verify a JWT token's HMAC-SHA256 signature against the given key.
///
/// Returns the decoded payload as a JSON string on success.
///
/// # Security
///
/// This function **enforces the algorithm**: it decodes the JWT header and
/// verifies that `alg` is exactly `"HS256"`. Tokens with `alg: "none"`,
/// `alg: "HS384"`, or any other algorithm are rejected outright. This
/// prevents the classic algorithm-confusion attack where an attacker sets
/// `alg: none` to skip signature validation or `alg: HS256` against an
/// RS256-configured server to trick it into using the RSA public key as
/// an HMAC secret.
///
/// # Errors
///
/// Returns [`AuthError::AuthenticationFailed`] if:
/// - The token is malformed (not three dot-separated parts)
/// - The header `alg` field is not exactly `"HS256"`
/// - The signature does not match the key
pub fn verify_hs256(token: &str, key: &[u8]) -> Result<String, AuthError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(AuthError::AuthenticationFailed(
            "JWT must have exactly 3 parts (header.payload.signature)".to_string(),
        ));
    }

    // --- Algorithm enforcement (CVE-2015-9235 / algorithm confusion) ---
    // Decode the header and verify the `alg` claim is exactly "HS256".
    // This prevents:
    //   - `alg: "none"` → skip signature verification
    //   - `alg: "HS384"` / `alg: "RS256"` → wrong algorithm acceptance
    let header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|e| {
            AuthError::AuthenticationFailed(format!("JWT header is not valid base64url: {e}"))
        })?;
    let header_json: serde_json::Value = serde_json::from_slice(&header_bytes).map_err(|e| {
        AuthError::AuthenticationFailed(format!("JWT header is not valid JSON: {e}"))
    })?;
    match header_json.get("alg").and_then(|v| v.as_str()) {
        Some("HS256") => {} // expected algorithm — proceed
        Some(other) => {
            return Err(AuthError::AuthenticationFailed(format!(
                "JWT algorithm mismatch: expected HS256, got {other:?}. \
                 Only HS256 is accepted; tokens with alg=none or other algorithms are rejected."
            )));
        }
        None => {
            return Err(AuthError::AuthenticationFailed(
                "JWT header missing 'alg' field".to_string(),
            ));
        }
    }

    let header_payload = format!("{}.{}", parts[0], parts[1]);
    let expected_sig = hmac_sha256(key, header_payload.as_bytes());

    let actual_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|e| {
            AuthError::AuthenticationFailed(format!("JWT signature is not valid base64url: {e}"))
        })?;

    if !constant_time_eq(&expected_sig, &actual_sig) {
        return Err(AuthError::AuthenticationFailed(
            "JWT signature verification failed".to_string(),
        ));
    }

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| {
            AuthError::AuthenticationFailed(format!("JWT payload is not valid base64url: {e}"))
        })?;

    String::from_utf8(payload_bytes).map_err(|e| {
        AuthError::AuthenticationFailed(format!("JWT payload is not valid UTF-8: {e}"))
    })
}

/// Create a signed JWT token with the given JSON payload using HMAC-SHA256.
///
/// The header is always `{"alg":"HS256","typ":"JWT"}`.
pub fn sign_hs256(payload_json: &str, key: &[u8]) -> String {
    let b64 = &base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header = r#"{"alg":"HS256","typ":"JWT"}"#;
    let h = b64.encode(header.as_bytes());
    let p = b64.encode(payload_json.as_bytes());
    let signing_input = format!("{h}.{p}");
    let sig = hmac_sha256(key, signing_input.as_bytes());
    let s = b64.encode(sig);
    format!("{signing_input}.{s}")
}

/// HMAC-SHA256 using the double-hash construction:
/// `HMAC(K, m) = H((K ^ opad) || H((K ^ ipad) || m))`
fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    const BLOCK_SIZE: usize = 64;

    let key_block = if key.len() > BLOCK_SIZE {
        let mut h = sha2::Sha256::new();
        h.update(key);
        let hashed = h.finalize();
        let mut block = [0u8; BLOCK_SIZE];
        block[..32].copy_from_slice(&hashed);
        block
    } else {
        let mut block = [0u8; BLOCK_SIZE];
        block[..key.len()].copy_from_slice(key);
        block
    };

    let mut ipad = [0x36u8; BLOCK_SIZE];
    let mut opad = [0x5cu8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }

    let mut inner = sha2::Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = sha2::Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().to_vec()
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify_roundtrip() {
        let key = b"my-secret-key-for-testing";
        let payload = r#"{"sub":"alice","role":"admin"}"#;

        let token = sign_hs256(payload, key);
        let decoded = verify_hs256(&token, key).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_wrong_key_fails_verification() {
        let key_a = b"key-A-used-for-signing";
        let key_b = b"key-B-used-for-verification";
        let payload = r#"{"sub":"alice","role":"admin"}"#;

        let token = sign_hs256(payload, key_a);
        let result = verify_hs256(&token, key_b);
        assert!(
            result.is_err(),
            "token signed with key A must fail verification with key B"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("signature verification failed"),
            "error must indicate signature failure, got: {err}"
        );
    }

    #[test]
    fn test_malformed_token_rejected() {
        let key = b"some-key";
        assert!(verify_hs256("not.a.valid.jwt.token", key).is_err());
        assert!(verify_hs256("only-one-part", key).is_err());
        assert!(verify_hs256("two.parts", key).is_err());
    }

    #[test]
    fn test_tampered_payload_fails() {
        let key = b"my-secret-key";
        let payload = r#"{"sub":"alice"}"#;
        let token = sign_hs256(payload, key);

        // Tamper with the payload part
        let parts: Vec<&str> = token.split('.').collect();
        let b64 = &base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let tampered_payload = b64.encode(r#"{"sub":"mallory"}"#.as_bytes());
        let tampered_token = format!("{}.{}.{}", parts[0], tampered_payload, parts[2]);

        assert!(
            verify_hs256(&tampered_token, key).is_err(),
            "tampered payload must fail verification"
        );
    }

    // --- Algorithm confusion attack tests (CVE-2015-9235) ---

    /// Craft a JWT with a custom header (for attack simulation).
    fn craft_token_with_header(header_json: &str, payload_json: &str, key: &[u8]) -> String {
        let b64 = &base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let h = b64.encode(header_json.as_bytes());
        let p = b64.encode(payload_json.as_bytes());
        let signing_input = format!("{h}.{p}");
        let sig = hmac_sha256(key, signing_input.as_bytes());
        let s = b64.encode(sig);
        format!("{signing_input}.{s}")
    }

    #[test]
    fn test_alg_none_rejected() {
        // An attacker sets alg=none and provides an empty signature.
        // A naive implementation might skip signature verification entirely.
        let b64 = &base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = b64.encode(r#"{"alg":"none","typ":"JWT"}"#.as_bytes());
        let payload = b64.encode(r#"{"sub":"attacker","role":"admin"}"#.as_bytes());
        // Empty signature (the "none" algorithm means no signature)
        let token = format!("{header}.{payload}.");

        let key = b"server-secret";
        let result = verify_hs256(&token, key);
        assert!(result.is_err(), "alg=none token must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("algorithm mismatch"),
            "error must mention algorithm mismatch, got: {err}"
        );
    }

    #[test]
    fn test_alg_none_case_variations_rejected() {
        // Test case variations that a naive string comparison might miss.
        let b64 = &base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let key = b"server-secret";

        for alg in &["none", "None", "NONE", "nOnE"] {
            let header = b64.encode(format!(r#"{{"alg":"{alg}","typ":"JWT"}}"#).as_bytes());
            let payload = b64.encode(r#"{"sub":"attacker"}"#.as_bytes());
            let token = format!("{header}.{payload}.");

            let result = verify_hs256(&token, key);
            assert!(result.is_err(), "alg={alg} token must be rejected");
        }
    }

    #[test]
    fn test_alg_hs384_rejected() {
        // A token claiming HS384 must be rejected even with valid HS256 signature.
        let key = b"server-secret";
        let payload = r#"{"sub":"alice"}"#;
        let token = craft_token_with_header(r#"{"alg":"HS384","typ":"JWT"}"#, payload, key);

        let result = verify_hs256(&token, key);
        assert!(result.is_err(), "alg=HS384 token must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("algorithm mismatch"),
            "error must mention algorithm mismatch, got: {err}"
        );
    }

    #[test]
    fn test_alg_rs256_rejected() {
        // An attacker might try to trick an HS256 verifier into using a
        // public key as an HMAC secret. We reject non-HS256 algorithms.
        let key = b"rsa-public-key-pretending-to-be-hmac-secret";
        let payload = r#"{"sub":"attacker","role":"admin"}"#;
        let token = craft_token_with_header(r#"{"alg":"RS256","typ":"JWT"}"#, payload, key);

        let result = verify_hs256(&token, key);
        assert!(result.is_err(), "alg=RS256 token must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("algorithm mismatch"),
            "error must mention algorithm mismatch, got: {err}"
        );
    }

    #[test]
    fn test_missing_alg_header_rejected() {
        // A token with no alg field in the header.
        let key = b"server-secret";
        let payload = r#"{"sub":"alice"}"#;
        let token = craft_token_with_header(r#"{"typ":"JWT"}"#, payload, key);

        let result = verify_hs256(&token, key);
        assert!(result.is_err(), "token without alg header must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("missing 'alg' field"),
            "error must mention missing alg, got: {err}"
        );
    }
}
