//! Counterparty profiles: what a venue requires beyond standard FIX at logon, and whether it
//! retransmits.
//!
//! Binance Spot drop copy authenticates the Logon with an Ed25519 signature over MsgType,
//! SenderCompID, TargetCompID, MsgSeqNum, and SendingTime joined by SOH. The engine fills the
//! header before `toAdmin`. Its later MsgSeqNum update for a Logon with ResetSeqNumFlag=Y writes
//! the same number, because the store was already reset when the Logon was built, so a signature
//! over the header seen in `toAdmin` matches the message sent.
//!
//! Binance requires ResetSeqNumFlag=Y at every logon and answers no ResendRequest: each logon
//! starts a new sequence series, and what it sent while no session was logged on is never
//! delivered.

use std::collections::BTreeMap;
use std::path::PathBuf;

use aws_lc_rs::signature::Ed25519KeyPair;
use base64::Engine as _;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer;

use super::config::{Credentials, FixSourceConfig};
use super::engine::{MessageRef, OutboundMessage, SessionKey};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

const BINANCE_TARGET_COMP_ID: &str = "SPOT";
const SOH: u8 = 1;

/// Session behaviour a counterparty requires beyond standard FIX.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Venue {
    /// Username/Password logon; the counterparty answers resend requests.
    Standard,
    /// Binance Spot FIX drop copy, signed with the Ed25519 key in `key_file`.
    Binance { key_file: PathBuf },
}

fn option<'a>(config: &'a ConnectorConfig, name: &str) -> Option<&'a str> {
    config
        .get(name)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn invalid(reason: String) -> ConnectorError {
    ConnectorError::ConfigurationError(reason)
}

impl Venue {
    /// Parse `venue` and check the session options the venue dictates.
    ///
    /// # Errors
    /// Returns a configuration error for an unknown venue or options it does not accept.
    pub(crate) fn parse(
        config: &ConnectorConfig,
        session: &SessionKey,
    ) -> Result<Self, ConnectorError> {
        let key_file = option(config, "logon.key.file").map(PathBuf::from);
        match option(config, "venue") {
            None => match key_file {
                Some(_) => Err(invalid(
                    "'logon.key.file' applies only to 'venue' = 'binance'".into(),
                )),
                None => Ok(Self::Standard),
            },
            Some("binance") => {
                binance_session(config, session)?;
                let key_file = key_file.ok_or_else(|| {
                    invalid("'venue' = 'binance' needs 'logon.key.file' (Ed25519 PEM)".into())
                })?;
                Ok(Self::Binance { key_file })
            }
            Some(other) => Err(invalid(format!(
                "FIX source 'venue' must be empty or 'binance', got '{other}'"
            ))),
        }
    }

    /// Profile name bound into the capture contract; `None` for standard FIX.
    pub(crate) fn name(&self) -> Option<&'static str> {
        match self {
            Self::Standard => None,
            Self::Binance { .. } => Some("binance"),
        }
    }

    /// Whether the counterparty retransmits after a reconnect. Without retransmission every
    /// sequence series after the first may follow lost messages.
    pub(crate) fn retransmits(&self) -> bool {
        matches!(self, Self::Standard)
    }

    /// Engine settings the venue fixes, and validation defaults for fields it adds without a
    /// new dictionary. An option set explicitly keeps its value unless the venue forbids it.
    ///
    /// # Errors
    /// Returns a configuration error for a setting the venue does not accept.
    pub(crate) fn constrain_engine(
        &self,
        config: &ConnectorConfig,
        engine: &mut BTreeMap<&'static str, String>,
    ) -> Result<(), ConnectorError> {
        if matches!(self, Self::Standard) {
            return Ok(());
        }
        let heartbeat: u64 = engine
            .get("HeartBtInt")
            .and_then(|value| value.parse().ok())
            .unwrap_or_default();
        if !(5..=60).contains(&heartbeat) {
            return Err(invalid(
                "Binance accepts 'heartbeat.interval.seconds' from 5 to 60".into(),
            ));
        }
        if option(config, "reset.on.logon") == Some("false") {
            return Err(invalid(
                "Binance requires ResetSeqNumFlag=Y at every logon; remove 'reset.on.logon' = 'false'"
                    .into(),
            ));
        }
        engine.insert("ResetOnLogon", "Y".into());
        // A rejected application message is consumed, so a field Binance adds before its
        // published dictionary would lose a drop-copy report.
        for (option_name, setting, value) in [
            ("allow.unknown.message.fields", "AllowUnknownMsgFields", "Y"),
            (
                "validate.user.defined.fields",
                "ValidateUserDefinedFields",
                "N",
            ),
        ] {
            if option(config, option_name).is_none() {
                engine.insert(setting, value.into());
            }
        }
        Ok(())
    }
}

fn binance_session(config: &ConnectorConfig, session: &SessionKey) -> Result<(), ConnectorError> {
    if session.begin_string != "FIX.4.4" {
        return Err(invalid(
            "Binance sessions use 'begin.string' = 'FIX.4.4'".into(),
        ));
    }
    if session.target_comp_id != BINANCE_TARGET_COMP_ID {
        return Err(invalid(format!(
            "Binance sessions use 'target.comp.id' = '{BINANCE_TARGET_COMP_ID}'"
        )));
    }
    let sender = &session.sender_comp_id;
    let valid_sender = (1..=8).contains(&sender.len())
        && sender
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if !valid_sender {
        return Err(invalid(
            "Binance 'sender.comp.id' is 1 to 8 letters, digits, '-' or '_', unique among the \
             account's sessions"
                .into(),
        ));
    }
    if option(config, "username").is_none() {
        return Err(invalid(
            "Binance sessions need 'username' = the Ed25519 API key".into(),
        ));
    }
    if option(config, "password").is_some() {
        return Err(invalid(
            "Binance sessions authenticate with 'logon.key.file', not 'password'".into(),
        ));
    }
    Ok(())
}

/// Fields this source adds to its Logon, with any signing key loaded at start.
pub(crate) struct Logon {
    credentials: Option<Credentials>,
    /// MaxMessageSize (383), which Binance's Logon does not define.
    max_message_bytes: Option<String>,
    binance: Option<Ed25519KeyPair>,
}

impl Logon {
    /// Prepare the Logon fields, reading the signing key like the TLS files: at start.
    ///
    /// # Errors
    /// Fails when the signing key cannot be read or is not an Ed25519 PKCS#8 key.
    pub(crate) fn load(config: &FixSourceConfig) -> Result<Self, ConnectorError> {
        let credentials = config.credentials.clone();
        match &config.venue {
            Venue::Standard => Ok(Self {
                credentials,
                // The engine buffers a whole message before any local limit applies, so ask the
                // counterparty for the bound.
                max_message_bytes: Some(config.max_message_bytes.to_string()),
                binance: None,
            }),
            Venue::Binance { key_file } => {
                let unusable = |reason: &str| {
                    invalid(format!("'logon.key.file' {} {reason}", key_file.display()))
                };
                let der = PrivatePkcs8KeyDer::from_pem_file(key_file)
                    .map_err(|_| unusable("is not a readable PKCS#8 PEM private key"))?;
                let key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(der.secret_pkcs8_der())
                    .map_err(|_| unusable("is not an Ed25519 private key"))?;
                Ok(Self {
                    credentials,
                    max_message_bytes: None,
                    binance: Some(key),
                })
            }
        }
    }

    /// Add this source's fields to an outbound Logon.
    pub(crate) fn apply(&self, message: &mut OutboundMessage<'_>) -> Result<(), String> {
        let credentials = self.credentials.as_ref();
        let username = credentials
            .and_then(|credentials| credentials.username.as_deref())
            .map(|value| (553, value.to_string()));
        let password = credentials
            .and_then(|credentials| credentials.password.as_ref())
            .map(|value| (554, value.expose().to_string()));
        let max_size = self.max_message_bytes.clone().map(|value| (383, value));
        let signature = match &self.binance {
            Some(key) => {
                let signature = binance_logon_signature(key, message.view())?;
                vec![
                    (95, signature.len().to_string()),
                    (96, signature),
                    // MessageHandling SEQUENTIAL: required, though a drop copy sends no orders.
                    (25035, "2".to_string()),
                    (9406, "Y".to_string()),
                ]
            }
            None => Vec::new(),
        };
        for (tag, value) in max_size
            .into_iter()
            .chain(username)
            .chain(password)
            .chain(signature)
        {
            message
                .set_body(tag, value.as_bytes())
                .map_err(|error| format!("cannot set logon field {tag}: {error}"))?;
        }
        Ok(())
    }
}

fn binance_logon_signature(key: &Ed25519KeyPair, logon: MessageRef<'_>) -> Result<String, String> {
    let header = logon.header().map_err(|error| error.to_string())?;
    let mut fields = [&[][..]; 4];
    for (slot, tag) in fields.iter_mut().zip([49, 56, 34, 52]) {
        *slot = header
            .field(tag)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("Logon has no header field {tag} to sign"))?;
    }
    Ok(sign_binance(key, fields))
}

/// Base64 Ed25519 signature of `A`, SenderCompID, TargetCompID, MsgSeqNum, and SendingTime
/// joined by SOH.
fn sign_binance(key: &Ed25519KeyPair, [sender, target, seq, sending_time]: [&[u8]; 4]) -> String {
    let mut payload = b"A".to_vec();
    for value in [sender, target, seq, sending_time] {
        payload.push(SOH);
        payload.extend_from_slice(value);
    }
    base64::engine::general_purpose::STANDARD.encode(key.sign(&payload).as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Base64 PKCS#8 (v1, as OpenSSL writes it) of the illustrative key Binance publishes with
    /// its signature test vector.
    const BINANCE_EXAMPLE_PKCS8: &str =
        "MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";

    fn binance() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("fix");
        for (key, value) in [
            ("venue", "binance"),
            ("sender.comp.id", "EXAMPLE"),
            ("target.comp.id", "SPOT"),
            ("username", "api-key"),
            ("logon.key.file", "binance.pem"),
        ] {
            config.set(key, value);
        }
        config
    }

    fn session(config: &ConnectorConfig) -> SessionKey {
        SessionKey {
            begin_string: config.get("begin.string").unwrap_or("FIX.4.4").to_string(),
            sender_comp_id: config.get("sender.comp.id").unwrap_or_default().to_string(),
            target_comp_id: config.get("target.comp.id").unwrap_or_default().to_string(),
            qualifier: None,
        }
    }

    #[test]
    fn signature_matches_the_published_binance_vector() {
        let der = base64::engine::general_purpose::STANDARD
            .decode(BINANCE_EXAMPLE_PKCS8)
            .expect("base64");
        let key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der).expect("key");
        assert_eq!(
            sign_binance(
                &key,
                [b"EXAMPLE", b"SPOT", b"1", b"20240627-11:17:25.223"]
            ),
            "4MHXelVVcpkdwuLbl6n73HQUXUf1dse2PCgT1DYqW9w8AVZ1RACFGM+5UdlGPrQHrgtS3CvsRURC1oj73j8gCA=="
        );
    }

    #[test]
    fn binance_requires_its_session_shape() {
        let parse = |config: &ConnectorConfig| Venue::parse(config, &session(config));
        assert_eq!(
            parse(&binance()).expect("binance"),
            Venue::Binance {
                key_file: "binance.pem".into()
            }
        );
        for (key, value) in [
            ("target.comp.id", "BINANCE"),
            ("sender.comp.id", "TOO_LONG_ID"),
            ("sender.comp.id", "BAD.ID"),
            ("begin.string", "FIX.4.2"),
            ("username", ""),
            ("password", "secret"),
            ("logon.key.file", ""),
            ("venue", "coinbase"),
        ] {
            let mut config = binance();
            config.set(key, value);
            assert!(parse(&config).is_err(), "{key} = {value}");
        }
        let mut standard = ConnectorConfig::new("fix");
        assert_eq!(parse(&standard).expect("standard"), Venue::Standard);
        standard.set("logon.key.file", "key.pem");
        assert!(parse(&standard).is_err(), "a key without a venue");
    }

    #[test]
    fn binance_resets_at_every_logon_and_tolerates_unpublished_fields() {
        let venue = Venue::Binance {
            key_file: "k".into(),
        };
        let engine = |config: &ConnectorConfig| {
            let mut engine = BTreeMap::from([
                ("HeartBtInt", "30".to_string()),
                ("ResetOnLogon", "N".to_string()),
                ("AllowUnknownMsgFields", "N".to_string()),
                ("ValidateUserDefinedFields", "Y".to_string()),
            ]);
            venue.constrain_engine(config, &mut engine).map(|()| engine)
        };
        let constrained = engine(&binance()).expect("constrain");
        assert_eq!(constrained["ResetOnLogon"], "Y");
        assert_eq!(constrained["AllowUnknownMsgFields"], "Y");
        assert_eq!(constrained["ValidateUserDefinedFields"], "N");

        let mut strict = binance();
        strict.set("allow.unknown.message.fields", "false");
        strict.set("validate.user.defined.fields", "true");
        let strict = engine(&strict).expect("explicit validation");
        assert_eq!(strict["AllowUnknownMsgFields"], "N");
        assert_eq!(strict["ValidateUserDefinedFields"], "Y");

        let mut no_reset = binance();
        no_reset.set("reset.on.logon", "false");
        assert!(engine(&no_reset).is_err());
        let mut slow = BTreeMap::from([("HeartBtInt", "120".to_string())]);
        assert!(venue.constrain_engine(&binance(), &mut slow).is_err());
        assert!(!venue.retransmits());
        assert!(Venue::Standard.retransmits());
    }
}
