//! Counterparty behaviour beyond standard FIX: extra Logon fields, venue profiles, and whether
//! the counterparty retransmits.
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
/// Option prefix of static Logon body fields: `logon.field.<tag>`.
pub(crate) const LOGON_FIELD_PREFIX: &str = "logon.field.";
/// Standard header and trailer tags, and Logon fields the engine or another option sets.
const RESERVED_LOGON_TAGS: &[i32] = &[
    8, 9, 10, 34, 35, 43, 49, 50, 52, 56, 57, 89, 90, 91, 93, 97, 115, 116, 122, 128, 129, 142,
    143, 144, 145, 212, 213, 347, 369, 370, 627, 628, 629, 630, 1128, 1129,
    1156, // header, trailer
    95, 96, 98, 108, 141, 383, 553, 554, 789,
    1137, // Logon fields owned by the engine or options
];

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

    /// Logon body tags the profile sets.
    fn logon_tags(&self) -> &'static [i32] {
        match self {
            Self::Standard => &[],
            Self::Binance { .. } => &[25035, 9406],
        }
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

/// Whether the counterparty answers ResendRequest with the original messages
/// (`counterparty.resends`). Binance never does.
///
/// # Errors
/// Returns a configuration error for a value other than `true` or `false`, or `true` for Binance.
pub(crate) fn resends(config: &ConnectorConfig, venue: &Venue) -> Result<bool, ConnectorError> {
    let configured = match option(config, "counterparty.resends") {
        None => None,
        Some("true") => Some(true),
        Some("false") => Some(false),
        Some(other) => {
            return Err(invalid(format!(
                "FIX source 'counterparty.resends' must be 'true' or 'false', got '{other}'"
            )))
        }
    };
    match (venue, configured) {
        (Venue::Standard, configured) => Ok(configured.unwrap_or(true)),
        (Venue::Binance { .. }, Some(true)) => Err(invalid(
            "Binance answers no ResendRequest; remove 'counterparty.resends' = 'true'".into(),
        )),
        (Venue::Binance { .. }, _) => Ok(false),
    }
}

/// Static Logon body fields from `logon.field.<tag>` options.
///
/// # Errors
/// Returns a configuration error for a tag that is not a positive integer, a tag the engine, a
/// header, or another option owns, or a value that is empty or holds control characters.
pub(crate) fn logon_fields(
    config: &ConnectorConfig,
    venue: &Venue,
) -> Result<BTreeMap<i32, String>, ConnectorError> {
    let mut fields = BTreeMap::new();
    for (key, value) in config.properties() {
        let Some(tag) = key.strip_prefix(LOGON_FIELD_PREFIX) else {
            continue;
        };
        let tag: i32 = tag
            .parse()
            .ok()
            .filter(|tag| *tag > 0)
            .ok_or_else(|| invalid(format!("'{key}' must name a positive FIX tag number")))?;
        if RESERVED_LOGON_TAGS.contains(&tag) || venue.logon_tags().contains(&tag) {
            return Err(invalid(format!(
                "'{key}': tag {tag} is a header or trailer field, or set by the engine or another \
                 option"
            )));
        }
        if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(invalid(format!(
                "'{key}' needs a value without control characters"
            )));
        }
        fields.insert(tag, value.clone());
    }
    Ok(fields)
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
    fields: Vec<(i32, String)>,
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
        let fields = config
            .logon_fields
            .iter()
            .map(|(tag, value)| (*tag, value.clone()))
            .collect();
        match &config.venue {
            Venue::Standard => Ok(Self {
                credentials,
                fields,
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
                    fields,
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
            .chain(self.fields.iter().cloned())
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
    }

    #[test]
    fn resends_default_on_and_binance_never_resends() {
        let binance_venue = Venue::Binance {
            key_file: "k".into(),
        };
        let mut config = ConnectorConfig::new("fix");
        assert!(resends(&config, &Venue::Standard).expect("default"));
        assert!(!resends(&config, &binance_venue).expect("binance"));
        config.set("counterparty.resends", "false");
        assert!(!resends(&config, &Venue::Standard).expect("configured"));
        config.set("counterparty.resends", "true");
        assert!(resends(&config, &binance_venue).is_err());
        config.set("counterparty.resends", "yes");
        assert!(resends(&config, &Venue::Standard).is_err());
    }

    #[test]
    fn logon_fields_skip_owned_tags_and_unsafe_values() {
        let mut config = ConnectorConfig::new("fix");
        config.set("logon.field.9406", "Y");
        config.set("logon.field.1603", "LaminarDB");
        assert_eq!(
            logon_fields(&config, &Venue::Standard).expect("fields"),
            BTreeMap::from([(1603, "LaminarDB".to_string()), (9406, "Y".to_string())])
        );
        let binance_venue = Venue::Binance {
            key_file: "k".into(),
        };
        assert!(
            logon_fields(&config, &binance_venue).is_err(),
            "the Binance profile sets 9406"
        );
        for (key, value) in [
            ("logon.field.553", "user"),
            ("logon.field.141", "Y"),
            ("logon.field.49", "SENDER"),
            ("logon.field.10", "000"),
            ("logon.field.x", "1"),
            ("logon.field.0", "1"),
            ("logon.field.5000", ""),
            ("logon.field.5000", "a\u{1}b"),
        ] {
            let mut config = ConnectorConfig::new("fix");
            config.set(key, value);
            assert!(
                logon_fields(&config, &Venue::Standard).is_err(),
                "{key} = {value:?}"
            );
        }
    }
}
