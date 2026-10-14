//! FIX source configuration parsed from connector properties.
//!
//! Only reviewed engine settings are exposed. Validation stays enabled by default; each
//! relaxation is a named, documented property rather than a passthrough of engine keys.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::path::PathBuf;

use sha2::{Digest, Sha256};

use super::dictionary::{DictionaryDigests, DictionaryFiles};
use super::engine::SessionKey;
use super::journal::{JournalOptions, SyncPolicy};
use super::venue::{self, Venue, LOGON_FIELD_PREFIX};
use crate::config::{ConfigKeySpec, ConnectorConfig};
use crate::error::ConnectorError;

/// FIX versions whose session behaviour this connector has qualified.
const SUPPORTED_BEGIN_STRINGS: &[&str] = &["FIX.4.2", "FIX.4.4", FIXT_BEGIN_STRING];
pub(crate) const FIXT_BEGIN_STRING: &str = "FIXT.1.1";
/// Upper bound on a dictionary file read before handing it to the engine.
pub(crate) const MAX_DICTIONARY_BYTES: u64 = 16 * 1024 * 1024;

/// Secret value whose `Debug` output is redacted.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Secret(String);

impl Secret {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// Logon credentials sent in Username(553)/Password(554).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Credentials {
    pub(crate) username: Option<String>,
    pub(crate) password: Option<Secret>,
}

/// Verified TLS client settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TlsSettings {
    /// PEM CA bundle; `None` uses the bundled public web roots.
    pub(crate) ca_file: Option<PathBuf>,
    /// Name verified against the server certificate; defaults to `host`.
    pub(crate) server_name: String,
    /// PEM client certificate chain and key for mutual TLS.
    pub(crate) client_identity: Option<(PathBuf, PathBuf)>,
}

/// Network transport to the counterparty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Transport {
    Tcp,
    Tls(TlsSettings),
}

/// What happens when the counterparty moves the inbound sequence forward with
/// SequenceReset-Reset, skipping messages that cannot be recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SequenceResetPolicy {
    /// Stop the source at the reset until an operator reconciles the gap.
    Fail,
    /// Continue and flag the next emitted row through the `@gap_before` metadata.
    Continue,
}

/// FIX 5.0 application version of a FIXT.1.1 session (ApplVerID, 1128).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplVerId {
    Fix50,
    Fix50Sp1,
    Fix50Sp2,
}

impl ApplVerId {
    const ALL: [Self; 3] = [Self::Fix50, Self::Fix50Sp1, Self::Fix50Sp2];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Fix50 => "FIX.5.0",
            Self::Fix50Sp1 => "FIX.5.0SP1",
            Self::Fix50Sp2 => "FIX.5.0SP2",
        }
    }

    /// The enumerated value carried in ApplVerID (1128) and DefaultApplVerID (1137).
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Fix50 => "7",
            Self::Fix50Sp1 => "8",
            Self::Fix50Sp2 => "9",
        }
    }
}

/// FIXT.1.1 transport settings. One application version is captured per source: the engine
/// validates a message of any other version against an empty dictionary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fixt {
    pub(crate) transport_dictionary_path: PathBuf,
    pub(crate) appl_ver_id: ApplVerId,
}

/// Fully parsed FIX source configuration.
#[derive(Debug, Clone)]
pub(crate) struct FixSourceConfig {
    pub(crate) session: SessionKey,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) transport: Transport,
    pub(crate) credentials: Option<Credentials>,
    pub(crate) dictionary_path: PathBuf,
    pub(crate) fixt: Option<Fixt>,
    pub(crate) messages: BTreeSet<String>,
    pub(crate) journal_path: PathBuf,
    pub(crate) journal: JournalOptions,
    pub(crate) max_message_bytes: usize,
    pub(crate) max_rows_per_message: usize,
    pub(crate) on_sequence_reset: SequenceResetPolicy,
    pub(crate) venue: Venue,
    /// Whether the counterparty answers ResendRequest with the original messages.
    pub(crate) resends: bool,
    pub(crate) logon_fields: BTreeMap<i32, String>,
    engine: BTreeMap<&'static str, String>,
}

struct Key {
    name: &'static str,
    description: &'static str,
    default: Option<&'static str>,
}

const fn required(name: &'static str, description: &'static str) -> Key {
    Key {
        name,
        description,
        default: None,
    }
}

const fn optional(name: &'static str, description: &'static str, default: &'static str) -> Key {
    Key {
        name,
        description,
        default: Some(default),
    }
}

const KEYS: &[Key] = &[
    required("host", "Counterparty host"),
    required("port", "Counterparty port"),
    optional(
        "transport",
        "'tls' (verified) or 'tcp' (loopback unless tcp.allow.remote)",
        "tls",
    ),
    optional(
        "tcp.allow.remote",
        "Allow plaintext TCP to a non-loopback host",
        "false",
    ),
    optional(
        "tls.ca.file",
        "PEM CA bundle (default: public web roots)",
        "",
    ),
    optional(
        "tls.server.name",
        "Name verified against the server certificate (default: host)",
        "",
    ),
    optional(
        "tls.client.cert.file",
        "PEM client certificate chain for mutual TLS",
        "",
    ),
    optional(
        "tls.client.key.file",
        "PEM client private key for mutual TLS",
        "",
    ),
    optional(
        "begin.string",
        "FIX version (FIX.4.4, FIX.4.2, or FIXT.1.1)",
        "FIX.4.4",
    ),
    required("sender.comp.id", "SenderCompID (49) sent by this source"),
    required("target.comp.id", "TargetCompID (56) of the counterparty"),
    optional("session.qualifier", "Engine session qualifier", ""),
    optional(
        "venue",
        "'binance' for a Binance Spot FIX drop-copy session; empty for standard FIX",
        "",
    ),
    optional(
        "counterparty.resends",
        "'false' when the counterparty answers no ResendRequest with the original messages",
        "true",
    ),
    optional(
        "logon.key.file",
        "PEM PKCS#8 private key that signs the Logon (venue 'binance': Ed25519)",
        "",
    ),
    optional("username", "Logon Username (553)", ""),
    optional(
        "password",
        "Logon Password (554); use an environment reference",
        "",
    ),
    required(
        "dictionary.path",
        "FIX data dictionary XML, the application dictionary under FIXT.1.1 (pinned by content at \
         activation)",
    ),
    optional(
        "transport.dictionary.path",
        "FIXT.1.1 transport dictionary XML (FIXT.1.1 only; pinned by content at activation)",
        "",
    ),
    optional(
        "appl.ver.id",
        "FIXT.1.1 application version: FIX.5.0, FIX.5.0SP1, or FIX.5.0SP2 (FIXT.1.1 only)",
        "",
    ),
    required(
        "messages",
        "Comma-separated application MsgType values to capture",
    ),
    required(
        "journal.path",
        "Directory of the durable receive journal and session state",
    ),
    optional(
        "journal.sync",
        "'fsync' (host failure) or 'os' (process failure only)",
        "fsync",
    ),
    optional(
        "journal.max.bytes",
        "Retained journal bytes before intake pauses",
        "1073741824",
    ),
    optional("journal.segment.bytes", "Journal segment size", "67108864"),
    optional(
        "max.message.bytes",
        "Largest accepted application message",
        "1048576",
    ),
    optional(
        "max.rows.per.message",
        "Most rows one message may expand into",
        "10000",
    ),
    optional("heartbeat.interval.seconds", "HeartBtInt (108)", "30"),
    optional(
        "reconnect.interval.seconds",
        "Fixed engine reconnect interval",
        "5",
    ),
    optional("logon.timeout.seconds", "Logon response timeout", "10"),
    optional("logout.timeout.seconds", "Logout response timeout", "2"),
    optional(
        "schedule.start.time",
        "Session start time (HH:MM:SS)",
        "00:00:00",
    ),
    optional(
        "schedule.end.time",
        "Session end time (HH:MM:SS)",
        "00:00:00",
    ),
    optional("schedule.start.day", "Weekly session start day", ""),
    optional("schedule.end.day", "Weekly session end day", ""),
    optional(
        "schedule.time.zone",
        "'utc' or 'local' (engine supports no other zones)",
        "utc",
    ),
    optional(
        "reset.on.logon",
        "Reset sequence numbers at every logon",
        "false",
    ),
    optional(
        "reset.on.logout",
        "Reset sequence numbers after logout",
        "false",
    ),
    optional(
        "reset.on.disconnect",
        "Reset sequence numbers after disconnect",
        "false",
    ),
    optional(
        "validate.user.defined.fields",
        "Validate user-defined fields (>= 5000)",
        "true",
    ),
    optional(
        "validate.fields.out.of.order",
        "Reject header/body/trailer ordering errors",
        "true",
    ),
    optional(
        "validate.fields.have.values",
        "Reject fields without values",
        "true",
    ),
    optional(
        "allow.unknown.message.fields",
        "Accept fields undefined for a message",
        "false",
    ),
    optional(
        "check.latency",
        "Reject messages whose SendingTime is too old",
        "true",
    ),
    optional("max.latency.seconds", "SendingTime accuracy bound", "120"),
    optional(
        "on.sequence.reset",
        "'fail' or 'continue' after an unrecoverable reset",
        "fail",
    ),
    optional(
        "timestamp.precision",
        "'exact' (sub-unit digits fail) or 'truncate' (sub-unit digits are dropped)",
        "exact",
    ),
    optional(
        "expand",
        "Repeating-group count tag whose entries become rows",
        "",
    ),
    optional(
        "expand.empty",
        "'row' or 'skip' for messages without expanded entries",
        "row",
    ),
];

/// Properties consumed by the runtime rather than the connector.
const RUNTIME_KEYS: &[&str] = &[
    "laminar.source.name",
    "_arrow_schema",
    "_primary_key_columns",
];
/// Column mapping properties: `column.<name>` and `column.<name>.<field>...`.
pub(crate) const COLUMN_PREFIX: &str = "column.";

/// Key specifications for registry metadata.
pub(crate) fn config_keys() -> Vec<ConfigKeySpec> {
    let mut keys: Vec<ConfigKeySpec> = KEYS
        .iter()
        .map(|key| match key.default {
            None => ConfigKeySpec::required(key.name, key.description),
            Some(default) => ConfigKeySpec::optional(key.name, key.description, default),
        })
        .collect();
    keys.push(ConfigKeySpec::optional(
        "column.<name>",
        "Field path for a declared column: TAG, header.TAG, trailer.TAG, GROUP[].TAG, GROUP, or @metadata",
        "",
    ));
    keys.push(ConfigKeySpec::optional(
        "logon.field.<tag>",
        "Static Logon body field (not for secrets)",
        "",
    ));
    keys
}

fn value<'a>(config: &'a ConnectorConfig, name: &str) -> Option<&'a str> {
    config
        .get(name)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            KEYS.iter()
                .find(|key| key.name == name)
                .and_then(|key| key.default)
                .filter(|value| !value.is_empty())
        })
}

fn required_value<'a>(config: &'a ConnectorConfig, name: &str) -> Result<&'a str, ConnectorError> {
    config
        .get(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ConnectorError::missing_config(name))
}

fn parsed<T: std::str::FromStr>(config: &ConnectorConfig, name: &str) -> Result<T, ConnectorError> {
    let text = value(config, name).ok_or_else(|| ConnectorError::missing_config(name))?;
    text.trim().parse().map_err(|_| {
        ConnectorError::ConfigurationError(format!(
            "FIX source option '{name}' has invalid value '{text}'"
        ))
    })
}

fn flag(config: &ConnectorConfig, name: &str) -> Result<bool, ConnectorError> {
    match value(config, name).map(str::trim) {
        Some("true") => Ok(true),
        Some("false") | None => Ok(false),
        Some(other) => Err(ConnectorError::ConfigurationError(format!(
            "FIX source option '{name}' must be 'true' or 'false', got '{other}'"
        ))),
    }
}

fn engine_flag(value: bool) -> String {
    if value { "Y" } else { "N" }.to_string()
}

fn bounded(
    config: &ConnectorConfig,
    name: &str,
    min: u64,
    max: u64,
) -> Result<u64, ConnectorError> {
    let value: u64 = parsed(config, name)?;
    if value < min || value > max {
        return Err(ConnectorError::ConfigurationError(format!(
            "FIX source option '{name}' must be between {min} and {max}"
        )));
    }
    Ok(value)
}

fn reject_unknown(config: &ConnectorConfig) -> Result<(), ConnectorError> {
    for key in config.properties().keys() {
        let known = KEYS.iter().any(|spec| spec.name == key)
            || RUNTIME_KEYS.contains(&key.as_str())
            || key.starts_with(COLUMN_PREFIX)
            || key.starts_with(LOGON_FIELD_PREFIX);
        if !known {
            return Err(ConnectorError::ConfigurationError(format!(
                "unknown FIX source option '{key}'"
            )));
        }
    }
    if config.get("format").is_some() {
        return Err(ConnectorError::ConfigurationError(
            "FIX sources decode FIX natively and do not accept a FORMAT clause".into(),
        ));
    }
    Ok(())
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn time_of_day(config: &ConnectorConfig, name: &str) -> Result<String, ConnectorError> {
    let text = parsed::<String>(config, name)?;
    chrono::NaiveTime::parse_from_str(&text, "%H:%M:%S").map_err(|_| {
        ConnectorError::ConfigurationError(format!("FIX source option '{name}' must be HH:MM:SS"))
    })?;
    Ok(text)
}

fn transport(config: &ConnectorConfig, host: &str) -> Result<Transport, ConnectorError> {
    match value(config, "transport").map(str::trim) {
        Some("tcp") => {
            if !is_loopback(host) && !flag(config, "tcp.allow.remote")? {
                return Err(ConnectorError::ConfigurationError(format!(
                    "plaintext FIX transport to non-loopback host '{host}' requires \
                     'tcp.allow.remote' = 'true'; use 'transport' = 'tls' otherwise"
                )));
            }
            Ok(Transport::Tcp)
        }
        Some("tls") | None => {
            let path = |name| value(config, name).map(PathBuf::from);
            let client_identity =
                match (path("tls.client.cert.file"), path("tls.client.key.file")) {
                    (Some(cert), Some(key)) => Some((cert, key)),
                    (None, None) => None,
                    _ => return Err(ConnectorError::ConfigurationError(
                        "mutual TLS needs both 'tls.client.cert.file' and 'tls.client.key.file'"
                            .into(),
                    )),
                };
            Ok(Transport::Tls(TlsSettings {
                ca_file: path("tls.ca.file"),
                server_name: value(config, "tls.server.name").unwrap_or(host).to_string(),
                client_identity,
            }))
        }
        Some(other) => Err(ConnectorError::ConfigurationError(format!(
            "FIX source 'transport' must be 'tls' or 'tcp', got '{other}'"
        ))),
    }
}

fn messages(config: &ConnectorConfig) -> Result<BTreeSet<String>, ConnectorError> {
    let messages: BTreeSet<String> = required_value(config, "messages")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect();
    let admin = ["0", "1", "2", "3", "4", "5", "A"];
    if let Some(admin) = messages
        .iter()
        .find(|msg_type| admin.contains(&msg_type.as_str()))
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "FIX source 'messages' selects application messages only; '{admin}' is administrative"
        )));
    }
    if messages.is_empty() {
        return Err(ConnectorError::missing_config("messages"));
    }
    Ok(messages)
}

impl FixSourceConfig {
    /// Parse and validate connector properties without I/O.
    ///
    /// # Errors
    /// Returns a configuration error for missing, unknown, or contradictory options.
    pub(crate) fn parse(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        reject_unknown(config)?;
        let begin_string = value(config, "begin.string")
            .unwrap_or("FIX.4.4")
            .to_string();
        if !SUPPORTED_BEGIN_STRINGS.contains(&begin_string.as_str()) {
            return Err(ConnectorError::FeatureUnsupported(format!(
                "FIX version '{begin_string}' is not qualified; supported: {}",
                SUPPORTED_BEGIN_STRINGS.join(", ")
            )));
        }
        let fixt = fixt(config, &begin_string)?;
        let session = SessionKey {
            begin_string,
            sender_comp_id: required_value(config, "sender.comp.id")?.to_string(),
            target_comp_id: required_value(config, "target.comp.id")?.to_string(),
            qualifier: value(config, "session.qualifier").map(str::to_string),
        };
        let host = required_value(config, "host")?.to_string();
        let port: u16 = parsed(config, "port")?;
        let transport = transport(config, &host)?;
        let credentials = match (value(config, "username"), value(config, "password")) {
            (None, None) => None,
            (username, password) => Some(Credentials {
                username: username.map(str::to_string),
                password: password.map(|password| Secret(password.to_string())),
            }),
        };
        let sync = match value(config, "journal.sync").map(str::trim) {
            Some("fsync") | None => SyncPolicy::Fsync,
            Some("os") => SyncPolicy::OperatingSystem,
            Some(other) => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "FIX source 'journal.sync' must be 'fsync' or 'os', got '{other}'"
                )))
            }
        };
        let max_message_bytes = bounded(config, "max.message.bytes", 1024, 16 * 1024 * 1024)?;
        let journal = JournalOptions {
            sync,
            segment_bytes: bounded(config, "journal.segment.bytes", 1 << 20, 1 << 32)?,
            max_bytes: bounded(config, "journal.max.bytes", 1 << 20, u64::MAX)?,
            // Application record envelope fields plus the message.
            max_record_bytes: max_message_bytes + 1024,
        };
        if journal.segment_bytes < journal.max_record_bytes * 2 {
            return Err(ConnectorError::ConfigurationError(
                "'journal.segment.bytes' must hold at least two maximum-size messages".into(),
            ));
        }
        let on_sequence_reset = match value(config, "on.sequence.reset").map(str::trim) {
            Some("fail") | None => SequenceResetPolicy::Fail,
            Some("continue") => SequenceResetPolicy::Continue,
            Some(other) => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "FIX source 'on.sequence.reset' must be 'fail' or 'continue', got '{other}'"
                )))
            }
        };
        let venue = Venue::parse(config, &session)?;
        let resends = venue::resends(config, &venue)?;
        let logon_fields = venue::logon_fields(config, &venue)?;
        let mut engine = engine_settings(config)?;
        venue.constrain_engine(config, &mut engine)?;
        Ok(Self {
            session,
            host,
            port,
            transport,
            credentials,
            dictionary_path: PathBuf::from(required_value(config, "dictionary.path")?),
            fixt,
            messages: messages(config)?,
            journal_path: PathBuf::from(required_value(config, "journal.path")?),
            journal,
            max_message_bytes: usize::try_from(max_message_bytes)
                .map_err(|_| ConnectorError::ConfigurationError("max.message.bytes".into()))?,
            max_rows_per_message: usize::try_from(bounded(
                config,
                "max.rows.per.message",
                1,
                1_000_000,
            )?)
            .map_err(|_| ConnectorError::ConfigurationError("max.rows.per.message".into()))?,
            on_sequence_reset,
            venue,
            resends,
            logon_fields,
            engine,
        })
    }

    /// The configured dictionary files.
    pub(crate) fn dictionary_files(&self) -> DictionaryFiles {
        DictionaryFiles {
            application: self.dictionary_path.clone(),
            transport: self
                .fixt
                .as_ref()
                .map(|fixt| fixt.transport_dictionary_path.clone()),
        }
    }

    /// Engine `[DEFAULT]` settings for this session.
    pub(crate) fn engine_defaults(
        &self,
        pinned: &DictionaryFiles,
        connect: (&str, u16),
    ) -> Vec<(&'static str, String)> {
        let mut settings: Vec<(&'static str, String)> = self
            .engine
            .iter()
            .map(|(key, value)| (*key, value.clone()))
            .collect();
        settings.extend([
            ("ConnectionType", "initiator".to_string()),
            ("SocketConnectHost", connect.0.to_string()),
            ("SocketConnectPort", connect.1.to_string()),
            ("UseDataDictionary", "Y".to_string()),
            // Outbound messages are not stored, so no logon secret is written at rest; resend
            // requests from the counterparty are answered with gap fills. The session store
            // rejects message persistence.
            ("PersistMessages", "N".to_string()),
            ("ValidateLengthAndChecksum", "Y".to_string()),
            ("CheckCompID", "Y".to_string()),
            ("RefreshOnLogon", "N".to_string()),
            ("SendRedundantResendRequests", "N".to_string()),
        ]);
        let application = pinned.application.display().to_string();
        match (&self.fixt, &pinned.transport) {
            (Some(fixt), Some(transport)) => settings.extend([
                ("TransportDataDictionary", transport.display().to_string()),
                ("AppDataDictionary", application),
                ("DefaultApplVerID", fixt.appl_ver_id.name().to_string()),
            ]),
            _ => settings.push(("DataDictionary", application)),
        }
        settings
    }

    /// SHA-256 of everything needed to interpret journaled messages. Column mappings are
    /// excluded: they belong to the pipeline identity, so a new mapping can replay the journal.
    pub(crate) fn capture_contract(&self, digests: &DictionaryDigests) -> String {
        let mut contract = serde_json::json!({
            "version": 1,
            "session": self.session.display(),
            "dictionary_sha256": digests.application,
            "messages": self.messages,
            "validation": {
                "user_defined_fields": self.engine.get("ValidateUserDefinedFields"),
                "fields_out_of_order": self.engine.get("ValidateFieldsOutOfOrder"),
                "fields_have_values": self.engine.get("ValidateFieldsHaveValues"),
                "allow_unknown_message_fields": self.engine.get("AllowUnknownMsgFields"),
            },
        });
        if let (Some(fixt), Some(object)) = (&self.fixt, contract.as_object_mut()) {
            object.insert(
                "transport_dictionary_sha256".into(),
                digests.transport.clone().into(),
            );
            object.insert("appl_ver_id".into(), fixt.appl_ver_id.code().into());
        }
        if let Some(object) = contract.as_object_mut() {
            if let Some(venue) = self.venue.name() {
                object.insert("venue".into(), venue.into());
            }
            if !self.resends {
                object.insert("resends".into(), false.into());
            }
        }
        hex(&Sha256::digest(contract.to_string().as_bytes()))
    }

    /// Credential- and endpoint-free identity of the captured stream.
    pub(crate) fn recovery_identity(&self) -> BTreeMap<String, String> {
        let mut identity = BTreeMap::from([
            (
                "begin.string".to_string(),
                self.session.begin_string.clone(),
            ),
            (
                "sender.comp.id".to_string(),
                self.session.sender_comp_id.clone(),
            ),
            (
                "target.comp.id".to_string(),
                self.session.target_comp_id.clone(),
            ),
            (
                "messages".to_string(),
                self.messages.iter().cloned().collect::<Vec<_>>().join(","),
            ),
        ]);
        if let Some(qualifier) = &self.session.qualifier {
            identity.insert("session.qualifier".to_string(), qualifier.clone());
        }
        if let Some(fixt) = &self.fixt {
            identity.insert(
                "appl.ver.id".to_string(),
                fixt.appl_ver_id.name().to_string(),
            );
        }
        if let Some(venue) = self.venue.name() {
            identity.insert("venue".to_string(), venue.to_string());
        }
        identity
    }
}

fn fixt(config: &ConnectorConfig, begin_string: &str) -> Result<Option<Fixt>, ConnectorError> {
    if begin_string != FIXT_BEGIN_STRING {
        if let Some(key) = ["transport.dictionary.path", "appl.ver.id"]
            .into_iter()
            .find(|key| value(config, key).is_some())
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "'{key}' applies only to 'begin.string' = '{FIXT_BEGIN_STRING}'"
            )));
        }
        return Ok(None);
    }
    let text = required_value(config, "appl.ver.id")?;
    let appl_ver_id = ApplVerId::ALL
        .into_iter()
        .find(|version| version.name() == text.trim())
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "'appl.ver.id' must be FIX.5.0, FIX.5.0SP1, or FIX.5.0SP2, got '{text}'"
            ))
        })?;
    Ok(Some(Fixt {
        transport_dictionary_path: PathBuf::from(required_value(
            config,
            "transport.dictionary.path",
        )?),
        appl_ver_id,
    }))
}

fn engine_settings(
    config: &ConnectorConfig,
) -> Result<BTreeMap<&'static str, String>, ConnectorError> {
    let mut engine = BTreeMap::new();
    engine.insert(
        "HeartBtInt",
        bounded(config, "heartbeat.interval.seconds", 1, 3600)?.to_string(),
    );
    engine.insert(
        "ReconnectInterval",
        bounded(config, "reconnect.interval.seconds", 1, 3600)?.to_string(),
    );
    engine.insert(
        "LogonTimeout",
        bounded(config, "logon.timeout.seconds", 1, 600)?.to_string(),
    );
    engine.insert(
        "LogoutTimeout",
        bounded(config, "logout.timeout.seconds", 1, 600)?.to_string(),
    );
    engine.insert("StartTime", time_of_day(config, "schedule.start.time")?);
    engine.insert("EndTime", time_of_day(config, "schedule.end.time")?);
    match (
        value(config, "schedule.start.day"),
        value(config, "schedule.end.day"),
    ) {
        (Some(start), Some(end)) => {
            engine.insert("StartDay", start.to_string());
            engine.insert("EndDay", end.to_string());
        }
        (None, None) => {}
        _ => {
            return Err(ConnectorError::ConfigurationError(
                "a weekly FIX schedule needs both 'schedule.start.day' and 'schedule.end.day'"
                    .into(),
            ))
        }
    }
    let local = match value(config, "schedule.time.zone").map(str::trim) {
        Some("utc") | None => false,
        Some("local") => true,
        Some(other) => {
            return Err(ConnectorError::FeatureUnsupported(format!(
                "the FIX engine schedules in UTC or the host's local time only, not '{other}'"
            )))
        }
    };
    engine.insert("UseLocalTime", engine_flag(local));
    for (key, setting) in [
        ("reset.on.logon", "ResetOnLogon"),
        ("reset.on.logout", "ResetOnLogout"),
        ("reset.on.disconnect", "ResetOnDisconnect"),
        ("validate.user.defined.fields", "ValidateUserDefinedFields"),
        ("validate.fields.out.of.order", "ValidateFieldsOutOfOrder"),
        ("validate.fields.have.values", "ValidateFieldsHaveValues"),
        ("allow.unknown.message.fields", "AllowUnknownMsgFields"),
        ("check.latency", "CheckLatency"),
    ] {
        engine.insert(setting, engine_flag(flag(config, key)?));
    }
    engine.insert(
        "MaxLatency",
        bounded(config, "max.latency.seconds", 1, 86_400)?.to_string(),
    );
    Ok(engine)
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn base() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("fix");
        for (key, value) in [
            ("host", "127.0.0.1"),
            ("port", "9878"),
            ("transport", "tcp"),
            ("sender.comp.id", "LAMINAR"),
            ("target.comp.id", "VENUE"),
            ("dictionary.path", "dict.xml"),
            ("messages", "8, U1"),
            ("journal.path", "journal"),
        ] {
            config.set(key, value);
        }
        config
    }

    #[test]
    fn defaults_keep_validation_on_and_resets_off() {
        let parsed = FixSourceConfig::parse(&base()).expect("parse");
        let defaults = parsed.engine_defaults(&parsed.dictionary_files(), ("127.0.0.1", 9878));
        let get = |key: &str| {
            defaults
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| value.as_str())
        };
        for key in [
            "ValidateUserDefinedFields",
            "ValidateFieldsOutOfOrder",
            "ValidateFieldsHaveValues",
            "CheckLatency",
        ] {
            assert_eq!(get(key), Some("Y"), "{key}");
        }
        for key in [
            "ResetOnLogon",
            "ResetOnLogout",
            "ResetOnDisconnect",
            "AllowUnknownMsgFields",
            "PersistMessages",
        ] {
            assert_eq!(get(key), Some("N"), "{key}");
        }
        assert_eq!(parsed.journal.sync, SyncPolicy::Fsync);
        assert_eq!(parsed.on_sequence_reset, SequenceResetPolicy::Fail);
        assert_eq!(
            parsed.messages.iter().cloned().collect::<Vec<_>>(),
            vec!["8".to_string(), "U1".to_string()]
        );
    }

    #[test]
    fn plaintext_is_loopback_only_unless_explicitly_allowed() {
        let mut config = base();
        config.set("host", "fix.example.com");
        assert!(FixSourceConfig::parse(&config).is_err());
        config.set("tcp.allow.remote", "true");
        assert!(FixSourceConfig::parse(&config).is_ok());
        config.set("transport", "tls");
        let parsed = FixSourceConfig::parse(&config).expect("tls");
        assert_eq!(
            parsed.transport,
            Transport::Tls(TlsSettings {
                ca_file: None,
                server_name: "fix.example.com".into(),
                client_identity: None
            })
        );
    }

    #[test]
    fn unknown_unqualified_and_contradictory_options_fail_before_io() {
        let mut unknown = base();
        unknown.set("SocketConnectHost", "x");
        assert!(FixSourceConfig::parse(&unknown).is_err());
        let mut fix43 = base();
        fix43.set("begin.string", "FIX.4.3");
        assert!(matches!(
            FixSourceConfig::parse(&fix43),
            Err(ConnectorError::FeatureUnsupported(_))
        ));
        let mut admin = base();
        admin.set("messages", "8,A");
        assert!(FixSourceConfig::parse(&admin).is_err());
        let mut half_mtls = base();
        half_mtls.set("transport", "tls");
        half_mtls.set("tls.client.cert.file", "c.pem");
        assert!(FixSourceConfig::parse(&half_mtls).is_err());
        let mut zone = base();
        zone.set("schedule.time.zone", "America/New_York");
        assert!(FixSourceConfig::parse(&zone).is_err());
    }

    #[test]
    fn secrets_are_redacted_and_excluded_from_identity() {
        let mut config = base();
        config.set("username", "trader");
        config.set("password", "hunter2");
        let parsed = FixSourceConfig::parse(&config).expect("parse");
        assert!(!format!("{parsed:?}").contains("hunter2"));
        let identity = parsed.recovery_identity();
        assert!(!format!("{identity:?}").contains("hunter2"));
        assert!(!identity.contains_key("host"));
        assert_ne!(
            parsed.capture_contract(&digests("a", None)),
            parsed.capture_contract(&digests("b", None)),
            "dictionary content binds the journal"
        );
    }

    fn digests(application: &str, transport: Option<&str>) -> DictionaryDigests {
        DictionaryDigests {
            application: application.into(),
            transport: transport.map(str::to_string),
        }
    }

    fn fixt() -> ConnectorConfig {
        let mut config = base();
        config.set("begin.string", "FIXT.1.1");
        config.set("transport.dictionary.path", "fixt.xml");
        config.set("appl.ver.id", "FIX.5.0SP2");
        config
    }

    #[test]
    fn fixt_sessions_name_a_transport_dictionary_and_one_application_version() {
        let parsed = FixSourceConfig::parse(&fixt()).expect("parse");
        let fixt_settings = parsed.fixt.clone().expect("fixt");
        assert_eq!(fixt_settings.appl_ver_id, ApplVerId::Fix50Sp2);
        assert_eq!(fixt_settings.appl_ver_id.code(), "9");
        let files = parsed.dictionary_files();
        assert_eq!(
            files.transport.as_deref(),
            Some(std::path::Path::new("fixt.xml"))
        );
        let defaults = parsed.engine_defaults(&files, ("127.0.0.1", 9878));
        let get = |key: &str| {
            defaults
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| value.as_str())
        };
        assert_eq!(get("TransportDataDictionary"), Some("fixt.xml"));
        assert_eq!(get("AppDataDictionary"), Some("dict.xml"));
        assert_eq!(get("DefaultApplVerID"), Some("FIX.5.0SP2"));
        assert_eq!(get("DataDictionary"), None);
        assert_eq!(
            parsed
                .recovery_identity()
                .get("appl.ver.id")
                .map(String::as_str),
            Some("FIX.5.0SP2")
        );
        assert_ne!(
            parsed.capture_contract(&digests("a", Some("t1"))),
            parsed.capture_contract(&digests("a", Some("t2"))),
            "transport dictionary content binds the journal"
        );
        let mut sp1 = fixt();
        sp1.set("appl.ver.id", "FIX.5.0SP1");
        let sp1 = FixSourceConfig::parse(&sp1).expect("parse");
        assert_ne!(
            parsed.capture_contract(&digests("a", Some("t"))),
            sp1.capture_contract(&digests("a", Some("t"))),
            "the application version binds the journal"
        );
    }

    #[test]
    fn fixt_options_are_required_for_fixt_and_rejected_otherwise() {
        for key in ["transport.dictionary.path", "appl.ver.id"] {
            let mut missing = fixt();
            missing.set(key, "");
            assert!(FixSourceConfig::parse(&missing).is_err(), "{key} required");
            let mut stray = base();
            stray.set(key, "x");
            let error = FixSourceConfig::parse(&stray).expect_err("rejected below FIXT");
            assert!(error.to_string().contains("FIXT.1.1"), "{error}");
        }
        let mut version = fixt();
        version.set("appl.ver.id", "FIX.4.4");
        assert!(FixSourceConfig::parse(&version).is_err());
        let plain = FixSourceConfig::parse(&base()).expect("parse");
        assert!(plain.fixt.is_none());
        assert!(!plain.recovery_identity().contains_key("appl.ver.id"));
    }
}
