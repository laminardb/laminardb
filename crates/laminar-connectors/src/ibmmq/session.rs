//! MQI calls of one connection. A session is created, used, and dropped on its worker thread,
//! because a `ThreadNone` connection handle belongs to the thread that opened it.
//!
//! On every distributed platform `MQDISC` commits an open unit of work, so each session backs
//! out explicitly when dropped, before the object and connection close.

use mqi::connection::{Ccdt, Credentials, ThreadNone, Tls};
use mqi::prelude::*;
use mqi::result::Error as MqError;
use mqi::types::{
    ApplName, CertificateLabel, CipherSpec, KeyRepo, QueueManagerName, QueueName, MQOO,
};
use mqi::{attribute, constants, structs, Connection, MqStr, Object, Syncpoint};

use super::config::{ConnectionConfig, Endpoint, Reconnect};
use super::library::Library;
use crate::error::ConnectorError;

/// One queue manager connection.
pub(super) type Conn = Connection<Library, ThreadNone>;

/// Classify an MQI failure by its reason code.
pub(super) fn classify(operation: &str, error: &MqError) -> ConnectorError {
    let reason = error.2;
    let message = format!("{operation}: {error}");
    if [
        constants::MQRC_CONNECTION_BROKEN,
        constants::MQRC_Q_MGR_NOT_AVAILABLE,
        constants::MQRC_HOST_NOT_AVAILABLE,
        constants::MQRC_Q_MGR_QUIESCING,
        constants::MQRC_Q_MGR_STOPPING,
        constants::MQRC_CONNECTION_QUIESCING,
        constants::MQRC_CONNECTION_STOPPING,
        constants::MQRC_RECONNECT_FAILED,
        constants::MQRC_CHANNEL_NOT_AVAILABLE,
    ]
    .contains(&reason)
    {
        ConnectorError::ConnectionFailed(message)
    } else if [
        constants::MQRC_NOT_AUTHORIZED,
        constants::MQRC_UNKNOWN_OBJECT_NAME,
        constants::MQRC_Q_MGR_NAME_ERROR,
        constants::MQRC_UNKNOWN_CHANNEL_NAME,
        constants::MQRC_SSL_INITIALIZATION_ERROR,
        constants::MQRC_SSL_PEER_NAME_MISMATCH,
        constants::MQRC_CIPHER_SPEC_NOT_SUITE_B,
        constants::MQRC_KEY_REPOSITORY_ERROR,
        constants::MQRC_GET_INHIBITED,
        constants::MQRC_PUT_INHIBITED,
    ]
    .contains(&reason)
    {
        ConnectorError::ConfigurationError(message)
    } else {
        ConnectorError::ReadError(message)
    }
}

fn mq_str<const N: usize>(key: &str, value: &str) -> Result<MqStr<N>, ConnectorError> {
    MqStr::from_str(value).map_err(|error| {
        ConnectorError::ConfigurationError(format!("'{key}' is not a valid MQI value: {error}"))
    })
}

fn padded<const N: usize>(
    key: &str,
    value: &str,
) -> Result<[mqi::types::MQCHAR; N], ConnectorError> {
    Ok(*mq_str::<N>(key, value)?.as_mqchar())
}

/// Connect with explicit reconnect behaviour and the configured channel selection.
pub(super) fn connect(config: &ConnectionConfig, library: Library) -> Result<Conn, ConnectorError> {
    let application = ApplName(mq_str("application.name", &config.application_name)?);
    let queue_manager = config
        .queue_manager
        .as_deref()
        .map(|name| mq_str("queue.manager", name).map(QueueManagerName))
        .transpose()?;
    let reconnect = match config.reconnect {
        Reconnect::Disabled => constants::MQCNO_RECONNECT_DISABLED,
        Reconnect::QueueManager => constants::MQCNO_RECONNECT_Q_MGR,
        Reconnect::Any => constants::MQCNO_RECONNECT,
    };
    let credentials = config
        .username
        .as_deref()
        .map(|user| Credentials::User(user, config.password.as_deref().unwrap_or_default().into()));
    let ccdt = match &config.endpoint {
        Endpoint::Ccdt(url) => Some(Ccdt(url)),
        Endpoint::Channel { .. } | Endpoint::ClientEnvironment => None,
    };
    // The peer-name buffer must outlive MQCONNX: the channel definition points into it.
    let peer_name = config.tls.as_ref().and_then(|tls| tls.peer_name.clone());
    let channel = match &config.endpoint {
        Endpoint::Channel {
            channel,
            connection_name,
        } => Some(channel_definition(
            channel,
            connection_name,
            peer_name.as_deref(),
        )?),
        Endpoint::Ccdt(_) | Endpoint::ClientEnvironment => None,
    };
    let key_repository = config
        .tls
        .as_ref()
        .and_then(|tls| tls.key_repository.as_deref())
        .map(|path| mq_str("tls.key.repository", path).map(KeyRepo))
        .transpose()?;
    let certificate_label = config
        .tls
        .as_ref()
        .and_then(|tls| tls.certificate_label.as_deref())
        .map(|label| mq_str("tls.certificate.label", label).map(CertificateLabel))
        .transpose()?;
    let cipher = config
        .tls
        .as_ref()
        .map(|tls| mq_str("tls.cipher.spec", &tls.cipher_spec).map(CipherSpec))
        .transpose()?;
    let repository_password = config
        .tls
        .as_ref()
        .and_then(|tls| tls.key_repository_password.as_deref());
    let tls = cipher.as_ref().map(|cipher| {
        let mut tls = Tls::new(
            key_repository.as_ref().unwrap_or(&KeyRepo(MqStr::empty())),
            certificate_label.as_ref(),
            cipher,
        );
        tls.key_repo_password(repository_password.map(mqi::types::ProtectedSecret::new));
        tls
    });
    // Options apply right to left: the channel definition first, then TLS fields into it.
    let options = (
        application,
        queue_manager,
        ccdt,
        credentials,
        tls,
        reconnect,
        channel,
    );
    mqi::connect_lib::<ThreadNone, _>(library, &options)
        .warn_as_error()
        .map_err(|error| classify("MQCONNX", &error))
}

fn channel_definition<'a>(
    channel: &str,
    connection_name: &str,
    peer_name: Option<&'a str>,
) -> Result<structs::MQCD<'a>, ConnectorError> {
    let mut cd = libmqm_default::MQCD_CLIENT_CONN_DEFAULT;
    cd.Version = libmqm_sys::MQCD_VERSION_11;
    cd.ChannelName = padded("channel", channel)?;
    cd.ConnectionName = padded("connection.name", connection_name)?;
    cd.TransportType = constants::MQXPT_TCP.0;
    if let Some(peer) = peer_name {
        cd.SSLPeerNamePtr = peer.as_ptr().cast_mut().cast();
        cd.SSLPeerNameLength = i32::try_from(peer.len()).map_err(|_| {
            ConnectorError::ConfigurationError("'tls.peer.name' is too long".into())
        })?;
    }
    Ok(structs::MQCD::new(cd))
}

/// Explicitly MQBACK, then let the object and connection close (`MQCLOSE`, `MQDISC`).
pub(super) struct Session {
    pub(super) queue: Object<Conn>,
    pub(super) queue_manager: String,
}

impl Session {
    /// Open `queue` on a new connection with `options` (plus fail-if-quiescing).
    pub(super) fn open(
        config: &ConnectionConfig,
        library: Library,
        queue: &str,
        target_queue_manager: Option<&str>,
        options: MQOO,
    ) -> Result<Self, ConnectorError> {
        let connection = connect(config, library)?;
        let name = QueueName(mq_str("queue", queue)?);
        let target = target_queue_manager
            .map(|name| mq_str("target.queue.manager", name).map(QueueManagerName))
            .transpose()?;
        let (queue, (_, resolved_queue_manager)): (
            Object<Conn>,
            (Option<QueueName>, Option<QueueManagerName>),
        ) = Object::open_with(
            connection,
            &(name, target, options | constants::MQOO_FAIL_IF_QUIESCING),
        )
        .warn_as_error()
        .map_err(|error| classify(&format!("MQOPEN '{queue}'"), &error))?;
        Ok(Self {
            queue,
            queue_manager: resolved_queue_manager
                .map(|name| name.0.to_string().trim().to_string())
                .unwrap_or_default(),
        })
    }

    pub(super) fn connection(&self) -> &Conn {
        self.queue.connection()
    }

    /// `MQCMIT`. An unknown outcome is reported as such, never as a definite failure.
    pub(super) fn commit(&self) -> Result<(), ConnectorError> {
        Syncpoint::new(self.connection())
            .commit()
            .warn_as_error()
            .map(drop)
            .map_err(|error| match error.2 {
                constants::MQRC_BACKED_OUT => ConnectorError::TransactionError(format!(
                    "MQCMIT backed out the unit of work: {error}"
                )),
                _ => ConnectorError::outcome_unknown(
                    format!("MQCMIT outcome unknown: {error}"),
                    true,
                ),
            })
    }

    /// `MQBACK`.
    pub(super) fn backout(&self) -> Result<(), ConnectorError> {
        Syncpoint::new(self.connection())
            .backout()
            .warn_as_error()
            .map(drop)
            .map_err(|error| classify("MQBACK", &error))
    }

    /// `BOTHRESH` and `BOQNAME` of the open queue (needs `+inq`).
    pub(super) fn backout_settings(&self) -> Result<(i32, String), ConnectorError> {
        let inquired = self
            .queue
            .inquire(&[
                attribute::MQIA_BACKOUT_THRESHOLD,
                attribute::MQCA_BACKOUT_REQ_Q_NAME,
            ])
            .warn_as_error()
            .map_err(|error| classify("MQINQ BOTHRESH/BOQNAME", &error))?;
        let mut threshold = 0;
        let mut queue = String::new();
        for item in inquired.iter() {
            match item.into_tuple().1 {
                attribute::AttributeValue::Long(value) => threshold = value,
                attribute::AttributeValue::Text(value) => queue = super::metadata::text(value),
            }
        }
        Ok((threshold, queue))
    }

    /// The queue manager a queue name resolves to, without keeping it open.
    pub(super) fn resolve(&self, queue: &str) -> Result<String, ConnectorError> {
        let (_, (_, resolved)): (Object<&Conn>, (Option<QueueName>, Option<QueueManagerName>)) =
            Object::open_with(
                self.connection(),
                &(
                    QueueName(mq_str("BOQNAME", queue)?),
                    constants::MQOO_OUTPUT | constants::MQOO_FAIL_IF_QUIESCING,
                ),
            )
            .warn_as_error()
            .map_err(|error| match classify("MQOPEN", &error) {
                failure @ ConnectorError::ConnectionFailed(_) => failure,
                failure => ConnectorError::ConfigurationError(format!(
                    "backout queue '{queue}' cannot be opened for output (+put needed): {failure}"
                )),
            })?;
        Ok(resolved
            .map(|name| name.0.to_string().trim().to_string())
            .unwrap_or_default())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // MQDISC would commit; uncheckpointed gets and uncommitted puts must be backed out.
        if let Err(error) = self.backout() {
            tracing::warn!(%error, "IBM MQ backout before disconnect failed");
        }
    }
}
