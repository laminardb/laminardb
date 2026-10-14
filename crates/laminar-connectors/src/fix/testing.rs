//! Local FIX counterparty for tests: a real QuickFIX acceptor, not a protocol simulator.
//!
//! SYNTHETIC test tooling. Passing tests against it is local conformance evidence for the
//! engine integration, not venue certification.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::engine::{
    self, Acceptor, Application, CallbackOutcome, GroupEntry, MessageRef, OwnedMessage, Section,
    SessionCallbacks, SessionKey, Settings,
};
use crate::error::ConnectorError;

/// One repeating-group entry of a test message: ordered fields, then nested groups.
#[derive(Debug, Clone, Default)]
pub struct TestGroupEntry {
    /// Fields in dictionary order; the first is the group delimiter.
    pub fields: Vec<(i32, String)>,
    /// Nested groups as `(count tag, entries)`.
    pub groups: Vec<(i32, Vec<TestGroupEntry>)>,
}

/// An application message sent by the test venue.
#[derive(Debug, Clone, Default)]
pub struct TestMessage {
    /// MsgType (35).
    pub msg_type: String,
    /// Body fields.
    pub fields: Vec<(i32, String)>,
    /// Body groups as `(count tag, entries)`.
    pub groups: Vec<(i32, Vec<TestGroupEntry>)>,
}

#[derive(Default)]
struct VenueCallbacks {
    logged_on: AtomicBool,
    logons: AtomicUsize,
    rejects: Mutex<Vec<Vec<u8>>>,
    application: Mutex<Vec<Vec<u8>>>,
}

impl SessionCallbacks for VenueCallbacks {
    fn on_logon(&self) {
        self.logons.fetch_add(1, Ordering::SeqCst);
        self.logged_on.store(true, Ordering::SeqCst);
    }

    fn on_logout(&self) {
        self.logged_on.store(false, Ordering::SeqCst);
    }

    fn inbound_admin(&self, message: MessageRef<'_>) -> CallbackOutcome {
        let is_reject = message
            .header()
            .and_then(|header| header.field(35))
            .is_ok_and(|value| value == Some(b"3".as_slice()));
        if is_reject {
            let mut bytes = Vec::new();
            if message.serialize_into(&mut bytes).is_ok() {
                self.rejects.lock().push(bytes);
            }
        }
        CallbackOutcome::Accept
    }

    fn inbound_app(&self, message: MessageRef<'_>) -> CallbackOutcome {
        let mut bytes = Vec::new();
        if message.serialize_into(&mut bytes).is_ok() {
            self.application.lock().push(bytes);
        }
        CallbackOutcome::Accept
    }

    fn on_panic(&self, _callback: &'static str) {}
}

/// A local acceptor that plays the counterparty. It persists outbound messages, so it answers
/// resend requests with real retransmissions.
pub struct FixTestVenue {
    port: u16,
    key: SessionKey,
    acceptor: Acceptor,
    callbacks: Arc<VenueCallbacks>,
}

fn engine_error(error: engine::EngineError) -> ConnectorError {
    ConnectorError::from(error)
}

impl FixTestVenue {
    /// Start listening on an ephemeral loopback port as `venue_comp_id` for a client that logs
    /// on as `client_comp_id`.
    ///
    /// # Errors
    /// Fails when the engine rejects the settings or cannot listen.
    pub fn start(
        store: &Path,
        dictionary: &Path,
        begin_string: &str,
        venue_comp_id: &str,
        client_comp_id: &str,
    ) -> Result<Self, ConnectorError> {
        Self::start_with_settings(
            store,
            dictionary,
            begin_string,
            venue_comp_id,
            client_comp_id,
            &[],
        )
    }

    /// As [`Self::start`], with additional engine settings for the venue.
    ///
    /// # Errors
    /// Fails when the engine rejects the settings or cannot listen.
    pub fn start_with_settings(
        store: &Path,
        dictionary: &Path,
        begin_string: &str,
        venue_comp_id: &str,
        client_comp_id: &str,
        settings: &[(&'static str, String)],
    ) -> Result<Self, ConnectorError> {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map(|address| address.port())?;
        let mut defaults = vec![
            ("ConnectionType", "acceptor".to_string()),
            ("SocketAcceptPort", port.to_string()),
            ("FileStorePath", store.display().to_string()),
            ("UseDataDictionary", "Y".to_string()),
            ("DataDictionary", dictionary.display().to_string()),
            ("StartTime", "00:00:00".to_string()),
            ("EndTime", "00:00:00".to_string()),
            ("PersistMessages", "Y".to_string()),
        ];
        defaults.extend(settings.iter().cloned());
        let key = SessionKey {
            begin_string: begin_string.to_string(),
            sender_comp_id: venue_comp_id.to_string(),
            target_comp_id: client_comp_id.to_string(),
            qualifier: None,
        };
        let settings = Settings::single_session(&defaults, &key, &[]).map_err(engine_error)?;
        let callbacks = Arc::new(VenueCallbacks::default());
        let application = Application::new(callbacks.clone()).map_err(engine_error)?;
        let acceptor = Acceptor::new(settings, application).map_err(engine_error)?;
        acceptor.start().map_err(engine_error)?;
        Ok(Self {
            port,
            key,
            acceptor,
            callbacks,
        })
    }

    /// Listening port.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Whether the client is logged on.
    #[must_use]
    pub fn logged_on(&self) -> bool {
        self.callbacks.logged_on.load(Ordering::SeqCst)
    }

    /// Completed logons so far.
    #[must_use]
    pub fn logons(&self) -> usize {
        self.callbacks.logons.load(Ordering::SeqCst)
    }

    /// Wait until the client is logged on, up to `timeout`.
    #[must_use]
    pub fn wait_logged_on(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while !self.logged_on() {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }

    /// Send an application message. The engine assigns MsgSeqNum and SendingTime.
    ///
    /// # Errors
    /// Fails when the message cannot be built or the session is not registered.
    pub fn send(&self, message: &TestMessage) -> Result<(), ConnectorError> {
        let mut outbound = OwnedMessage::new().map_err(engine_error)?;
        outbound
            .set_field(Section::Header, 35, message.msg_type.as_bytes())
            .map_err(engine_error)?;
        for (tag, value) in &message.fields {
            outbound
                .set_field(Section::Body, *tag, value.as_bytes())
                .map_err(engine_error)?;
        }
        for (count_tag, entries) in &message.groups {
            for entry in entries {
                outbound
                    .add_group(*count_tag, &borrow(entry))
                    .map_err(engine_error)?;
            }
        }
        outbound.send_to(&self.key).map_err(engine_error)
    }

    /// Skip `count` outbound sequence numbers, so the client sees a gap and requests a resend
    /// that the venue answers with a gap fill.
    ///
    /// # Errors
    /// Fails when the session is not registered.
    pub fn skip_sequence_numbers(&self, count: u64) -> Result<(), ConnectorError> {
        let state = engine::session_state(&self.key).map_err(engine_error)?;
        engine::set_next_sender(&self.key, state.next_sender + count).map_err(engine_error)
    }

    /// Send SequenceReset-Reset to `new_seq` and continue numbering from it, skipping messages
    /// the client can never recover.
    ///
    /// # Errors
    /// Fails when the message cannot be sent.
    pub fn sequence_reset(&self, new_seq: u64) -> Result<(), ConnectorError> {
        let mut reset = OwnedMessage::new().map_err(engine_error)?;
        reset
            .set_field(Section::Header, 35, b"4")
            .map_err(engine_error)?;
        reset
            .set_field(Section::Body, 36, new_seq.to_string().as_bytes())
            .map_err(engine_error)?;
        reset.send_to(&self.key).map_err(engine_error)?;
        engine::set_next_sender(&self.key, new_seq).map_err(engine_error)
    }

    /// Reset the venue session: Logout, disconnect, and restart both sequence numbers at 1.
    ///
    /// # Errors
    /// Fails when the session is not registered.
    pub fn reset_session(&self) -> Result<(), ConnectorError> {
        engine::reset_session(&self.key).map_err(engine_error)
    }

    /// Next outbound sequence number of the venue.
    ///
    /// # Errors
    /// Fails when the session is not registered.
    pub fn next_sender(&self) -> Result<u64, ConnectorError> {
        Ok(engine::session_state(&self.key)
            .map_err(engine_error)?
            .next_sender)
    }

    /// Session-level rejects received from the client, as engine serializations.
    #[must_use]
    pub fn rejects(&self) -> Vec<Vec<u8>> {
        self.callbacks.rejects.lock().clone()
    }

    /// Application messages received from the client, as engine serializations.
    #[must_use]
    pub fn received_application(&self) -> Vec<Vec<u8>> {
        self.callbacks.application.lock().clone()
    }

    /// Stop the acceptor and join its threads.
    ///
    /// # Errors
    /// Fails when the engine reports a stop error.
    pub fn stop(&self) -> Result<(), ConnectorError> {
        self.acceptor.stop(true).map_err(engine_error)
    }
}

fn borrow(entry: &TestGroupEntry) -> GroupEntry<'_> {
    GroupEntry {
        fields: entry
            .fields
            .iter()
            .map(|(tag, value)| (*tag, value.as_str()))
            .collect(),
        groups: entry
            .groups
            .iter()
            .map(|(tag, entries)| (*tag, entries.iter().map(borrow).collect()))
            .collect(),
    }
}
