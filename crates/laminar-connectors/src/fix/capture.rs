//! Session callbacks that durably capture inbound application messages.
//!
//! Ordering invariant: `from_app` returns `Accept` only after the message's journal record was
//! appended. The engine advances its inbound sequence number after `Accept`, and the session
//! store persists it only up to the journal's durability floor, so persisted session progress
//! never passes application data a crash can lose. Any condition that prevents capture returns
//! `Disconnect`, which leaves the sequence number unchanged and makes the counterparty
//! retransmit after the engine reconnects.
//!
//! Sequence series: the engine resets its store, restarting both sequence numbers at 1, for an
//! accepted Logon with ResetSeqNumFlag=Y, for a configured reset, and at a new schedule period.
//! Every reset is followed by a Logon before application messages, so the Logon callback starts a
//! new journal series when the Logon carries ResetSeqNumFlag=Y or the store's creation time
//! changed.
//!
//! Retransmissions: after a restart the engine's persisted target may trail the journal. A
//! message at or below the journal's high-water mark is a retransmission of an already captured
//! sequence number; it is compared with the original by content digest, ignoring header fields
//! that retransmission may change, and is never journaled twice.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

use super::config::Credentials;
use super::engine::{
    CallbackOutcome, MessageRef, OutboundMessage, SessionCallbacks, SessionKey, StoreBackend,
};
use super::journal::{ApplicationRecord, Journal, Record};
use super::metrics::{saturating, FixMetrics};
use super::session_store::SessionStore;

/// Header fields a retransmission may legitimately change: BodyLength, PossDupFlag,
/// SendingTime, PossResend, OrigSendingTime, LastMsgSeqNumProcessed.
const RETRANSMISSION_HEADER_TAGS: [i32; 6] = [9, 43, 52, 97, 122, 369];

/// Shared session status read by the source.
#[derive(Default)]
pub(crate) struct SessionStatus {
    fatal: Mutex<Option<String>>,
}

impl SessionStatus {
    /// The condition that stopped capture, if any. Terminal for this source generation.
    pub(crate) fn fatal(&self) -> Option<String> {
        self.fatal.lock().clone()
    }

    fn fail(&self, reason: String) {
        let mut fatal = self.fatal.lock();
        if fatal.is_none() {
            tracing::error!(reason = %reason, "FIX capture stopped");
            *fatal = Some(reason);
        }
    }
}

struct CaptureState {
    /// `(seq, digest)` of journaled messages the engine may redeliver after a restart.
    redelivery: Option<Vec<(u64, u64)>>,
    serialized: Vec<u8>,
    digest_input: Vec<u8>,
}

/// Session callbacks bound to one journal.
pub(crate) struct Capture {
    journal: Arc<Journal>,
    store: Arc<SessionStore>,
    session: SessionKey,
    messages: Vec<String>,
    credentials: Option<Credentials>,
    max_message_bytes: usize,
    status: Arc<SessionStatus>,
    metrics: Arc<FixMetrics>,
    state: Mutex<CaptureState>,
}

impl Capture {
    #[allow(clippy::too_many_arguments)] // INVARIANT: every field is fixed for the session's life.
    pub(crate) fn new(
        journal: Arc<Journal>,
        store: Arc<SessionStore>,
        session: SessionKey,
        messages: Vec<String>,
        credentials: Option<Credentials>,
        max_message_bytes: usize,
        status: Arc<SessionStatus>,
        metrics: Arc<FixMetrics>,
    ) -> Self {
        Self {
            journal,
            store,
            session,
            messages,
            credentials,
            max_message_bytes,
            status,
            metrics,
            state: Mutex::new(CaptureState {
                redelivery: None,
                serialized: Vec::new(),
                digest_input: Vec::new(),
            }),
        }
    }

    /// Why input must not be accepted right now.
    fn refusal(&self) -> Option<String> {
        if let Some(fatal) = self.status.fatal() {
            return Some(fatal);
        }
        if let Some(poisoned) = self.journal.poisoned() {
            return Some(poisoned);
        }
        if let Some(poisoned) = self.store.poisoned() {
            return Some(poisoned);
        }
        if self.journal.over_capacity() {
            self.metrics.journal_paused.set(1);
            return Some(
                "journal is over 'journal.max.bytes'; intake pauses until checkpoints commit"
                    .into(),
            );
        }
        self.metrics.journal_paused.set(0);
        None
    }

    fn append(&self, record: &Record) -> CallbackOutcome {
        match self.journal.append(record) {
            Ok(_) => {
                self.metrics
                    .journal_retained_bytes
                    .set(saturating(self.journal.retained_bytes()));
                CallbackOutcome::Accept
            }
            Err(error) => {
                self.status
                    .fail(format!("FIX journal append failed: {error}"));
                CallbackOutcome::Disconnect
            }
        }
    }

    fn logon(&self, message: MessageRef<'_>) -> CallbackOutcome {
        let reset_flag = message
            .body()
            .and_then(|body| body.field(141))
            .is_ok_and(|value| value == Some(b"Y".as_slice()));
        let engine = self.store.values();
        let progress = self.journal.progress();
        let window = self.journal.take_redelivery_window(engine.next_target);
        let mut state = self.state.lock();
        if reset_flag
            || progress.series == 0
            || progress.store_created_unix_micros != engine.creation_unix_micros
        {
            state.redelivery = Some(Vec::new());
            drop(state);
            let Some(series) = progress.series.checked_add(1) else {
                self.status
                    .fail("FIX sequence series counter overflow".into());
                return CallbackOutcome::Disconnect;
            };
            tracing::info!(
                session = %self.session.display(),
                series,
                previous = progress.series,
                "FIX session store reset; starting a new inbound sequence series"
            );
            return self.append(&Record::SeriesStarted {
                series,
                store_created_unix_micros: engine.creation_unix_micros,
            });
        }
        if state.redelivery.is_none() {
            state.redelivery = Some(window);
        }
        CallbackOutcome::Accept
    }

    fn sequence_reset(&self, message: MessageRef<'_>) -> CallbackOutcome {
        let fields = (|| -> Result<_, super::engine::EngineError> {
            let body = message.body()?;
            let header = message.header()?;
            Ok((
                body.field(123)?.is_some_and(|flag| flag == b"Y"),
                parse_seq(body.field(36)?),
                parse_seq(header.field(34)?),
            ))
        })();
        let Ok((gap_fill, Some(new_seq), Some(seq))) = fields else {
            // The engine rejects a malformed SequenceReset after this callback.
            return CallbackOutcome::Accept;
        };
        let progress = self.journal.progress();
        if gap_fill {
            if new_seq <= seq || new_seq <= progress.high_water.saturating_add(1) {
                return CallbackOutcome::Accept;
            }
            self.metrics
                .gap_fill_seqs
                .inc_by(new_seq - seq.max(progress.high_water.saturating_add(1)));
            return self.append(&Record::GapFill {
                series: progress.series,
                from_seq: seq,
                new_seq,
            });
        }
        let expected = self.store.values().next_target;
        if new_seq <= expected {
            return CallbackOutcome::Accept;
        }
        self.metrics.sequence_resets.inc();
        tracing::warn!(
            session = %self.session.display(),
            from = expected,
            to = new_seq,
            "counterparty SequenceReset-Reset skipped inbound sequence numbers; history in the range is unrecoverable"
        );
        self.append(&Record::SequenceReset {
            series: progress.series,
            from_seq: expected,
            new_seq,
        })
    }

    fn application(&self, message: MessageRef<'_>) -> Result<CallbackOutcome, String> {
        let header = message.header().map_err(|error| error.to_string())?;
        let msg_type = header
            .field(35)
            .map_err(|error| error.to_string())?
            .and_then(|value| std::str::from_utf8(value).ok())
            .ok_or("message has no MsgType")?
            .to_string();
        if !self.messages.contains(&msg_type) {
            self.metrics.messages_excluded.inc();
            return Ok(CallbackOutcome::Accept);
        }
        let seq = parse_seq(header.field(34).map_err(|error| error.to_string())?)
            .ok_or("message has no MsgSeqNum")?;
        let poss_dup =
            header.field(43).map_err(|error| error.to_string())? == Some(b"Y".as_slice());
        let poss_resend =
            header.field(97).map_err(|error| error.to_string())? == Some(b"Y".as_slice());
        let engine = self.store.values();
        let progress = self.journal.progress();
        if engine.creation_unix_micros != progress.store_created_unix_micros {
            return Err("session store identity changed without a logon; refusing to attribute the message to a series".into());
        }
        let mut state = self.state.lock();
        let state = &mut *state;
        message
            .serialize_without_header_tags(&RETRANSMISSION_HEADER_TAGS, &mut state.digest_input)
            .map_err(|error| error.to_string())?;
        let digest = xxh3_64(&state.digest_input);
        if seq <= progress.high_water {
            return Ok(self.redelivered(state, seq, digest));
        }
        message
            .serialize_into(&mut state.serialized)
            .map_err(|error| error.to_string())?;
        if state.serialized.len() > self.max_message_bytes {
            return Err(format!(
                "message seq {seq} of {} bytes exceeds 'max.message.bytes' ({})",
                state.serialized.len(),
                self.max_message_bytes
            ));
        }
        let record = Record::Application(ApplicationRecord {
            series: progress.series,
            seq,
            received_at_nanos: now_nanos(),
            poss_dup,
            poss_resend,
            msg_type,
            content_digest: digest,
            message: std::mem::take(&mut state.serialized),
        });
        let outcome = self.append(&record);
        if let Record::Application(record) = record {
            state.serialized = record.message;
        }
        if outcome == CallbackOutcome::Accept {
            self.metrics.messages_captured.inc();
            self.metrics.next_target_seq.set(saturating(seq + 1));
        }
        Ok(outcome)
    }

    fn redelivered(&self, state: &CaptureState, seq: u64, digest: u64) -> CallbackOutcome {
        let original = state.redelivery.as_ref().and_then(|window| {
            window
                .binary_search_by_key(&seq, |(seq, _)| *seq)
                .ok()
                .map(|position| window[position].1)
        });
        match original {
            Some(original) if original == digest => {
                self.metrics.duplicates.inc();
                CallbackOutcome::Accept
            }
            Some(_) => {
                self.metrics.duplicate_conflicts.inc();
                self.status.fail(format!(
                    "counterparty retransmitted seq {seq} of session {} with content that differs \
                     from the journaled original",
                    self.session.display()
                ));
                CallbackOutcome::Disconnect
            }
            None => {
                self.metrics.duplicates_unverified.inc();
                tracing::warn!(
                    session = %self.session.display(),
                    seq,
                    "dropping a retransmission of an already captured sequence number whose \
                     original is no longer retained"
                );
                CallbackOutcome::Accept
            }
        }
    }
}

impl SessionCallbacks for Capture {
    fn on_logon(&self) {
        self.metrics.logged_on.set(1);
    }

    fn on_logout(&self) {
        self.metrics.logged_on.set(0);
    }

    fn to_admin(&self, message: &mut OutboundMessage<'_>) {
        let msg_type = msg_type(message.view());
        match msg_type.as_deref() {
            Some(b"A") => {
                // The engine buffers a whole message before any limit applies, so ask the
                // counterparty for the bound (MaxMessageSize, 383).
                let max_size = self.max_message_bytes.to_string();
                let username = self
                    .credentials
                    .as_ref()
                    .and_then(|credentials| credentials.username.as_deref())
                    .map(|value| (553, value));
                let password = self
                    .credentials
                    .as_ref()
                    .and_then(|credentials| credentials.password.as_ref())
                    .map(|value| (554, value.expose()));
                let fields = [(383, max_size.as_str())]
                    .into_iter()
                    .chain(username)
                    .chain(password);
                for (tag, value) in fields {
                    if let Err(error) = message.set_body(tag, value.as_bytes()) {
                        self.status
                            .fail(format!("cannot set logon field {tag}: {error}"));
                    }
                }
            }
            Some(b"3") => self.rejected(message.view()),
            _ => {}
        }
    }

    fn inbound_admin(&self, message: MessageRef<'_>) -> CallbackOutcome {
        if let Some(reason) = self.refusal() {
            tracing::warn!(session = %self.session.display(), reason = %reason, "refusing FIX session input");
            return CallbackOutcome::Disconnect;
        }
        match msg_type(message).as_deref() {
            Some(b"A") => self.logon(message),
            Some(b"4") => self.sequence_reset(message),
            _ => CallbackOutcome::Accept,
        }
    }

    fn inbound_app(&self, message: MessageRef<'_>) -> CallbackOutcome {
        if let Some(reason) = self.refusal() {
            tracing::warn!(session = %self.session.display(), reason = %reason, "refusing FIX application message");
            return CallbackOutcome::Disconnect;
        }
        match self.application(message) {
            Ok(outcome) => outcome,
            Err(reason) => {
                self.status.fail(format!("FIX capture failed: {reason}"));
                CallbackOutcome::Disconnect
            }
        }
    }

    fn on_event(&self, text: &str) {
        tracing::debug!(session = %self.session.display(), event = text, "FIX engine event");
    }

    fn on_panic(&self, callback: &'static str) {
        self.status
            .fail(format!("FIX callback {callback} panicked"));
    }
}

impl Capture {
    fn rejected(&self, message: MessageRef<'_>) {
        let fields = (|| -> Result<_, super::engine::EngineError> {
            let body = message.body()?;
            Ok((
                parse_seq(body.field(45)?),
                body.field(373)?.map(<[u8]>::to_vec),
            ))
        })();
        let Ok((Some(ref_seq), reason)) = fields else {
            return;
        };
        let reason = reason
            .and_then(|value| {
                std::str::from_utf8(&value)
                    .ok()
                    .and_then(|text| text.parse().ok())
            })
            .unwrap_or(-1);
        self.metrics.rejects_sent.inc();
        let series = self.journal.progress().series;
        if self.append(&Record::Rejected {
            series,
            ref_seq,
            reason,
        }) != CallbackOutcome::Accept
        {
            tracing::error!(ref_seq, "could not journal a session-level reject");
        }
    }
}

fn msg_type(message: MessageRef<'_>) -> Option<Vec<u8>> {
    message.header().ok()?.field(35).ok()?.map(<[u8]>::to_vec)
}

fn parse_seq(value: Option<&[u8]>) -> Option<u64> {
    std::str::from_utf8(value?).ok()?.parse().ok()
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .unwrap_or(0)
}
