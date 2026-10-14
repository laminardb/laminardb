//! Engine qualification: proves the callback, sequencing, and ownership properties the durable
//! handoff relies on, against a real local QuickFIX acceptor.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::application::OutboundMessage;
use super::*;

pub(crate) const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fix/FIX44-synthetic.xml"
);

/// Engine sessions are process-global, so concurrently running tests use distinct CompIDs.
pub(crate) fn initiator_key(name: &str) -> SessionKey {
    SessionKey {
        begin_string: "FIX.4.4".into(),
        sender_comp_id: format!("LAMINAR_{name}"),
        target_comp_id: format!("VENUE_{name}"),
        qualifier: None,
    }
}

pub(crate) fn acceptor_key(name: &str) -> SessionKey {
    SessionKey {
        begin_string: "FIX.4.4".into(),
        sender_comp_id: format!("VENUE_{name}"),
        target_comp_id: format!("LAMINAR_{name}"),
        qualifier: None,
    }
}

fn common(store: &std::path::Path) -> Vec<(&'static str, String)> {
    // Client-side engine defaults; the venue is configured by `FixTestVenue`.
    vec![
        ("FileStorePath", store.display().to_string()),
        ("UseDataDictionary", "Y".into()),
        ("DataDictionary", DICTIONARY.into()),
        ("StartTime", "00:00:00".into()),
        ("EndTime", "00:00:00".into()),
        ("HeartBtInt", "30".into()),
    ]
}

struct Venue(crate::fix::testing::FixTestVenue);

impl Venue {
    fn start(dir: &std::path::Path, name: &str) -> Self {
        let key = acceptor_key(name);
        Self(
            crate::fix::testing::FixTestVenue::start(
                &dir.join("venue-store"),
                std::path::Path::new(DICTIONARY),
                &key.begin_string,
                &key.sender_comp_id,
                &key.target_comp_id,
            )
            .expect("venue"),
        )
    }

    fn port(&self) -> u16 {
        self.0.port()
    }

    fn wait_logged_on(&self) {
        assert!(
            self.0.wait_logged_on(Duration::from_secs(20)),
            "venue logon"
        );
    }

    /// Send an ExecutionReport; `exec_id = None` omits a required field.
    fn send_execution_report(&self, exec_id: Option<&str>, fee_count: usize) {
        let mut fields: Vec<(i32, String)> = vec![(37, "ORD-1".into())];
        if let Some(exec_id) = exec_id {
            fields.push((17, exec_id.into()));
        }
        for (tag, value) in [
            (150, "F"),
            (39, "1"),
            (55, "ABC"),
            (54, "1"),
            (151, "0"),
            (14, "10"),
            (6, "100.25"),
        ] {
            fields.push((tag, value.into()));
        }
        let fees = (0..fee_count)
            .map(|fee| crate::fix::testing::TestGroupEntry {
                fields: vec![(137, format!("0.{fee}1")), (138, "USD".into())],
                groups: Vec::new(),
            })
            .collect();
        self.0
            .send(&crate::fix::testing::TestMessage {
                msg_type: "8".into(),
                fields,
                groups: vec![(136, fees)],
            })
            .expect("send");
    }

    fn rejects(&self) -> Vec<Vec<u8>> {
        self.0.rejects()
    }
}

impl Drop for Venue {
    fn drop(&mut self) {
        let _ = self.0.stop();
    }
}

pub(crate) fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A delivered inbound application message as observed inside `from_app`.
#[derive(Debug, Clone)]
struct Delivery {
    seq: u64,
    poss_dup: bool,
    target_during_callback: u64,
    thread: std::thread::ThreadId,
    outcome: CallbackOutcome,
}

enum Fault {
    None,
    DisconnectOnce(u64),
    PanicOnce(u64),
}

struct ClientCallbacks {
    key: SessionKey,
    fault: Mutex<Fault>,
    deliveries: Mutex<Vec<Delivery>>,
    outbound_rejects: Mutex<Vec<u64>>,
    panics: AtomicU64,
}

impl ClientCallbacks {
    fn new(name: &str, fault: Fault) -> Arc<Self> {
        Arc::new(Self {
            key: initiator_key(name),
            fault: Mutex::new(fault),
            deliveries: Mutex::new(Vec::new()),
            outbound_rejects: Mutex::new(Vec::new()),
            panics: AtomicU64::new(0),
        })
    }
}

fn decimal(bytes: Option<&[u8]>) -> u64 {
    std::str::from_utf8(bytes.expect("field present"))
        .expect("ascii")
        .parse()
        .expect("number")
}

impl SessionCallbacks for ClientCallbacks {
    fn to_admin(&self, message: &mut OutboundMessage<'_>) {
        let view = message.view();
        let header = view.header().expect("header");
        if header.field(35).expect("type") == Some(b"3".as_slice()) {
            let body = view.body().expect("body");
            self.outbound_rejects
                .lock()
                .push(decimal(body.field(45).expect("ref")));
        }
    }
    fn from_admin(&self, _message: MessageRef<'_>) -> CallbackOutcome {
        CallbackOutcome::Accept
    }
    fn from_app(&self, message: MessageRef<'_>) -> CallbackOutcome {
        let header = message.header().expect("header");
        let seq = decimal(header.field(34).expect("seq"));
        let poss_dup = header.field(43).expect("dup") == Some(b"Y".as_slice());
        let target = session_state(&self.key).expect("state").next_target;
        let mut fault = self.fault.lock();
        let outcome = match *fault {
            Fault::DisconnectOnce(at) if at == seq => {
                *fault = Fault::None;
                CallbackOutcome::Disconnect
            }
            Fault::PanicOnce(at) if at == seq => {
                *fault = Fault::None;
                drop(fault);
                self.deliveries.lock().push(Delivery {
                    seq,
                    poss_dup,
                    target_during_callback: target,
                    thread: std::thread::current().id(),
                    outcome: CallbackOutcome::Disconnect,
                });
                panic!("injected callback panic");
            }
            _ => CallbackOutcome::Accept,
        };
        self.deliveries.lock().push(Delivery {
            seq,
            poss_dup,
            target_during_callback: target,
            thread: std::thread::current().id(),
            outcome,
        });
        outcome
    }
    fn on_panic(&self, _callback: &'static str) {
        self.panics.fetch_add(1, Ordering::SeqCst);
    }
}

struct Client {
    initiator: Initiator,
}

impl Client {
    fn start(dir: &std::path::Path, port: u16, callbacks: Arc<ClientCallbacks>) -> Self {
        let mut defaults = common(&dir.join("client-store"));
        defaults.push(("ConnectionType", "initiator".into()));
        defaults.push(("SocketConnectHost", "127.0.0.1".into()));
        defaults.push(("SocketConnectPort", port.to_string()));
        defaults.push(("ReconnectInterval", "1".into()));
        defaults.push(("PersistMessages", "N".into()));
        let settings = Settings::single_session(&defaults, &callbacks.key, &[]).expect("settings");
        let application = Application::new(callbacks).expect("application");
        let initiator = Initiator::new(settings, application).expect("initiator");
        initiator.start().expect("start");
        Self { initiator }
    }

    fn stop(self) {
        self.initiator.stop(true).expect("stop");
        assert!(self.initiator.is_stopped());
    }
}

fn accepted_sequence(deliveries: &[Delivery]) -> Vec<u64> {
    deliveries
        .iter()
        .filter(|delivery| delivery.outcome == CallbackOutcome::Accept)
        .map(|delivery| delivery.seq)
        .collect()
}

/// The inbound target sequence advances only after `from_app` returns `Accept`; `Disconnect`
/// leaves it unchanged and the counterparty retransmits the same sequence number with
/// PossDupFlag=Y after the engine reconnects.
#[test]
fn disconnect_outcome_does_not_advance_target_and_message_is_retransmitted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let test_thread = std::thread::current().id();
    let venue = Venue::start(dir.path(), "disconnect");
    let callbacks = ClientCallbacks::new("disconnect", Fault::DisconnectOnce(3));
    let client = Client::start(dir.path(), venue.port(), callbacks.clone());
    venue.wait_logged_on();
    for exec in ["E1", "E2", "E3"] {
        venue.send_execution_report(Some(exec), 1);
    }
    wait_until("three accepted messages", || {
        accepted_sequence(&callbacks.deliveries.lock()).len() == 3
    });
    client.stop();

    let deliveries = callbacks.deliveries.lock().clone();
    // Venue seq 1 is Logon; application messages are 2, 3, 4.
    assert_eq!(accepted_sequence(&deliveries), vec![2, 3, 4]);
    let refused = deliveries
        .iter()
        .find(|delivery| delivery.outcome == CallbackOutcome::Disconnect)
        .expect("refused delivery");
    assert_eq!(refused.seq, 3);
    assert!(!refused.poss_dup);
    assert_eq!(refused.target_during_callback, 3);
    let redelivered = deliveries
        .iter()
        .find(|delivery| delivery.seq == 3 && delivery.outcome == CallbackOutcome::Accept)
        .expect("redelivery");
    assert!(redelivered.poss_dup, "retransmission carries PossDupFlag=Y");
    assert_eq!(
        redelivered.target_during_callback, 3,
        "target did not advance"
    );
    for delivery in &deliveries {
        assert_eq!(delivery.target_during_callback, delivery.seq);
        assert_ne!(
            delivery.thread, test_thread,
            "callbacks run on engine threads"
        );
    }
}

/// A panic in `from_app` is contained, reported, and treated as `Disconnect`, never as success.
#[test]
fn panic_in_callback_is_contained_and_not_reported_as_success() {
    let dir = tempfile::tempdir().expect("tempdir");
    let venue = Venue::start(dir.path(), "panic");
    let callbacks = ClientCallbacks::new("panic", Fault::PanicOnce(2));
    let client = Client::start(dir.path(), venue.port(), callbacks.clone());
    venue.wait_logged_on();
    venue.send_execution_report(Some("E1"), 0);
    venue.send_execution_report(Some("E2"), 0);
    wait_until("two accepted messages", || {
        accepted_sequence(&callbacks.deliveries.lock()).len() == 2
    });
    client.stop();
    assert_eq!(callbacks.panics.load(Ordering::SeqCst), 1);
    assert_eq!(accepted_sequence(&callbacks.deliveries.lock()), vec![2, 3]);
}

/// Session-level validation failures never reach `from_app`: the engine rejects the message
/// and advances the target sequence before the outbound Reject reaches `to_admin`.
#[test]
fn validation_reject_advances_target_without_application_callback() {
    let dir = tempfile::tempdir().expect("tempdir");
    let venue = Venue::start(dir.path(), "reject");
    let callbacks = ClientCallbacks::new("reject", Fault::None);
    let client = Client::start(dir.path(), venue.port(), callbacks.clone());
    venue.wait_logged_on();
    venue.send_execution_report(None, 0);
    venue.send_execution_report(Some("E2"), 0);
    wait_until("valid message accepted", || {
        accepted_sequence(&callbacks.deliveries.lock()).len() == 1
    });
    client.stop();
    assert_eq!(*callbacks.outbound_rejects.lock(), vec![2]);
    let deliveries = callbacks.deliveries.lock().clone();
    assert_eq!(accepted_sequence(&deliveries), vec![3]);
    assert_eq!(deliveries[0].target_during_callback, 3);
    assert_eq!(venue.rejects().len(), 1);
}

/// Length-aware access preserves SOH and NUL bytes in data fields through engine
/// serialization and dictionary parsing; repeating groups keep their entry boundaries.
#[test]
fn data_fields_and_groups_round_trip_through_engine_serialization() {
    let dictionary = DataDictionary::from_path(DICTIONARY).expect("dictionary");
    let mut message = OwnedMessage::new().expect("message");
    let header = [(8, "FIX.4.4"), (35, "8"), (49, "VENUE"), (56, "LAMINAR")];
    for (tag, value) in header {
        message
            .set_field(Section::Header, tag, value.as_bytes())
            .expect("header");
    }
    message.set_field(Section::Header, 34, b"7").expect("seq");
    message
        .set_field(Section::Header, 52, b"20261014-12:00:00.123456789")
        .expect("time");
    for (tag, value) in [
        (37, "0007"),
        (17, "E"),
        (150, "F"),
        (39, "2"),
        (55, "X"),
        (54, "2"),
    ] {
        message
            .set_field(Section::Body, tag, value.as_bytes())
            .expect("body");
    }
    for (tag, value) in [(151, "0"), (14, "5"), (6, "1.5")] {
        message
            .set_field(Section::Body, tag, value.as_bytes())
            .expect("body");
    }
    let binary = b"a\x01b\x00c";
    message
        .set_field(Section::Body, 354, binary.len().to_string().as_bytes())
        .expect("len");
    message.set_field(Section::Body, 355, binary).expect("data");
    for fields in [vec![(137, "1.01"), (138, "USD")], vec![(137, "2.02")]] {
        let entry = GroupEntry {
            fields,
            groups: Vec::new(),
        };
        message.add_group(136, &entry).expect("fee");
    }

    let mut serialized = Vec::new();
    message
        .view()
        .serialize_into(&mut serialized)
        .expect("serialize");
    assert!(serialized
        .windows(binary.len())
        .any(|window| window == binary));

    let parsed = dictionary.parse(&serialized).expect("parse");
    let body = parsed.view().body().expect("body");
    assert_eq!(body.field(355).expect("data"), Some(binary.as_slice()));
    assert_eq!(body.field(37).expect("order"), Some(b"0007".as_slice()));
    assert_eq!(body.group_count(136).expect("count"), 2);
    let second = body.group_entry(136, 2).expect("entry");
    assert_eq!(second.field(137).expect("amount"), Some(b"2.02".as_slice()));
    assert_eq!(second.field(138).expect("currency"), None);
    assert!(body.group_entry(136, 3).is_err());
    assert_eq!(body.group_count(453).expect("absent group"), 0);

    let mut reparsed = Vec::new();
    parsed
        .view()
        .serialize_into(&mut reparsed)
        .expect("serialize");
    assert_eq!(reparsed, serialized, "engine serialization is stable");
}

mod properties {
    use proptest::prelude::*;

    use super::*;

    fn valid_message() -> Vec<u8> {
        let mut message = OwnedMessage::new().expect("message");
        for (tag, value) in [
            (8, "FIX.4.4"),
            (35, "8"),
            (49, "A"),
            (56, "B"),
            (34, "2"),
            (52, "20261014-12:00:00"),
        ] {
            message
                .set_field(Section::Header, tag, value.as_bytes())
                .expect("header");
        }
        for (tag, value) in [
            (37, "O"),
            (17, "E"),
            (150, "F"),
            (39, "1"),
            (55, "X"),
            (54, "1"),
            (151, "0"),
            (14, "1"),
            (6, "1"),
        ] {
            message
                .set_field(Section::Body, tag, value.as_bytes())
                .expect("body");
        }
        let mut bytes = Vec::new();
        message
            .view()
            .serialize_into(&mut bytes)
            .expect("serialize");
        bytes
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Untrusted bytes, and corrupted valid messages, either parse or fail with an error;
        /// no input crashes the engine boundary.
        #[test]
        fn parsing_untrusted_bytes_is_contained(
            noise in proptest::collection::vec(any::<u8>(), 0..512),
            flips in proptest::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 0..8),
        ) {
            let dictionary = DataDictionary::from_path(DICTIONARY).expect("dictionary");
            let _ = dictionary.parse(&noise);
            let mut corrupted = valid_message();
            for (index, byte) in flips {
                let position = index.index(corrupted.len());
                corrupted[position] = byte;
            }
            if let Ok(parsed) = dictionary.parse(&corrupted) {
                let body = parsed.view().body().expect("body");
                let _ = body.field(37);
                let _ = body.group_count(453);
            }
        }
    }
}
