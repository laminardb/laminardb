use pgwire_replication::PgWireError;

use super::super::checkpoint::SnapshotPosition;
use super::creation::{create_failure, CreateFailure};
use super::*;

const ID: &str = "0123456789abcdef";
const INCARNATION: &str = "89abcdef";

fn claim() -> SlotClaim {
    SlotClaim::parse("orders", ID).unwrap()
}

fn holder(application_name: &str) -> SlotHolder {
    SlotHolder {
        pid: 42,
        application_name: application_name.into(),
        client_addr: Some("10.0.0.7".into()),
    }
}

fn slot(confirmed: Option<u64>, holder: Option<SlotHolder>, unusable: Option<&str>) -> SlotFacts {
    SlotFacts {
        confirmed_flush_lsn: confirmed.map(Lsn::new),
        holder,
        unusable: unusable.map(String::from),
    }
}

#[test]
fn claims_name_slots_and_sessions() {
    let generated = SlotClaim::generate("orders");
    assert_eq!(generated.id().len(), 16);
    assert!(generated
        .id()
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(generated.slot(), format!("orders_{}", generated.id()));
    assert_ne!(
        generated,
        SlotClaim::generate("orders"),
        "claims are never reused"
    );
    assert_eq!(generated.slot().len(), "orders".len() + 17);
    assert!(SlotClaim::generate(&"p".repeat(46)).slot().len() <= 63);

    assert_eq!(claim().slot(), "orders_0123456789abcdef");
    assert_eq!(
        claim().application_name(INCARNATION),
        "laminar:0123456789abcdef:89abcdef"
    );
    let incarnation = incarnation();
    assert_eq!(incarnation.len(), 8);
    assert!(incarnation.bytes().all(|byte| byte.is_ascii_hexdigit()));
}

#[test]
fn claim_ids_are_sixteen_lower_case_hex_digits() {
    assert!(SlotClaim::parse("orders", ID).is_some());
    for invalid in [
        "0123456789ABCDEF",
        "0123456789abcde",
        "0123456789abcdef0",
        "0123456789abcdeg",
        "",
    ] {
        assert!(SlotClaim::parse("orders", invalid).is_none(), "{invalid}");
    }
}

#[test]
fn claim_slots_are_recognized_exactly_under_their_prefix() {
    assert_eq!(
        SlotClaim::from_slot("orders", "orders_0123456789abcdef"),
        Some(claim())
    );
    for (prefix, name) in [
        ("orders", "orders_eu_0123456789abcdef"),
        ("orders_eu", "orders_0123456789abcdef"),
        ("orders", "ordersx_0123456789abcdef"),
        ("orders", "orders_0123456789abcdef_1"),
        ("orders", "orders"),
        ("orders", "orders_"),
    ] {
        assert_eq!(
            SlotClaim::from_slot(prefix, name),
            None,
            "{prefix} / {name}"
        );
    }
    assert!(SlotClaim::from_slot("orders_eu", "orders_eu_0123456789abcdef").is_some());
}

#[test]
fn orphans_are_other_claims_under_the_prefix_only() {
    let listed = |name: &str, holder: Option<&str>| PrefixSlot {
        name: name.into(),
        holder: holder.map(String::from),
        retained_bytes: Some(1),
        inactive_since: None,
    };
    let slots = [
        listed("orders_0123456789abcdef", None),
        listed("orders_1111111111111111", None),
        listed(
            "orders_2222222222222222",
            Some("laminar:2222222222222222:00000000"),
        ),
        listed("orders_eu_3333333333333333", None),
        listed("orders_legacy", None),
    ];
    let claim = claim();
    let others: Vec<&str> = other_claims(&slots, "orders", &claim)
        .map(|slot| slot.name.as_str())
        .collect();
    assert_eq!(
        others,
        ["orders_1111111111111111", "orders_2222222222222222"]
    );
    assert_eq!(
        slots::drop_statement(&slots[1].name),
        "SELECT pg_drop_replication_slot('orders_1111111111111111')"
    );
}

#[test]
fn only_another_incarnation_of_this_claim_is_a_stale_holder() {
    let claim = claim();
    assert!(claim.is_stale_holder(&holder("laminar:0123456789abcdef:deadbeef"), INCARNATION));
    for other in [
        "laminar:0123456789abcdef:89abcdef",
        "laminar:0123456789abcdef:",
        "laminar:0123456789abcdef",
        "laminar:1111111111111111:deadbeef",
        "laminar:0123456789abcdefx:deadbeef",
        "pgwire-replication",
        "",
    ] {
        assert!(
            !claim.is_stale_holder(&holder(other), INCARNATION),
            "{other}"
        );
    }
}

/// Every phase against every slot state and both output modes.
#[test]
fn restart_matrix() {
    use ResumeAction::{Adopt, Busy, Create, FailClosed, NewClaim, TerminateStale};

    let stale = holder("laminar:0123456789abcdef:deadbeef");
    let foreign = holder("pg_recvlogical");
    let same_incarnation = holder("laminar:0123456789abcdef:89abcdef");
    let healthy = slot(Some(0x500), None, None);
    let unusable = slot(Some(0x500), None, Some("is invalidated (wal_removed)"));
    let positionless = slot(None, None, None);
    let held_stale = slot(Some(0x500), Some(stale.clone()), None);
    let held_foreign = slot(Some(0x500), Some(foreign.clone()), None);
    let held_by_us = slot(Some(0x500), Some(same_incarnation.clone()), None);
    let held_unusable = slot(Some(0x500), Some(stale.clone()), Some("is invalidated"));

    let claimed = CursorPhase::Claimed;
    let streaming = |consistent_point, lsn| CursorPhase::Streaming {
        consistent_point: Lsn::new(consistent_point),
        lsn: Lsn::new(lsn),
    };
    let copying = |consistent_point, lsn| {
        CursorPhase::Snapshot(SnapshotPosition {
            consistent_point: Lsn::new(consistent_point),
            lsn: Lsn::new(lsn),
            bound: Some(vec!["17".into()]),
            chunks: 3,
            pending: vec![901],
            floor: None,
        })
    };

    let action = |phase: &CursorPhase, facts: Option<&SlotFacts>, output| {
        resume_action(phase, facts, output, &claim(), INCARNATION)
    };
    for output in [OutputMode::Upsert, OutputMode::Changelog] {
        assert_eq!(action(&claimed, None, output), Create);
        // Nothing was emitted under a claim, so a healthy slot is adopted from its consistent
        // point; a copy takes its floor from a probe slot.
        assert_eq!(
            action(&claimed, Some(&healthy), output),
            Adopt(Lsn::new(0x500))
        );
        assert!(matches!(
            action(&claimed, Some(&positionless), output),
            NewClaim(_)
        ));
        assert!(
            matches!(action(&claimed, Some(&unusable), output), NewClaim(reason) if reason.contains("invalidated"))
        );
        assert_eq!(
            action(&claimed, Some(&held_stale), output),
            TerminateStale(stale.clone())
        );
        assert_eq!(
            action(&claimed, Some(&held_unusable), output),
            TerminateStale(stale.clone())
        );
        assert_eq!(
            action(&claimed, Some(&held_foreign), output),
            Busy(foreign.clone())
        );
        assert_eq!(
            action(&claimed, Some(&held_by_us), output),
            Busy(same_incarnation.clone())
        );

        for position in [streaming(0x400, 0x600), copying(0x400, 0x600)] {
            if output == OutputMode::Changelog && matches!(position, CursorPhase::Snapshot(_)) {
                continue;
            }
            assert!(
                matches!(action(&position, None, output), FailClosed(reason) if reason.contains("missing"))
            );
            assert_eq!(
                action(&position, Some(&healthy), output),
                Adopt(Lsn::new(0x600))
            );
            assert!(
                matches!(action(&position, Some(&unusable), output), FailClosed(reason) if reason.contains("invalidated"))
            );
            assert!(matches!(
                action(&position, Some(&positionless), output),
                FailClosed(_)
            ));
            assert_eq!(
                action(&position, Some(&held_stale), output),
                TerminateStale(stale.clone())
            );
            assert_eq!(
                action(&position, Some(&held_foreign), output),
                Busy(foreign.clone())
            );
            assert_eq!(
                action(&position, Some(&held_by_us), output),
                Busy(same_incarnation.clone())
            );
        }
        // A slot behind the cursor is safe (slots persist at server checkpoints); one ahead lost
        // WAL; one before the consistent point is not the slot the cursor was taken from.
        for phase in [streaming as fn(u64, u64) -> CursorPhase, copying] {
            if output == OutputMode::Changelog && matches!(phase(0, 0), CursorPhase::Snapshot(_)) {
                continue;
            }
            assert_eq!(
                action(&phase(0x400, 0x900), Some(&healthy), output),
                Adopt(Lsn::new(0x900))
            );
            assert_eq!(
                action(&phase(0x500, 0x500), Some(&healthy), output),
                Adopt(Lsn::new(0x500))
            );
            assert!(
                matches!(action(&phase(0x400, 0x4ff), Some(&healthy), output), FailClosed(reason) if reason.contains("advanced"))
            );
            assert!(
                matches!(action(&phase(0x501, 0x900), Some(&healthy), output), FailClosed(reason) if reason.contains("consistent point"))
            );
        }
    }
    // An upsert copy resumes inside the snapshot; a changelog copy cannot, whatever the slot.
    for facts in [
        None,
        Some(&healthy),
        Some(&unusable),
        Some(&positionless),
        Some(&held_stale),
        Some(&held_foreign),
    ] {
        assert!(
            matches!(action(&copying(0x400, 0x600), facts, OutputMode::Changelog), FailClosed(reason) if reason.contains("initial snapshot") && reason.contains("pg_drop_replication_slot('orders_0123456789abcdef')")),
            "{facts:?}"
        );
    }
}

#[test]
fn busy_slots_name_the_holder_and_are_retryable() {
    let error = busy("orders_0123456789abcdef", &holder("pg_recvlogical"));
    let text = error.to_string();
    assert!(error.is_transient(), "{error:?}");
    for needle in [
        "orders_0123456789abcdef",
        "pid 42",
        "pg_recvlogical",
        "10.0.0.7",
    ] {
        assert!(text.contains(needle), "{needle}: {text}");
    }
}

#[test]
fn slot_creation_failures_are_classified_by_sqlstate() {
    let server = |message: &str| PgWireError::Server(message.into());
    assert_eq!(
        create_failure(&server(
            "replication slot \"orders_0123456789abcdef\" already exists (SQLSTATE 42710)"
        )),
        CreateFailure::Exists
    );
    assert!(matches!(
        create_failure(&server("all replication slots are in use (SQLSTATE 53400)")),
        CreateFailure::SlotsFull(_)
    ));
    for transient in [
        server("terminating connection due to administrator command (SQLSTATE 57P01)"),
        server("sorry, too many clients already (SQLSTATE 53300)"),
        PgWireError::Io(Arc::new(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        ))),
    ] {
        assert!(
            matches!(create_failure(&transient), CreateFailure::Transient(_)),
            "{transient}"
        );
    }
    for fatal in [
        server("permission denied to create replication slot (SQLSTATE 42501)"),
        PgWireError::Configuration("slot must be at most 63 bytes".into()),
        PgWireError::Auth("password authentication failed".into()),
    ] {
        assert!(
            matches!(create_failure(&fatal), CreateFailure::Fatal(_)),
            "{fatal}"
        );
    }
}
