//! Matched EndTxn ambiguity scenarios and evidence validation.
//!
//! A scenario succeeds only when the proxy transcript proves the configured applied or unapplied
//! outcome and the committed Kafka view agrees with that byte-level evidence.

use super::*;

pub(super) fn run_ambiguity_probe(cli: &AmbiguityCli) -> ProbeResult<()> {
    print_client_version();
    let prepared = PreparedAmbiguity::new(cli)?;

    println!(
        "scenario={}-{} brokers={} proxy_upstream={} topic={} transactional_id={}",
        prepared.kind_label,
        prepared.outcome_label,
        cli.brokers,
        cli.proxy_upstream,
        prepared.topic,
        prepared.transactional_id
    );
    let proxy = EndTxnProxy::start(
        cli.proxy_listen,
        cli.proxy_upstream,
        TargetSpec {
            client_id: prepared.old_client_id.clone(),
            transactional_id: prepared.transactional_id.clone(),
            outcome: prepared.proxy_outcome,
        },
    )?;
    let admin = create_admin(&cli.brokers)?;
    create_topic(&admin, &prepared.topic)?;
    require_topic_inventory(&admin, &prepared.topic)?;
    require_proxy_route(&admin, cli.proxy_listen)?;
    println!(
        "step=proxy-route advertised={} origin={} partitions=0,1,2 result=PASS",
        cli.proxy_listen, cli.proxy_upstream
    );

    let old = create_producer(
        &cli.brokers,
        &prepared.transactional_id,
        &prepared.old_client_id,
    )?;
    old.init_transactions(IO_TIMEOUT)
        .map_err(|error| format!("ambiguity producer A init failed: {error}"))?;
    commit_marker(&old, &prepared.topic, &prepared.first_marker)?;
    stage_and_observe_ambiguity(cli, &prepared, &proxy, old)?;

    let successor = create_producer(
        &cli.brokers,
        &prepared.transactional_id,
        &prepared.successor_client_id,
    )?;
    successor
        .init_transactions(IO_TIMEOUT)
        .map_err(|error| format!("ambiguity producer B init failed: {error}"))?;
    println!("step=same-id-successor-after-a-close result=PASS");

    let intermediate = capture_cut(&admin, cli, &prepared)?;
    verify_intermediate_cut(cli, &prepared, &intermediate)?;
    reconcile(cli, &prepared, &successor, &intermediate.committed)?;

    let final_cut = capture_cut(&admin, cli, &prepared)?;
    verify_final_cut(cli, &prepared, &final_cut)?;
    require_topic_inventory(&admin, &prepared.topic)?;
    println!(
        "step=frozen-cut intermediate={:?} final={:?} ru_counts={:?} rc_counts={:?} result=PASS",
        intermediate.high_watermarks,
        final_cut.high_watermarks,
        final_cut.uncommitted.each_ref().map(Vec::len),
        final_cut.committed.each_ref().map(Vec::len)
    );

    drop(successor);
    drop(admin);
    proxy.shutdown()?;
    Ok(())
}

struct PreparedAmbiguity {
    kind_label: &'static str,
    outcome_label: &'static str,
    proxy_outcome: FaultOutcome,
    first_marker: Vec<u8>,
    successor_marker: Vec<u8>,
    transactional_id: String,
    topic: String,
    old_client_id: String,
    successor_client_id: String,
}

impl PreparedAmbiguity {
    fn new(cli: &AmbiguityCli) -> ProbeResult<Self> {
        let (kind_label, outcome_label, proxy_outcome) = scenario_labels(cli.scenario);
        let shard_id = format!("ambiguity-{}", cli.run_id);
        let transactional_id =
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", &shard_id)?;

        Ok(Self {
            kind_label,
            outcome_label,
            proxy_outcome,
            first_marker: decode_and_validate_marker(FIRST_MARKER_GOLDEN_HEX, false)?,
            successor_marker: decode_and_validate_marker(SUCCESSOR_MARKER_GOLDEN_HEX, true)?,
            transactional_id,
            topic: unique_topic(Some(&cli.run_id))?,
            old_client_id: format!("ldb-kafka-ambiguity-{kind_label}-{outcome_label}-a"),
            successor_client_id: format!("ldb-kafka-ambiguity-{kind_label}-{outcome_label}-b"),
        })
    }
}

fn scenario_labels(scenario: AmbiguityScenario) -> (&'static str, &'static str, FaultOutcome) {
    match scenario {
        AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Applied,
        } => ("marker", "applied", FaultOutcome::AppliedResponseLost),
        AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Unapplied,
        } => ("marker", "unapplied", FaultOutcome::UnappliedRequestHeld),
        AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Applied,
        } => ("data", "applied", FaultOutcome::AppliedResponseLost),
        AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Unapplied,
        } => ("data", "unapplied", FaultOutcome::UnappliedRequestHeld),
    }
}

fn stage_and_observe_ambiguity(
    cli: &AmbiguityCli,
    prepared: &PreparedAmbiguity,
    proxy: &EndTxnProxy,
    old: BaseProducer<ProbeProducerContext>,
) -> ProbeResult<()> {
    match cli.scenario.kind {
        AmbiguityKind::Marker => {
            stage_marker(&old, &prepared.topic, &prepared.successor_marker)?;
        }
        AmbiguityKind::Data => stage_replay_data(&old, &prepared.topic, &FIRST_INTERVAL)?,
    }
    proxy.arm()?;

    let (actuation, commit_join) = thread::scope(|scope| {
        let commit = scope.spawn(|| old.commit_transaction(AMBIGUOUS_COMMIT_TIMEOUT));
        let actuation = proxy.wait_for_actuation(IO_TIMEOUT);
        (actuation, commit.join())
    });
    actuation?;
    let commit_result = commit_join.map_err(|_| "ambiguity commit thread panicked".to_owned())?;
    require_ambiguity_timeout(commit_result)?;
    println!(
        "step=caller-ambiguity code=OperationTimedOut retriable=true fatal=false abortable=false result=PASS"
    );

    drop(old);
    proxy.wait_for_target_connections_closed(IO_TIMEOUT)?;
    let evidence = proxy.finish_target()?;
    require_endtxn_evidence(
        &evidence,
        prepared.proxy_outcome,
        &prepared.old_client_id,
        &prepared.transactional_id,
    )?;
    print_endtxn_evidence(&evidence);
    Ok(())
}

struct CapturedCut {
    high_watermarks: [i64; 3],
    uncommitted: [Vec<Record>; 3],
    committed: [Vec<Record>; 3],
}

fn capture_cut(
    admin: &AdminClient<DefaultClientContext>,
    cli: &AmbiguityCli,
    prepared: &PreparedAmbiguity,
) -> ProbeResult<CapturedCut> {
    let high_watermarks = freeze_high_watermarks(admin, &prepared.topic)?;
    let uncommitted = capture_at_cut(
        &cli.brokers,
        &prepared.topic,
        "read_uncommitted",
        &high_watermarks,
    )?;
    let committed = capture_at_cut(
        &cli.brokers,
        &prepared.topic,
        "read_committed",
        &high_watermarks,
    )?;
    Ok(CapturedCut {
        high_watermarks,
        uncommitted,
        committed,
    })
}

fn verify_intermediate_cut(
    cli: &AmbiguityCli,
    prepared: &PreparedAmbiguity,
    cut: &CapturedCut,
) -> ProbeResult<()> {
    let expected_uncommitted = expected_ambiguity_intermediate(
        cli.scenario.kind,
        cli.scenario.outcome,
        true,
        &prepared.first_marker,
        &prepared.successor_marker,
    )?;
    let expected_committed = expected_ambiguity_intermediate(
        cli.scenario.kind,
        cli.scenario.outcome,
        false,
        &prepared.first_marker,
        &prepared.successor_marker,
    )?;
    require_capture(
        "ambiguity intermediate read_uncommitted",
        &cut.uncommitted,
        &expected_uncommitted,
    )?;
    require_capture(
        "ambiguity intermediate read_committed",
        &cut.committed,
        &expected_committed,
    )
}

fn reconcile(
    cli: &AmbiguityCli,
    prepared: &PreparedAmbiguity,
    successor: &BaseProducer<ProbeProducerContext>,
    committed: &[Vec<Record>; 3],
) -> ProbeResult<()> {
    match cli.scenario.kind {
        AmbiguityKind::Marker => {
            let selected = reconcile_marker_candidate(committed, &prepared.successor_marker)?;
            let expected = if cli.scenario.outcome == AmbiguityOutcome::Applied {
                &SUCCESSOR_INTERVAL
            } else {
                &FIRST_INTERVAL
            };
            if selected != expected {
                return Err(format!(
                    "marker reconciliation selected {selected:02x?}, expected {expected:02x?}"
                ));
            }
            commit_selection_data(successor, &prepared.topic, selected)?;
            println!(
                "step=marker-reconciliation selected_interval={} result=PASS",
                if selected == &SUCCESSOR_INTERVAL {
                    "candidate"
                } else {
                    "last-confirmed"
                }
            );
        }
        AmbiguityKind::Data => {
            let candidate_visible = reconcile_data_candidate(committed)?;
            let expected_visible = cli.scenario.outcome == AmbiguityOutcome::Applied;
            if candidate_visible != expected_visible {
                return Err(format!(
                    "data reconciliation visibility was {candidate_visible}, expected {expected_visible}"
                ));
            }
            commit_marker(successor, &prepared.topic, &prepared.successor_marker)?;
            commit_replay_data(successor, &prepared.topic, &SUCCESSOR_INTERVAL)?;
            println!(
                "step=data-reconciliation predecessor_visible={candidate_visible} successor_replay=true result=PASS"
            );
        }
    }
    Ok(())
}

fn verify_final_cut(
    cli: &AmbiguityCli,
    prepared: &PreparedAmbiguity,
    cut: &CapturedCut,
) -> ProbeResult<()> {
    let expected_uncommitted = expected_ambiguity_final(
        cli.scenario.kind,
        cli.scenario.outcome,
        true,
        &prepared.first_marker,
        &prepared.successor_marker,
    )?;
    let expected_committed = expected_ambiguity_final(
        cli.scenario.kind,
        cli.scenario.outcome,
        false,
        &prepared.first_marker,
        &prepared.successor_marker,
    )?;
    require_capture(
        "ambiguity final read_uncommitted",
        &cut.uncommitted,
        &expected_uncommitted,
    )?;
    require_capture(
        "ambiguity final read_committed",
        &cut.committed,
        &expected_committed,
    )
}

fn require_endtxn_evidence(
    evidence: &EndTxnEvidence,
    expected_outcome: FaultOutcome,
    expected_client_id: &str,
    expected_tx_id: &str,
) -> ProbeResult<()> {
    if evidence.classification != expected_outcome.classification()
        || evidence.api_version != 1
        || evidence.client_id != expected_client_id
        || evidence.transactional_id != expected_tx_id
        || !evidence.committed
        || evidence.request_frame.len() != 27 + expected_client_id.len() + expected_tx_id.len()
        || evidence.response_downstream_bytes != 0
    {
        return Err(format!(
            "matched EndTxn evidence identity drift: {evidence:?}"
        ));
    }
    match expected_outcome {
        FaultOutcome::AppliedResponseLost
            if evidence.request_upstream_bytes == evidence.request_frame.len()
                && evidence
                    .response_frame
                    .as_ref()
                    .is_some_and(|frame| frame.len() == 14)
                && evidence.response_throttle_ms.is_some()
                && evidence.response_error_code == Some(0)
                && evidence.response_sha256.is_some() =>
        {
            Ok(())
        }
        FaultOutcome::UnappliedRequestHeld
            if evidence.request_upstream_bytes == 0
                && evidence.response_frame.is_none()
                && evidence.response_throttle_ms.is_none()
                && evidence.response_error_code.is_none()
                && evidence.response_sha256.is_none() =>
        {
            Ok(())
        }
        _ => Err(format!(
            "matched EndTxn byte disposition did not prove {}: {evidence:?}",
            expected_outcome.classification()
        )),
    }
}

fn print_endtxn_evidence(evidence: &EndTxnEvidence) {
    println!(
        "step=endtxn-actuation classification={} connection={} api_key=26 api_version={} correlation={} client_id={} transactional_id={} producer_id={} producer_epoch={} committed={} request_bytes={} request_upstream_bytes={} request_sha256={} response_bytes={} response_downstream_bytes={} response_throttle_ms={} response_error={} response_sha256={} result=PASS",
        evidence.classification,
        evidence.connection_id,
        evidence.api_version,
        evidence.correlation_id,
        evidence.client_id,
        evidence.transactional_id,
        evidence.producer_id,
        evidence.producer_epoch,
        evidence.committed,
        evidence.request_frame.len(),
        evidence.request_upstream_bytes,
        evidence.request_sha256,
        evidence.response_frame.as_ref().map_or(0, Vec::len),
        evidence.response_downstream_bytes,
        evidence
            .response_throttle_ms
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        evidence
            .response_error_code
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        evidence.response_sha256.as_deref().unwrap_or("none"),
    );
    println!("endtxn_request_hex={}", evidence.request_hex());
    println!(
        "endtxn_response_hex={}",
        evidence.response_hex().as_deref().unwrap_or("none")
    );
    println!("endtxn_events={}", evidence.events.join("|"));
}
