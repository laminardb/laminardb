use super::super::scenario::{held_start, start_pair, wait_open, Process};
use super::*;

async fn observed(processes: &mut [Process]) -> Result<Transcript> {
    let mut callbacks = Vec::new();
    let mut output = Vec::new();
    for process in processes {
        let observation = process.observe().await?;
        if let Some(fault) = observation.fault {
            return Err(anyhow!("process fixture fault: {fault}"));
        }
        callbacks.extend(observation.callbacks);
        output.extend(observation.activity);
    }
    callbacks.sort_by_key(|callback| callback.id);
    output.sort_by(|left, right| left.0.cmp(&right.0));
    Ok((callbacks, output))
}

async fn rows(processes: &mut [Process], expected: usize) -> Result<Transcript> {
    let mut last = None;
    tokio::time::timeout(DEADLINE, async {
        loop {
            let transcript = observed(processes).await?;
            if transcript.1.len() > expected {
                return Err(anyhow!("duplicate process output: {transcript:?}"));
            }
            if transcript.1.len() == expected {
                return Ok(transcript);
            }
            last = Some(transcript);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("process output did not reach {expected}; last: {last:?}"))?
}

async fn prefix(processes: &mut [Process], count: u64) -> Result<()> {
    for process in processes {
        process.done(&Command::Prefix(count)).await?;
    }
    Ok(())
}

async fn source_cut(
    process: &mut Process,
    cursor: &str,
    watermark: i64,
) -> Result<CommittedCheckpointRef> {
    let Response::Committed {
        reference,
        offsets,
        channels,
        watermark: actual,
        ..
    } = process.request(&Command::Checkpoint).await?
    else {
        return Err(anyhow!("expected committed process cut"));
    };
    assert_eq!(offsets, BTreeMap::from([("cursor".into(), cursor.into())]));
    assert_eq!(channels, [CHANNEL.to_vec()]);
    assert_eq!(actual, Some(watermark));
    println!("committed process cut: {reference:?}; cursor={cursor}, watermark={watermark}");
    Ok(reference)
}

fn expected() -> Vec<ActivityRow> {
    let mut output = vec![
        (source::key(0, "a"), "running".into(), 110, true, 108_000),
        (source::key(0, "a"), "inactive".into(), 110, false, 118_000),
        (source::key(1, "b"), "running".into(), 20, false, 107_000),
        (source::key(1, "b"), "inactive".into(), 20, false, 117_000),
        (source::key(0, "c"), "running".into(), 1, false, 125_000),
        (source::key(0, "c"), "inactive".into(), 1, false, 135_000),
        (source::key(1, "d"), "running".into(), 2, false, 145_000),
        (source::key(1, "d"), "inactive".into(), 2, false, 155_000),
        (source::key(0, "e"), "running".into(), 0, false, 165_000),
    ];
    output.sort_by(|left, right| left.0.cmp(&right.0));
    output
}

async fn finish(processes: &mut [Process]) -> Result<Transcript> {
    prefix(processes, 7).await?;
    rows(processes, 5).await?;
    source_cut(&mut processes[0], "7", 164).await?;
    let transcript = rows(processes, 9).await?;
    assert_eq!(transcript.1, expected());
    assert_eq!(transcript.0.len(), 9);
    assert!(transcript.0.windows(2).all(|pair| pair[0].id < pair[1].id));
    assert_eq!(
        transcript
            .0
            .iter()
            .filter(|callback| callback.timer)
            .count(),
        4
    );
    assert!(!transcript
        .0
        .iter()
        .any(|callback| callback.timer && callback.timestamp == 110_000));
    Ok(transcript)
}

async fn restore(
    survivor: &mut Process,
    failed: u64,
    reference: &CommittedCheckpointRef,
    first: &RecoveryAnnouncement,
) -> Result<()> {
    survivor.done(&Command::RemoveFailedPeer(failed)).await?;
    let held = held_start(survivor, reference, first.round.id.generation).await?;
    assert!(held.fenced);
    let start = held.intent.as_ref().unwrap();
    let owner = if failed == 7 { 8 } else { 7 };
    assert_eq!(start.round.assignment_fence.assignment_version, 2);
    assert_eq!(start.round.assignment_fence.participant_ids(), [owner]);
    assert!(start.round.assignment_fence.matches_owner_map(&[owner; 2]));
    assert_eq!(held.handoff.as_ref(), Some(reference));
    let resume = held.starts.last().unwrap();
    assert_eq!(resume.assignment, 2);
    assert_eq!(
        resume.offsets,
        BTreeMap::from([("cursor".into(), "2".into())])
    );
    assert_eq!(resume.channels, [CHANNEL.to_vec()]);
    assert_eq!(held.release.as_ref().unwrap().round, first.round);
    survivor.done(&Command::ClearProcessObservation).await?;
    survivor.done(&Command::Prefix(7)).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let still = survivor.observe().await?;
    assert!(still.fenced);
    assert_eq!(still.polls, held.polls);
    assert!(still.callbacks.is_empty() && still.activity.is_empty());
    assert_eq!(still.release.as_ref().unwrap().round, first.round);
    survivor.done(&Command::Release).await?;
    let release = wait_open(survivor, reference.epoch).await?;
    assert_eq!(release.round, start.round);
    assert_eq!(
        survivor.observe().await?.assignment.as_ref(),
        Some(&release.round.assignment_fence)
    );
    println!(
        "restored process owner {owner}: cursor 2, physical channel intact, Release {release:?}"
    );
    Ok(())
}

pub(in super::super) async fn qualify(
    processes: &mut [Process; 2],
    failed: u64,
) -> Result<Transcript> {
    let first = start_pair(processes).await?;
    prefix(processes, 2).await?;
    rows(processes, 2).await?;
    let reference = source_cut(&mut processes[0], "2", 104).await?;
    for process in &mut *processes {
        process.done(&Command::ClearProcessObservation).await?;
    }
    if failed == 0 {
        return finish(processes).await;
    }
    prefix(processes, 4).await?;
    let partial = rows(processes, 2).await?;
    assert_eq!(
        partial.1.iter().map(|row| row.2).collect::<Vec<_>>(),
        [110, 20]
    );
    let failed_index = usize::from(failed == 8);
    let survivor_index = 1 - failed_index;
    processes[survivor_index]
        .done(&Command::HoldRecovery)
        .await?;
    processes[failed_index].kill().await?;
    restore(&mut processes[survivor_index], failed, &reference, &first).await?;
    finish(std::slice::from_mut(&mut processes[survivor_index])).await
}
