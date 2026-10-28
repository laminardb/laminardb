//! A draining database hands its vnodes over live: the other database takes the keyed and
//! row-placed views' slices and adopts the append view's log, and reads every view at its next
//! checkpoint. The drained database takes part in no later checkpoint and reads them too.
//! Subscribers on both databases, attached before the drain, fold to the same rows: the moved
//! slices and the adopted log are not changes.

use super::scenario::{
    checkpoint, expected_views, leader, read_views, start_pair, wait_totals, Process, VIEWS,
};
use super::*;

pub(super) async fn qualify(processes: &mut [Process; 2]) -> Result<()> {
    start_pair(processes).await?;
    let keys = connectors::keys()?;
    for process in &mut *processes {
        process.done(&Command::Prefix(2)).await?;
    }
    wait_totals(processes, &BTreeMap::from([(keys[0], 32), (keys[1], 30)])).await?;
    let first = leader(processes).await?;
    let epoch = checkpoint(&mut processes[first], &[7, 8]).await?;
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 2)
        );
        subscribe(process).await?;
    }
    for process in &mut *processes {
        process.done(&Command::Drain(8)).await?;
    }
    wait_owned_by(processes, 7).await?;
    let fence = processes[1].observe().await?.assignment;
    assert_eq!(
        fence.map(|fence| fence.participant_ids()),
        Some(vec![7]),
        "node 8 is no longer a checkpoint participant"
    );
    // Before any later input, node 7 holds node 8's rows only through the moved slices and log.
    let after = leader(processes).await?;
    let epoch = checkpoint(&mut processes[after], &[7]).await?;
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 2)
        );
    }
    for process in &mut *processes {
        process.done(&Command::Prefix(3)).await?;
    }
    wait_totals(
        &mut processes[..1],
        &BTreeMap::from([(keys[0], 63), (keys[1], 60)]),
    )
    .await?;
    let after = leader(processes).await?;
    let epoch = checkpoint(&mut processes[after], &[7]).await?;
    // Node 8 learns of node 7's checkpoints from the authority's committed head.
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 3)
        );
    }
    assert_eq!(
        read_views(&mut processes[1], &VIEWS, None).await?,
        expected_views(keys, 3)
    );
    for process in &mut *processes {
        assert_eq!(
            read_subscriptions(process, epoch).await?,
            expected_views(keys, 3)
        );
    }
    Ok(())
}

/// The columns of each view that [`VIEWS`] reads.
const SUBSCRIBED: [(&str, &[&str]); 3] = [
    ("recovery_totals", &["account", "total", "n"]),
    ("recovery_amounts", &["total"]),
    ("recovery_rows", &["account", "amount"]),
];

async fn subscribe(process: &mut Process) -> Result<()> {
    for (view, columns) in SUBSCRIBED {
        let columns = columns.iter().map(|column| (*column).to_owned()).collect();
        process
            .done(&Command::Subscribe(view.to_owned(), columns))
            .await?;
    }
    Ok(())
}

async fn read_subscriptions(process: &mut Process, through: u64) -> Result<Vec<Vec<Vec<i64>>>> {
    let mut read = Vec::new();
    for (view, _) in SUBSCRIBED {
        match process
            .request(&Command::ReadSubscription(view.to_owned(), through))
            .await?
        {
            Response::Rows(rows) => read.push(rows),
            response => return Err(anyhow!("expected subscription rows: {response:?}")),
        }
    }
    Ok(read)
}

/// Wait until both databases run an assignment in which `owner` owns every vnode.
async fn wait_owned_by(processes: &mut [Process; 2], owner: u64) -> Result<()> {
    let mut last = Vec::new();
    tokio::time::timeout(DEADLINE, async {
        loop {
            let mut observed = Vec::new();
            for process in processes.iter_mut() {
                let observation = process.observe().await?;
                if observation.fault.is_some() {
                    return Err(anyhow!("fault during the drain: {:?}", observation.fault));
                }
                observed.push(observation.assignment);
            }
            last = observed;
            if last.iter().all(|assignment| {
                assignment
                    .as_ref()
                    .is_some_and(|fence| fence.matches_owner_map(&[owner, owner]))
            }) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| format!("the drain did not move every vnode to {owner}; last: {last:?}"))?
}
