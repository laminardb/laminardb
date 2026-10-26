//! A draining database hands its vnodes over live: the other database takes the keyed and
//! row-placed views' slices and adopts the append view's log, and reads every view at its next
//! checkpoint.

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
    }
    for process in &mut *processes {
        process.done(&Command::Drain(8)).await?;
    }
    wait_owned_by(processes, 7).await?;
    // Before any later input, node 7 holds node 8's rows only through the moved slices and log.
    let after = leader(processes).await?;
    let epoch = checkpoint(&mut processes[after], &[7]).await?;
    assert_eq!(
        read_views(&mut processes[0], &VIEWS, Some(epoch)).await?,
        expected_views(keys, 2)
    );
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
    // Node 8 owns no vnodes now and takes part in no later checkpoint, so it never learns of
    // this one; only node 7 can read it.
    assert_eq!(
        read_views(&mut processes[0], &VIEWS, Some(epoch)).await?,
        expected_views(keys, 3)
    );
    Ok(())
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
