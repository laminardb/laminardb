//! Source, sink-shard, and ledger topology validation for oracle fixtures.

use super::*;

pub(super) fn validate_source_topology(fixture: &Fixture) -> Result<BTreeSet<CutKey>, CheckErrors> {
    if fixture.source_topology.partitions.is_empty() {
        return Err(CheckErrors::one(
            "source topology must contain at least one partition",
        ));
    }
    let mut partitions = BTreeSet::new();
    for partition in &fixture.source_topology.partitions {
        if partition.topic.is_empty()
            || partition.partition < 0
            || !partitions.insert((partition.topic.clone(), partition.partition))
        {
            return Err(CheckErrors::one(
                "source topology contains an empty, negative, or duplicate partition",
            ));
        }
    }
    Ok(partitions)
}

pub(super) fn validate_topology(fixture: &Fixture) -> Result<Topology, CheckErrors> {
    if fixture.vnode_count == 0 {
        return Err(CheckErrors::one("fixture.vnode_count must be positive"));
    }
    if fixture.sink_topology.topic.is_empty()
        || fixture.sink_topology.baseline.is_empty()
        || fixture.sink_topology.shards.is_empty()
    {
        return Err(CheckErrors::one(
            "sink topology must contain a topic, baseline, and shards",
        ));
    }
    let mut baseline = BTreeMap::new();
    for cut in &fixture.sink_topology.baseline {
        if cut.topic != fixture.sink_topology.topic
            || cut.partition < 0
            || cut.exclusive_end < 0
            || baseline
                .insert((cut.topic.clone(), cut.partition), cut.exclusive_end)
                .is_some()
        {
            return Err(CheckErrors::one(
                "sink topology contains an invalid or duplicate baseline",
            ));
        }
    }
    let mut partition_shard = BTreeMap::new();
    let mut shard_vnodes = BTreeMap::new();
    let mut vnode_shard = BTreeMap::new();
    for shard in &fixture.sink_topology.shards {
        if shard.shard_id.is_empty()
            || shard.partitions.is_empty()
            || shard.vnodes.is_empty()
            || shard_vnodes.contains_key(&shard.shard_id)
        {
            return Err(CheckErrors::one(
                "sink topology contains an empty or duplicate shard",
            ));
        }
        let mut vnodes = BTreeSet::new();
        for vnode in &shard.vnodes {
            if *vnode >= fixture.vnode_count
                || !vnodes.insert(*vnode)
                || vnode_shard.insert(*vnode, shard.shard_id.clone()).is_some()
            {
                return Err(CheckErrors::one(
                    "sink topology contains an invalid or duplicate vnode",
                ));
            }
        }
        for partition in &shard.partitions {
            if *partition < 0
                || partition_shard
                    .insert(*partition, shard.shard_id.clone())
                    .is_some()
            {
                return Err(CheckErrors::one(
                    "sink topology contains an invalid or duplicate partition",
                ));
            }
        }
        shard_vnodes.insert(shard.shard_id.clone(), vnodes);
    }
    if vnode_shard.len() != usize::from(fixture.vnode_count)
        || (0..fixture.vnode_count).any(|vnode| !vnode_shard.contains_key(&vnode))
    {
        return Err(CheckErrors::one(
            "sink topology must assign every fixture vnode exactly once",
        ));
    }
    let baseline_partitions = baseline
        .keys()
        .map(|(_, partition)| *partition)
        .collect::<BTreeSet<_>>();
    if baseline_partitions != partition_shard.keys().copied().collect() {
        return Err(CheckErrors::one(
            "sink topology baseline must cover every sink partition exactly once",
        ));
    }
    Ok(Topology {
        topic: fixture.sink_topology.topic.clone(),
        baseline,
        partition_shard,
        shard_vnodes,
        vnode_shard,
    })
}

pub(super) fn validate_ledger(
    fixture: &Fixture,
    source_partitions: &BTreeSet<CutKey>,
    errors: &mut Vec<String>,
) {
    let mut event_ids = BTreeSet::new();
    let mut positions = BTreeSet::new();
    let mut key_partitions = BTreeMap::<&str, (&str, i32)>::new();
    for record in &fixture.ledger {
        if record.event_id.is_empty() || record.topic.is_empty() {
            errors.push("ledger contains an empty event_id or topic".to_owned());
        }
        if record.partition < 0 || record.offset < 0 || record.offset == i64::MAX {
            errors.push("ledger contains an invalid partition or offset".to_owned());
        }
        if !source_partitions.contains(&(record.topic.clone(), record.partition)) {
            errors.push("ledger contains a partition outside source topology".to_owned());
        }
        if !event_ids.insert(record.event_id.as_str()) {
            errors.push("ledger contains a duplicate event_id".to_owned());
        }
        if !positions.insert((record.topic.as_str(), record.partition, record.offset)) {
            errors.push("ledger contains a duplicate topic/partition/offset".to_owned());
        }
        let partition = (record.topic.as_str(), record.partition);
        if key_partitions
            .insert(record.logical_key.as_str(), partition)
            .is_some_and(|previous| previous != partition)
        {
            errors.push(
                "fixture routes one logical key through multiple source partitions".to_owned(),
            );
        }
    }
}
