//! A keyed materialized view over a change feed with `id` and `amount` columns, and its oracle:
//! once the writer stops, every node's committed read of the view must equal the same aggregate
//! over the input table.

use std::time::{Duration, Instant};

use super::kill_rejoin::SoakSettings;
use super::view_reads::{connect, ReadFailure};
use super::{wait_for_converged_durable_checkpoint, wait_until, DurableCheckpointStatus, Node};

const VIEW: &str = "change_buckets";
pub(super) const BUCKETS: i64 = 16;
const READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Each key of a bucket: `(bucket, n, total, peak)`.
pub(super) type Buckets = Vec<[i64; 4]>;

pub(super) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("soak runtime")
}

/// The aggregate the view, the output stream and the oracle compute over the table.
pub(super) fn bucket_sql(table: &str) -> String {
    format!(
        "SELECT id % {BUCKETS} AS bucket, COUNT(*) AS n, SUM(amount) AS total, MAX(amount) AS peak \
         FROM {table} GROUP BY id % {BUCKETS}"
    )
}

/// The `sql` block that creates the view over `source`.
pub(super) fn view_sql(source: &str) -> String {
    format!(
        r#"sql = """
CREATE MATERIALIZED VIEW {VIEW} AS {};
"""
"#,
        bucket_sql(source)
    )
}

/// Wait until the first node's committed read of the view equals `expected`, then require every
/// node to read the same rows at that epoch or later. Returns the epoch.
pub(super) fn converged_reads(
    nodes: &mut [Node],
    expected: &Buckets,
    settings: &SoakSettings,
    latest: DurableCheckpointStatus,
) -> u64 {
    wait_for_converged_durable_checkpoint(
        nodes,
        Instant::now() + settings.recovery_ceiling,
        "durable progress after the writer stopped",
        Some(latest),
    );
    let runtime = runtime();
    let mut last = None;
    let converged = wait_until(settings.recovery_ceiling + Duration::from_secs(60), || {
        match runtime.block_on(committed_read(nodes[0].id, 0)) {
            Ok(read) => last = Some(read),
            Err(ReadFailure::Retryable(_)) => {}
            Err(ReadFailure::Fatal(reason)) => panic!("node0 committed view read: {reason}"),
        }
        last.as_ref().is_some_and(|(_, rows)| rows == expected)
    });
    let epoch = match (converged, last) {
        (true, Some((epoch, _))) => epoch,
        (_, last) => panic!(
            "no committed view read equals the final snapshot {expected:?}; last read {last:?}"
        ),
    };
    for node in nodes.iter() {
        match runtime.block_on(committed_read(node.id, epoch)) {
            Ok((read_epoch, rows)) => {
                assert!(
                    read_epoch >= epoch,
                    "node{} read epoch {read_epoch}",
                    node.id
                );
                assert_eq!(&rows, expected, "node{} view at {read_epoch}", node.id);
            }
            Err(ReadFailure::Retryable(reason) | ReadFailure::Fatal(reason)) => {
                panic!("node{} committed view read failed: {reason}", node.id)
            }
        }
    }
    epoch
}

/// One committed read of the view on node `node` at epoch `floor` or later.
async fn committed_read(node: usize, floor: u64) -> Result<(u64, Buckets), ReadFailure> {
    let read = async {
        let client = connect(node, "committed").await?;
        client
            .batch_execute(&format!("SET laminar.min_epoch = {floor}"))
            .await?;
        let rows = client
            .simple_query(&format!(
                "SELECT bucket, n, total, peak FROM {VIEW} ORDER BY bucket"
            ))
            .await?;
        let epoch = client.simple_query("SHOW laminar.read_epoch").await?;
        Ok::<_, ReadFailure>((rows, epoch))
    };
    let (rows, epoch) = tokio::time::timeout(READ_TIMEOUT, read)
        .await
        .map_err(|_| ReadFailure::Retryable("read timeout".to_owned()))??;
    let fatal = |reason: &str| ReadFailure::Fatal(reason.to_owned());
    let epoch = rows_of(&epoch)
        .next()
        .and_then(|row| row.get(0)?.parse::<u64>().ok())
        .ok_or_else(|| fatal("SHOW laminar.read_epoch returned no epoch"))?;
    let buckets = rows_of(&rows)
        .map(|row| {
            let mut values = [0; 4];
            for (index, value) in values.iter_mut().enumerate() {
                *value = row
                    .get(index)
                    .and_then(|text| text.parse().ok())
                    .ok_or_else(|| fatal("a view column is not a BIGINT"))?;
            }
            Ok(values)
        })
        .collect::<Result<_, ReadFailure>>()?;
    Ok((epoch, buckets))
}

fn rows_of(
    messages: &[tokio_postgres::SimpleQueryMessage],
) -> impl Iterator<Item = &tokio_postgres::SimpleQueryRow> {
    messages.iter().filter_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    })
}
