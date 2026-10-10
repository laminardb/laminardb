//! The source-owned task that creates a committed claim's slot.
//!
//! `CREATE_REPLICATION_SLOT` waits for every running transaction that holds a transaction id, so
//! it runs here rather than under the deadline of a poll or a commit notification.

use std::time::Duration;

use pgwire_replication::{CreatedSlot, PgWireError, SlotSnapshot};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::connector::{ConnectorTaskAdmission, ConnectorTaskGuard, ConnectorTaskOwner};

use super::super::super::config::{PostgresCdcConfig, SnapshotMode};
use super::super::super::postgres_io::slots;
use super::super::super::postgres_io::{self, ControlConnection, PostgresCheckpointBinding};
use super::super::super::typed_rows::RowLayout;
use super::super::checkpoint::CursorPhase;
use super::super::snapshot::SnapshotReader;
use super::super::{Arc, ConnectorError, Lsn, Notify};
use super::{busy, other_claims, settle, warn_orphaning, ResumeAction, SlotClaim};

const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const BLOCKER_REPORT_INTERVAL: Duration = Duration::from_secs(30);

/// What the task leaves for the next poll to install.
pub(super) enum ClaimOutcome {
    /// The slot was created; `snapshot` holds the imported copy in `initial` mode.
    Created {
        consistent_point: Lsn,
        snapshot: Option<Box<SnapshotReader>>,
    },
    /// The claim's existing slot streams from its confirmed position.
    Adopted(Lsn),
    /// The claimed slot cannot be used; the source claims another name.
    NewClaim,
}

/// Everything the creation task owns.
pub(super) struct ClaimJob {
    pub(super) config: PostgresCdcConfig,
    pub(super) claim: SlotClaim,
    pub(super) incarnation: String,
    pub(super) application_name: String,
    pub(super) binding: PostgresCheckpointBinding,
    pub(super) layout: RowLayout,
    pub(super) admission: ConnectorTaskAdmission,
    pub(super) data_ready: Arc<Notify>,
}

/// The running creation task and the signal that its claim committed.
pub(crate) struct ClaimTask {
    committed: watch::Sender<bool>,
    handle: JoinHandle<Result<ClaimOutcome, ConnectorError>>,
}

impl ClaimTask {
    pub(super) fn spawn(
        job: ClaimJob,
        committed: bool,
        owner: &ConnectorTaskOwner,
    ) -> Result<Self, ConnectorError> {
        let guard = owner.track().ok_or_else(|| {
            ConnectorError::Internal(
                "PostgreSQL CDC connector generation is already retired".into(),
            )
        })?;
        let (committed, committed_rx) = watch::channel(committed);
        let handle = tokio::spawn(job.run(committed_rx, guard));
        Ok(Self { committed, handle })
    }

    /// A task that never finishes, observed through [`Self::is_committed`].
    #[cfg(test)]
    pub(crate) fn pending() -> Self {
        Self {
            committed: watch::channel(false).0,
            handle: tokio::spawn(std::future::pending()),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_committed(&self) -> bool {
        *self.committed.borrow()
    }

    /// A checkpoint carrying the claim has committed: the slot may be created.
    pub(crate) fn commit(&self) {
        self.committed.send_replace(true);
    }

    pub(super) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub(super) async fn outcome(mut self) -> Result<ClaimOutcome, ConnectorError> {
        (&mut self.handle).await.map_err(|error| {
            ConnectorError::Internal(format!("PostgreSQL CDC slot creation task failed: {error}"))
        })?
    }

    /// Stop the task and wait for it to end. Its client sessions close, but a `CREATE` blocked on
    /// the server keeps waiting there until the transactions it waits for end or the next start
    /// ends it as a stale holder of this claim.
    pub(crate) async fn cancel(mut self) {
        self.handle.abort();
        let _ = (&mut self.handle).await;
    }
}

impl Drop for ClaimTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// How a failed `CREATE_REPLICATION_SLOT` is handled.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CreateFailure {
    /// The name exists: inspect the slot again and apply the restart matrix.
    Exists,
    /// `max_replication_slots` is exhausted.
    SlotsFull(String),
    /// The session was lost; a slot still being created is dropped with it.
    Transient(String),
    Fatal(String),
}

pub(super) fn create_failure(error: &PgWireError) -> CreateFailure {
    let text = error.to_string();
    let sqlstate = |code: &str| text.contains(&format!("(SQLSTATE {code})"));
    if sqlstate("42710") {
        return CreateFailure::Exists;
    }
    if sqlstate("53400") {
        return CreateFailure::SlotsFull(text);
    }
    if error.is_transient()
        || ["57P01", "57P02", "57P03", "53300"]
            .into_iter()
            .any(sqlstate)
    {
        return CreateFailure::Transient(text);
    }
    CreateFailure::Fatal(text)
}

impl ClaimJob {
    async fn run(
        self,
        mut committed: watch::Receiver<bool>,
        _guard: ConnectorTaskGuard,
    ) -> Result<ClaimOutcome, ConnectorError> {
        if committed.wait_for(|committed| *committed).await.is_err() {
            return Err(ConnectorError::Closed);
        }
        let outcome = self.until_settled().await;
        self.data_ready.notify_one();
        outcome
    }

    /// Retries lost connections with backoff; ends with an outcome, any other error, or the
    /// task's abort by `close`.
    async fn until_settled(&self) -> Result<ClaimOutcome, ConnectorError> {
        let mut delay = INITIAL_RETRY_DELAY;
        loop {
            match self.attempt().await {
                Err(ConnectorError::ConnectionFailed(reason)) => {
                    tracing::warn!(
                        slot = self.claim.slot(),
                        %reason,
                        retry_in = ?delay,
                        "PostgreSQL CDC slot creation will retry"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
                outcome => return outcome,
            }
        }
    }

    async fn connect(&self) -> Result<ControlConnection, ConnectorError> {
        let guard = self.admission.track().ok_or_else(|| {
            ConnectorError::Internal(
                "PostgreSQL CDC connector generation is already retired".into(),
            )
        })?;
        postgres_io::connect(&self.config, &self.application_name, guard).await
    }

    async fn attempt(&self) -> Result<ClaimOutcome, ConnectorError> {
        let control = self.connect().await?;
        let outcome = self.attempt_on(&control).await;
        control.close().await;
        outcome
    }

    async fn attempt_on(
        &self,
        control: &ControlConnection,
    ) -> Result<ClaimOutcome, ConnectorError> {
        let slot = self.claim.slot();
        let action = settle(
            control,
            &self.config,
            &self.claim,
            &self.incarnation,
            CursorPhase::Claimed,
            &self.binding,
        )
        .await?;
        match action {
            ResumeAction::Create => {}
            ResumeAction::Adopt(lsn) => {
                tracing::info!(slot, %lsn, "adopted the claimed PostgreSQL replication slot");
                return Ok(ClaimOutcome::Adopted(lsn));
            }
            ResumeAction::NewClaim(reason) => {
                warn_orphaning(slot, &reason);
                return Ok(ClaimOutcome::NewClaim);
            }
            ResumeAction::Busy(holder) | ResumeAction::TerminateStale(holder) => {
                return Err(busy(slot, &holder));
            }
            ResumeAction::FailClosed(reason) => {
                return Err(ConnectorError::ConfigurationError(reason));
            }
        }
        match self.create().await {
            Ok(created) => self.finish(created).await,
            Err(CreateFailure::Exists) => Err(ConnectorError::ConnectionFailed(format!(
                "PostgreSQL replication slot '{slot}' appeared while it was being created"
            ))),
            Err(CreateFailure::SlotsFull(reason)) => Err(self.slots_full(control, &reason).await),
            Err(CreateFailure::Transient(reason)) => Err(ConnectorError::ConnectionFailed(
                format!("create PostgreSQL replication slot '{slot}': {reason}"),
            )),
            Err(CreateFailure::Fatal(reason)) => Err(ConnectorError::ConfigurationError(format!(
                "create PostgreSQL replication slot '{slot}': {reason}"
            ))),
        }
    }

    /// The `CREATE` wait has no deadline: it ends with the transactions it waits for, or when
    /// `close` aborts the task. Every 30 s it reports what it waits for.
    async fn create(&self) -> Result<CreatedSlot, CreateFailure> {
        let snapshot = match self.config.snapshot_mode {
            SnapshotMode::Initial => SlotSnapshot::Export,
            SnapshotMode::Never => SlotSnapshot::Nothing,
        };
        let replication = postgres_io::build_replication_config(
            &self.config,
            self.claim.slot(),
            &self.application_name,
        );
        let started = tokio::time::Instant::now();
        let create = pgwire_replication::create_logical_slot(&replication, snapshot);
        tokio::pin!(create);
        loop {
            tokio::select! {
                created = &mut create => {
                    return created.map_err(|error| create_failure(&error));
                }
                () = tokio::time::sleep(BLOCKER_REPORT_INTERVAL) => {
                    self.report_blockers(started.elapsed()).await;
                }
            }
        }
    }

    async fn report_blockers(&self, waited: Duration) {
        let listed = async {
            let control = self.connect().await?;
            let listed = slots::blocking_transactions(control.client()).await;
            control.close().await;
            listed
        }
        .await;
        let slot = self.claim.slot();
        let waited_secs = waited.as_secs();
        match listed {
            Ok(blockers) => tracing::warn!(
                slot,
                waited_secs,
                blockers = %blockers.join("; "),
                "PostgreSQL replication slot creation is waiting for these transactions to end"
            ),
            Err(error) => tracing::warn!(
                slot,
                waited_secs,
                %error,
                "PostgreSQL replication slot creation is still waiting; the transactions it \
                 waits for could not be listed"
            ),
        }
    }

    async fn finish(&self, created: CreatedSlot) -> Result<ClaimOutcome, ConnectorError> {
        let slot = self.claim.slot();
        let created_on = (created.system_identifier, created.timeline_id);
        let bound_to = (self.binding.system_identifier, self.binding.timeline_id);
        if created_on != bound_to {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL replication slot '{slot}' was created on system {} timeline {}, but \
                 the source is bound to system {} timeline {}: the replication and control \
                 connections reach different servers; drop the slot there with {}",
                created_on.0,
                created_on.1,
                bound_to.0,
                bound_to.1,
                slots::drop_statement(slot)
            )));
        }
        let consistent_point = Lsn::new(created.consistent_point.as_u64());
        tracing::info!(slot, %consistent_point, "created PostgreSQL replication slot");
        let snapshot = match self.config.snapshot_mode {
            SnapshotMode::Initial => Some(self.import(&created, consistent_point).await?),
            SnapshotMode::Never => None,
        };
        // The importer holds its own copy of the snapshot, so the exporting session can end now.
        if let Err(error) = created.release().await {
            tracing::debug!(%error, "PostgreSQL slot-creating session closed with an error");
        }
        Ok(ClaimOutcome::Created {
            consistent_point,
            snapshot,
        })
    }

    async fn import(
        &self,
        created: &CreatedSlot,
        consistent_point: Lsn,
    ) -> Result<Box<SnapshotReader>, ConnectorError> {
        let name = created
            .snapshot_name
            .as_deref()
            .ok_or_else(|| ConnectorError::ReadError("PostgreSQL exported no snapshot".into()))?;
        let connection = self.connect().await?;
        SnapshotReader::open(
            connection,
            &self.config,
            &self.layout,
            name,
            consistent_point,
        )
        .await
        .map(Box::new)
    }

    async fn slots_full(&self, control: &ControlConnection, reason: &str) -> ConnectorError {
        let prefix = self.config.slot_name.as_str();
        let orphans = match slots::prefix_slots(control.client(), prefix).await {
            Ok(listed) => other_claims(&listed, prefix, &self.claim)
                .filter(|slot| slot.holder.is_none())
                .map(|slot| slots::drop_statement(&slot.name))
                .collect::<Vec<_>>()
                .join("; "),
            Err(error) => format!("they could not be listed: {error}"),
        };
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL has no free replication slot to create '{}' ({reason}); raise \
             max_replication_slots or drop slots no pipeline uses. Orphaned slots under \
             slot.name '{prefix}': [{orphans}]",
            self.claim.slot()
        ))
    }
}
