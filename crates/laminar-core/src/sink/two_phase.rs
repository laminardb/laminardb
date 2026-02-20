//! Two-Phase Commit (2PC) Protocol Implementation
//!
//! Provides distributed transaction coordination across multiple sinks with:
//! - Coordinator/participant protocol
//! - Transaction log for crash recovery
//! - Presumed abort semantics
//! - Timeout handling
//!
//! # Protocol Overview
//!
//! ```text
//! Coordinator                         Participants
//!     │                                   │
//!     │──────── PREPARE ─────────────────▶│
//!     │                                   │
//!     │◀─────── VOTE (Yes/No) ───────────│
//!     │                                   │
//!     │ (Log decision)                    │
//!     │                                   │
//!     │──────── COMMIT/ABORT ────────────▶│
//!     │                                   │
//!     │◀─────── ACK ─────────────────────│
//!     │                                   │
//! ```
//!
//! # Presumed Abort
//!
//! If the coordinator crashes before logging a COMMIT decision, all participants
//! will abort the transaction on recovery. This is the "presumed abort" protocol.
//!
//! # Recovery
//!
//! On coordinator restart:
//! 1. Read transaction log
//! 2. For COMMITTED transactions: re-send commit to participants
//! 3. For PREPARING transactions: abort (presumed abort)
//! 4. Clean up completed transactions

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::error::SinkError;
use super::traits::TransactionId;

/// Decision made by the coordinator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorDecision {
    /// Preparing - sent prepare to all participants
    Preparing,
    /// Committed - all participants voted yes, commit logged
    Committed,
    /// Aborted - at least one participant voted no, or timeout
    Aborted,
}

impl CoordinatorDecision {
    /// Serialize to byte
    #[must_use]
    pub fn to_byte(&self) -> u8 {
        match self {
            Self::Preparing => 0,
            Self::Committed => 1,
            Self::Aborted => 2,
        }
    }

    /// Deserialize from byte
    #[must_use]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Preparing),
            1 => Some(Self::Committed),
            2 => Some(Self::Aborted),
            _ => None,
        }
    }
}

/// Vote from a participant
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantVote {
    /// Ready to commit
    Yes,
    /// Cannot commit
    No,
    /// Timed out waiting for vote
    Timeout,
}

/// State of a participant in a transaction
#[derive(Debug, Clone)]
pub struct ParticipantState {
    /// Participant identifier
    pub id: String,
    /// Current vote (if received)
    pub vote: Option<ParticipantVote>,
    /// Whether prepare was sent
    pub prepare_sent: bool,
    /// Whether commit/abort was acknowledged
    pub acknowledged: bool,
    /// Last activity timestamp
    pub last_activity: Instant,
}

impl ParticipantState {
    /// Create a new participant state
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            vote: None,
            prepare_sent: false,
            acknowledged: false,
            last_activity: Instant::now(),
        }
    }

    /// Record that prepare was sent
    pub fn mark_prepare_sent(&mut self) {
        self.prepare_sent = true;
        self.last_activity = Instant::now();
    }

    /// Record participant's vote
    pub fn record_vote(&mut self, vote: ParticipantVote) {
        self.vote = Some(vote);
        self.last_activity = Instant::now();
    }

    /// Record acknowledgment
    pub fn mark_acknowledged(&mut self) {
        self.acknowledged = true;
        self.last_activity = Instant::now();
    }

    /// Check if participant has timed out
    #[must_use]
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }

    /// Check if participant voted yes
    #[must_use]
    pub fn voted_yes(&self) -> bool {
        matches!(self.vote, Some(ParticipantVote::Yes))
    }

    /// Check if participant voted no or timed out
    #[must_use]
    pub fn voted_no_or_timeout(&self) -> bool {
        matches!(
            self.vote,
            Some(ParticipantVote::No | ParticipantVote::Timeout)
        )
    }
}

/// Transaction state in the coordinator
#[derive(Debug, Clone)]
pub struct TransactionRecord {
    /// Transaction ID
    pub tx_id: TransactionId,
    /// Current decision
    pub decision: CoordinatorDecision,
    /// Participant states
    pub participants: HashMap<String, ParticipantState>,
    /// Transaction start time
    pub started_at: Instant,
    /// When decision was made
    pub decision_at: Option<Instant>,
}

impl TransactionRecord {
    /// Create a new transaction record
    #[must_use]
    pub fn new(tx_id: TransactionId, participant_ids: &[String]) -> Self {
        let participants = participant_ids
            .iter()
            .map(|id| (id.clone(), ParticipantState::new(id)))
            .collect();

        Self {
            tx_id,
            decision: CoordinatorDecision::Preparing,
            participants,
            started_at: Instant::now(),
            decision_at: None,
        }
    }

    /// Check if all participants have voted yes
    #[must_use]
    pub fn all_voted_yes(&self) -> bool {
        self.participants.values().all(ParticipantState::voted_yes)
    }

    /// Check if any participant voted no or timed out
    #[must_use]
    pub fn any_voted_no_or_timeout(&self) -> bool {
        self.participants
            .values()
            .any(ParticipantState::voted_no_or_timeout)
    }

    /// Check if all participants have acknowledged
    #[must_use]
    pub fn all_acknowledged(&self) -> bool {
        self.participants.values().all(|p| p.acknowledged)
    }

    /// Get participants that haven't voted yet
    #[must_use]
    pub fn pending_voters(&self) -> Vec<&str> {
        self.participants
            .iter()
            .filter(|(_, p)| p.vote.is_none())
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get participants that haven't acknowledged yet
    #[must_use]
    pub fn pending_acknowledgments(&self) -> Vec<&str> {
        self.participants
            .iter()
            .filter(|(_, p)| !p.acknowledged)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Mark decision and timestamp
    pub fn make_decision(&mut self, decision: CoordinatorDecision) {
        self.decision = decision;
        self.decision_at = Some(Instant::now());
    }
}

/// Entry in the transaction log
#[derive(Debug, Clone)]
pub struct TransactionLogEntry {
    /// Transaction ID
    pub tx_id: TransactionId,
    /// Decision
    pub decision: CoordinatorDecision,
    /// Participant IDs
    pub participants: Vec<String>,
    /// Timestamp (epoch millis)
    pub timestamp: u64,
}

impl TransactionLogEntry {
    /// Serialize to bytes
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Wire format uses u32 for string/collection lengths
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Version
        bytes.push(1u8);

        // Transaction ID
        let tx_bytes = self.tx_id.to_bytes();
        bytes.extend_from_slice(&(tx_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&tx_bytes);

        // Decision
        bytes.push(self.decision.to_byte());

        // Timestamp
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());

        // Participants
        bytes.extend_from_slice(&(self.participants.len() as u32).to_le_bytes());
        for participant in &self.participants {
            bytes.extend_from_slice(&(participant.len() as u32).to_le_bytes());
            bytes.extend_from_slice(participant.as_bytes());
        }

        bytes
    }

    /// Deserialize from bytes
    ///
    /// # Panics
    ///
    /// Will not panic — all slice indexing is bounds-checked via `Option` returns.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        let mut pos = 0;

        // Version
        let version = bytes[pos];
        pos += 1;
        if version != 1 {
            return None;
        }

        // Transaction ID
        if pos + 4 > bytes.len() {
            return None;
        }
        let tx_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + tx_len > bytes.len() {
            return None;
        }
        let tx_id = TransactionId::from_bytes(&bytes[pos..pos + tx_len])?;
        pos += tx_len;

        // Decision
        if pos >= bytes.len() {
            return None;
        }
        let decision = CoordinatorDecision::from_byte(bytes[pos])?;
        pos += 1;

        // Timestamp
        if pos + 8 > bytes.len() {
            return None;
        }
        let timestamp = u64::from_le_bytes(bytes[pos..pos + 8].try_into().ok()?);
        pos += 8;

        // Participants
        if pos + 4 > bytes.len() {
            return None;
        }
        let num_participants = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        let mut participants = Vec::with_capacity(num_participants);
        for _ in 0..num_participants {
            if pos + 4 > bytes.len() {
                return None;
            }
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + len > bytes.len() {
                return None;
            }
            let participant = String::from_utf8_lossy(&bytes[pos..pos + len]).to_string();
            pos += len;
            participants.push(participant);
        }

        Some(Self {
            tx_id,
            decision,
            participants,
            timestamp,
        })
    }
}

/// Durable transaction log for the coordinator
///
/// Stores coordinator decisions to enable crash recovery with presumed abort semantics.
#[derive(Debug)]
pub struct TransactionLog {
    /// Log entries (in-memory, should be persisted to storage)
    entries: Vec<TransactionLogEntry>,
    /// Index by transaction ID
    index: HashMap<u64, usize>,
}

impl TransactionLog {
    /// Create a new transaction log
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            index: HashMap::new(),
        }
    }

    /// Log a coordinator decision
    #[allow(clippy::cast_possible_truncation)] // Timestamp ms fits i64 for ~292 years from epoch
    pub fn log_decision(
        &mut self,
        tx_id: &TransactionId,
        decision: CoordinatorDecision,
        participants: &[String],
    ) {
        let entry = TransactionLogEntry {
            tx_id: tx_id.clone(),
            decision,
            participants: participants.to_vec(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        let idx = self.entries.len();
        self.entries.push(entry);
        self.index.insert(tx_id.id(), idx);
    }

    /// Update an existing decision
    pub fn update_decision(&mut self, tx_id: &TransactionId, decision: CoordinatorDecision) {
        if let Some(&idx) = self.index.get(&tx_id.id()) {
            if let Some(entry) = self.entries.get_mut(idx) {
                entry.decision = decision;
            }
        }
    }

    /// Get the decision for a transaction
    #[must_use]
    pub fn get_decision(&self, tx_id: &TransactionId) -> Option<CoordinatorDecision> {
        self.index
            .get(&tx_id.id())
            .and_then(|&idx| self.entries.get(idx))
            .map(|e| e.decision)
    }

    /// Get an entry by transaction ID
    #[must_use]
    pub fn get_entry(&self, tx_id: &TransactionId) -> Option<&TransactionLogEntry> {
        self.index
            .get(&tx_id.id())
            .and_then(|&idx| self.entries.get(idx))
    }

    /// Get all entries
    #[must_use]
    pub fn entries(&self) -> &[TransactionLogEntry] {
        &self.entries
    }

    /// Get transactions that need recovery action.
    ///
    /// Returns a tuple of committed and to-abort entries:
    /// - Committed: need to re-send commit
    /// - To abort: were in PREPARING state (presumed abort)
    #[must_use]
    pub fn get_pending_for_recovery(
        &self,
    ) -> (Vec<&TransactionLogEntry>, Vec<&TransactionLogEntry>) {
        let mut committed = Vec::new();
        let mut to_abort = Vec::new();

        for entry in &self.entries {
            match entry.decision {
                CoordinatorDecision::Committed => committed.push(entry),
                CoordinatorDecision::Preparing => to_abort.push(entry),
                CoordinatorDecision::Aborted => {
                    // Already aborted, no action needed
                }
            }
        }

        (committed, to_abort)
    }

    /// Remove completed transactions from the log
    pub fn prune_completed(&mut self, tx_ids: &[TransactionId]) {
        let ids_to_remove: std::collections::HashSet<u64> =
            tx_ids.iter().map(TransactionId::id).collect();

        self.entries
            .retain(|e| !ids_to_remove.contains(&e.tx_id.id()));

        // Rebuild index
        self.index.clear();
        for (idx, entry) in self.entries.iter().enumerate() {
            self.index.insert(entry.tx_id.id(), idx);
        }
    }

    /// Serialize the entire log to bytes
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Wire format uses u32 for entry/string lengths
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Version
        bytes.push(1u8);

        // Number of entries
        bytes.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        // Each entry
        for entry in &self.entries {
            let entry_bytes = entry.to_bytes();
            bytes.extend_from_slice(&(entry_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&entry_bytes);
        }

        bytes
    }

    /// Deserialize from bytes
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are malformed.
    #[allow(clippy::missing_panics_doc)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SinkError> {
        if bytes.is_empty() {
            return Err(SinkError::CheckpointError(
                "Empty transaction log".to_string(),
            ));
        }

        let mut pos = 0;

        // Version
        let version = bytes[pos];
        pos += 1;
        if version != 1 {
            return Err(SinkError::CheckpointError(format!(
                "Unsupported log version: {version}"
            )));
        }

        // Number of entries
        if pos + 4 > bytes.len() {
            return Err(SinkError::CheckpointError(
                "Unexpected end of log".to_string(),
            ));
        }
        // SAFETY: Bounds check above guarantees slice is exactly 4 bytes
        let num_entries = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut entries = Vec::with_capacity(num_entries);
        let mut index = HashMap::new();

        for i in 0..num_entries {
            if pos + 4 > bytes.len() {
                return Err(SinkError::CheckpointError(
                    "Unexpected end of log".to_string(),
                ));
            }
            // SAFETY: Bounds check above guarantees slice is exactly 4 bytes
            let entry_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + entry_len > bytes.len() {
                return Err(SinkError::CheckpointError(
                    "Invalid entry length".to_string(),
                ));
            }
            let entry = TransactionLogEntry::from_bytes(&bytes[pos..pos + entry_len])
                .ok_or_else(|| SinkError::CheckpointError("Invalid log entry".to_string()))?;
            pos += entry_len;

            index.insert(entry.tx_id.id(), i);
            entries.push(entry);
        }

        Ok(Self { entries, index })
    }
}

impl Default for TransactionLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for the 2PC coordinator
#[derive(Debug, Clone)]
pub struct TwoPhaseConfig {
    /// Timeout for participant prepare response
    pub prepare_timeout: Duration,
    /// Timeout for participant commit/abort acknowledgment
    pub commit_timeout: Duration,
    /// Maximum number of retries for commit/abort
    pub max_retries: u32,
    /// Delay between retries
    pub retry_delay: Duration,
}

impl Default for TwoPhaseConfig {
    fn default() -> Self {
        Self {
            prepare_timeout: Duration::from_secs(30),
            commit_timeout: Duration::from_secs(60),
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

/// Result of a 2PC transaction
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TwoPhaseResult {
    /// Transaction committed successfully
    Committed,
    /// Transaction aborted (with reason)
    Aborted(String),
    /// Transaction timed out
    Timeout,
}

/// Two-Phase Commit Coordinator
///
/// Coordinates distributed transactions across multiple sinks using the
/// two-phase commit protocol with presumed abort semantics.
pub struct TwoPhaseCoordinator {
    /// Configuration
    config: TwoPhaseConfig,
    /// Transaction log for durability
    log: TransactionLog,
    /// Active transactions
    active: HashMap<u64, TransactionRecord>,
    /// Registered participant IDs
    participants: Vec<String>,
    /// Transaction ID counter
    next_tx_id: AtomicU64,
}

impl TwoPhaseCoordinator {
    /// Create a new 2PC coordinator
    #[must_use]
    pub fn new(config: TwoPhaseConfig) -> Self {
        Self {
            config,
            log: TransactionLog::new(),
            active: HashMap::new(),
            participants: Vec::new(),
            next_tx_id: AtomicU64::new(1),
        }
    }

    /// Create with default configuration
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(TwoPhaseConfig::default())
    }

    /// Register a participant sink
    pub fn register_participant(&mut self, sink_id: impl Into<String>) {
        let id = sink_id.into();
        if !self.participants.contains(&id) {
            self.participants.push(id);
        }
    }

    /// Unregister a participant
    pub fn unregister_participant(&mut self, sink_id: &str) {
        self.participants.retain(|id| id != sink_id);
    }

    /// Get registered participants
    #[must_use]
    pub fn participants(&self) -> &[String] {
        &self.participants
    }

    /// Begin a new 2PC transaction
    ///
    /// # Errors
    ///
    /// Returns an error if no participants are registered.
    pub fn begin_transaction(&mut self) -> Result<TransactionId, SinkError> {
        if self.participants.is_empty() {
            return Err(SinkError::ConfigurationError(
                "No participants registered".to_string(),
            ));
        }

        let tx_id = TransactionId::new(self.next_tx_id.fetch_add(1, Ordering::SeqCst));
        let record = TransactionRecord::new(tx_id.clone(), &self.participants);

        // Log the preparing state
        self.log
            .log_decision(&tx_id, CoordinatorDecision::Preparing, &self.participants);

        self.active.insert(tx_id.id(), record);

        Ok(tx_id)
    }

    /// Get participants that need prepare sent
    #[must_use]
    pub fn get_prepare_targets(&self, tx_id: &TransactionId) -> Vec<String> {
        self.active
            .get(&tx_id.id())
            .map(|record| {
                record
                    .participants
                    .iter()
                    .filter(|(_, p)| !p.prepare_sent)
                    .map(|(id, _)| id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Mark that prepare was sent to a participant
    pub fn mark_prepare_sent(&mut self, tx_id: &TransactionId, participant_id: &str) {
        if let Some(record) = self.active.get_mut(&tx_id.id()) {
            if let Some(participant) = record.participants.get_mut(participant_id) {
                participant.mark_prepare_sent();
            }
        }
    }

    /// Record a participant's vote
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not found.
    pub fn record_vote(
        &mut self,
        tx_id: &TransactionId,
        participant_id: &str,
        vote: ParticipantVote,
    ) -> Result<(), SinkError> {
        let record = self
            .active
            .get_mut(&tx_id.id())
            .ok_or_else(|| SinkError::Internal(format!("Transaction not found: {}", tx_id.id())))?;

        if let Some(participant) = record.participants.get_mut(participant_id) {
            participant.record_vote(vote);
        }

        Ok(())
    }

    /// Check if we can make a decision (all votes received or any no/timeout)
    #[must_use]
    pub fn can_decide(&self, tx_id: &TransactionId) -> bool {
        self.active
            .get(&tx_id.id())
            .is_some_and(|record| record.all_voted_yes() || record.any_voted_no_or_timeout())
    }

    /// Make the commit/abort decision
    ///
    /// Returns the decision and logs it durably.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not found.
    pub fn decide(&mut self, tx_id: &TransactionId) -> Result<CoordinatorDecision, SinkError> {
        let record = self
            .active
            .get_mut(&tx_id.id())
            .ok_or_else(|| SinkError::Internal(format!("Transaction not found: {}", tx_id.id())))?;

        let decision = if record.all_voted_yes() {
            CoordinatorDecision::Committed
        } else {
            CoordinatorDecision::Aborted
        };

        // Update in-memory state
        record.make_decision(decision);

        // Log the decision durably (CRITICAL for recovery)
        self.log.update_decision(tx_id, decision);

        Ok(decision)
    }

    /// Mark that a participant acknowledged the commit/abort
    pub fn mark_acknowledged(&mut self, tx_id: &TransactionId, participant_id: &str) {
        if let Some(record) = self.active.get_mut(&tx_id.id()) {
            if let Some(participant) = record.participants.get_mut(participant_id) {
                participant.mark_acknowledged();
            }
        }
    }

    /// Check if transaction is complete (all participants acknowledged)
    #[must_use]
    pub fn is_complete(&self, tx_id: &TransactionId) -> bool {
        self.active
            .get(&tx_id.id())
            .is_some_and(TransactionRecord::all_acknowledged)
    }

    /// Complete a transaction and clean up
    pub fn complete(&mut self, tx_id: &TransactionId) {
        self.active.remove(&tx_id.id());
        self.log.prune_completed(std::slice::from_ref(tx_id));
    }

    /// Get the current decision for a transaction
    #[must_use]
    pub fn get_decision(&self, tx_id: &TransactionId) -> Option<CoordinatorDecision> {
        self.active
            .get(&tx_id.id())
            .map(|r| r.decision)
            .or_else(|| self.log.get_decision(tx_id))
    }

    /// Check and mark timed out participants
    pub fn check_timeouts(&mut self, tx_id: &TransactionId) {
        if let Some(record) = self.active.get_mut(&tx_id.id()) {
            for participant in record.participants.values_mut() {
                if participant.vote.is_none()
                    && participant.is_timed_out(self.config.prepare_timeout)
                {
                    participant.record_vote(ParticipantVote::Timeout);
                }
            }
        }
    }

    /// Get participants that need commit sent
    #[must_use]
    pub fn get_commit_targets(&self, tx_id: &TransactionId) -> Vec<String> {
        self.active
            .get(&tx_id.id())
            .map(|record| {
                record
                    .participants
                    .iter()
                    .filter(|(_, p)| !p.acknowledged)
                    .map(|(id, _)| id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the transaction log for persistence
    #[must_use]
    pub fn transaction_log(&self) -> &TransactionLog {
        &self.log
    }

    /// Get mutable reference to transaction log
    pub fn transaction_log_mut(&mut self) -> &mut TransactionLog {
        &mut self.log
    }

    /// Get active transaction record
    #[must_use]
    pub fn get_transaction(&self, tx_id: &TransactionId) -> Option<&TransactionRecord> {
        self.active.get(&tx_id.id())
    }

    /// Recover from a transaction log after restart.
    ///
    /// Returns a tuple of committed and to-abort entries that need action.
    #[must_use]
    pub fn recover(
        &mut self,
        log_bytes: &[u8],
    ) -> (Vec<TransactionLogEntry>, Vec<TransactionLogEntry>) {
        match TransactionLog::from_bytes(log_bytes) {
            Ok(log) => {
                let (committed, to_abort) = log.get_pending_for_recovery();
                let committed_owned: Vec<_> = committed.iter().map(|e| (*e).clone()).collect();
                let abort_owned: Vec<_> = to_abort.iter().map(|e| (*e).clone()).collect();
                self.log = log;
                (committed_owned, abort_owned)
            }
            Err(_) => (Vec::new(), Vec::new()),
        }
    }

    /// Force abort a transaction (for timeout or error)
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not found.
    pub fn force_abort(&mut self, tx_id: &TransactionId) -> Result<(), SinkError> {
        if let Some(record) = self.active.get_mut(&tx_id.id()) {
            record.make_decision(CoordinatorDecision::Aborted);
            self.log
                .update_decision(tx_id, CoordinatorDecision::Aborted);
            Ok(())
        } else {
            Err(SinkError::Internal(format!(
                "Transaction not found: {}",
                tx_id.id()
            )))
        }
    }

    /// Get configuration
    #[must_use]
    pub fn config(&self) -> &TwoPhaseConfig {
        &self.config
    }
}

impl Default for TwoPhaseCoordinator {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_decision_serialization() {
        assert_eq!(
            CoordinatorDecision::from_byte(CoordinatorDecision::Preparing.to_byte()),
            Some(CoordinatorDecision::Preparing)
        );
        assert_eq!(
            CoordinatorDecision::from_byte(CoordinatorDecision::Committed.to_byte()),
            Some(CoordinatorDecision::Committed)
        );
        assert_eq!(
            CoordinatorDecision::from_byte(CoordinatorDecision::Aborted.to_byte()),
            Some(CoordinatorDecision::Aborted)
        );
        assert_eq!(CoordinatorDecision::from_byte(99), None);
    }

    #[test]
    fn test_participant_state() {
        let mut state = ParticipantState::new("sink-1");
        assert!(state.vote.is_none());
        assert!(!state.prepare_sent);
        assert!(!state.acknowledged);

        state.mark_prepare_sent();
        assert!(state.prepare_sent);

        state.record_vote(ParticipantVote::Yes);
        assert!(state.voted_yes());
        assert!(!state.voted_no_or_timeout());

        state.mark_acknowledged();
        assert!(state.acknowledged);
    }

    #[test]
    fn test_participant_timeout_vote() {
        let mut state = ParticipantState::new("sink-1");
        state.record_vote(ParticipantVote::Timeout);
        assert!(state.voted_no_or_timeout());
        assert!(!state.voted_yes());
    }

    #[test]
    fn test_transaction_record() {
        let tx_id = TransactionId::new(1);
        let participants = vec!["sink-1".to_string(), "sink-2".to_string()];
        let mut record = TransactionRecord::new(tx_id, &participants);

        assert!(!record.all_voted_yes());
        assert!(!record.any_voted_no_or_timeout());
        assert_eq!(record.pending_voters().len(), 2);

        // Vote yes from sink-1
        record
            .participants
            .get_mut("sink-1")
            .unwrap()
            .record_vote(ParticipantVote::Yes);
        assert!(!record.all_voted_yes());
        assert_eq!(record.pending_voters().len(), 1);

        // Vote yes from sink-2
        record
            .participants
            .get_mut("sink-2")
            .unwrap()
            .record_vote(ParticipantVote::Yes);
        assert!(record.all_voted_yes());
        assert!(record.pending_voters().is_empty());
    }

    #[test]
    fn test_transaction_record_with_no_vote() {
        let tx_id = TransactionId::new(1);
        let participants = vec!["sink-1".to_string(), "sink-2".to_string()];
        let mut record = TransactionRecord::new(tx_id, &participants);

        record
            .participants
            .get_mut("sink-1")
            .unwrap()
            .record_vote(ParticipantVote::Yes);
        record
            .participants
            .get_mut("sink-2")
            .unwrap()
            .record_vote(ParticipantVote::No);

        assert!(!record.all_voted_yes());
        assert!(record.any_voted_no_or_timeout());
    }

    #[test]
    fn test_transaction_log_entry_serialization() {
        let entry = TransactionLogEntry {
            tx_id: TransactionId::new(123),
            decision: CoordinatorDecision::Committed,
            participants: vec!["sink-1".to_string(), "sink-2".to_string()],
            timestamp: 1_706_000_000_000,
        };

        let bytes = entry.to_bytes();
        let restored = TransactionLogEntry::from_bytes(&bytes).unwrap();

        assert_eq!(restored.tx_id.id(), 123);
        assert_eq!(restored.decision, CoordinatorDecision::Committed);
        assert_eq!(restored.participants, vec!["sink-1", "sink-2"]);
        assert_eq!(restored.timestamp, 1_706_000_000_000);
    }

    #[test]
    fn test_transaction_log() {
        let mut log = TransactionLog::new();

        let tx1 = TransactionId::new(1);
        let tx2 = TransactionId::new(2);
        let participants = vec!["sink-1".to_string()];

        log.log_decision(&tx1, CoordinatorDecision::Preparing, &participants);
        log.log_decision(&tx2, CoordinatorDecision::Committed, &participants);

        assert_eq!(log.get_decision(&tx1), Some(CoordinatorDecision::Preparing));
        assert_eq!(log.get_decision(&tx2), Some(CoordinatorDecision::Committed));

        // Update decision
        log.update_decision(&tx1, CoordinatorDecision::Committed);
        assert_eq!(log.get_decision(&tx1), Some(CoordinatorDecision::Committed));
    }

    #[test]
    fn test_transaction_log_recovery() {
        let mut log = TransactionLog::new();

        let tx1 = TransactionId::new(1);
        let tx2 = TransactionId::new(2);
        let tx3 = TransactionId::new(3);
        let participants = vec!["sink-1".to_string()];

        log.log_decision(&tx1, CoordinatorDecision::Preparing, &participants);
        log.log_decision(&tx2, CoordinatorDecision::Committed, &participants);
        log.log_decision(&tx3, CoordinatorDecision::Aborted, &participants);

        let (committed, to_abort) = log.get_pending_for_recovery();

        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].tx_id.id(), 2);

        assert_eq!(to_abort.len(), 1);
        assert_eq!(to_abort[0].tx_id.id(), 1);
    }

    #[test]
    fn test_transaction_log_serialization() {
        let mut log = TransactionLog::new();

        let tx1 = TransactionId::new(1);
        let participants = vec!["sink-1".to_string(), "sink-2".to_string()];

        log.log_decision(&tx1, CoordinatorDecision::Committed, &participants);

        let bytes = log.to_bytes();
        let restored = TransactionLog::from_bytes(&bytes).unwrap();

        assert_eq!(
            restored.get_decision(&tx1),
            Some(CoordinatorDecision::Committed)
        );
        assert_eq!(restored.entries().len(), 1);
    }

    #[test]
    fn test_coordinator_begin_transaction() {
        let mut coord = TwoPhaseCoordinator::with_defaults();
        coord.register_participant("sink-1");
        coord.register_participant("sink-2");

        let tx_id = coord.begin_transaction().unwrap();
        assert!(coord.get_transaction(&tx_id).is_some());
        assert_eq!(
            coord.get_decision(&tx_id),
            Some(CoordinatorDecision::Preparing)
        );
    }

    #[test]
    fn test_coordinator_no_participants() {
        let mut coord = TwoPhaseCoordinator::with_defaults();
        let result = coord.begin_transaction();
        assert!(matches!(result, Err(SinkError::ConfigurationError(_))));
    }

    #[test]
    fn test_coordinator_full_commit_flow() {
        let mut coord = TwoPhaseCoordinator::with_defaults();
        coord.register_participant("sink-1");
        coord.register_participant("sink-2");

        // Begin transaction
        let tx_id = coord.begin_transaction().unwrap();

        // Mark prepare sent
        coord.mark_prepare_sent(&tx_id, "sink-1");
        coord.mark_prepare_sent(&tx_id, "sink-2");

        // Record votes
        coord
            .record_vote(&tx_id, "sink-1", ParticipantVote::Yes)
            .unwrap();
        assert!(!coord.can_decide(&tx_id));

        coord
            .record_vote(&tx_id, "sink-2", ParticipantVote::Yes)
            .unwrap();
        assert!(coord.can_decide(&tx_id));

        // Make decision
        let decision = coord.decide(&tx_id).unwrap();
        assert_eq!(decision, CoordinatorDecision::Committed);

        // Record acknowledgments
        coord.mark_acknowledged(&tx_id, "sink-1");
        assert!(!coord.is_complete(&tx_id));

        coord.mark_acknowledged(&tx_id, "sink-2");
        assert!(coord.is_complete(&tx_id));

        // Complete
        coord.complete(&tx_id);
        assert!(coord.get_transaction(&tx_id).is_none());
    }

    #[test]
    fn test_coordinator_abort_on_no_vote() {
        let mut coord = TwoPhaseCoordinator::with_defaults();
        coord.register_participant("sink-1");
        coord.register_participant("sink-2");

        let tx_id = coord.begin_transaction().unwrap();

        coord
            .record_vote(&tx_id, "sink-1", ParticipantVote::Yes)
            .unwrap();
        coord
            .record_vote(&tx_id, "sink-2", ParticipantVote::No)
            .unwrap();

        assert!(coord.can_decide(&tx_id));

        let decision = coord.decide(&tx_id).unwrap();
        assert_eq!(decision, CoordinatorDecision::Aborted);
    }

    #[test]
    fn test_coordinator_abort_on_timeout() {
        let mut coord = TwoPhaseCoordinator::new(TwoPhaseConfig {
            prepare_timeout: Duration::from_millis(1),
            ..Default::default()
        });
        coord.register_participant("sink-1");

        let tx_id = coord.begin_transaction().unwrap();
        coord.mark_prepare_sent(&tx_id, "sink-1");

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(5));
        coord.check_timeouts(&tx_id);

        assert!(coord.can_decide(&tx_id));

        let decision = coord.decide(&tx_id).unwrap();
        assert_eq!(decision, CoordinatorDecision::Aborted);
    }

    #[test]
    fn test_coordinator_force_abort() {
        let mut coord = TwoPhaseCoordinator::with_defaults();
        coord.register_participant("sink-1");

        let tx_id = coord.begin_transaction().unwrap();
        coord.force_abort(&tx_id).unwrap();

        assert_eq!(
            coord.get_decision(&tx_id),
            Some(CoordinatorDecision::Aborted)
        );
    }

    #[test]
    fn test_coordinator_recovery() {
        // Create coordinator and complete a transaction
        let mut coord1 = TwoPhaseCoordinator::with_defaults();
        coord1.register_participant("sink-1");

        let tx_id = coord1.begin_transaction().unwrap();
        coord1
            .record_vote(&tx_id, "sink-1", ParticipantVote::Yes)
            .unwrap();
        coord1.decide(&tx_id).unwrap();

        // Serialize the log
        let log_bytes = coord1.transaction_log().to_bytes();

        // Create new coordinator and recover
        let mut coord2 = TwoPhaseCoordinator::with_defaults();
        let (committed, to_abort) = coord2.recover(&log_bytes);

        assert_eq!(committed.len(), 1);
        assert!(to_abort.is_empty());
    }

    #[test]
    fn test_coordinator_recovery_presumed_abort() {
        // Create coordinator with preparing transaction
        let mut coord1 = TwoPhaseCoordinator::with_defaults();
        coord1.register_participant("sink-1");

        let _tx_id = coord1.begin_transaction().unwrap();
        // Don't complete - simulates crash during prepare

        // Serialize the log
        let log_bytes = coord1.transaction_log().to_bytes();

        // Create new coordinator and recover
        let mut coord2 = TwoPhaseCoordinator::with_defaults();
        let (committed, to_abort) = coord2.recover(&log_bytes);

        // Presumed abort: preparing transactions should be aborted
        assert!(committed.is_empty());
        assert_eq!(to_abort.len(), 1);
    }

    #[test]
    fn test_get_prepare_and_commit_targets() {
        let mut coord = TwoPhaseCoordinator::with_defaults();
        coord.register_participant("sink-1");
        coord.register_participant("sink-2");

        let tx_id = coord.begin_transaction().unwrap();

        // Initially all need prepare
        let targets = coord.get_prepare_targets(&tx_id);
        assert_eq!(targets.len(), 2);

        // Mark one as sent
        coord.mark_prepare_sent(&tx_id, "sink-1");
        let targets = coord.get_prepare_targets(&tx_id);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0], "sink-2");

        // After voting and decision, get commit targets
        coord.mark_prepare_sent(&tx_id, "sink-2");
        coord
            .record_vote(&tx_id, "sink-1", ParticipantVote::Yes)
            .unwrap();
        coord
            .record_vote(&tx_id, "sink-2", ParticipantVote::Yes)
            .unwrap();
        coord.decide(&tx_id).unwrap();

        let commit_targets = coord.get_commit_targets(&tx_id);
        assert_eq!(commit_targets.len(), 2);

        coord.mark_acknowledged(&tx_id, "sink-1");
        let commit_targets = coord.get_commit_targets(&tx_id);
        assert_eq!(commit_targets.len(), 1);
        assert_eq!(commit_targets[0], "sink-2");
    }

    #[test]
    fn test_config_values() {
        let config = TwoPhaseConfig::default();
        assert_eq!(config.prepare_timeout, Duration::from_secs(30));
        assert_eq!(config.commit_timeout, Duration::from_secs(60));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_participant_registration() {
        let mut coord = TwoPhaseCoordinator::with_defaults();

        coord.register_participant("sink-1");
        coord.register_participant("sink-2");
        coord.register_participant("sink-1"); // Duplicate

        assert_eq!(coord.participants().len(), 2);

        coord.unregister_participant("sink-1");
        assert_eq!(coord.participants().len(), 1);
        assert_eq!(coord.participants()[0], "sink-2");
    }
}
