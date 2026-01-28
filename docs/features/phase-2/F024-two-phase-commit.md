# F024: Two-Phase Commit

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F024 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F023 |
| **Owner** | TBD |

## Summary

Implement distributed two-phase commit for exactly-once across multiple sinks. Coordinates atomic commits when writing to multiple destinations.

## Goals

- Coordinator/participant protocol
- Timeout and recovery handling
- Presumed abort semantics
- Logging for crash recovery

## Implementation

Location: `crates/laminar-core/src/sink/two_phase.rs`

### Key Types

- **`TwoPhaseCoordinator`**: Main coordinator managing 2PC transactions
- **`TransactionLog`**: Durable log for coordinator decisions (crash recovery)
- **`TransactionRecord`**: Active transaction state with participant tracking
- **`ParticipantState`**: Per-participant vote and acknowledgment state
- **`CoordinatorDecision`**: Preparing/Committed/Aborted decision enum
- **`ParticipantVote`**: Yes/No/Timeout vote enum
- **`TwoPhaseConfig`**: Configurable timeouts and retry settings

### Protocol Flow

```text
Coordinator                         Participants
    │                                   │
    │──────── PREPARE ─────────────────▶│
    │                                   │
    │◀─────── VOTE (Yes/No) ───────────│
    │                                   │
    │ (Log decision)                    │
    │                                   │
    │──────── COMMIT/ABORT ────────────▶│
    │                                   │
    │◀─────── ACK ─────────────────────│
    │                                   │
```

### Presumed Abort

If the coordinator crashes before logging a COMMIT decision, all participants
abort the transaction on recovery. The transaction log enables this:

1. On prepare: Log `Preparing` state
2. On commit decision: Log `Committed` (CRITICAL - must be durable before sending commit)
3. On recovery: `Preparing` → abort, `Committed` → re-send commit

### API Usage

```rust
use laminar_core::sink::{TwoPhaseCoordinator, TwoPhaseConfig, ParticipantVote};

// Create coordinator
let mut coord = TwoPhaseCoordinator::new(TwoPhaseConfig::default());

// Register participants
coord.register_participant("kafka-sink");
coord.register_participant("delta-sink");

// Begin transaction
let tx_id = coord.begin_transaction()?;

// Phase 1: Prepare
for target in coord.get_prepare_targets(&tx_id) {
    coord.mark_prepare_sent(&tx_id, &target);
    // Send prepare to participant...
    // On response:
    coord.record_vote(&tx_id, &target, ParticipantVote::Yes)?;
}

// Check timeouts
coord.check_timeouts(&tx_id);

// Make decision
if coord.can_decide(&tx_id) {
    let decision = coord.decide(&tx_id)?;

    // Phase 2: Commit/Abort
    for target in coord.get_commit_targets(&tx_id) {
        // Send commit/abort to participant...
        coord.mark_acknowledged(&tx_id, &target);
    }

    if coord.is_complete(&tx_id) {
        coord.complete(&tx_id);
    }
}
```

### Recovery

```rust
// On restart, recover from persisted log
let log_bytes = load_from_storage();
let (committed, to_abort) = coord.recover(&log_bytes);

// Re-send commit for committed transactions
for entry in committed {
    // Re-send commit to all participants
}

// Abort transactions that were still preparing (presumed abort)
for entry in to_abort {
    // Send abort to all participants
}
```

## Tests

20 unit tests covering:
- Coordinator decision serialization
- Participant state management
- Transaction record voting
- Transaction log persistence
- Full commit flow
- Abort on no vote
- Abort on timeout
- Force abort
- Recovery (presumed abort semantics)
- Prepare and commit target tracking

## Completion Checklist

- [x] 2PC protocol implemented
- [x] Crash recovery working (presumed abort)
- [x] Timeout handling tested
- [x] Multiple participants coordinated
- [x] Transaction log serialization
- [x] 20 unit tests
