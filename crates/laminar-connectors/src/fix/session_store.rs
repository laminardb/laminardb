//! Durable engine session state: store creation time and both sequence numbers of the session.
//!
//! Two files under the journal directory hold two checksummed slots each, written alternately
//! so that a torn write leaves the previous generation intact:
//!
//! - `session.identity` holds the creation time and the next outbound sequence number. Under
//!   `SyncPolicy::Fsync` every change is synced before the engine sends with the number, so a
//!   restarted session never reuses an outbound sequence number.
//! - `session.inbound` holds the inbound sequence number to resume from. It never passes the
//!   journal's durability floor, so every value ever written is safe to resume from, and it is
//!   written without a sync.
//!
//! A reset first makes every appended journal record durable: the counterparty resets too and
//! never retransmits the previous series.
//!
//! Lock order: the engine calls in under its session-state lock and the journal calls
//! `DurabilityFloor::set` under its state lock. The store never calls the journal while holding
//! its own lock.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use laminar_core::durable_fs::{durable_rename, DurableRenameMode};
use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

use super::engine::{StoreBackend, StoreValues};
use super::journal::{DurabilityFloor, SyncPolicy};
use crate::error::ConnectorError;

const IDENTITY_FILE: &str = "session.identity";
const INBOUND_FILE: &str = "session.inbound";
const IDENTITY_MAGIC: [u8; 8] = *b"LDBFIXI1";
const INBOUND_MAGIC: [u8; 8] = *b"LDBFIXT1";
/// Magic, generation, creation time, sequence number, checksum.
const SLOT_LEN: usize = 40;
const BODY_LEN: usize = 32;

/// Makes every appended journal record durable. Runs before a reset is persisted.
pub(crate) type ResetBarrier = Box<dyn Fn() -> Result<(), ConnectorError> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slot {
    generation: u64,
    creation_unix_micros: i64,
    seq: u64,
}

impl Slot {
    fn encode(self, magic: [u8; 8]) -> [u8; SLOT_LEN] {
        let mut bytes = [0_u8; SLOT_LEN];
        bytes[..8].copy_from_slice(&magic);
        bytes[8..16].copy_from_slice(&self.generation.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.creation_unix_micros.to_le_bytes());
        bytes[24..BODY_LEN].copy_from_slice(&self.seq.to_le_bytes());
        let checksum = xxh3_64(&bytes[..BODY_LEN]);
        bytes[BODY_LEN..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    fn decode(magic: [u8; 8], bytes: &[u8]) -> Option<Self> {
        let word = |at: usize| -> Option<[u8; 8]> { bytes.get(at..at + 8)?.try_into().ok() };
        if word(0)? != magic || u64::from_le_bytes(word(BODY_LEN)?) != xxh3_64(&bytes[..BODY_LEN]) {
            return None;
        }
        Some(Self {
            generation: u64::from_le_bytes(word(8)?),
            creation_unix_micros: i64::from_le_bytes(word(16)?),
            seq: u64::from_le_bytes(word(24)?),
        })
    }

    /// Generation `g` lives in slot `g % 2`, so a write never overwrites the newest generation.
    fn offset(generation: u64) -> u64 {
        (generation % 2) * SLOT_LEN as u64
    }
}

struct SlotFile {
    file: File,
    magic: [u8; 8],
    generation: u64,
}

impl SlotFile {
    /// Open `name` under `dir`, creating it durably with `initial` when absent. Returns the
    /// newest valid slot.
    fn open(
        dir: &Path,
        name: &str,
        magic: [u8; 8],
        initial: (i64, u64),
    ) -> Result<(Self, Option<Slot>), ConnectorError> {
        let path = dir.join(name);
        if !path.exists() {
            let temp = dir.join(format!("{name}.tmp"));
            let first = Slot {
                generation: 1,
                creation_unix_micros: initial.0,
                seq: initial.1,
            };
            // Generation 1 lives in the second slot.
            let mut bytes = [0_u8; 2 * SLOT_LEN];
            bytes[SLOT_LEN..].copy_from_slice(&first.encode(magic));
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            durable_rename(&temp, &path, DurableRenameMode::NoReplace)?;
        }
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut bytes = Vec::with_capacity(2 * SLOT_LEN);
        Read::by_ref(&mut file)
            .take(2 * SLOT_LEN as u64)
            .read_to_end(&mut bytes)?;
        let newest = bytes
            .chunks(SLOT_LEN)
            .filter_map(|slot| Slot::decode(magic, slot))
            .max_by_key(|slot| slot.generation);
        let generation = newest.map_or(0, |slot| slot.generation);
        Ok((
            Self {
                file,
                magic,
                generation,
            },
            newest,
        ))
    }

    fn write(&mut self, creation_unix_micros: i64, seq: u64, sync: bool) -> std::io::Result<()> {
        let slot = Slot {
            generation: self.generation + 1,
            creation_unix_micros,
            seq,
        };
        self.file
            .seek(SeekFrom::Start(Slot::offset(slot.generation)))?;
        self.file.write_all(&slot.encode(self.magic))?;
        if sync {
            self.file.sync_data()?;
        }
        self.generation = slot.generation;
        Ok(())
    }
}

struct StoreState {
    identity: SlotFile,
    inbound: SlotFile,
    values: StoreValues,
    floor: Option<u64>,
    persisted_target: u64,
    poisoned: Option<String>,
}

impl StoreState {
    fn check(&self) -> Result<(), ConnectorError> {
        match &self.poisoned {
            Some(cause) => Err(ConnectorError::WriteError(format!(
                "FIX session store stopped after an earlier failure: {cause}"
            ))),
            None => Ok(()),
        }
    }

    fn poison(&mut self, operation: &str, cause: &dyn std::fmt::Display) -> ConnectorError {
        let cause = format!("{operation}: {cause}");
        tracing::error!(cause = %cause, "FIX session store failed; the session stops");
        self.poisoned.get_or_insert(cause.clone());
        ConnectorError::outcome_unknown(format!("FIX session store failed: {cause}"), false)
    }

    /// Persist the inbound sequence number to resume from when it changed.
    fn persist_target(&mut self) -> Result<(), ConnectorError> {
        let target = self.floor.map_or(self.values.next_target, |floor| {
            floor.min(self.values.next_target)
        });
        if target == self.persisted_target {
            return Ok(());
        }
        if let Err(error) = self
            .inbound
            .write(self.values.creation_unix_micros, target, false)
        {
            return Err(self.poison("inbound sequence update", &error));
        }
        self.persisted_target = target;
        Ok(())
    }
}

/// Durable state of one session's engine store.
pub(crate) struct SessionStore {
    sync: bool,
    barrier: ResetBarrier,
    state: Mutex<StoreState>,
}

impl SessionStore {
    /// Open the session state under `dir`, creating a new store when there is none.
    ///
    /// # Errors
    /// Fails on I/O errors and when the identity file has no valid slot.
    pub(crate) fn open(
        dir: &Path,
        sync: SyncPolicy,
        barrier: ResetBarrier,
    ) -> Result<Self, ConnectorError> {
        let (identity, slot) =
            SlotFile::open(dir, IDENTITY_FILE, IDENTITY_MAGIC, (now_unix_micros(), 1))?;
        let identity_slot = slot.ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "FIX session state '{}' is damaged; the outbound sequence number is unknown. \
                 Agree the sequence numbers with the counterparty and start a new journal path",
                dir.join(IDENTITY_FILE).display()
            ))
        })?;
        let creation_unix_micros = identity_slot.creation_unix_micros;
        let (inbound, slot) =
            SlotFile::open(dir, INBOUND_FILE, INBOUND_MAGIC, (creation_unix_micros, 1))?;
        let next_target = match slot {
            Some(slot) if slot.creation_unix_micros == creation_unix_micros => slot.seq,
            // Written before the latest reset.
            Some(_) => 1,
            None => {
                tracing::warn!(
                    path = %dir.join(INBOUND_FILE).display(),
                    "FIX inbound sequence state is damaged; requesting retransmission from 1"
                );
                1
            }
        };
        Ok(Self {
            sync: sync == SyncPolicy::Fsync,
            barrier,
            state: Mutex::new(StoreState {
                identity,
                inbound,
                values: StoreValues {
                    creation_unix_micros,
                    next_sender: identity_slot.seq,
                    next_target,
                },
                floor: None,
                persisted_target: next_target,
                poisoned: None,
            }),
        })
    }

    /// Why the store stopped accepting changes, if it did.
    pub(crate) fn poisoned(&self) -> Option<String> {
        self.state.lock().poisoned.clone()
    }
}

impl StoreBackend for SessionStore {
    fn values(&self) -> StoreValues {
        self.state.lock().values
    }

    fn set_next_sender(&self, next: u64) -> Result<(), ConnectorError> {
        let mut state = self.state.lock();
        state.check()?;
        let creation = state.values.creation_unix_micros;
        if let Err(error) = state.identity.write(creation, next, self.sync) {
            return Err(state.poison("outbound sequence update", &error));
        }
        state.values.next_sender = next;
        Ok(())
    }

    fn set_next_target(&self, next: u64) -> Result<(), ConnectorError> {
        let mut state = self.state.lock();
        state.check()?;
        state.values.next_target = next;
        state.persist_target()
    }

    fn reset(&self, now_unix_micros: i64) -> Result<i64, ConnectorError> {
        self.state.lock().check()?;
        // Outside the store lock: the journal updates the floor under its own lock.
        let barrier = (self.barrier)();
        let mut state = self.state.lock();
        if let Err(error) = barrier {
            return Err(state.poison("journal sync before reset", &error));
        }
        state.check()?;
        let creation = now_unix_micros.max(state.values.creation_unix_micros + 1);
        if let Err(error) = state.identity.write(creation, 1, self.sync) {
            return Err(state.poison("reset", &error));
        }
        state.values = StoreValues {
            creation_unix_micros: creation,
            next_sender: 1,
            next_target: 1,
        };
        if let Err(error) = state.inbound.write(creation, 1, false) {
            return Err(state.poison("reset", &error));
        }
        state.persisted_target = 1;
        Ok(creation)
    }
}

impl DurabilityFloor for SessionStore {
    fn set(&self, floor: Option<u64>) {
        let mut state = self.state.lock();
        state.floor = floor;
        if state.poisoned.is_none() {
            // A failure poisons the store; the engine's next store call disconnects.
            let _ = state.persist_target();
        }
    }
}

fn now_unix_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_micros()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::*;

    fn no_barrier() -> ResetBarrier {
        Box::new(|| Ok(()))
    }

    fn open(dir: &Path) -> SessionStore {
        SessionStore::open(dir, SyncPolicy::Fsync, no_barrier()).expect("open")
    }

    #[test]
    fn state_survives_reopen() {
        let dir = tempfile::tempdir().expect("dir");
        let store = open(dir.path());
        let created = store.values();
        assert_eq!((created.next_sender, created.next_target), (1, 1));
        for next in 2..=5 {
            store.set_next_sender(next).expect("sender");
        }
        store.set_next_target(7).expect("target");
        drop(store);
        assert_eq!(
            open(dir.path()).values(),
            StoreValues {
                creation_unix_micros: created.creation_unix_micros,
                next_sender: 5,
                next_target: 7,
            }
        );
    }

    #[test]
    fn persisted_inbound_progress_never_passes_the_durability_floor() {
        let dir = tempfile::tempdir().expect("dir");
        let store = open(dir.path());
        store.set_next_target(3).expect("target");
        store.set(Some(3));
        store.set_next_target(10).expect("target");
        assert_eq!(
            store.values().next_target,
            10,
            "the engine sees its own progress"
        );
        assert_eq!(open(dir.path()).values().next_target, 3);
        store.set(None);
        assert_eq!(open(dir.path()).values().next_target, 10);
    }

    #[test]
    fn a_reset_syncs_the_journal_before_persisting_a_new_series() {
        let dir = tempfile::tempdir().expect("dir");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let (path, seen) = (dir.path().to_path_buf(), observed.clone());
        let store = SessionStore::open(
            dir.path(),
            SyncPolicy::Fsync,
            Box::new(move || {
                seen.lock().push(open(&path).values());
                Ok(())
            }),
        )
        .expect("open");
        store.set_next_sender(9).expect("sender");
        store.set_next_target(12).expect("target");
        let before = store.values();
        let created = store.reset(before.creation_unix_micros - 5).expect("reset");
        assert_eq!(*observed.lock(), vec![before], "the barrier ran first");
        assert_eq!(
            created,
            before.creation_unix_micros + 1,
            "creation times increase"
        );
        let expected = StoreValues {
            creation_unix_micros: created,
            next_sender: 1,
            next_target: 1,
        };
        assert_eq!(store.values(), expected);
        assert_eq!(open(dir.path()).values(), expected);
    }

    #[test]
    fn a_failed_barrier_keeps_the_previous_series_and_stops_the_store() {
        let dir = tempfile::tempdir().expect("dir");
        let store = SessionStore::open(
            dir.path(),
            SyncPolicy::Fsync,
            Box::new(|| Err(ConnectorError::WriteError("journal poisoned".into()))),
        )
        .expect("open");
        store.set_next_sender(4).expect("sender");
        let before = store.values();
        assert!(store.reset(before.creation_unix_micros + 1_000).is_err());
        assert!(store.poisoned().is_some());
        assert!(store.set_next_sender(5).is_err());
        assert!(store.set_next_target(2).is_err());
        assert_eq!(open(dir.path()).values(), before);
    }

    #[test]
    fn a_torn_newest_slot_falls_back_to_the_previous_generation() {
        let dir = tempfile::tempdir().expect("dir");
        let store = open(dir.path());
        // Generation 1 is in slot 1, so these land in slots 0 and 1.
        store.set_next_sender(2).expect("sender");
        store.set_next_sender(3).expect("sender");
        drop(store);
        let path = dir.path().join(IDENTITY_FILE);
        let mut bytes = std::fs::read(&path).expect("read");
        bytes[SLOT_LEN + 20] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");
        let store = open(dir.path());
        assert_eq!(store.values().next_sender, 2);
        store.set_next_sender(3).expect("rewrites the damaged slot");
        assert_eq!(open(dir.path()).values().next_sender, 3);
    }

    #[test]
    fn damaged_identity_fails_closed_and_stale_inbound_state_resumes_from_one() {
        let dir = tempfile::tempdir().expect("dir");
        let store = open(dir.path());
        store.set_next_target(8).expect("target");
        let inbound = std::fs::read(dir.path().join(INBOUND_FILE)).expect("read");
        store.reset(0).expect("reset");
        drop(store);
        // An inbound write from before the reset, as after a crash between the two writes.
        std::fs::write(dir.path().join(INBOUND_FILE), &inbound).expect("restore");
        assert_eq!(open(dir.path()).values().next_target, 1);
        std::fs::write(dir.path().join(INBOUND_FILE), [0_u8; 2 * SLOT_LEN]).expect("damage");
        assert_eq!(open(dir.path()).values().next_target, 1);
        std::fs::write(dir.path().join(IDENTITY_FILE), [0_u8; 2 * SLOT_LEN]).expect("damage");
        let error = SessionStore::open(dir.path(), SyncPolicy::Fsync, no_barrier())
            .err()
            .expect("fails closed");
        assert!(error.to_string().contains("damaged"), "{error}");
    }
}
