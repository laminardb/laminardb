#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, TableHandle,
    WriteTransaction,
};
use serde::Serialize;

const NOTICE: &str = "CONSTRUCTION ONLY / NO DECISION / NOT PRESCREEN OR QUALIFICATION EVIDENCE";
const LANE: &str = "construction-only-no-decision";
const CACHE_BYTES: usize = 8 * 1024 * 1024 * 1024;
const KEY_BYTES: usize = 32;
const VALUE_BYTES: usize = 992;
const BASE_ROWS_PER_TABLE: u64 = 16_384;
const TABLE_NAMES: [&str; 4] = ["state", "timer", "join_left", "join_right"];
const EXPORT_HEADER: &[u8; 8] = b"LDBCNST1";
const EXPORT_TRAILER: &[u8; 8] = b"LDBEND01";

const STATE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");
const TIMER: TableDefinition<&[u8], &[u8]> = TableDefinition::new("timer");
const JOIN_LEFT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("join_left");
const JOIN_RIGHT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("join_right");

#[derive(Clone, Copy)]
enum Mode {
    I1,
    I2,
    Qr,
}

impl Mode {
    fn code(self) -> &'static str {
        match self {
            Self::I1 => "I1",
            Self::I2 => "I2",
            Self::Qr => "QR",
        }
    }
}

#[derive(Serialize)]
struct ModeObservation {
    mode: &'static str,
    mutations: u64,
    construction_wall_ns: u64,
    setter_trace: [&'static str; 3],
}

#[derive(Serialize)]
struct HoldObservation {
    mode: &'static str,
    hold_ns: u64,
    victim_begin_write_dispatch_after_holder_ns: u64,
    victim_dispatched_before_holder_release: bool,
    victim_begin_write_not_returned_while_holder_live: bool,
    victim_begin_write_return_after_release_ns: u64,
}

#[derive(Serialize)]
struct EvidenceScope {
    qualification_eligible: bool,
    candidate_admission_eligible: bool,
    backend_selection_eligible: bool,
    production_eligible: bool,
    independent_soak_eligible: bool,
    c1_c2_c3_eligible: bool,
    fault_endurance_eligible: bool,
    checkpoint_exactly_once_eligible: bool,
    source_sink_delivery_eligible: bool,
}

#[derive(Serialize)]
struct ConstructionReport {
    schema_version: &'static str,
    notice: &'static str,
    lane: &'static str,
    disposition: Option<String>,
    redb_version: &'static str,
    redb_default_features: bool,
    redb_features: Vec<String>,
    cache_bytes: u64,
    table_names: [&'static str; 4],
    key_bytes: u64,
    value_bytes: u64,
    base_rows_per_table: u64,
    base_logical_bytes: u64,
    base_build_wall_ns: u64,
    modes: Vec<ModeObservation>,
    hold: HoldObservation,
    evidence_scope: EvidenceScope,
}

fn main() -> ExitCode {
    eprintln!("{NOTICE}");
    match run_cli() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("REDB_CONSTRUCTION_ERROR {error}");
            ExitCode::from(2)
        }
    }
}

fn run_cli() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().as_deref() != Some(std::ffi::OsStr::new(LANE)) {
        return Err(usage().into());
    }
    let command = args.next().ok_or_else(usage)?;
    let first = PathBuf::from(args.next().ok_or_else(usage)?);
    let second = PathBuf::from(args.next().ok_or_else(usage)?);
    if args.next().is_some() {
        return Err(usage().into());
    }

    match command.to_str() {
        Some("run") => run_construction(&first, &second, BASE_ROWS_PER_TABLE),
        Some("scan") => export_database(&first, &second),
        _ => Err(usage().into()),
    }
}

fn usage() -> String {
    format!(
        "usage: candidate {LANE} run <new-db-path> <new-report-path>\n       \
         candidate {LANE} scan <existing-db-path> <new-export-path>"
    )
}

fn new_file(path: &Path) -> Result<File, Box<dyn Error>> {
    let parent = path.parent().ok_or("path has no parent")?;
    let metadata = std::fs::metadata(parent)?;
    if !metadata.is_dir() {
        return Err("path parent is not a directory".into());
    }
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?)
}

fn builder() -> redb::Builder {
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    builder
}

fn configure(
    transaction: &mut WriteTransaction,
    mode: Mode,
) -> Result<[&'static str; 3], Box<dyn Error>> {
    transaction.set_durability(Durability::Immediate)?;
    let trace = match mode {
        Mode::I1 => {
            transaction.set_two_phase_commit(false);
            transaction.set_quick_repair(false);
            [
                "set_durability(Immediate)",
                "set_two_phase_commit(false)",
                "set_quick_repair(false)",
            ]
        }
        Mode::I2 => {
            transaction.set_two_phase_commit(true);
            transaction.set_quick_repair(false);
            [
                "set_durability(Immediate)",
                "set_two_phase_commit(true)",
                "set_quick_repair(false)",
            ]
        }
        Mode::Qr => {
            transaction.set_two_phase_commit(false);
            transaction.set_quick_repair(true);
            [
                "set_durability(Immediate)",
                "set_two_phase_commit(false)",
                "set_quick_repair(true)",
            ]
        }
    };
    Ok(trace)
}

fn table_definition(tag: u8) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match tag {
        0 => STATE,
        1 => TIMER,
        2 => JOIN_LEFT,
        3 => JOIN_RIGHT,
        _ => unreachable!("closed table tag"),
    }
}

fn key(tag: u8, domain: u8, index: u64) -> [u8; KEY_BYTES] {
    let mut key = [0_u8; KEY_BYTES];
    key[..4].copy_from_slice(b"LDBR");
    key[4] = tag;
    key[5] = domain;
    key[6..14].copy_from_slice(&index.to_be_bytes());
    for (offset, byte) in key[14..].iter_mut().enumerate() {
        *byte = tag
            .wrapping_mul(37)
            .wrapping_add(domain.wrapping_mul(17))
            .wrapping_add(index as u8)
            .wrapping_add(offset as u8);
    }
    key
}

fn value(tag: u8, domain: u8, index: u64, epoch: u8) -> [u8; VALUE_BYTES] {
    let mut value = [0_u8; VALUE_BYTES];
    let index_bytes = index.to_be_bytes();
    for (offset, byte) in value.iter_mut().enumerate() {
        *byte = index_bytes[offset % index_bytes.len()]
            .wrapping_add((offset as u8).wrapping_mul(29))
            .wrapping_add(tag.wrapping_mul(31))
            .wrapping_add(domain.wrapping_mul(13))
            .wrapping_add(epoch.wrapping_mul(7));
    }
    value
}

fn run_construction(
    database_path: &Path,
    report_path: &Path,
    base_rows_per_table: u64,
) -> Result<(), Box<dyn Error>> {
    if database_path == report_path {
        return Err("database and report paths must differ".into());
    }
    let mut report_file = new_file(report_path)?;
    let database_file = new_file(database_path)?;
    let database = builder().create_file(database_file)?;

    let base_started = Instant::now();
    let mut transaction = database.begin_write()?;
    let _base_setter_trace = configure(&mut transaction, Mode::I1)?;
    for tag in 0..4 {
        let mut table = transaction.open_table(table_definition(tag))?;
        for index in 0..base_rows_per_table {
            let key = key(tag, 0, index);
            let value = value(tag, 0, index, 0);
            if table.insert(key.as_slice(), value.as_slice())?.is_some() {
                return Err("base insert replaced an existing row".into());
            }
        }
    }
    transaction.commit()?;
    let base_build_wall_ns = nanos(base_started.elapsed())?;

    let mut modes = Vec::new();
    for (step, mode) in [(1_u8, Mode::I1), (2, Mode::I2), (3, Mode::Qr)] {
        let started = Instant::now();
        let (mutations, setter_trace) =
            apply_mode_transaction(&database, mode, step, base_rows_per_table)?;
        modes.push(ModeObservation {
            mode: mode.code(),
            mutations,
            construction_wall_ns: nanos(started.elapsed())?,
            setter_trace,
        });
    }

    let hold = run_hold(Arc::new(database))?;
    let report = ConstructionReport {
        schema_version: "state-backend-redb-construction-observation/v1",
        notice: NOTICE,
        lane: LANE,
        disposition: None,
        redb_version: "4.1.0",
        redb_default_features: false,
        redb_features: Vec::new(),
        cache_bytes: CACHE_BYTES as u64,
        table_names: TABLE_NAMES,
        key_bytes: KEY_BYTES as u64,
        value_bytes: VALUE_BYTES as u64,
        base_rows_per_table,
        base_logical_bytes: base_rows_per_table
            .checked_mul(4)
            .and_then(|rows| rows.checked_mul((KEY_BYTES + VALUE_BYTES) as u64))
            .ok_or("base logical byte overflow")?,
        base_build_wall_ns,
        modes,
        hold,
        evidence_scope: EvidenceScope {
            qualification_eligible: false,
            candidate_admission_eligible: false,
            backend_selection_eligible: false,
            production_eligible: false,
            independent_soak_eligible: false,
            c1_c2_c3_eligible: false,
            fault_endurance_eligible: false,
            checkpoint_exactly_once_eligible: false,
            source_sink_delivery_eligible: false,
        },
    };

    serde_json::to_writer_pretty(&mut report_file, &report)?;
    report_file.write_all(b"\n")?;
    report_file.sync_all()?;
    Ok(())
}

fn apply_mode_transaction(
    database: &Database,
    mode: Mode,
    step: u8,
    base_rows_per_table: u64,
) -> Result<(u64, [&'static str; 3]), Box<dyn Error>> {
    if base_rows_per_table < 64 {
        return Err("base_rows_per_table must be at least 64".into());
    }
    let mut transaction = database.begin_write()?;
    let setter_trace = configure(&mut transaction, mode)?;
    for tag in 0..4 {
        let mut table = transaction.open_table(table_definition(tag))?;
        let overwrite_start = match step {
            1 => 0,
            2 => 16,
            3 => 0,
            _ => return Err("invalid construction step".into()),
        };
        let delete_start = match step {
            1 => 48,
            2 => 56,
            3 => 40,
            _ => unreachable!(),
        };
        for index in overwrite_start..overwrite_start + 16 {
            let key = key(tag, 0, index);
            let previous_epoch = if step == 3 { 1 } else { 0 };
            let expected_previous = value(tag, 0, index, previous_epoch);
            let value = value(tag, 0, index, step);
            let previous = table
                .insert(key.as_slice(), value.as_slice())?
                .ok_or("overwrite did not replace an existing row")?;
            if previous.value() != expected_previous.as_slice() {
                return Err("overwrite observed an unexpected prior value".into());
            }
        }
        for index in delete_start..delete_start + 8 {
            let key = key(tag, 0, index);
            let expected_previous = value(tag, 0, index, 0);
            let previous = table
                .remove(key.as_slice())?
                .ok_or("delete did not remove an existing row")?;
            if previous.value() != expected_previous.as_slice() {
                return Err("delete observed an unexpected prior value".into());
            }
        }
        for index in 0..8 {
            let key = key(tag, step, index);
            let value = value(tag, step, index, step);
            if table.insert(key.as_slice(), value.as_slice())?.is_some() {
                return Err("new-domain insert replaced an existing row".into());
            }
        }
    }
    transaction.commit()?;
    Ok((128, setter_trace))
}

fn run_hold(database: Arc<Database>) -> Result<HoldObservation, Box<dyn Error>> {
    let (holder_acquired_tx, holder_acquired_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let holder_database = Arc::clone(&database);
    let holder = thread::spawn(move || -> Result<(), String> {
        let mut transaction = holder_database.begin_write().map_err(|e| e.to_string())?;
        let _setter_trace = configure(&mut transaction, Mode::I1).map_err(|e| e.to_string())?;
        holder_acquired_tx.send(()).map_err(|e| e.to_string())?;
        release_rx.recv().map_err(|e| e.to_string())?;
        transaction.commit().map_err(|e| e.to_string())
    });
    holder_acquired_rx.recv_timeout(Duration::from_secs(10))?;
    let hold_started = Instant::now();

    let (victim_ready_tx, victim_ready_rx) = mpsc::sync_channel(0);
    let (victim_start_tx, victim_start_rx) = mpsc::sync_channel(0);
    let (victim_dispatched_tx, victim_dispatched_rx) = mpsc::sync_channel(1);
    let (victim_returned_tx, victim_returned_rx) = mpsc::sync_channel(1);
    let victim_database = Arc::clone(&database);
    let victim = thread::spawn(move || -> Result<(), String> {
        victim_ready_tx.send(()).map_err(|e| e.to_string())?;
        victim_start_rx.recv().map_err(|e| e.to_string())?;
        victim_dispatched_tx
            .send(Instant::now())
            .map_err(|e| e.to_string())?;
        let mut transaction = victim_database.begin_write().map_err(|e| e.to_string())?;
        victim_returned_tx
            .send(Instant::now())
            .map_err(|e| e.to_string())?;
        let _setter_trace = configure(&mut transaction, Mode::I1).map_err(|e| e.to_string())?;
        transaction.commit().map_err(|e| e.to_string())
    });
    victim_ready_rx.recv_timeout(Duration::from_secs(10))?;

    let until_victim_start = Duration::from_millis(50).saturating_sub(hold_started.elapsed());
    thread::sleep(until_victim_start);
    victim_start_tx.send(())?;
    let victim_dispatched_at = victim_dispatched_rx.recv_timeout(Duration::from_secs(10))?;

    let remaining = Duration::from_millis(250).saturating_sub(hold_started.elapsed());
    thread::sleep(remaining);
    let released_at = Instant::now();
    let victim_dispatched_before_release = victim_dispatched_at < released_at;
    let victim_not_returned = matches!(
        victim_returned_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    );
    if !victim_dispatched_before_release || !victim_not_returned {
        return Err("victim begin_write returned while holder transaction was live".into());
    }
    release_tx.send(())?;
    let victim_returned_at = victim_returned_rx.recv_timeout(Duration::from_secs(10))?;
    let victim_after_release = victim_returned_at
        .checked_duration_since(released_at)
        .ok_or("victim begin_write return preceded holder release")?;

    holder.join().map_err(|_| "holder thread panicked")??;
    victim.join().map_err(|_| "victim thread panicked")??;
    Ok(HoldObservation {
        mode: Mode::I1.code(),
        hold_ns: nanos(released_at.duration_since(hold_started))?,
        victim_begin_write_dispatch_after_holder_ns: nanos(
            victim_dispatched_at.duration_since(hold_started),
        )?,
        victim_dispatched_before_holder_release: victim_dispatched_before_release,
        victim_begin_write_not_returned_while_holder_live: victim_not_returned,
        victim_begin_write_return_after_release_ns: nanos(victim_after_release)?,
    })
}

fn export_database(database_path: &Path, export_path: &Path) -> Result<(), Box<dyn Error>> {
    if database_path == export_path {
        return Err("database and export paths must differ".into());
    }
    let output = new_file(export_path)?;
    let metadata = std::fs::symlink_metadata(database_path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err("database path must be a regular non-symlink file".into());
    }
    let database = builder().open_read_only(database_path)?;
    let read = database.begin_read()?;
    let mut normal_names = read
        .list_tables()?
        .map(|handle| handle.name().to_owned())
        .collect::<Vec<_>>();
    normal_names.sort();
    let mut expected_names = TABLE_NAMES.map(str::to_owned).to_vec();
    expected_names.sort();
    if normal_names != expected_names {
        return Err(format!("unexpected normal table inventory: {normal_names:?}").into());
    }
    if read.list_multimap_tables()?.next().is_some() {
        return Err("multimap tables are forbidden".into());
    }

    let mut writer = BufWriter::new(output);
    writer.write_all(EXPORT_HEADER)?;
    let mut total = 0_u64;
    let mut counts = [0_u64; 4];
    for tag in 0..4_u8 {
        let table = read.open_table(table_definition(tag))?;
        let mut previous: Option<Vec<u8>> = None;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let key = key.value();
            let value = value.value();
            if key.len() != KEY_BYTES || value.len() != VALUE_BYTES {
                return Err("candidate row has unexpected key/value width".into());
            }
            if previous.as_deref().is_some_and(|old| old >= key) {
                return Err("candidate table iteration is not strictly increasing".into());
            }
            writer.write_all(&[tag])?;
            writer.write_all(key)?;
            writer.write_all(value)?;
            previous = Some(key.to_vec());
            counts[tag as usize] = counts[tag as usize]
                .checked_add(1)
                .ok_or("table row count overflow")?;
            total = total.checked_add(1).ok_or("total row count overflow")?;
        }
    }
    writer.write_all(EXPORT_TRAILER)?;
    writer.write_all(&total.to_be_bytes())?;
    for count in counts {
        writer.write_all(&count.to_be_bytes())?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    println!("CONSTRUCTION_EXPORT rows={total}");
    Ok(())
}

fn nanos(duration: Duration) -> Result<u64, Box<dyn Error>> {
    u64::try_from(duration.as_nanos()).map_err(|_| "duration exceeds u64 nanoseconds".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_modes_keep_exact_setter_order() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock after Unix epoch")
            .as_nanos();
        let database_path = std::env::temp_dir().join(format!(
            "laminardb-redb-mode-test-{}-{}.redb",
            std::process::id(),
            unique
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&database_path)
            .expect("exclusive test database");
        let database = builder().create_file(file).expect("create test database");
        let expected = [
            (
                Mode::I1,
                [
                    "set_durability(Immediate)",
                    "set_two_phase_commit(false)",
                    "set_quick_repair(false)",
                ],
            ),
            (
                Mode::I2,
                [
                    "set_durability(Immediate)",
                    "set_two_phase_commit(true)",
                    "set_quick_repair(false)",
                ],
            ),
            (
                Mode::Qr,
                [
                    "set_durability(Immediate)",
                    "set_two_phase_commit(false)",
                    "set_quick_repair(true)",
                ],
            ),
        ];
        for (mode, trace) in expected {
            let mut transaction = database.begin_write().expect("begin test write");
            assert_eq!(configure(&mut transaction, mode).expect("configure"), trace);
            transaction.abort().expect("abort test write");
        }
        drop(database);
        std::fs::remove_file(&database_path).expect("remove test database");
    }

    #[test]
    fn deterministic_keys_are_table_and_domain_separated() {
        assert_ne!(key(0, 0, 1), key(1, 0, 1));
        assert_ne!(key(0, 0, 1), key(0, 1, 1));
        assert!(key(0, 0, 1) < key(0, 0, 2));
    }
}
