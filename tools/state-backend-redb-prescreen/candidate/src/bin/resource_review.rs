//! Reproduces the bounded redb 4.1.0 space/reclamation observation.
//!
//! This is elimination evidence, not a benchmark, backend adapter, or qualification runner.

#![forbid(unsafe_code)]

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

const CACHE_BYTES: usize = 256 * 1024 * 1024;
const ROWS: u64 = 262_144;
const KEY_BYTES: usize = 32;
const VALUE_BYTES: usize = 208;
const STATE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");
const JOURNAL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("journal");

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("REDB_RESOURCE_REVIEW_ERROR {error:#}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let root = PathBuf::from(
        arguments
            .next()
            .ok_or("usage: resource_review <existing-empty-directory> [baseline-barrier]")?,
    );
    let baseline_barrier = match arguments.next().as_deref() {
        None => false,
        Some(value) if value == "baseline-barrier" => true,
        Some(_) => return Err("second argument must be 'baseline-barrier'".into()),
    };
    if arguments.next().is_some() {
        return Err("too many arguments".into());
    }
    if !root.is_dir() || fs::read_dir(&root)?.next().is_some() {
        return Err("review directory must exist and be empty".into());
    }
    let database_path = root.join("working.redb");
    let mut database = create_database(&database_path)?;

    populate(&database)?;
    print_space("baseline", &database, &database_path)?;
    if baseline_barrier {
        durability_barrier(&database, "baseline")?;
    }
    println!("scenario_baseline_barrier={baseline_barrier}");

    let snapshot = database.begin_read()?;
    mutate(&database, 128, 200, Durability::None)?;
    mutate(&database, 1_000, 50, Durability::None)?;
    mutate(&database, 8_192, 8, Durability::None)?;
    print_space("snapshot_held", &database, &database_path)?;

    snapshot.close()?;
    mutate(&database, 1_000, 50, Durability::None)?;
    print_space("snapshot_released", &database, &database_path)?;

    durability_barrier(&database, "first")?;
    mutate(&database, 1_000, 50, Durability::None)?;
    print_space("post_barrier_churn", &database, &database_path)?;
    durability_barrier(&database, "second")?;
    print_space("post_second_barrier", &database, &database_path)?;
    mutate(&database, 1_000, 50, Durability::None)?;
    print_space("post_second_barrier_churn", &database, &database_path)?;
    durability_barrier(&database, "third")?;
    print_space("post_third_barrier", &database, &database_path)?;

    let compact_started = Instant::now();
    let compacted = database.compact()?;
    println!(
        "offline_compact performed={compacted} elapsed_ms={:.3}",
        compact_started.elapsed().as_secs_f64() * 1_000.0
    );
    print_space("after_compact", &database, &database_path)?;

    let drop_started = Instant::now();
    drop(database);
    let (file_length, physical) = file_usage(&database_path)?;
    println!(
        "clean_drop elapsed_ms={:.3} file_length_bytes={file_length} physical_bytes={}",
        drop_started.elapsed().as_secs_f64() * 1_000.0,
        physical
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unsupported".to_owned())
    );
    println!("result=COMPLETED scope=bounded-resource-review-not-qualification");
    Ok(())
}

fn create_database(path: &Path) -> Result<Database, Box<dyn Error>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    Ok(builder.create_file(file)?)
}

fn populate(database: &Database) -> Result<(), Box<dyn Error>> {
    for first in (0..ROWS).step_by(4_096) {
        let mut transaction = database.begin_write()?;
        transaction.set_durability(Durability::None)?;
        if first == 0 {
            drop(transaction.open_table(JOURNAL)?);
        }
        {
            let mut state = transaction.open_table(STATE)?;
            for row in first..(first + 4_096).min(ROWS) {
                let key = key(row);
                let value = value(row, 0);
                state.insert(key.as_slice(), value.as_slice())?;
            }
        }
        transaction.commit()?;
    }
    Ok(())
}

fn mutate(
    database: &Database,
    batch_rows: u64,
    iterations: usize,
    durability: Durability,
) -> Result<(), Box<dyn Error>> {
    for iteration in 0..iterations {
        let mut transaction = database.begin_write()?;
        transaction.set_durability(durability)?;
        {
            let mut state = transaction.open_table(STATE)?;
            for offset in 0..batch_rows {
                let row = (iteration as u64 * batch_rows + offset) % ROWS;
                let key = key(row);
                black_box(state.get(key.as_slice())?);
                let value = value(row, iteration as u64 + 1);
                state.insert(key.as_slice(), value.as_slice())?;
            }
        }
        {
            let mut journal = transaction.open_table(JOURNAL)?;
            let journal_key = key(iteration as u64);
            let journal_value = value(batch_rows, iteration as u64);
            journal.insert(journal_key.as_slice(), journal_value.as_slice())?;
        }
        transaction.commit()?;
    }
    Ok(())
}

fn durability_barrier(database: &Database, label: &str) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    let mut transaction = database.begin_write()?;
    transaction.set_durability(Durability::Immediate)?;
    transaction.commit()?;
    println!(
        "durability_barrier label={label} elapsed_ms={:.3}",
        started.elapsed().as_secs_f64() * 1_000.0
    );
    Ok(())
}

fn print_space(label: &str, database: &Database, path: &Path) -> Result<(), Box<dyn Error>> {
    let logical = logical_live_bytes(database)?;
    let (file_length, physical) = file_usage(path)?;
    match physical {
        Some(physical) => println!(
            "space_{label} logical_live_bytes={logical} file_length_bytes={file_length} physical_bytes={physical} physical_ratio={:.3}",
            physical as f64 / logical as f64
        ),
        None => println!(
            "space_{label} logical_live_bytes={logical} file_length_bytes={file_length} physical_bytes=unsupported"
        ),
    }
    Ok(())
}

fn logical_live_bytes(database: &Database) -> Result<u64, Box<dyn Error>> {
    let read = database.begin_read()?;
    let mut bytes = 0_u64;
    for definition in [STATE, JOURNAL] {
        let table = read.open_table(definition)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            bytes = bytes
                .checked_add(key.value().len() as u64)
                .and_then(|value_bytes| value_bytes.checked_add(value.value().len() as u64))
                .ok_or("logical byte count overflow")?;
        }
    }
    read.close()?;
    Ok(bytes)
}

fn file_usage(path: &Path) -> Result<(u64, Option<u64>), Box<dyn Error>> {
    let metadata = fs::metadata(path)?;
    #[cfg(unix)]
    let physical = {
        use std::os::unix::fs::MetadataExt;
        Some(
            metadata
                .blocks()
                .checked_mul(512)
                .ok_or("physical byte count overflow")?,
        )
    };
    #[cfg(not(unix))]
    let physical = None;
    Ok((metadata.len(), physical))
}

fn key(row: u64) -> [u8; KEY_BYTES] {
    let mut key = [0_u8; KEY_BYTES];
    key[..4].copy_from_slice(&1_u32.to_be_bytes());
    key[4..6].copy_from_slice(&((row % 256) as u16).to_be_bytes());
    key[6..14].copy_from_slice(&row.to_be_bytes());
    key[14..22].copy_from_slice(&row.rotate_left(17).to_be_bytes());
    key
}

fn value(row: u64, epoch: u64) -> [u8; VALUE_BYTES] {
    let mut value = [0_u8; VALUE_BYTES];
    value[..8].copy_from_slice(&row.to_be_bytes());
    value[8..16].copy_from_slice(&epoch.to_be_bytes());
    for (index, byte) in value[16..].iter_mut().enumerate() {
        *byte = (row as u8)
            .wrapping_mul(31)
            .wrapping_add(epoch as u8)
            .wrapping_add(index as u8);
    }
    value
}
