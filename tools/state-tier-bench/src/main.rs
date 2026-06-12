//! Benchmark gate for the state cold tier.
//!
//! Exercises fjall with workloads shaped like demoted operator state and
//! reports the numbers the go/no-go decision needs:
//!
//! - **group mode**: point upserts/reads of `(operator, vnode, group-key) →
//!   accumulator-state` records, Zipfian-skewed writes, uniform cold reads.
//! - **slice mode**: whole `(operator, vnode) → serialized-slice` blobs
//!   (tens of KB to MB), Zipfian-skewed slice rewrites, uniform slice reads.
//!
//! Gate criteria (run on target-class Linux NVMe, not a dev laptop):
//! cold-read p99 ≤ 1 ms from a non-writer thread under sustained write load;
//! background CPU bounded; write amplification acceptable at the sustained
//! upsert rate. Reads run on their own thread, mirroring how promotion
//! fetches run off the compute thread.
//!
//! Write amplification and CPU are sampled from `/proc` and reported as
//! n/a elsewhere.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Group,
    Slice,
}

struct Args {
    path: PathBuf,
    mode: Mode,
    /// group mode: number of distinct group keys. slice mode: number of slices.
    keys: u64,
    /// group mode: value size. slice mode: slice blob size.
    value_bytes: usize,
    /// Steady-state measurement window.
    duration_secs: u64,
    /// Zipf exponent for the write key distribution.
    zipf_s: f64,
    /// Target sustained upsert rate (ops/s); 0 = unthrottled.
    write_rate: u64,
    keep: bool,
}

fn usage() -> ! {
    eprintln!(
        "state-tier-bench --path <dir> [--mode group|slice] [--keys N] \
         [--value-bytes N] [--duration-secs N] [--zipf-s F] \
         [--write-rate OPS] [--keep]\n\
         defaults: group mode, 2,000,000 keys x 256 B (group) / \
         256 slices x 4 MiB (slice), 60 s, zipf 0.99, 100,000 ops/s"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut args = Args {
        path: PathBuf::new(),
        mode: Mode::Group,
        keys: 0,
        value_bytes: 0,
        duration_secs: 60,
        zipf_s: 0.99,
        write_rate: 100_000,
        keep: false,
    };
    let mut keys_set = false;
    let mut value_set = false;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let val = |it: &mut dyn Iterator<Item = String>| -> String {
            it.next().unwrap_or_else(|| usage())
        };
        match flag.as_str() {
            "--path" => args.path = PathBuf::from(val(&mut it)),
            "--mode" => {
                args.mode = match val(&mut it).as_str() {
                    "group" => Mode::Group,
                    "slice" => Mode::Slice,
                    _ => usage(),
                }
            }
            "--keys" => {
                args.keys = val(&mut it).parse().unwrap_or_else(|_| usage());
                keys_set = true;
            }
            "--value-bytes" => {
                args.value_bytes = val(&mut it).parse().unwrap_or_else(|_| usage());
                value_set = true;
            }
            "--duration-secs" => {
                args.duration_secs = val(&mut it).parse().unwrap_or_else(|_| usage());
            }
            "--zipf-s" => args.zipf_s = val(&mut it).parse().unwrap_or_else(|_| usage()),
            "--write-rate" => args.write_rate = val(&mut it).parse().unwrap_or_else(|_| usage()),
            "--keep" => args.keep = true,
            _ => usage(),
        }
    }
    if args.path.as_os_str().is_empty() {
        usage();
    }
    if !keys_set {
        args.keys = match args.mode {
            Mode::Group => 2_000_000,
            Mode::Slice => 256,
        };
    }
    if !value_set {
        args.value_bytes = match args.mode {
            Mode::Group => 256,
            Mode::Slice => 4 << 20,
        };
    }
    args
}

/// Key layout mirrors the engine's: operator name, vnode, then (group mode
/// only) the group key — so prefix locality matches what the tier will see.
fn make_key(mode: Mode, i: u64, buf: &mut Vec<u8>) {
    const VNODES: u64 = 256;
    buf.clear();
    buf.extend_from_slice(b"agg-0/");
    match mode {
        Mode::Group => {
            buf.extend_from_slice(&u32::try_from(i % VNODES).unwrap().to_be_bytes());
            buf.push(b'/');
            buf.extend_from_slice(&i.to_be_bytes());
        }
        Mode::Slice => {
            buf.extend_from_slice(&u32::try_from(i).unwrap_or(u32::MAX).to_be_bytes());
        }
    }
}

fn fill_value(rng: &mut SmallRng, buf: &mut [u8]) {
    rng.fill(buf);
}

#[cfg(target_os = "linux")]
mod sys {
    /// Bytes this process has caused to be written to storage.
    pub fn write_bytes() -> Option<u64> {
        let io = std::fs::read_to_string("/proc/self/io").ok()?;
        io.lines()
            .find_map(|l| l.strip_prefix("write_bytes: "))
            .and_then(|v| v.trim().parse().ok())
    }

    /// Total process CPU time (user + system).
    pub fn cpu_time() -> Option<std::time::Duration> {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        // Fields 14/15 (utime/stime) counted after the parenthesized comm,
        // which may itself contain spaces.
        let rest = stat.rsplit_once(')')?.1;
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let utime: u64 = fields.get(11)?.parse().ok()?;
        let stime: u64 = fields.get(12)?.parse().ok()?;
        let tick = 100u64; // CLK_TCK on every Linux that matters here
        Some(std::time::Duration::from_millis(
            (utime + stime) * 1000 / tick,
        ))
    }
}

#[cfg(not(target_os = "linux"))]
mod sys {
    pub fn write_bytes() -> Option<u64> {
        None
    }
    pub fn cpu_time() -> Option<std::time::Duration> {
        None
    }
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else {
                total += e.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

#[allow(clippy::too_many_lines)]
fn main() {
    let args = parse_args();
    let mode_name = match args.mode {
        Mode::Group => "group",
        Mode::Slice => "slice",
    };
    println!(
        "mode={mode_name} keys={} value_bytes={} duration={}s zipf_s={} write_rate={}/s",
        args.keys, args.value_bytes, args.duration_secs, args.zipf_s, args.write_rate
    );

    if args.path.exists() {
        std::fs::remove_dir_all(&args.path).expect("clear bench dir");
    }
    std::fs::create_dir_all(&args.path).expect("create bench dir");

    let db = fjall::Database::builder(&args.path)
        .open()
        .expect("open database");
    // Slice blobs are exactly what key-value separation is for; without it
    // every compaction rewrites the multi-MB values and the write
    // amplification number is meaningless.
    let kv_separation = args.mode == Mode::Slice;
    let ks = db
        .keyspace("state", move || {
            fjall::KeyspaceCreateOptions::default().with_kv_separation(if kv_separation {
                Some(fjall::KvSeparationOptions::default())
            } else {
                None
            })
        })
        .expect("open keyspace");

    // ---- Phase A: bulk load every key once (the demoted working set). ----
    let load_start = Instant::now();
    let mut rng = SmallRng::seed_from_u64(42);
    let mut key = Vec::with_capacity(64);
    let mut value = vec![0u8; args.value_bytes];
    let mut logical_bytes: u64 = 0;
    for i in 0..args.keys {
        make_key(args.mode, i, &mut key);
        fill_value(&mut rng, &mut value);
        ks.insert(&key, &value).expect("load insert");
        logical_bytes += (key.len() + value.len()) as u64;
    }
    db.persist(fjall::PersistMode::SyncAll).expect("persist");
    println!(
        "loaded {} keys ({:.1} MiB logical) in {:.1}s",
        args.keys,
        logical_bytes as f64 / (1 << 20) as f64,
        load_start.elapsed().as_secs_f64()
    );

    // ---- Phase B: steady state — Zipfian writer + uniform cold reader. ----
    let stop = Arc::new(AtomicBool::new(false));
    let writes_done = Arc::new(AtomicU64::new(0));
    let write_logical = Arc::new(AtomicU64::new(0));

    let disk_before = sys::write_bytes();
    let cpu_before = sys::cpu_time();
    let steady_start = Instant::now();

    let writer = {
        let ks = ks.clone();
        let stop = Arc::clone(&stop);
        let writes_done = Arc::clone(&writes_done);
        let write_logical = Arc::clone(&write_logical);
        let mode = args.mode;
        let keys = args.keys;
        let value_bytes = args.value_bytes;
        let zipf_s = args.zipf_s;
        let write_rate = args.write_rate;
        std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(7);
            let zipf = Zipf::new(keys as f64, zipf_s).expect("zipf");
            let mut key = Vec::with_capacity(64);
            let mut value = vec![0u8; value_bytes];
            let start = Instant::now();
            let mut done: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                // Zipf yields ranks in [1, keys]; spread the hot ranks over
                // the id space so skew isn't aligned with insertion order.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let rank = zipf.sample(&mut rng) as u64;
                let i = rank.wrapping_mul(0x9E37_79B9_7F4A_7C15) % keys;
                make_key(mode, i, &mut key);
                fill_value(&mut rng, &mut value);
                ks.insert(&key, &value).expect("steady insert");
                write_logical.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
                done += 1;
                writes_done.store(done, Ordering::Relaxed);
                if write_rate > 0 {
                    // Pace precisely: sleep off the deficit to the target rate.
                    let target_elapsed = done as f64 / write_rate as f64;
                    let actual = start.elapsed().as_secs_f64();
                    if target_elapsed > actual {
                        std::thread::sleep(Duration::from_secs_f64(target_elapsed - actual));
                    }
                }
            }
        })
    };

    let reader = {
        let ks = ks.clone();
        let stop = Arc::clone(&stop);
        let mode = args.mode;
        let keys = args.keys;
        std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(13);
            let mut key = Vec::with_capacity(64);
            let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("hist");
            let mut read_bytes: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                // Uniform over the whole key space: dominated by keys the
                // Zipfian writer rarely touches, i.e. genuinely cold reads.
                let i = rng.random_range(0..keys);
                make_key(mode, i, &mut key);
                let t = Instant::now();
                let v = ks.get(&key).expect("read");
                let us = u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX);
                hist.record(us.max(1)).ok();
                read_bytes += v.map(|v| v.len() as u64).unwrap_or(0);
            }
            (hist, read_bytes)
        })
    };

    std::thread::sleep(Duration::from_secs(args.duration_secs));
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer join");
    let (hist, read_bytes) = reader.join().expect("reader join");
    let steady_wall = steady_start.elapsed();

    // Let queued background work land before sampling disk/CPU deltas.
    db.persist(fjall::PersistMode::SyncAll).expect("persist");
    std::thread::sleep(Duration::from_secs(2));

    // ---- Report. ----
    let writes = writes_done.load(Ordering::Relaxed);
    let logical = write_logical.load(Ordering::Relaxed);
    println!("\n== steady state ({:.1}s wall) ==", steady_wall.as_secs_f64());
    println!(
        "writes: {} ({:.0}/s, {:.1} MiB logical)",
        writes,
        writes as f64 / steady_wall.as_secs_f64(),
        logical as f64 / (1 << 20) as f64
    );
    println!(
        "reads:  {} ({:.0}/s, {:.1} MiB returned)",
        hist.len(),
        hist.len() as f64 / steady_wall.as_secs_f64(),
        read_bytes as f64 / (1 << 20) as f64
    );
    println!(
        "read latency (us): p50={} p90={} p99={} p999={} max={}",
        hist.value_at_quantile(0.50),
        hist.value_at_quantile(0.90),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.max()
    );

    match (disk_before, sys::write_bytes()) {
        (Some(before), Some(after)) if logical > 0 => {
            let physical = after.saturating_sub(before);
            println!(
                "write amplification: {:.2}x ({:.1} MiB physical / {:.1} MiB logical)",
                physical as f64 / logical as f64,
                physical as f64 / (1 << 20) as f64,
                logical as f64 / (1 << 20) as f64
            );
        }
        _ => println!("write amplification: n/a (needs /proc, run on Linux)"),
    }
    match (cpu_before, sys::cpu_time()) {
        (Some(before), Some(after)) => {
            let cpu = after.saturating_sub(before);
            println!(
                "process CPU during steady state: {:.1}s ({:.0}% of one core)",
                cpu.as_secs_f64(),
                100.0 * cpu.as_secs_f64() / steady_wall.as_secs_f64()
            );
        }
        _ => println!("process CPU: n/a (needs /proc, run on Linux)"),
    }
    println!(
        "on-disk size: {:.1} MiB ({:.2}x of logical loaded)",
        dir_size(&args.path) as f64 / (1 << 20) as f64,
        dir_size(&args.path) as f64 / logical_bytes.max(1) as f64
    );

    let p99_us = hist.value_at_quantile(0.99);
    println!(
        "\nGATE: cold-read p99 {} 1ms -> {}",
        if p99_us <= 1000 { "<=" } else { ">" },
        if p99_us <= 1000 { "PASS" } else { "FAIL" }
    );

    drop(ks);
    drop(db);
    if !args.keep {
        std::fs::remove_dir_all(&args.path).ok();
    }
}
