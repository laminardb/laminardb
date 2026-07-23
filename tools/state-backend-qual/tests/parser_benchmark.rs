//! Non-gating release observation for the real synthetic mechanism-bundle CLI path.
//!
//! Run explicitly with `cargo test --release --test parser_benchmark -- --ignored --nocapture`.
//! Measure maximum RSS around that command with the host OS. Results characterize validator
//! tooling only and are never qualification evidence.

use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use state_backend_qual::resource_samples::MAX_RESOURCE_ARTIFACT_BYTES;

const PROFILE: &[u8] = include_bytes!("../profiles/linux-nvme-v3.candidate.json");
const OBSERVED_MAPPING: &[u8] = include_bytes!("fixtures/mechanism-mapping-observed-v1.json");
const BUNDLE_TEMPLATE: &[u8] = include_bytes!("fixtures/mechanism-bundle-v1/bundle.json");
const CUTS_HEX: &str = include_str!("fixtures/mechanism-bundle-v1/common-cuts-v2.hex");
const DEVICE_HEX: &str = include_str!("fixtures/mechanism-bundle-v1/target-device-io-v1.hex");
const DOMAIN: &[u8] = b"LDB-SBQ-RESOURCE-SAMPLES-V2\0";
const RECORD_BYTES: u64 = 160;
const DEFAULT_RECORDS: u64 = 400_000;
const DEFAULT_ROUNDS: usize = 5;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct FixtureDirectory(PathBuf);

impl FixtureDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "state-backend-bundle-benchmark-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self, file_name: &str) -> PathBuf {
        self.0.join(file_name)
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
#[ignore = "non-gating release CLI throughput/RSS observation; run explicitly"]
fn streaming_mechanism_bundle_cli_observation() {
    let record_count = environment_u64("LDB_PARSER_BENCH_RECORDS", DEFAULT_RECORDS);
    let rounds = usize::try_from(environment_u64(
        "LDB_PARSER_BENCH_ROUNDS",
        DEFAULT_ROUNDS as u64,
    ))
    .expect("round count must fit usize");
    assert!(record_count > 0, "record count must be positive");
    assert!(rounds > 0, "round count must be positive");
    assert!(rounds <= 100, "round count must not exceed 100");

    let sample_bytes = u64::try_from(DOMAIN.len())
        .unwrap()
        .checked_add(8)
        .and_then(|header| {
            record_count
                .checked_mul(RECORD_BYTES)
                .and_then(|records| header.checked_add(records))
        })
        .expect("benchmark byte length must not overflow");
    assert!(
        sample_bytes <= MAX_RESOURCE_ARTIFACT_BYTES,
        "benchmark input exceeds the production parser cap"
    );

    let fixture = FixtureDirectory::new();
    std::fs::write(fixture.path("profile.json"), PROFILE).unwrap();
    let mapping = not_applicable_mapping();
    std::fs::write(fixture.path("mapping.json"), &mapping).unwrap();
    let cuts = decode_hex(CUTS_HEX);
    std::fs::write(fixture.path("common-cuts.bin"), &cuts).unwrap();
    let device = decode_hex(DEVICE_HEX);
    std::fs::write(fixture.path("device.bin"), &device).unwrap();
    let samples_hash = write_common_samples(&fixture.path("common-samples.bin"), record_count);

    let mut input: Value = serde_json::from_slice(BUNDLE_TEMPLATE).unwrap();
    input["limits"]["expected_nominal_resource_samples"] = record_count.into();
    input["artifacts"]["mechanism_mapping"]["byte_length"] = mapping.len().into();
    input["artifacts"]["mechanism_mapping"]["sha256"] = hex(&Sha256::digest(&mapping)).into();
    input["artifacts"]["common_resource_samples"]["byte_length"] = sample_bytes.into();
    input["artifacts"]["common_resource_samples"]["sha256"] = hex(&samples_hash).into();
    input["artifacts"]["maintenance_debt_samples"] = Value::Null;
    input["artifacts"]["stall_intervals"] = Value::Null;
    let input_path = fixture.path("bundle.json");
    std::fs::write(&input_path, serde_json::to_vec_pretty(&input).unwrap()).unwrap();

    let artifact_bytes = sample_bytes
        + u64::try_from(PROFILE.len() + mapping.len() + cuts.len() + device.len()).unwrap();
    let mut elapsed = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let start = Instant::now();
        let status = Command::new(env!("CARGO_BIN_EXE_state-backend-qual"))
            .arg("validate-mechanism-bundle")
            .arg(&input_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        elapsed.push(start.elapsed());
        assert!(status.success());
    }
    elapsed.sort_unstable();
    let median = elapsed[elapsed.len() / 2];
    let mebibytes = artifact_bytes as f64 / (1024.0 * 1024.0);
    let throughput = mebibytes / median.as_secs_f64();
    println!(
        "NOT QUALIFICATION EVIDENCE mechanism_bundle_cli_observation \
         artifact_bytes={artifact_bytes} records={record_count} rounds={rounds} median_ms={} \
         median_mib_per_second={throughput:.2}",
        median.as_millis()
    );
}

fn write_common_samples(path: &Path, record_count: u64) -> [u8; 32] {
    let mut output = BufWriter::new(File::create(path).unwrap());
    let mut hasher = Sha256::new();
    write_hashed(&mut output, &mut hasher, DOMAIN);
    write_hashed(&mut output, &mut hasher, &record_count.to_be_bytes());
    let mut record = [0_u8; RECORD_BYTES as usize];
    for index in 0..record_count {
        record.fill(0);
        record[0..8].copy_from_slice(&index.to_be_bytes());
        let begin = 1_000_000_000_u64
            .checked_add(index.checked_mul(2).expect("benchmark timestamp overflow"))
            .expect("benchmark timestamp overflow");
        record[8..16].copy_from_slice(&begin.to_be_bytes());
        record[16..24].copy_from_slice(&(begin + 1).to_be_bytes());
        write_hashed(&mut output, &mut hasher, &record);
    }
    output.flush().unwrap();
    hasher.finalize().into()
}

fn write_hashed(output: &mut BufWriter<File>, hasher: &mut Sha256, bytes: &[u8]) {
    output.write_all(bytes).unwrap();
    hasher.update(bytes);
}

fn not_applicable_mapping() -> Vec<u8> {
    let mut mapping: Value = serde_json::from_slice(OBSERVED_MAPPING).unwrap();
    let proof = |role: &str, byte: char| {
        json!({
            "role": role,
            "byte_length": 1,
            "sha256": byte.to_string().repeat(64),
            "media_type": "application/octet-stream"
        })
    };
    mapping["background_maintenance_debt"] = json!({
        "kind": "not_applicable",
        "reason_code": "no-background-maintenance-mechanism-in-exact-build",
        "claim_scope": "complete-candidate-process",
        "source_proof": proof("mechanism-source-proof", 'a'),
        "configuration_proof": proof("mechanism-configuration-proof", 'b'),
        "bounded_probe_proof": proof("mechanism-bounded-probe-proof", 'c')
    });
    mapping["engine_pressure_stalls"] = json!({
        "kind": "not_applicable",
        "reason_code": "no-engine-pressure-stall-mechanism-in-exact-build",
        "claim_scope": "complete-candidate-process",
        "source_proof": proof("mechanism-source-proof", 'a'),
        "configuration_proof": proof("mechanism-configuration-proof", 'b'),
        "bounded_probe_proof": proof("mechanism-bounded-probe-proof", 'c')
    });
    serde_json::to_vec_pretty(&mapping).unwrap()
}

fn decode_hex(source: &str) -> Vec<u8> {
    let digits = source
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert!(digits.len().is_multiple_of(2));
    digits
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn environment_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |source| {
        source
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
    })
}
