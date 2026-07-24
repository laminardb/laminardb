#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::Deserialize;
use sha2::{Digest as _, Sha256};

const NOTICE: &str = "CONSTRUCTION ONLY / NO DECISION / NOT PRESCREEN OR QUALIFICATION EVIDENCE";
const LANE: &str = "construction-only-no-decision";
const KEY_BYTES: usize = 32;
const VALUE_BYTES: usize = 992;
const BASE_ROWS_PER_TABLE: u64 = 16_384;
const EXPORT_HEADER: &[u8; 8] = b"LDBCNST1";
const EXPORT_TRAILER: &[u8; 8] = b"LDBEND01";
const EXPECTED_EXPORT_BYTES: usize = 67_174_456;
const EXPECTED_EXPORT_SHA256: &str =
    "a82240b51daf373ce03bbff9cd70bede90eda1b8433ef39e6be0754dd76e7290";
const MAX_REPORT_BYTES: u64 = 1_048_576;
const MAX_EXPORT_BYTES: u64 = 80 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModeObservation {
    mode: String,
    mutations: u64,
    construction_wall_ns: u64,
    setter_trace: [String; 3],
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldObservation {
    mode: String,
    hold_ns: u64,
    victim_begin_write_dispatch_after_holder_ns: u64,
    victim_dispatched_before_holder_release: bool,
    victim_begin_write_not_returned_while_holder_live: bool,
    victim_begin_write_return_after_release_ns: u64,
    victim_return_within_500_ms: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConstructionReport {
    schema_version: String,
    notice: String,
    lane: String,
    disposition: Option<String>,
    redb_version: String,
    redb_default_features: bool,
    redb_features: Vec<String>,
    cache_bytes: u64,
    table_names: [String; 4],
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
            eprintln!("INVALID_REDB_CONSTRUCTION {error}");
            ExitCode::from(2)
        }
    }
}

fn run_cli() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().as_deref() != Some(std::ffi::OsStr::new("verify-construction")) {
        return Err(usage().into());
    }
    let report = PathBuf::from(args.next().ok_or_else(usage)?);
    let export = PathBuf::from(args.next().ok_or_else(usage)?);
    if args.next().is_some() {
        return Err(usage().into());
    }
    verify(&report, &export)
}

fn usage() -> String {
    "usage: gate verify-construction <report-json-path> <canonical-export-path>".to_owned()
}

fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, Box<dyn Error>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(format!("{} is not a regular non-symlink file", path.display()).into());
    }
    if metadata.len() > maximum {
        return Err(format!("{} exceeds {maximum} bytes", path.display()).into());
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len())?);
    File::open(path)?
        .take(maximum + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() {
        return Err(format!("{} changed while being read", path.display()).into());
    }
    Ok(bytes)
}

fn verify(report_path: &Path, export_path: &Path) -> Result<(), Box<dyn Error>> {
    let report_bytes = read_bounded(report_path, MAX_REPORT_BYTES)?;
    let report: ConstructionReport = serde_json::from_slice(&report_bytes)?;
    verify_report(&report)?;
    let export_bytes = read_bounded(export_path, MAX_EXPORT_BYTES)?;
    let export_sha256 = format!("{:x}", Sha256::digest(&export_bytes));
    if export_bytes.len() != EXPECTED_EXPORT_BYTES || export_sha256 != EXPECTED_EXPORT_SHA256 {
        return Err("canonical export byte length or detached SHA-256 mismatch".into());
    }
    let rows = verify_export(&export_bytes)?;
    println!(
        "VALID_REDB_CONSTRUCTION_NO_DECISION rows={rows} export_bytes={} export_sha256={export_sha256}",
        export_bytes.len()
    );
    Ok(())
}

fn verify_report(report: &ConstructionReport) -> Result<(), Box<dyn Error>> {
    if report.schema_version != "state-backend-redb-construction-observation/v1"
        || report.notice != NOTICE
        || report.lane != LANE
        || report.disposition.is_some()
        || report.redb_version != "4.1.0"
        || report.redb_default_features
        || !report.redb_features.is_empty()
        || report.cache_bytes != 8 * 1024 * 1024 * 1024
        || report.table_names != ["state", "timer", "join_left", "join_right"]
        || report.key_bytes != KEY_BYTES as u64
        || report.value_bytes != VALUE_BYTES as u64
        || report.base_rows_per_table != BASE_ROWS_PER_TABLE
        || report.base_logical_bytes != BASE_ROWS_PER_TABLE * 4 * 1024
        || report.base_build_wall_ns == 0
    {
        return Err("construction report identity/configuration mismatch".into());
    }
    let expected_modes = [
        (
            "I1",
            [
                "set_durability(Immediate)",
                "set_two_phase_commit(false)",
                "set_quick_repair(false)",
            ],
        ),
        (
            "I2",
            [
                "set_durability(Immediate)",
                "set_two_phase_commit(true)",
                "set_quick_repair(false)",
            ],
        ),
        (
            "QR",
            [
                "set_durability(Immediate)",
                "set_two_phase_commit(false)",
                "set_quick_repair(true)",
            ],
        ),
    ];
    if report.modes.len() != expected_modes.len() {
        return Err("construction report mode count mismatch".into());
    }
    for (observed, (mode, setters)) in report.modes.iter().zip(expected_modes) {
        if observed.mode != mode
            || observed.mutations != 128
            || observed.construction_wall_ns == 0
            || observed.setter_trace != setters
        {
            return Err(format!("invalid {mode} observation").into());
        }
    }
    if report.hold.mode != "I1"
        || !report.hold.victim_dispatched_before_holder_release
        || !report
            .hold
            .victim_begin_write_not_returned_while_holder_live
        || !report.hold.victim_return_within_500_ms
        || report.hold.hold_ns < 250_000_000
        || !(40_000_000..=225_000_000)
            .contains(&report.hold.victim_begin_write_dispatch_after_holder_ns)
        || report.hold.victim_begin_write_return_after_release_ns > 500_000_000
    {
        return Err("invalid HOLD observation".into());
    }
    let scope = &report.evidence_scope;
    if scope.qualification_eligible
        || scope.candidate_admission_eligible
        || scope.backend_selection_eligible
        || scope.production_eligible
        || scope.independent_soak_eligible
        || scope.c1_c2_c3_eligible
        || scope.fault_endurance_eligible
        || scope.checkpoint_exactly_once_eligible
        || scope.source_sink_delivery_eligible
    {
        return Err("construction report claims forbidden evidence eligibility".into());
    }
    Ok(())
}

fn verify_export(bytes: &[u8]) -> Result<u64, Box<dyn Error>> {
    let minimum = EXPORT_HEADER.len() + EXPORT_TRAILER.len() + 8 + 4 * 8;
    if bytes.len() < minimum || &bytes[..8] != EXPORT_HEADER {
        return Err("invalid export header or length".into());
    }
    let trailer_offset = bytes
        .len()
        .checked_sub(EXPORT_TRAILER.len() + 8 + 4 * 8)
        .ok_or("export trailer offset underflow")?;
    if &bytes[trailer_offset..trailer_offset + 8] != EXPORT_TRAILER {
        return Err("invalid export trailer".into());
    }
    let record_bytes = 1 + KEY_BYTES + VALUE_BYTES;
    if !(trailer_offset - EXPORT_HEADER.len()).is_multiple_of(record_bytes) {
        return Err("export has truncated or extra record bytes".into());
    }
    let derived_count = (trailer_offset - EXPORT_HEADER.len()) / record_bytes;
    let mut cursor = trailer_offset + 8;
    let total = take_u64(bytes, &mut cursor)?;
    let mut counts = [0_u64; 4];
    for count in &mut counts {
        *count = take_u64(bytes, &mut cursor)?;
    }
    if cursor != bytes.len() || total != u64::try_from(derived_count)? {
        return Err("export total count mismatch".into());
    }

    let mut record_cursor = EXPORT_HEADER.len();
    let mut observed_counts = [0_u64; 4];
    for tag in 0..4_u8 {
        let expected = expected_table(tag);
        for (expected_key, expected_value) in expected {
            if record_cursor + record_bytes > trailer_offset {
                return Err("export is missing expected rows".into());
            }
            let observed_tag = bytes[record_cursor];
            record_cursor += 1;
            let observed_key = &bytes[record_cursor..record_cursor + KEY_BYTES];
            record_cursor += KEY_BYTES;
            let observed_value = &bytes[record_cursor..record_cursor + VALUE_BYTES];
            record_cursor += VALUE_BYTES;
            if observed_tag != tag
                || observed_key != expected_key.as_slice()
                || observed_value != expected_value.as_slice()
            {
                return Err(format!("export row mismatch in table tag {tag}").into());
            }
            observed_counts[tag as usize] += 1;
        }
    }
    if record_cursor != trailer_offset || observed_counts != counts {
        return Err("export contains extra rows or per-table count mismatch".into());
    }
    Ok(total)
}

fn expected_table(tag: u8) -> BTreeMap<[u8; KEY_BYTES], [u8; VALUE_BYTES]> {
    let mut rows = BTreeMap::new();
    for index in 0..BASE_ROWS_PER_TABLE {
        rows.insert(key(tag, 0, index), value(tag, 0, index, 0));
    }
    for step in 1_u8..=3 {
        let overwrite_start = match step {
            1 => 0,
            2 => 16,
            3 => 0,
            _ => unreachable!(),
        };
        let delete_start = match step {
            1 => 48,
            2 => 56,
            3 => 40,
            _ => unreachable!(),
        };
        for index in overwrite_start..overwrite_start + 16 {
            rows.insert(key(tag, 0, index), value(tag, 0, index, step));
        }
        for index in delete_start..delete_start + 8 {
            rows.remove(&key(tag, 0, index));
        }
        for index in 0..8 {
            rows.insert(key(tag, step, index), value(tag, step, index, step));
        }
    }
    rows
}

fn key(tag: u8, domain: u8, index: u64) -> [u8; KEY_BYTES] {
    let mut key = [0_u8; KEY_BYTES];
    key[..4].copy_from_slice(b"LDBR");
    key[4] = tag;
    key[5] = domain;
    key[6..14].copy_from_slice(&index.to_be_bytes());
    for offset in 0..18 {
        key[14 + offset] = tag
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
    for offset in 0..VALUE_BYTES {
        value[offset] = index_bytes[offset % 8]
            .wrapping_add((offset as u8).wrapping_mul(29))
            .wrapping_add(tag.wrapping_mul(31))
            .wrapping_add(domain.wrapping_mul(13))
            .wrapping_add(epoch.wrapping_mul(7));
    }
    value
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, Box<dyn Error>> {
    let end = cursor.checked_add(8).ok_or("cursor overflow")?;
    let slice = bytes.get(*cursor..end).ok_or("truncated u64")?;
    *cursor = end;
    Ok(u64::from_be_bytes(slice.try_into()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_report() -> ConstructionReport {
        let mode = |name: &str, two_phase: bool, quick_repair: bool| ModeObservation {
            mode: name.to_owned(),
            mutations: 128,
            construction_wall_ns: 1,
            setter_trace: [
                "set_durability(Immediate)".to_owned(),
                format!("set_two_phase_commit({two_phase})"),
                format!("set_quick_repair({quick_repair})"),
            ],
        };
        ConstructionReport {
            schema_version: "state-backend-redb-construction-observation/v1".to_owned(),
            notice: NOTICE.to_owned(),
            lane: LANE.to_owned(),
            disposition: None,
            redb_version: "4.1.0".to_owned(),
            redb_default_features: false,
            redb_features: Vec::new(),
            cache_bytes: 8 * 1024 * 1024 * 1024,
            table_names: ["state", "timer", "join_left", "join_right"].map(str::to_owned),
            key_bytes: KEY_BYTES as u64,
            value_bytes: VALUE_BYTES as u64,
            base_rows_per_table: BASE_ROWS_PER_TABLE,
            base_logical_bytes: BASE_ROWS_PER_TABLE * 4 * 1024,
            base_build_wall_ns: 1,
            modes: vec![
                mode("I1", false, false),
                mode("I2", true, false),
                mode("QR", false, true),
            ],
            hold: HoldObservation {
                mode: "I1".to_owned(),
                hold_ns: 250_000_000,
                victim_begin_write_dispatch_after_holder_ns: 50_000_000,
                victim_dispatched_before_holder_release: true,
                victim_begin_write_not_returned_while_holder_live: true,
                victim_begin_write_return_after_release_ns: 1,
                victim_return_within_500_ms: true,
            },
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
        }
    }

    fn canonical_export() -> Vec<u8> {
        let mut bytes = Vec::with_capacity(EXPECTED_EXPORT_BYTES);
        bytes.extend_from_slice(EXPORT_HEADER);
        let mut counts = [0_u64; 4];
        for tag in 0..4_u8 {
            for (key, value) in expected_table(tag) {
                bytes.push(tag);
                bytes.extend_from_slice(&key);
                bytes.extend_from_slice(&value);
                counts[tag as usize] += 1;
            }
        }
        bytes.extend_from_slice(EXPORT_TRAILER);
        bytes.extend_from_slice(&counts.iter().sum::<u64>().to_be_bytes());
        for count in counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        bytes
    }

    #[test]
    fn expected_state_has_balanced_table_counts() {
        let count = expected_table(0).len();
        assert_eq!(count, BASE_ROWS_PER_TABLE as usize);
        for tag in 1..4 {
            assert_eq!(expected_table(tag).len(), count);
        }
    }

    #[test]
    fn report_parser_rejects_unknown_fields() {
        let input = br#"{"schema_version":"x","unknown":true}"#;
        assert!(serde_json::from_slice::<ConstructionReport>(input).is_err());
    }

    #[test]
    fn report_rejects_disposition_and_evidence_claims() {
        let mut report = valid_report();
        verify_report(&report).expect("valid construction report");

        report.disposition = Some("PRESCREEN_PASS".to_owned());
        assert!(verify_report(&report).is_err());
        report.disposition = None;
        report.evidence_scope.backend_selection_eligible = true;
        assert!(verify_report(&report).is_err());
    }

    #[test]
    fn report_rejects_reordered_modes_and_weak_hold() {
        let mut report = valid_report();
        report.modes.swap(0, 1);
        assert!(verify_report(&report).is_err());

        let mut report = valid_report();
        report.hold.victim_dispatched_before_holder_release = false;
        assert!(verify_report(&report).is_err());
    }

    #[test]
    fn independent_schedule_covers_overwrite_delete_and_insert() {
        let rows = expected_table(0);
        assert_eq!(rows.get(&key(0, 0, 0)), Some(&value(0, 0, 0, 3)));
        assert!(!rows.contains_key(&key(0, 0, 40)));
        assert_eq!(rows.get(&key(0, 3, 7)), Some(&value(0, 3, 7, 3)));
    }

    #[test]
    fn detached_canonical_export_identity_matches_independent_model() {
        let bytes = canonical_export();
        assert_eq!(bytes.len(), EXPECTED_EXPORT_BYTES);
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            EXPECTED_EXPORT_SHA256
        );
        assert_eq!(verify_export(&bytes).expect("canonical export"), 65_536);
    }

    #[test]
    fn export_verifier_rejects_row_and_frame_corruption() {
        let mut bytes = canonical_export();
        bytes[EXPORT_HEADER.len()] = 4;
        assert!(verify_export(&bytes).is_err());

        bytes[EXPORT_HEADER.len()] = 0;
        bytes[EXPORT_HEADER.len() + 1] ^= 1;
        assert!(verify_export(&bytes).is_err());

        let mut truncated = canonical_export();
        truncated.pop();
        assert!(verify_export(&truncated).is_err());
    }
}
