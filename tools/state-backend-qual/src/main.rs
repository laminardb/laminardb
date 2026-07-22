#![forbid(unsafe_code)]

use std::io::{Read as _, Write as _};
use std::path::Path;
use std::process::ExitCode;

use state_backend_qual::model_result::validate_model_result;
use state_backend_qual::{validate_profile, MAX_MODEL_RESULT_BYTES, MAX_PROFILE_BYTES, NOTICE};

fn main() -> ExitCode {
    println!("{NOTICE}");
    let _ = std::io::stdout().flush();

    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(command) = arguments.next() else {
        return usage();
    };
    match command.to_str() {
        Some("validate-profile") => {
            let Some(path) = arguments.next() else {
                return usage();
            };
            if arguments.next().is_some() {
                return usage();
            }
            validate_profile_path(Path::new(&path))
        }
        Some("validate-model-result") => {
            let Some(profile_path) = arguments.next() else {
                return usage();
            };
            let Some(result_path) = arguments.next() else {
                return usage();
            };
            if arguments.next().is_some() {
                return usage();
            }
            validate_model_result_paths(Path::new(&profile_path), Path::new(&result_path))
        }
        _ => usage(),
    }
}

fn validate_profile_path(path: &Path) -> ExitCode {
    let bytes = match read_bounded(path, MAX_PROFILE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", path.display());
            return ExitCode::from(2);
        }
    };

    match validate_profile(&bytes) {
        Ok(summary) => {
            println!(
                "VALID_INELIGIBLE_PROFILE schema={} profile={} status={}",
                summary.schema_version, summary.profile_id, summary.status
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("INVALID_PROFILE {error}");
            ExitCode::from(2)
        }
    }
}

fn validate_model_result_paths(profile_path: &Path, result_path: &Path) -> ExitCode {
    let profile_bytes = match read_bounded(profile_path, MAX_PROFILE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", profile_path.display());
            return ExitCode::from(2);
        }
    };
    let result_bytes = match read_bounded(result_path, MAX_MODEL_RESULT_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", result_path.display());
            return ExitCode::from(2);
        }
    };

    match validate_model_result(&profile_bytes, &result_bytes) {
        Ok(summary) => {
            println!(
                "VALID_INELIGIBLE_MODEL_RESULT profile={} requests={}",
                summary.profile_id, summary.requests
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("INVALID_MODEL_RESULT {error}");
            ExitCode::from(2)
        }
    }
}

fn read_bounded(path: &Path, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take((maximum_bytes + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: state-backend-qual validate-profile <profile-json-path>\n       \
         state-backend-qual validate-model-result <profile-json-path> <result-json-path>"
    );
    ExitCode::from(64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_reader_stops_after_the_rejection_sentinel() {
        let path = std::env::temp_dir().join(format!(
            "state-backend-qual-reader-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, vec![b' '; MAX_PROFILE_BYTES + 32]).unwrap();
        let bytes = read_bounded(&path, MAX_PROFILE_BYTES).unwrap();
        std::fs::remove_file(path).unwrap();

        assert_eq!(bytes.len(), MAX_PROFILE_BYTES + 1);
    }
}
