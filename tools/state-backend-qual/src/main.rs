#![forbid(unsafe_code)]

use std::io::{Read as _, Write as _};
use std::path::Path;
use std::process::ExitCode;

use state_backend_qual::{validate_profile, MAX_PROFILE_BYTES, NOTICE};

fn main() -> ExitCode {
    println!("{NOTICE}");
    let _ = std::io::stdout().flush();

    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(command) = arguments.next() else {
        return usage();
    };
    let Some(path) = arguments.next() else {
        return usage();
    };
    if arguments.next().is_some() || command != "validate-profile" {
        return usage();
    }

    let path = Path::new(&path);
    let bytes = match read_bounded(path) {
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

fn read_bounded(path: &Path) -> std::io::Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take((MAX_PROFILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn usage() -> ExitCode {
    eprintln!("usage: state-backend-qual validate-profile <json-path>");
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
        let bytes = read_bounded(&path).unwrap();
        std::fs::remove_file(path).unwrap();

        assert_eq!(bytes.len(), MAX_PROFILE_BYTES + 1);
    }
}
