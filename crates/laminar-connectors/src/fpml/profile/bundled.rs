//! Profile files shipped in the binary.

/// Bundled profiles: `(name, root file)`.
pub const BUNDLED_PROFILES: &[(&str, &str)] = &[
    (
        "fpml-5-13-confirmation-trade",
        "fpml-5-13/confirmation/trade.toml",
    ),
    (
        "fpml-5-13-confirmation-rates",
        "fpml-5-13/confirmation/trade-rates.toml",
    ),
    (
        "fpml-5-13-confirmation-fx",
        "fpml-5-13/confirmation/trade-fx.toml",
    ),
    (
        "fpml-5-13-confirmation-commodity",
        "fpml-5-13/confirmation/trade-commodity.toml",
    ),
    (
        "fpml-5-13-confirmation-credit",
        "fpml-5-13/confirmation/trade-credit.toml",
    ),
    (
        "fpml-5-13-confirmation-equity-options",
        "fpml-5-13/confirmation/trade-equity-options.toml",
    ),
    (
        "fpml-5-13-confirmation-equity-swaps",
        "fpml-5-13/confirmation/trade-equity-swaps.toml",
    ),
    (
        "fpml-5-13-confirmation-equity-volatility",
        "fpml-5-13/confirmation/trade-equity-volatility.toml",
    ),
    (
        "fpml-5-13-confirmation-securities",
        "fpml-5-13/confirmation/trade-securities.toml",
    ),
    (
        "fpml-5-13-confirmation-cross-asset",
        "fpml-5-13/confirmation/trade-cross-asset.toml",
    ),
];

macro_rules! profile_files {
    ($($path:literal),* $(,)?) => {
        &[$(($path, include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/fpml/profiles/", $path)))),*]
    };
}

/// Every bundled profile file, by path relative to `fpml/profiles/`.
const FILES: &[(&str, &str)] = profile_files!(
    "fpml-5-13/confirmation/assets.toml",
    "fpml-5-13/confirmation/common.toml",
    "fpml-5-13/confirmation/options.toml",
    "fpml-5-13/confirmation/products/commodity.toml",
    "fpml-5-13/confirmation/products/credit.toml",
    "fpml-5-13/confirmation/products/cross-asset.toml",
    "fpml-5-13/confirmation/products/equity.toml",
    "fpml-5-13/confirmation/products/fx.toml",
    "fpml-5-13/confirmation/products/rates.toml",
    "fpml-5-13/confirmation/products/securities.toml",
    "fpml-5-13/confirmation/trade-columns.toml",
    "fpml-5-13/confirmation/trade-commodity.toml",
    "fpml-5-13/confirmation/trade-credit.toml",
    "fpml-5-13/confirmation/trade-cross-asset.toml",
    "fpml-5-13/confirmation/trade-equity-options.toml",
    "fpml-5-13/confirmation/trade-equity-swaps.toml",
    "fpml-5-13/confirmation/trade-equity-volatility.toml",
    "fpml-5-13/confirmation/trade-fx.toml",
    "fpml-5-13/confirmation/trade-rates.toml",
    "fpml-5-13/confirmation/trade-securities.toml",
    "fpml-5-13/confirmation/trade.toml",
);

pub(super) fn root(name: &str) -> Option<&'static str> {
    BUNDLED_PROFILES
        .iter()
        .find(|(bundled, _)| *bundled == name)
        .map(|(_, path)| *path)
}

pub(super) fn file(path: &str) -> Option<&'static str> {
    FILES
        .iter()
        .find(|(bundled, _)| *bundled == path)
        .map(|(_, content)| *content)
}

/// Resolve `include` relative to the bundled file `from`, without leaving the bundle.
pub(super) fn join(from: &str, include: &str) -> Result<String, String> {
    let mut parts: Vec<&str> = from.split('/').collect();
    parts.pop();
    for part in include.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts
                    .pop()
                    .ok_or_else(|| format!("include '{include}' leaves the bundled profiles"))?;
            }
            part => parts.push(part),
        }
    }
    Ok(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every profile file shipped in the source tree is embedded, and nothing else.
    #[test]
    fn every_profile_file_is_bundled() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fpml/profiles");
        let mut on_disk = Vec::new();
        let mut pending = vec![root.clone()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "toml")
                {
                    let relative = path.strip_prefix(&root).unwrap();
                    on_disk.push(relative.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        on_disk.sort();
        let mut bundled: Vec<String> = FILES.iter().map(|(path, _)| (*path).to_owned()).collect();
        bundled.sort();
        assert_eq!(on_disk, bundled);
        for (_, root) in BUNDLED_PROFILES {
            assert!(file(root).is_some(), "{root}");
        }
    }
}
