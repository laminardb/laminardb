use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::Span;
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, ImplItemFn, ItemFn, TraitItemFn};

const MODULE_REVIEW_LIMIT: usize = 800;
const FUNCTION_JUSTIFICATION_LIMIT: usize = 120;

#[derive(Debug)]
struct BaselineEntry {
    production_lines: usize,
    max_function_lines: usize,
    owner: String,
    reason: String,
    follow_up: String,
}

#[derive(Debug)]
struct FunctionBaselineEntry {
    max_lines: usize,
    category: String,
    reason: String,
    follow_up: String,
}

#[derive(Debug)]
struct ModuleMetrics {
    production_lines: usize,
    max_function_lines: usize,
    oversized_functions: usize,
    functions: BTreeMap<String, Vec<usize>>,
}

#[derive(Default)]
struct MetricsVisitor<'a> {
    source_lines: &'a [&'a str],
    test_ranges: Vec<(usize, usize)>,
    functions: Vec<FunctionMetrics>,
    test_depth: usize,
}

#[derive(Debug)]
struct FunctionMetrics {
    name: String,
    logical_lines: usize,
    is_test: bool,
}

impl<'a> MetricsVisitor<'a> {
    fn new(source_lines: &'a [&'a str], path_is_test: bool) -> Self {
        Self {
            source_lines,
            test_depth: usize::from(path_is_test),
            ..Self::default()
        }
    }

    fn record_function(&mut self, name: &str, span: Span, attrs: &[Attribute]) {
        let start = span.start().line.max(1);
        let end = span.end().line.max(start);
        self.functions.push(FunctionMetrics {
            name: name.to_owned(),
            logical_lines: logical_lines(self.source_lines, start, end),
            is_test: self.test_depth > 0 || has_attr(attrs, "test"),
        });
    }
}

impl<'ast> Visit<'ast> for MetricsVisitor<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        let is_test = has_cfg_test(&node.attrs);
        if is_test {
            let span = node.span();
            self.test_ranges
                .push((span.start().line.max(1), span.end().line.max(1)));
            self.test_depth += 1;
        }
        visit::visit_item_mod(self, node);
        if is_test {
            self.test_depth -= 1;
        }
    }

    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        let is_test_item = has_cfg_test(&node.attrs);
        if is_test_item {
            let span = node.span();
            self.test_ranges
                .push((span.start().line.max(1), span.end().line.max(1)));
            self.test_depth += 1;
        }
        self.record_function(&node.sig.ident.to_string(), node.span(), &node.attrs);
        visit::visit_block(self, &node.block);
        if is_test_item {
            self.test_depth -= 1;
        }
    }

    fn visit_impl_item_fn(&mut self, node: &'ast ImplItemFn) {
        self.record_function(&node.sig.ident.to_string(), node.span(), &node.attrs);
        visit::visit_block(self, &node.block);
    }

    fn visit_trait_item_fn(&mut self, node: &'ast TraitItemFn) {
        if let Some(block) = &node.default {
            self.record_function(&node.sig.ident.to_string(), node.span(), &node.attrs);
            visit::visit_block(self, block);
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("readability check failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let root = PathBuf::from(env::args().nth(1).unwrap_or_else(|| ".".to_owned()));
    let baseline_path = root.join("tools/readability-check/baseline.tsv");
    let baseline = read_baseline(&baseline_path)?;
    let function_baseline_path = root.join("tools/readability-check/function-baseline.tsv");
    let function_baseline = read_function_baseline(&function_baseline_path)?;
    let mut rust_files = Vec::new();
    collect_rust_files(&root, &mut rust_files).map_err(|error| error.to_string())?;
    rust_files.sort();

    let mut measured = BTreeMap::new();
    let mut violations = Vec::new();
    let mut oversized_function_count = 0;
    for path in rust_files {
        let relative = normalized_relative(&root, &path);
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("cannot read {relative}: {error}"))?;
        let metrics = measure_module(&relative, &source)?;
        oversized_function_count += metrics.oversized_functions;
        if metrics.production_lines > MODULE_REVIEW_LIMIT && !baseline.contains_key(&relative) {
            violations.push(format!(
                "{relative} has {} production lines and no review-baseline entry",
                metrics.production_lines
            ));
        }
        for (name, lengths) in &metrics.functions {
            let oversized = lengths
                .iter()
                .filter(|lines| **lines > FUNCTION_JUSTIFICATION_LIMIT)
                .count();
            if oversized > 1 {
                violations.push(format!(
                    "{relative} contains {oversized} oversized functions named {name}; names must be unique for no-growth enforcement"
                ));
            }
            if oversized == 1 && !function_baseline.contains_key(&(relative.clone(), name.clone()))
            {
                violations.push(format!(
                    "{relative}::{name} exceeds {FUNCTION_JUSTIFICATION_LIMIT} logical lines without a function-baseline entry"
                ));
            }
        }
        measured.insert(relative, metrics);
    }

    for (path, entry) in &baseline {
        let Some(current) = measured.get(path) else {
            violations.push(format!(
                "baseline entry {path} no longer exists; remove the resolved exception"
            ));
            continue;
        };
        if current.production_lines > entry.production_lines {
            violations.push(format!(
                "{path} grew from {} to {} production lines (owner: {})",
                entry.production_lines, current.production_lines, entry.owner
            ));
        }
        if current.max_function_lines > entry.max_function_lines {
            violations.push(format!(
                "{path} maximum function grew from {} to {} logical lines (owner: {})",
                entry.max_function_lines, current.max_function_lines, entry.owner
            ));
        }
        if entry.reason.is_empty() || entry.follow_up.is_empty() {
            violations.push(format!("baseline entry {path} lacks a reason or follow-up"));
        }
    }

    for ((path, name), entry) in &function_baseline {
        let Some(module) = measured.get(path) else {
            violations.push(format!(
                "function baseline entry {path}::{name} no longer exists; remove the resolved exception"
            ));
            continue;
        };
        let Some(current_lines) = module
            .functions
            .get(name)
            .and_then(|lengths| lengths.iter().max())
        else {
            violations.push(format!(
                "function baseline entry {path}::{name} no longer exists; remove the resolved exception"
            ));
            continue;
        };
        if *current_lines > entry.max_lines {
            violations.push(format!(
                "{path}::{name} grew from {} to {current_lines} logical lines ({})",
                entry.max_lines, entry.category
            ));
        }
        if *current_lines <= FUNCTION_JUSTIFICATION_LIMIT {
            violations.push(format!(
                "{path}::{name} is now {current_lines} logical lines; remove the resolved function exception"
            ));
        }
        if entry.category.is_empty() || entry.reason.is_empty() || entry.follow_up.is_empty() {
            violations.push(format!(
                "function baseline entry {path}::{name} lacks a category, reason, or follow-up"
            ));
        }
    }

    if violations.is_empty() {
        println!(
            "readability check passed: {} reviewed module exceptions; {} reviewed function exceptions over {} lines",
            baseline.len(),
            function_baseline.len(),
            FUNCTION_JUSTIFICATION_LIMIT
        );
        debug_assert_eq!(oversized_function_count, function_baseline.len());
        return Ok(());
    }

    Err(violations.join("\n"))
}

fn read_function_baseline(
    path: &Path,
) -> Result<BTreeMap<(String, String), FunctionBaselineEntry>, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut entries = BTreeMap::new();
    for (index, line) in text.lines().enumerate().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 6 {
            return Err(format!(
                "{}:{} must contain six tab-separated fields",
                path.display(),
                index + 1
            ));
        }
        let key = (fields[0].to_owned(), fields[1].to_owned());
        let entry = FunctionBaselineEntry {
            max_lines: parse_limit(path, index, fields[2])?,
            category: fields[3].to_owned(),
            reason: fields[4].to_owned(),
            follow_up: fields[5].to_owned(),
        };
        if entries.insert(key.clone(), entry).is_some() {
            return Err(format!("duplicate function baseline {}::{}", key.0, key.1));
        }
    }
    Ok(entries)
}

fn read_baseline(path: &Path) -> Result<BTreeMap<String, BaselineEntry>, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut entries = BTreeMap::new();
    for (index, line) in text.lines().enumerate().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 6 {
            return Err(format!(
                "{}:{} must contain six tab-separated fields",
                path.display(),
                index + 1
            ));
        }
        let entry = BaselineEntry {
            production_lines: parse_limit(path, index, fields[1])?,
            max_function_lines: parse_limit(path, index, fields[2])?,
            owner: fields[3].to_owned(),
            reason: fields[4].to_owned(),
            follow_up: fields[5].to_owned(),
        };
        if entries.insert(fields[0].to_owned(), entry).is_some() {
            return Err(format!("duplicate baseline path {}", fields[0]));
        }
    }
    Ok(entries)
}

fn parse_limit(path: &Path, index: usize, value: &str) -> Result<usize, String> {
    value.parse().map_err(|error| {
        format!(
            "{}:{} has invalid numeric limit {value:?}: {error}",
            path.display(),
            index + 1
        )
    })
}

fn measure_module(path: &str, source: &str) -> Result<ModuleMetrics, String> {
    let parsed =
        syn::parse_file(source).map_err(|error| format!("cannot parse {path}: {error}"))?;
    let source_lines = source.lines().collect::<Vec<_>>();
    let path_is_test = is_test_path(path);
    let mut visitor = MetricsVisitor::new(&source_lines, path_is_test);
    visitor.visit_file(&parsed);
    let logical = logical_lines(&source_lines, 1, source_lines.len().max(1));
    let test_lines = if path_is_test {
        logical
    } else {
        logical_lines_in_ranges(&source_lines, &visitor.test_ranges)
    };
    let production_lines = logical.saturating_sub(test_lines);
    let production_functions = visitor
        .functions
        .into_iter()
        .filter(|function| !function.is_test)
        .collect::<Vec<_>>();
    let max_function_lines = production_functions
        .iter()
        .map(|function| function.logical_lines)
        .max()
        .unwrap_or(0);
    let oversized_functions = production_functions
        .iter()
        .filter(|function| function.logical_lines > FUNCTION_JUSTIFICATION_LIMIT)
        .count();
    let mut functions = BTreeMap::<String, Vec<usize>>::new();
    for function in production_functions {
        functions
            .entry(function.name)
            .or_default()
            .push(function.logical_lines);
    }
    Ok(ModuleMetrics {
        production_lines,
        max_function_lines,
        oversized_functions,
        functions,
    })
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if matches!(
                name,
                "target" | "vendor" | ".git" | "node_modules" | "private" | ".claude"
            ) {
                continue;
            }
            collect_rust_files(&path, files)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
    Ok(())
}

fn normalized_relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn has_attr(attrs: &[Attribute], expected: &str) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident(expected))
}

fn has_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("test")
            || (attr.path().is_ident("cfg")
                && attr.meta.to_token_stream().to_string().contains("test"))
    })
}

fn logical_lines(lines: &[&str], start: usize, end: usize) -> usize {
    let start = start.saturating_sub(1).min(lines.len());
    let end = end.min(lines.len());
    let mut in_block_comment = false;
    lines[start..end]
        .iter()
        .filter(|line| {
            let trimmed = line.trim();
            if in_block_comment {
                if trimmed.contains("*/") {
                    in_block_comment = false;
                }
                return false;
            }
            if trimmed.starts_with("/*") {
                in_block_comment = !trimmed.contains("*/");
                return false;
            }
            !trimmed.is_empty() && !trimmed.starts_with("//")
        })
        .count()
}

fn logical_lines_in_ranges(lines: &[&str], ranges: &[(usize, usize)]) -> usize {
    let mut covered = BTreeSet::new();
    for (start, end) in ranges {
        for line in *start..=*end {
            covered.insert(line);
        }
    }
    covered
        .into_iter()
        .filter(|line| logical_lines(lines, *line, *line) == 1)
        .count()
}

fn is_test_path(path: &str) -> bool {
    path.contains("/tests/")
        || path.ends_with("/tests.rs")
        || path.contains("_test.rs")
        || path.contains("_tests.rs")
        || path.contains("/benches/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excludes_cfg_test_modules_from_production_lines() {
        let source = r#"
fn production() {}

#[cfg(test)]
mod tests {
    #[test]
    fn test_only() {}
}
"#;
        let metrics = measure_module("crates/example/src/lib.rs", source).unwrap();
        assert_eq!(metrics.production_lines, 1);
        assert_eq!(metrics.max_function_lines, 1);
        assert_eq!(metrics.functions["production"], [1]);
    }

    #[test]
    fn standalone_test_files_have_no_production_lines() {
        let source = "fn long_test_helper() { let value = 1; assert_eq!(value, 1); }";
        let metrics = measure_module("crates/example/src/tests.rs", source).unwrap();
        assert_eq!(metrics.production_lines, 0);
        assert_eq!(metrics.max_function_lines, 0);
        assert!(metrics.functions.is_empty());
    }
}
