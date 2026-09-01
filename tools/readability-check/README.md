# Readability check

This syntax-aware check prevents production Rust modules over 800 logical lines and non-test
functions over 120 logical lines from appearing or growing silently. Existing module exceptions
are listed in `baseline.tsv`; reviewed function exceptions are listed in
`function-baseline.tsv`. Each has a rationale and follow-up action. Limits may shrink, but not grow.

Adding or growing an exception fails the check. When an exception is reduced below its review
threshold, its stale baseline entry must be removed.

Run from the repository root:

```text
cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .
```
