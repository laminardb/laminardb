## What

<!-- Brief description of changes -->

## Why

<!-- Explain the motivation — link to an issue or describe the problem you're solving.
     "Fixes #123" alone is not sufficient; explain WHY the change is needed. -->

## How

<!-- Technical approach taken -->

## Human Review Attestation

<!--
  AI-generated code is welcome, but a human must review and understand every line.
  Filling this section is REQUIRED. PRs without a genuine human attestation will be closed.
-->

- [ ] **I have personally reviewed this entire diff** and understand what it does
- [ ] My review comments (if any) explain *why* I agree or disagree, not just what to change

**Reviewer notes** (required — write 2-3 sentences about what you verified, any concerns, or why this is correct):

<!-- Example: "Verified the new window operator handles late events correctly by tracing
     through the state transitions. The checkpoint serialization matches the existing
     pattern in session_window.rs. No Ring 0 allocations introduced." -->

## Testing

- [ ] Unit tests added/updated
- [ ] Integration tests pass (`cargo test --all`)
- [ ] No clippy warnings (`cargo clippy --all -- -D warnings`)
- [ ] Code is formatted (`cargo fmt`)

## Ring 0 (if applicable)

- [ ] No heap allocations on hot path
- [ ] No locks on hot path
- [ ] Benchmarks run before and after

## Checklist

- [ ] Public APIs are documented
- [ ] Breaking changes documented (if any)
