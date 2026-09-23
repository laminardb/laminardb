# quick-xml 0.39.4 security backport

Maintained compatibility backport, 2026-09-21; review/removal due 2026-10-21.

Base: the crates.io quick-xml 0.39.4 archive, SHA-256
`cdcc8dd4e2f670d309a5f0e83fe36dfdc05af317008fea29144da1a2ac858e5e`.
The original MIT license and upstream sources are retained.

Source changes are the upstream fixes and their regression tests:

- [07f3db8](https://github.com/tafia/quick-xml/commit/07f3db8343cf152f5bc3483ef5b3164582489bea):
  switch large attribute sets to a hash prefilter (RUSTSEC-2026-0194).
- [7ca2526](https://github.com/tafia/quick-xml/commit/7ca25266e94987210daa864889ab15c9332c8a2a):
  bound namespace declarations to 256 per element (RUSTSEC-2026-0195).

Compatibility adjustments: retain the 0.39 imports without the later XmlVersion API;
add the upstream mutable namespace resolver accessor to the 0.39 reader. No XML limit is raised
by LaminarDB. Version 0.39.4 is retained to satisfy object_store 0.13/OpenDAL 0.57.
Local regression tests additionally exercise the default cap through both reader APIs and
Serde response deserialization at 256 and 257 namespace declarations.

`tools/check_dependency_exceptions.py` verifies the complete source tree (LF-normalized,
excluding the standalone test Cargo.lock and build target directories). The root lockfile
and patch path are separately checked. Run the upstream unit suites with:

```sh
cargo test --manifest-path vendor/quick-xml/Cargo.toml --all-features --lib
```

The root Cargo patch applies to workspace builds. Cargo does not propagate it to consumers
of published LaminarDB crates; see `security/dependency-exceptions.md` for that scope.
