# Vendored libxml2 for FpML schema validation

Status: adopted 2026-10-15 for the optional `fpml` feature of `laminar-connectors`. Builds
without that feature neither compile nor link this crate.

## Why

LaminarDB validates FpML documents against a pinned XML Schema closure. libxml2 is the selected
XSD 1.0 validator. The published Rust binding, `libxml` 0.3.22 (KWARC/rust-libxml), was
evaluated and not used:

- its build needs pkg-config plus libclang (bindgen at build time) on Unix and vcpkg on MSVC,
  and links whatever system libxml2 it finds, so the validator version is not reproducible;
- without those tools it falls back to bindings generated for libxml2 older than 2.12, which
  lack `xmlSchemaSetResourceLoader`/`xmlCtxtSetResourceLoader` (2.14+), the only per-context way
  to restrict schema `include`/`import` resolution without mutating the process-global entity
  loader;
- its schema and parser wrappers `panic!` on allocation and NUL-byte errors, and its README
  states it is not thread-safe.

This crate instead builds one pinned libxml2 statically with CMake and exposes only the 23
functions LaminarDB calls, through bindings generated once and committed. The safe wrapper,
thread confinement, and resource policy live in `laminar-connectors/src/fpml/xsd/`.

## Provenance

| Part | Source |
|---|---|
| `libxml2/` | `https://download.gnome.org/sources/libxml2/2.15/libxml2-2.15.4.tar.xz`, SHA-256 `98087fd181d9070724f3fbc65c7377db03038eb92bd882374daff44940138821` (matches the upstream `.sha256sum`), released 2026-09-01 |
| `src/bindings.rs` | `update-bindings.sh` with bindgen-cli 0.72.1 and the `xmlversion.h` configured by `build.rs`, on x86_64 Windows MSVC |
| `build.rs`, `src/lib.rs`, `src/wrapper.h`, `update-bindings.sh` | LaminarDB |

Upstream was actively maintained when adopted (2.15.4 is a security release); recheck the
upstream NEWS and the GNOME security tracker when bumping.

Only what the CMake build reads is kept: the library `.c`/`.h` sources, `include/`,
`codegen/*.inc`, `win32/`, `doc/` (referenced by install rules), `CMakeLists.txt`, the
`*.in` templates, `Copyright`, `NEWS`, `README.md`, and `VERSION`. Removed: `test/`, `result/`,
`dist-doc/`, `fuzz/`, `python/`, `example/`, `m4/`, the `codegen/` generator scripts, the
autotools and Meson build files, and the test and command-line program sources.

## Build configuration

`build.rs` always builds the Release configuration as a static library with only the parser,
tree, pattern, regexp, and XML Schema modules. Compiled out: HTTP (removed upstream in 2.15),
catalogs, XInclude, XPath/XPointer, DTD validation, HTML, Relax-NG, Schematron, iconv/ICU and
ISO-8859-x converters, zlib, dynamic modules, SAX1, push/reader/writer interfaces, and
serialization. Consumers link `bcrypt` on Windows (libxml2 seeds its hash randomization with
`BCryptGenRandom`) and `m`/`pthread` elsewhere.

The committed bindings carry layout assertions; a target with a different ABI fails to build
rather than misreading libxml2 structures. Only 64-bit targets are qualified.

## Local changes

- `libxml2/xmlschemas.c`, `xmlSchemaParseNewDoc`: the temporary parser context that parses an
  included or imported schema document now inherits the caller's resource loader. Upstream
  (2.15.4, and the `2.15` and `master` branches on 2026-10-15) creates it without one, so the
  includes of an included document bypass a custom loader and fall back to the default
  filesystem loader. Guarded by `fpml::xsd::tests::nested_includes_use_the_closure_loader`.
  Report upstream and drop this change once a release contains the fix.

## Updating

1. Download the new release tarball and its `.sha256sum`, verify, and replace `libxml2/` with
   the same pruning. Reapply or drop the local change above.
2. `cargo build` this crate, then run
   `./update-bindings.sh <OUT_DIR>/include/libxml2` with bindgen-cli and libclang.
3. Run `cargo test -p laminar-connectors --features fpml --lib fpml::` (security tests and the
   official FpML corpus).
