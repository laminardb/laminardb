//! Raw bindings to the vendored libxml2 2.15.4 build: the parser, tree, and XML Schema validator
//! entry points LaminarDB uses, and nothing else. All safety obligations belong to the caller.
//!
//! The bindings are generated once by `update-bindings.sh` against this exact source and its
//! configured `xmlversion.h`; the layout assertions fail the build on a target whose ABI differs.

#![no_std]
#![allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    clippy::all,
    clippy::pedantic
)]

include!("bindings.rs");
