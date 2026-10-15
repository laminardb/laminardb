//! Builds the vendored libxml2 as a static library with only the parser, tree, and XML Schema
//! 1.0 validator. Network, catalog, XInclude, DTD validation, encoding converters, and dynamic
//! modules are compiled out, so no build of this crate can fetch or dlopen anything.

use std::env;
use std::path::PathBuf;

const OFF: &[&str] = &[
    "BUILD_SHARED_LIBS",
    "LIBXML2_WITH_C14N",
    "LIBXML2_WITH_CATALOG",
    "LIBXML2_WITH_DEBUG",
    "LIBXML2_WITH_DOCS",
    "LIBXML2_WITH_HTML",
    "LIBXML2_WITH_HTTP",
    "LIBXML2_WITH_ICONV",
    "LIBXML2_WITH_ICU",
    "LIBXML2_WITH_ISO8859X",
    "LIBXML2_WITH_LEGACY",
    "LIBXML2_WITH_MODULES",
    "LIBXML2_WITH_OUTPUT",
    "LIBXML2_WITH_PROGRAMS",
    "LIBXML2_WITH_PUSH",
    "LIBXML2_WITH_PYTHON",
    "LIBXML2_WITH_READER",
    "LIBXML2_WITH_READLINE",
    "LIBXML2_WITH_RELAXNG",
    "LIBXML2_WITH_SAX1",
    "LIBXML2_WITH_SCHEMATRON",
    "LIBXML2_WITH_TESTS",
    "LIBXML2_WITH_THREAD_ALLOC",
    "LIBXML2_WITH_TLS",
    "LIBXML2_WITH_VALID",
    "LIBXML2_WITH_WRITER",
    "LIBXML2_WITH_XINCLUDE",
    "LIBXML2_WITH_XPATH",
    "LIBXML2_WITH_XPTR",
    "LIBXML2_WITH_ZLIB",
];

const ON: &[&str] = &[
    "LIBXML2_WITH_PATTERN",
    "LIBXML2_WITH_REGEXPS",
    "LIBXML2_WITH_SCHEMAS",
    "LIBXML2_WITH_THREADS",
];

fn main() {
    println!("cargo:rerun-if-changed=libxml2");
    let source = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"))
        .join("libxml2");

    let mut config = cmake::Config::new(&source);
    // One optimized C build for every Rust profile keeps the library name and CRT stable.
    config.profile("Release");
    config.define("CMAKE_INSTALL_LIBDIR", "lib");
    for option in OFF {
        config.define(option, "OFF");
    }
    for option in ON {
        config.define(option, "ON");
    }
    let destination = config.build();

    println!(
        "cargo:rustc-link-search=native={}",
        destination.join("lib").display()
    );
    let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_env == "msvc" {
        println!("cargo:rustc-link-lib=static=libxml2s");
    } else {
        println!("cargo:rustc-link-lib=static=xml2");
    }
    match target_os.as_str() {
        "windows" => println!("cargo:rustc-link-lib=bcrypt"),
        "macos" | "ios" => {}
        _ => {
            println!("cargo:rustc-link-lib=m");
            println!("cargo:rustc-link-lib=pthread");
        }
    }
    println!(
        "cargo:include={}",
        destination.join("include").join("libxml2").display()
    );
}
