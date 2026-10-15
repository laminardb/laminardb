//! libxml2 XML Schema validation, confined to the thread that compiled the schema.
//!
//! Schema compilation resolves every `xsd:include`/`xsd:import` through a loader that serves
//! only the approved in-memory closure; any other URL is refused, recorded, and fails
//! compilation. Instance documents are parsed with a loader that refuses every resource,
//! without DTD loading, entity substitution, XInclude, or catalogs (network support is not
//! compiled into the vendored library). Validation always uses the precompiled schema, so
//! `xsi:schemaLocation` hints are never followed.
//!
//! libxml2 objects are not shared across threads: [`Validator`] is `!Send`, and the decoder
//! creates it on the worker thread that uses it. A validation that has started runs to
//! completion; input bounds, not cancellation, bound its work.

mod tree;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::marker::PhantomData;
use std::ptr;
use std::sync::{Arc, Once};

use libxml2_ffi as ffi;

use super::document::{Document, Interner};
pub(crate) use tree::TreeNames;
use super::error::{Rejection, Stage};
use super::limits::Limits;

/// Scheme of the synthetic base URL under which closure files are served. Relative
/// `schemaLocation` values resolve against it; nothing else can be loaded.
const RESOURCE_BASE: &str = "laminardb-schema:/";
/// URL given to instance documents; relative references resolve to nothing loadable.
const DOCUMENT_URL: &CStr = c"laminardb-document";
/// Diagnostics kept per operation; the rest are only counted.
const MAX_DIAGNOSTICS: usize = 8;
/// Refused resource URLs kept for the error report.
const MAX_REFUSALS: usize = 8;
const MAX_MESSAGE: usize = 240;
#[allow(clippy::cast_possible_wrap)] // 21504 fits; the binding types the macro as u32.
const LIBXML_VERSION: i32 = ffi::LIBXML_VERSION as i32;

/// Parser options for instance documents. Deliberately absent: `NOENT` (entity substitution),
/// `DTDLOAD`/`DTDATTR`/`DTDVALID`, `XINCLUDE`, `HUGE` (lifting libxml2's own limits), and
/// `RECOVER`.
const INSTANCE_OPTIONS: i32 =
    ffi::XML_PARSE_NONET | ffi::XML_PARSE_NO_XXE | ffi::XML_PARSE_NO_SYS_CATALOG;

/// The approved schema closure: file name to bytes, plus the entry point.
#[derive(Debug, Clone)]
pub(crate) struct SchemaFiles {
    pub(crate) entry: String,
    pub(crate) files: BTreeMap<String, Arc<[u8]>>,
}

/// One libxml2 diagnostic, copied out of the callback and redacted.
struct Diagnostic {
    line: u32,
    message: String,
}

#[derive(Default)]
struct ErrorSink {
    diagnostics: Vec<Diagnostic>,
    errors: usize,
}

impl ErrorSink {
    fn clear(&mut self) {
        self.diagnostics.clear();
        self.errors = 0;
    }

    fn report(&self) -> String {
        let mut report = self
            .diagnostics
            .iter()
            .map(|diagnostic| format!("line {}: {}", diagnostic.line, diagnostic.message))
            .collect::<Vec<_>>()
            .join("; ");
        if self.errors > self.diagnostics.len() {
            report.push_str(&format!(
                " (+{} more)",
                self.errors - self.diagnostics.len()
            ));
        }
        report
    }
}

/// Resource requests seen by a loader; `files` is `None` for a loader that refuses everything.
struct LoaderState<'a> {
    files: Option<&'a SchemaFiles>,
    refused: Vec<String>,
    attempts: usize,
}

/// Every resource URL libxml2 asked for and that was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusals {
    pub(crate) attempts: usize,
    pub(crate) urls: Vec<String>,
}

/// Why the closure could not be compiled.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompileError {
    #[error("schema closure entry '{0}' is not a closure file")]
    MissingEntry(String),
    #[error("schema closure requested resources outside the approved closure: {0:?}")]
    Escape(Vec<String>),
    #[error("schema closure failed to compile: {0}")]
    Invalid(String),
    #[error("libxml2 could not allocate a {0}")]
    Allocation(&'static str),
}

fn init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // SAFETY: xmlCheckVersion aborts on an ABI mismatch between these bindings and the linked
        // library, which are built from the same vendored source; xmlInitParser is idempotent and
        // runs before any other libxml2 call made by this module.
        unsafe {
            ffi::xmlCheckVersion(LIBXML_VERSION);
            ffi::xmlInitParser();
        }
    });
}

/// A compiled schema plus a reusable validation context. `!Send` and `!Sync`.
pub(crate) struct Validator {
    schema: *mut ffi::xmlSchema,
    valid: *mut ffi::xmlSchemaValidCtxt,
    /// Boxed so the address registered with libxml2 stays stable.
    sink: Box<ErrorSink>,
    _thread_confined: PhantomData<*mut ()>,
}

impl Validator {
    /// Compile `files` with resource loading restricted to the closure.
    pub(crate) fn compile(files: &SchemaFiles) -> Result<Self, CompileError> {
        init();
        if !files.files.contains_key(&files.entry) {
            return Err(CompileError::MissingEntry(files.entry.clone()));
        }
        let url = CString::new(format!("{RESOURCE_BASE}{}", files.entry))
            .map_err(|_| CompileError::MissingEntry(files.entry.clone()))?;
        let mut loader = LoaderState {
            files: Some(files),
            refused: Vec::new(),
            attempts: 0,
        };
        let mut sink = Box::<ErrorSink>::default();
        // SAFETY: `url` outlives the call; libxml2 copies it into the new context.
        let parser = unsafe { ffi::xmlSchemaNewParserCtxt(url.as_ptr()) };
        if parser.is_null() {
            return Err(CompileError::Allocation("schema parser context"));
        }
        // SAFETY: `parser` is a live context owned here. `loader` and `sink` outlive every
        // callback, which libxml2 only invokes during xmlSchemaParse below; the context is freed
        // before either is dropped.
        let schema = unsafe {
            ffi::xmlSchemaSetResourceLoader(
                parser,
                Some(load_from_closure),
                ptr::from_mut(&mut loader).cast::<c_void>(),
            );
            ffi::xmlSchemaSetParserStructuredErrors(
                parser,
                Some(record_error),
                ptr::from_mut(sink.as_mut()).cast::<c_void>(),
            );
            let schema = ffi::xmlSchemaParse(parser);
            ffi::xmlSchemaFreeParserCtxt(parser);
            schema
        };
        if !loader.refused.is_empty() {
            free_schema(schema);
            return Err(CompileError::Escape(loader.refused));
        }
        if schema.is_null() || sink.errors > 0 {
            free_schema(schema);
            return Err(CompileError::Invalid(sink.report()));
        }
        // SAFETY: `schema` is the live schema compiled above; the validation context borrows it
        // and is freed before it in Drop.
        let valid = unsafe { ffi::xmlSchemaNewValidCtxt(schema) };
        if valid.is_null() {
            free_schema(schema);
            return Err(CompileError::Allocation("schema validation context"));
        }
        sink.clear();
        // SAFETY: `valid` is live; `sink` is boxed and owned by the returned Validator, which
        // frees the context before dropping the sink.
        unsafe {
            // Materialize schema default attributes so the published tree is the validated one.
            ffi::xmlSchemaSetValidOptions(valid, ffi::XML_SCHEMA_VAL_VC_I_CREATE);
            ffi::xmlSchemaSetValidStructuredErrors(
                valid,
                Some(record_error),
                ptr::from_mut(sink.as_mut()).cast::<c_void>(),
            );
        }
        Ok(Self {
            schema,
            valid,
            sink,
            _thread_confined: PhantomData,
        })
    }

    /// Parse and validate one complete document and copy the accepted tree out of libxml2.
    ///
    /// The caller has already pre-scanned `bytes` (encoding, DTD, and structural limits).
    pub(crate) fn validate(
        &mut self,
        bytes: &[u8],
        limits: &Limits,
        interner: &mut Interner,
        names: &TreeNames,
    ) -> Result<(Document, Refusals), Rejection> {
        let size = i32::try_from(bytes.len())
            .map_err(|_| Rejection::new(Stage::Prescan, "document exceeds the parser size"))?;
        self.sink.clear();
        let mut loader = LoaderState {
            files: None,
            refused: Vec::new(),
            attempts: 0,
        };
        // SAFETY: creates a fresh parser context owned by `ParserGuard`.
        let parser = ParserGuard(unsafe { ffi::xmlNewParserCtxt() });
        if parser.0.is_null() {
            return Err(Rejection::new(Stage::Worker, "libxml2 parser allocation failed"));
        }
        // SAFETY: `parser` is live until the guard drops at the end of this function; `loader`
        // and the boxed sink outlive it. `bytes` outlives the read, which copies nothing it
        // keeps. The returned document is owned by `DocumentGuard`.
        let document = DocumentGuard(unsafe {
            ffi::xmlCtxtSetResourceLoader(
                parser.0,
                Some(refuse_all),
                ptr::from_mut(&mut loader).cast::<c_void>(),
            );
            ffi::xmlCtxtSetErrorHandler(
                parser.0,
                Some(record_error),
                ptr::from_mut(self.sink.as_mut()).cast::<c_void>(),
            );
            ffi::xmlCtxtReadMemory(
                parser.0,
                bytes.as_ptr().cast::<c_char>(),
                size,
                DOCUMENT_URL.as_ptr(),
                ptr::null(),
                INSTANCE_OPTIONS,
            )
        });
        if document.0.is_null() || self.sink.errors > 0 {
            return Err(Rejection::new(Stage::Parse, self.sink.report()));
        }
        // SAFETY: `document.0` is a live document returned by the parser above.
        if unsafe { !(*document.0).intSubset.is_null() || !(*document.0).extSubset.is_null() } {
            return Err(Rejection::new(Stage::Parse, "DTDs are not accepted"));
        }
        // SAFETY: `self.valid` borrows the live compiled schema; `document.0` is live and may be
        // modified (default attributes are added) by validation.
        let verdict = unsafe { ffi::xmlSchemaValidateDoc(self.valid, document.0) };
        if verdict < 0 {
            return Err(Rejection::new(
                Stage::Worker,
                "libxml2 reported an internal validation error",
            ));
        }
        if verdict > 0 || self.sink.errors > 0 {
            return Err(Rejection::new(Stage::Schema, self.sink.report()));
        }
        // SAFETY: the document is live, validated, and not modified while it is copied.
        let tree = unsafe { tree::copy(document.0, limits, interner, names) }?;
        Ok((
            tree,
            Refusals {
                attempts: loader.attempts,
                urls: loader.refused,
            },
        ))
    }
}

impl Drop for Validator {
    fn drop(&mut self) {
        // SAFETY: both pointers were created in `compile`, are freed exactly once here, and the
        // validation context is freed before the schema it references.
        unsafe {
            ffi::xmlSchemaFreeValidCtxt(self.valid);
            ffi::xmlSchemaFree(self.schema);
        }
    }
}

fn free_schema(schema: *mut ffi::xmlSchema) {
    if !schema.is_null() {
        // SAFETY: a schema returned by xmlSchemaParse and not yet freed.
        unsafe { ffi::xmlSchemaFree(schema) };
    }
}

struct ParserGuard(*mut ffi::xmlParserCtxt);

impl Drop for ParserGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: a parser context created by xmlNewParserCtxt and freed only here.
            unsafe { ffi::xmlFreeParserCtxt(self.0) };
        }
    }
}

struct DocumentGuard(*mut ffi::xmlDoc);

impl Drop for DocumentGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: a document returned by xmlCtxtReadMemory and freed only here.
            unsafe { ffi::xmlFreeDoc(self.0) };
        }
    }
}

/// Serve a closure file, or refuse and record the URL.
///
/// Must not unwind: a Rust panic cannot cross this C callback (it aborts the process).
unsafe extern "C" fn load_from_closure(
    data: *mut c_void,
    url: *const c_char,
    _public_id: *const c_char,
    _kind: ffi::xmlResourceType,
    _flags: ffi::xmlParserInputFlags,
    out: *mut *mut ffi::xmlParserInput,
) -> ffi::xmlParserErrors {
    // SAFETY: libxml2 passes back the LoaderState registered by `compile`, which is alive and
    // not otherwise borrowed while xmlSchemaParse runs.
    let state = unsafe { &mut *data.cast::<LoaderState<'_>>() };
    state.attempts += 1;
    if url.is_null() || out.is_null() {
        return ffi::XML_IO_EACCES;
    }
    // SAFETY: libxml2 passes a NUL-terminated URL valid for the duration of the call.
    let requested = unsafe { CStr::from_ptr(url) };
    let file = state.files.and_then(|files| {
        let name = requested.to_str().ok()?.strip_prefix(RESOURCE_BASE)?;
        files.files.get(name)
    });
    let Some(bytes) = file else {
        refuse(state, requested);
        return ffi::XML_IO_EACCES;
    };
    // SAFETY: `requested` and `bytes` are valid for the call; flags 0 makes libxml2 copy the
    // memory, so the input does not borrow `bytes` after this returns.
    let input = unsafe {
        ffi::xmlNewInputFromMemory(requested.as_ptr(), bytes.as_ptr().cast::<c_void>(), bytes.len(), 0)
    };
    if input.is_null() {
        return ffi::XML_ERR_NO_MEMORY;
    }
    // SAFETY: `out` is a valid out-pointer supplied by libxml2, which takes ownership.
    unsafe { *out = input };
    ffi::XML_ERR_OK
}

/// Refuse every resource an instance document asks for (external entities, DTDs, XInclude).
unsafe extern "C" fn refuse_all(
    data: *mut c_void,
    url: *const c_char,
    _public_id: *const c_char,
    _kind: ffi::xmlResourceType,
    _flags: ffi::xmlParserInputFlags,
    _out: *mut *mut ffi::xmlParserInput,
) -> ffi::xmlParserErrors {
    // SAFETY: the LoaderState registered by `validate`, alive for the whole parse.
    let state = unsafe { &mut *data.cast::<LoaderState<'_>>() };
    state.attempts += 1;
    if !url.is_null() {
        // SAFETY: a NUL-terminated URL valid for the duration of the call.
        refuse(state, unsafe { CStr::from_ptr(url) });
    }
    ffi::XML_IO_EACCES
}

fn refuse(state: &mut LoaderState<'_>, url: &CStr) {
    if state.refused.len() < MAX_REFUSALS {
        let mut text = url.to_string_lossy().into_owned();
        text.truncate(MAX_MESSAGE.min(text.len()));
        state.refused.push(text);
    }
}

/// Copy a structured error into the sink. Must not unwind.
unsafe extern "C" fn record_error(data: *mut c_void, error: *const ffi::xmlError) {
    if data.is_null() || error.is_null() {
        return;
    }
    // SAFETY: the boxed ErrorSink registered with this context, alive while it can call back.
    let sink = unsafe { &mut *data.cast::<ErrorSink>() };
    // SAFETY: libxml2 passes a valid error for the duration of the call.
    let error = unsafe { &*error };
    if error.level < ffi::XML_ERR_ERROR {
        return;
    }
    sink.errors += 1;
    if sink.diagnostics.len() >= MAX_DIAGNOSTICS {
        return;
    }
    let message = if error.message.is_null() {
        String::new()
    } else {
        // SAFETY: a NUL-terminated message owned by libxml2 for the duration of the call.
        redact(&unsafe { CStr::from_ptr(error.message) }.to_string_lossy())
    };
    sink.diagnostics.push(Diagnostic {
        line: u32::try_from(error.line).unwrap_or(0),
        message,
    });
}

/// Keep libxml2's sentence structure and schema-component names; replace quoted instance
/// values, which may be trade data, with `…`.
pub(crate) fn redact(message: &str) -> String {
    let message = message.trim();
    let mut out = String::with_capacity(message.len().min(MAX_MESSAGE));
    let mut rest = message;
    while let Some(open) = rest.find('\'') {
        out.push_str(&rest[..=open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('\'') else {
            rest = "";
            out.push('…');
            break;
        };
        let quoted = &after[..close];
        let preceding = out[..out.len() - 1].trim_end();
        let structural = quoted.starts_with('{')
            || ((preceding.ends_with("Element") || preceding.ends_with("attribute"))
                && is_name(quoted));
        out.push_str(if structural { quoted } else { "…" });
        out.push('\'');
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    if out.len() > MAX_MESSAGE {
        let mut end = MAX_MESSAGE;
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
        out.push('…');
    }
    out
}

fn is_name(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && text.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
}
