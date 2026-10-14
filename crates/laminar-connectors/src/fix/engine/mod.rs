//! Safe ownership boundary over the vendored QuickFIX C++ engine.
//!
//! All `unsafe` code of the FIX connector lives in this module family. Engine objects are owned
//! by Rust values that delete them exactly once; borrowed engine views carry the lifetime of the
//! message or callback that produced them. Every bridge function catches C++ exceptions, and
//! every Rust callback catches panics (`application.rs`), so neither unwinds across the ABI.

mod application;
mod socket;

#[cfg(test)]
mod tests;

pub(crate) use application::{Application, CallbackOutcome, OutboundMessage, SessionCallbacks};
#[cfg(any(test, feature = "testing"))]
pub(crate) use socket::Acceptor;
pub(crate) use socket::Initiator;

use std::ffi::{c_char, c_void, CStr, CString};
use std::marker::PhantomData;

use quickfix_ffi as ffi;

use crate::error::ConnectorError;

/// Error raised by the engine boundary.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EngineError {
    /// An engine call failed; the text is the engine's exception message.
    #[error("FIX engine {operation} failed: {message}")]
    Call {
        /// Bridge operation that failed.
        operation: &'static str,
        /// Engine exception text.
        message: String,
    },
    /// A value passed to the engine contained an interior NUL byte.
    #[error("FIX engine {operation} received a value containing a NUL byte")]
    InteriorNul {
        /// Bridge operation that rejected the value.
        operation: &'static str,
    },
}

impl From<EngineError> for ConnectorError {
    fn from(error: EngineError) -> Self {
        ConnectorError::Internal(error.to_string())
    }
}

/// Error raised by an upstream `quickfix-bind` call, read from that binding's error slot.
fn engine_error(operation: &'static str) -> EngineError {
    // SAFETY: the upstream binding returns either null or a NUL-terminated thread-local
    // string that stays valid until the next binding call on this thread.
    let message = match unsafe { ffi::Fix_getLastErrorMessage() } {
        Some(message) => unsafe { CStr::from_ptr(message.as_ptr()) }
            .to_string_lossy()
            .into_owned(),
        None => "unknown engine error".to_string(),
    };
    EngineError::Call { operation, message }
}

/// Error raised by a LaminarDB bridge call, read from the bridge's error slot.
fn bridge_error(operation: &'static str) -> EngineError {
    let mut buffer = [0_u8; 512];
    // SAFETY: the buffer is writable for its full length and the bridge writes at most
    // `capacity` bytes including the NUL terminator.
    let full = unsafe { ffi::LmFix_lastError(buffer.as_mut_ptr().cast::<c_char>(), buffer.len()) };
    let copied = full.min(buffer.len() - 1);
    EngineError::Call {
        operation,
        message: String::from_utf8_lossy(&buffer[..copied]).into_owned(),
    }
}

fn c_string(operation: &'static str, value: &str) -> Result<CString, EngineError> {
    CString::new(value).map_err(|_| EngineError::InteriorNul { operation })
}

/// FIX session identity (BeginString, SenderCompID, TargetCompID, optional qualifier).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SessionKey {
    pub(crate) begin_string: String,
    pub(crate) sender_comp_id: String,
    pub(crate) target_comp_id: String,
    pub(crate) qualifier: Option<String>,
}

impl SessionKey {
    /// Stable textual identity, matching the engine's `SessionID::toString` layout.
    pub(crate) fn display(&self) -> String {
        match &self.qualifier {
            Some(qualifier) => format!(
                "{}:{}->{}:{qualifier}",
                self.begin_string, self.sender_comp_id, self.target_comp_id
            ),
            None => format!(
                "{}:{}->{}",
                self.begin_string, self.sender_comp_id, self.target_comp_id
            ),
        }
    }

    fn to_engine(&self) -> Result<OwnedSessionId, EngineError> {
        let begin = c_string("session id", &self.begin_string)?;
        let sender = c_string("session id", &self.sender_comp_id)?;
        let target = c_string("session id", &self.target_comp_id)?;
        let qualifier = c_string("session id", self.qualifier.as_deref().unwrap_or(""))?;
        // SAFETY: all arguments are valid NUL-terminated strings that outlive the call; the
        // engine copies them into the new SessionID.
        let raw = unsafe {
            ffi::FixSessionID_new(
                begin.as_ptr(),
                sender.as_ptr(),
                target.as_ptr(),
                qualifier.as_ptr(),
            )
        }
        .ok_or_else(|| engine_error("session id"))?;
        Ok(OwnedSessionId(raw))
    }
}

struct OwnedSessionId(ffi::FixSessionID_t);

impl Drop for OwnedSessionId {
    fn drop(&mut self) {
        // SAFETY: the pointer was created by `FixSessionID_new` and is deleted only here.
        unsafe { ffi::FixSessionID_delete(self.0) };
    }
}

/// Engine session settings: one `[DEFAULT]` dictionary and one session dictionary.
pub(crate) struct Settings {
    raw: ffi::FixSessionSettings_t,
}

// SAFETY: `SessionSettings` is a plain value object; after construction it is only read by the
// engine objects that copy it, and every mutation happens through `&mut self` here.
unsafe impl Send for Settings {}

impl Settings {
    /// Build settings for exactly one session.
    pub(crate) fn single_session(
        defaults: &[(&str, String)],
        session: &SessionKey,
        session_values: &[(&str, String)],
    ) -> Result<Self, EngineError> {
        // SAFETY: plain constructor; the returned pointer is owned by `Settings`.
        let raw =
            unsafe { ffi::FixSessionSettings_new() }.ok_or_else(|| engine_error("settings"))?;
        let settings = Self { raw };
        let defaults_dictionary = Dictionary::new("DEFAULT", defaults)?;
        // SAFETY: both pointers are live; the engine copies the dictionary.
        if unsafe { ffi::FixSessionSettings_setGlobal(settings.raw, defaults_dictionary.raw) } != 0
        {
            return Err(engine_error("settings defaults"));
        }
        let mut identity = vec![
            ("BeginString", session.begin_string.clone()),
            ("SenderCompID", session.sender_comp_id.clone()),
            ("TargetCompID", session.target_comp_id.clone()),
        ];
        if let Some(qualifier) = &session.qualifier {
            identity.push(("SessionQualifier", qualifier.clone()));
        }
        identity.extend(
            session_values
                .iter()
                .map(|(key, value)| (*key, value.clone())),
        );
        let session_dictionary = Dictionary::new("SESSION", &identity)?;
        let id = session.to_engine()?;
        // SAFETY: all pointers are live; the engine copies the id and dictionary and merges
        // the defaults set above.
        if unsafe { ffi::FixSessionSettings_setSession(settings.raw, id.0, session_dictionary.raw) }
            != 0
        {
            return Err(engine_error("settings session"));
        }
        Ok(settings)
    }
}

impl Drop for Settings {
    fn drop(&mut self) {
        // SAFETY: created by `FixSessionSettings_new`, deleted only here.
        unsafe { ffi::FixSessionSettings_delete(self.raw) };
    }
}

struct Dictionary {
    raw: ffi::FixDictionary_t,
}

impl Dictionary {
    fn new(name: &str, values: &[(&str, String)]) -> Result<Self, EngineError> {
        let name = c_string("settings dictionary", name)?;
        // SAFETY: `name` is a valid C string; the engine copies it.
        let raw = unsafe { ffi::FixDictionary_new(name.as_ptr()) }
            .ok_or_else(|| engine_error("settings dictionary"))?;
        let dictionary = Self { raw };
        for (key, value) in values {
            let key_c = c_string("settings dictionary", key)?;
            let value_c = c_string("settings dictionary", value)?;
            // SAFETY: all pointers are live for the call; the engine copies key and value.
            let result = unsafe {
                ffi::FixDictionary_setString(dictionary.raw, key_c.as_ptr(), value_c.as_ptr())
            };
            if result != 0 {
                return Err(engine_error("settings dictionary"));
            }
        }
        Ok(dictionary)
    }
}

impl Drop for Dictionary {
    fn drop(&mut self) {
        // SAFETY: created by `FixDictionary_new`, deleted only here.
        unsafe { ffi::FixDictionary_delete(self.raw) };
    }
}

/// A loaded, immutable FIX data dictionary.
pub(crate) struct DataDictionary {
    raw: ffi::FixDataDictionary_t,
}

// SAFETY: a QuickFIX `DataDictionary` is not mutated after loading; parsing only reads it, so
// concurrent shared use from several threads is sound.
unsafe impl Send for DataDictionary {}
// SAFETY: see `Send`; every method takes `&self` and the engine only reads the dictionary.
unsafe impl Sync for DataDictionary {}

impl DataDictionary {
    /// Load a dictionary file. The XML parser (pugixml) never resolves external entities.
    pub(crate) fn from_path(path: &str) -> Result<Self, EngineError> {
        let path = c_string("dictionary load", path)?;
        // SAFETY: `path` is a valid C string for the call.
        let raw = unsafe { ffi::FixDataDictionary_fromPath(path.as_ptr()) }
            .ok_or_else(|| engine_error("dictionary load"))?;
        Ok(Self { raw })
    }

    /// Engine type of a defined field.
    pub(crate) fn field_type(&self, tag: i32) -> Result<Option<FieldType>, EngineError> {
        // SAFETY: the dictionary is live and only read.
        let code = unsafe { ffi::LmFix_dictionaryFieldType(self.raw, tag) };
        match code {
            -1 => Ok(None),
            code if code >= 0 => Ok(Some(FieldType::from_code(code))),
            _ => Err(bridge_error("dictionary field type")),
        }
    }

    pub(crate) fn has_message(&self, msg_type: &str) -> Result<bool, EngineError> {
        let msg_type = c_string("dictionary message", msg_type)?;
        // SAFETY: the dictionary and string are live for the call.
        match unsafe { ffi::LmFix_dictionaryHasMessage(self.raw, msg_type.as_ptr()) } {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(bridge_error("dictionary message")),
        }
    }

    /// Where `tag` may appear in messages of `msg_type`.
    pub(crate) fn placement(
        &self,
        msg_type: &str,
        tag: i32,
    ) -> Result<Option<Section>, EngineError> {
        let msg_type = c_string("dictionary placement", msg_type)?;
        // SAFETY: the dictionary and string are live for the call.
        match unsafe { ffi::LmFix_dictionaryFieldPlacement(self.raw, msg_type.as_ptr(), tag) } {
            0 => Ok(None),
            1 => Ok(Some(Section::Header)),
            2 => Ok(Some(Section::Body)),
            3 => Ok(Some(Section::Trailer)),
            _ => Err(bridge_error("dictionary placement")),
        }
    }

    /// Member dictionary of body group `count_tag` in `msg_type`.
    pub(crate) fn group(
        &self,
        msg_type: &str,
        count_tag: i32,
    ) -> Result<Option<GroupDictionary<'_>>, EngineError> {
        group_raw(self.raw, msg_type, count_tag)
    }

    /// Parse one serialized message using this dictionary's group structure.
    pub(crate) fn parse(&self, bytes: &[u8]) -> Result<OwnedMessage, EngineError> {
        // SAFETY: `bytes` is valid for `len` bytes during the call; the engine copies it.
        let raw = unsafe {
            ffi::LmFix_messageParse(bytes.as_ptr().cast::<c_char>(), bytes.len(), self.raw)
        }
        .ok_or_else(|| bridge_error("message parse"))?;
        Ok(OwnedMessage { raw })
    }
}

impl Drop for DataDictionary {
    fn drop(&mut self) {
        // SAFETY: created by `FixDataDictionary_fromPath`, deleted only here.
        unsafe { ffi::FixDataDictionary_delete(self.raw) };
    }
}

fn group_raw<'a>(
    raw: ffi::FixDataDictionary_t,
    msg_type: &str,
    count_tag: i32,
) -> Result<Option<GroupDictionary<'a>>, EngineError> {
    let msg_type = c_string("dictionary group", msg_type)?;
    // SAFETY: the dictionary and string are live for the call; the returned group dictionary
    // is owned by `raw` and borrowed for `'a`.
    let group = unsafe { ffi::LmFix_dictionaryGroup(raw, msg_type.as_ptr(), count_tag) };
    Ok(group.map(|raw| GroupDictionary {
        raw,
        _lifetime: PhantomData,
    }))
}

/// Member dictionary of a repeating group, borrowed from its message dictionary.
#[derive(Clone, Copy)]
pub(crate) struct GroupDictionary<'a> {
    raw: ffi::FixDataDictionary_t,
    _lifetime: PhantomData<&'a DataDictionary>,
}

impl<'a> GroupDictionary<'a> {
    pub(crate) fn has_field(&self, tag: i32) -> Result<bool, EngineError> {
        // SAFETY: the group dictionary is live for `'a` and only read.
        match unsafe { ffi::LmFix_dictionaryGroupHasField(self.raw, tag) } {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(bridge_error("dictionary group field")),
        }
    }

    /// Nested group `count_tag` inside this group.
    pub(crate) fn group(
        &self,
        msg_type: &str,
        count_tag: i32,
    ) -> Result<Option<GroupDictionary<'a>>, EngineError> {
        group_raw(self.raw, msg_type, count_tag)
    }
}

/// FIX field type families relevant to projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldType {
    /// Integer-valued: INT, SEQNUM, LENGTH, NUMINGROUP, TAGNUM, DAYOFMONTH.
    Integer,
    /// Decimal-valued: PRICE, AMT, QTY, FLOAT, PRICEOFFSET, PERCENTAGE.
    Decimal,
    Boolean,
    UtcTimestamp,
    /// LOCALMKTDATE and UTCDATEONLY (`YYYYMMDD`).
    Date,
    /// Length-prefixed DATA and XMLDATA.
    Data,
    /// Every other type, carried as text.
    Text,
}

impl FieldType {
    fn from_code(code: i32) -> Self {
        // Codes follow `FIX::TYPE::Type` in `FieldTypes.h`.
        match code {
            4 | 20 | 23 | 25 | 26 | 27 => Self::Integer,
            3 | 5 | 6 | 17 | 18 | 24 => Self::Decimal,
            13 => Self::Boolean,
            12 => Self::UtcTimestamp,
            15 | 21 => Self::Date,
            16 | 31 => Self::Data,
            _ => Self::Text,
        }
    }
}

/// A message owned by Rust.
pub(crate) struct OwnedMessage {
    raw: ffi::FixMessage_t,
}

// SAFETY: an owned `Message` has no thread affinity; it is moved, never shared.
unsafe impl Send for OwnedMessage {}

impl OwnedMessage {
    #[cfg(any(test, feature = "testing"))]
    /// Create an empty message.
    pub(crate) fn new() -> Result<Self, EngineError> {
        // SAFETY: plain constructor; ownership moves into `OwnedMessage`.
        let raw = unsafe { ffi::FixMessage_new() }.ok_or_else(|| engine_error("message new"))?;
        Ok(Self { raw })
    }

    /// Borrow the message.
    pub(crate) fn view(&self) -> MessageRef<'_> {
        MessageRef {
            raw: self.raw,
            _lifetime: PhantomData,
        }
    }

    #[cfg(any(test, feature = "testing"))]
    /// Set a field from bytes, which may contain NUL bytes.
    pub(crate) fn set_field(
        &mut self,
        section: Section,
        tag: i32,
        value: &[u8],
    ) -> Result<(), EngineError> {
        set_field_raw(self.raw, section, tag, value)
    }

    #[cfg(any(test, feature = "testing"))]
    /// Append one repeating-group entry. The entry's first field is the group delimiter.
    pub(crate) fn add_group(
        &mut self,
        count_tag: i32,
        entry: &GroupEntry<'_>,
    ) -> Result<(), EngineError> {
        let Some(group) = build_group(count_tag, entry)? else {
            return Ok(());
        };
        // SAFETY: both objects are live; the engine copies the group into the message.
        if unsafe { ffi::FixMessage_addGroup(self.raw, group.0) } != 0 {
            return Err(engine_error("message add group"));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "testing"))]
    /// Send through the engine session registered for `session`.
    pub(crate) fn send_to(&mut self, session: &SessionKey) -> Result<(), EngineError> {
        let id = session.to_engine()?;
        // SAFETY: message and id are live; the engine copies the message before sending.
        if unsafe { ffi::FixSession_sendToTarget(self.raw, id.0) } < 0 {
            return Err(engine_error("send"));
        }
        Ok(())
    }
}

impl Drop for OwnedMessage {
    fn drop(&mut self) {
        // SAFETY: created by the engine for Rust ownership, deleted only here.
        unsafe { ffi::FixMessage_delete(self.raw) };
    }
}

#[cfg(any(test, feature = "testing"))]
/// One repeating-group entry used to construct messages: ordered fields, then nested groups.
pub(crate) struct GroupEntry<'a> {
    pub(crate) fields: Vec<(i32, &'a str)>,
    pub(crate) groups: Vec<(i32, Vec<GroupEntry<'a>>)>,
}

#[cfg(any(test, feature = "testing"))]
fn build_group(count_tag: i32, entry: &GroupEntry<'_>) -> Result<Option<OwnedGroup>, EngineError> {
    let Some((delimiter, _)) = entry.fields.first() else {
        return Ok(None);
    };
    let mut order: Vec<i32> = entry.fields.iter().map(|(tag, _)| *tag).collect();
    order.extend(entry.groups.iter().map(|(tag, _)| *tag));
    order.push(0);
    // SAFETY: `order` is a zero-terminated tag list that the engine copies.
    let group = unsafe { ffi::FixGroup_new(count_tag, *delimiter, order.as_ptr()) }
        .ok_or_else(|| engine_error("group new"))?;
    let group = OwnedGroup(group);
    for (tag, value) in &entry.fields {
        let value = c_string("group set field", value)?;
        // SAFETY: group and value are live; the engine copies the value.
        if unsafe { ffi::FixGroup_setField(group.0, *tag, value.as_ptr()) } != 0 {
            return Err(engine_error("group set field"));
        }
    }
    for (nested_tag, entries) in &entry.groups {
        for nested in entries {
            if let Some(nested) = build_group(*nested_tag, nested)? {
                // SAFETY: both groups are live; the engine copies the nested group.
                if unsafe { ffi::FixGroup_addGroup(group.0, nested.0) } != 0 {
                    return Err(engine_error("group add group"));
                }
            }
        }
    }
    Ok(Some(group))
}

#[cfg(any(test, feature = "testing"))]
struct OwnedGroup(ffi::FixGroup_t);

#[cfg(any(test, feature = "testing"))]
impl Drop for OwnedGroup {
    fn drop(&mut self) {
        // SAFETY: created by `FixGroup_new`, deleted only here.
        unsafe { ffi::FixGroup_delete(self.0) };
    }
}

/// Part of a message that owns a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Section {
    Body,
    Header,
    Trailer,
}

fn set_field_raw(
    raw: ffi::FixMessage_t,
    section: Section,
    tag: i32,
    value: &[u8],
) -> Result<(), EngineError> {
    let section = match section {
        Section::Body => 0,
        Section::Header => 1,
        Section::Trailer => 2,
    };
    // SAFETY: the caller holds the message exclusively; `value` is valid for its length and the
    // engine copies it.
    let result = unsafe {
        ffi::LmFix_messageSetFieldBytes(
            raw,
            section,
            tag,
            value.as_ptr().cast::<c_char>(),
            value.len(),
        )
    };
    if result != 0 {
        return Err(bridge_error("message set field"));
    }
    Ok(())
}

/// A borrowed message: valid for `'a` and never mutated while borrowed.
#[derive(Clone, Copy)]
pub(crate) struct MessageRef<'a> {
    raw: ffi::FixMessage_t,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> MessageRef<'a> {
    /// # Safety
    /// `raw` must point to a live message that is not mutated or freed during `'a`.
    pub(crate) unsafe fn from_raw(raw: ffi::FixMessage_t) -> Self {
        Self {
            raw,
            _lifetime: PhantomData,
        }
    }

    pub(crate) fn header(&self) -> Result<FieldMapRef<'a>, EngineError> {
        // SAFETY: `raw` is live for `'a`; the header is owned by the message.
        let map = unsafe { ffi::LmFix_messageHeader(self.raw) }
            .ok_or_else(|| bridge_error("message header"))?;
        Ok(FieldMapRef::new(map))
    }

    pub(crate) fn body(&self) -> Result<FieldMapRef<'a>, EngineError> {
        // SAFETY: `raw` is live for `'a`; the body is the message itself.
        let map = unsafe { ffi::LmFix_messageBody(self.raw) }
            .ok_or_else(|| bridge_error("message body"))?;
        Ok(FieldMapRef::new(map))
    }

    pub(crate) fn trailer(&self) -> Result<FieldMapRef<'a>, EngineError> {
        // SAFETY: `raw` is live for `'a`; the trailer is owned by the message.
        let map = unsafe { ffi::LmFix_messageTrailer(self.raw) }
            .ok_or_else(|| bridge_error("message trailer"))?;
        Ok(FieldMapRef::new(map))
    }

    /// Engine serialization of the parsed message. It recomputes BodyLength and CheckSum and
    /// follows the engine's field order, so it is not guaranteed to equal the wire bytes.
    pub(crate) fn serialize_into(&self, output: &mut Vec<u8>) -> Result<(), EngineError> {
        output.clear();
        // SAFETY: `output` outlives the call and the sink only appends to it.
        let result = unsafe {
            ffi::LmFix_messageSerialize(
                self.raw,
                std::ptr::from_mut(output).cast::<c_void>(),
                append_bytes,
            )
        };
        if result != 0 {
            return Err(bridge_error("message serialize"));
        }
        Ok(())
    }

    /// Engine serialization of a copy with `header_tags` removed.
    pub(crate) fn serialize_without_header_tags(
        &self,
        header_tags: &[i32],
        output: &mut Vec<u8>,
    ) -> Result<(), EngineError> {
        output.clear();
        // SAFETY: `header_tags` is valid for `len` entries and `output` outlives the call.
        let result = unsafe {
            ffi::LmFix_messageSerializeWithoutHeaderTags(
                self.raw,
                header_tags.as_ptr(),
                header_tags.len(),
                std::ptr::from_mut(output).cast::<c_void>(),
                append_bytes,
            )
        };
        if result != 0 {
            return Err(bridge_error("message serialize"));
        }
        Ok(())
    }
}

extern "C" fn append_bytes(ctx: *mut c_void, ptr: *const c_char, len: usize) {
    if ctx.is_null() || (ptr.is_null() && len > 0) {
        return;
    }
    // SAFETY: `ctx` is the `&mut Vec<u8>` passed by the caller of the serialize call, which is
    // blocked in that call; `ptr` is valid for `len` bytes for the duration of this callback.
    let output = unsafe { &mut *ctx.cast::<Vec<u8>>() };
    if len > 0 {
        output.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) });
    }
}

/// A borrowed field map (body, header, trailer, or group entry).
#[derive(Clone, Copy)]
pub(crate) struct FieldMapRef<'a> {
    raw: ffi::LmFixFieldMap_t,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> FieldMapRef<'a> {
    fn new(raw: ffi::LmFixFieldMap_t) -> Self {
        Self {
            raw,
            _lifetime: PhantomData,
        }
    }

    /// Field value bytes, including embedded NUL bytes, or `None` when absent.
    pub(crate) fn field(&self, tag: i32) -> Result<Option<&'a [u8]>, EngineError> {
        let mut ptr: *const c_char = std::ptr::null();
        let mut len = 0_usize;
        // SAFETY: the map is live for `'a`; outputs are valid writable locations.
        let found = unsafe { ffi::LmFix_fieldBytes(self.raw, tag, &raw mut ptr, &raw mut len) };
        match found {
            0 => Ok(None),
            1 if len == 0 => Ok(Some(&[])),
            1 if !ptr.is_null() => {
                // SAFETY: the engine returned a pointer to `len` bytes owned by the field,
                // which lives as long as the unmodified message (`'a`).
                Ok(Some(unsafe {
                    std::slice::from_raw_parts(ptr.cast::<u8>(), len)
                }))
            }
            _ => Err(bridge_error("field read")),
        }
    }

    /// Number of entries in a repeating group (0 when the group is absent).
    pub(crate) fn group_count(&self, count_tag: i32) -> Result<usize, EngineError> {
        // SAFETY: the map is live for `'a`.
        let count = unsafe { ffi::LmFix_groupCount(self.raw, count_tag) };
        usize::try_from(count).map_err(|_| bridge_error("group count"))
    }

    /// One-based group entry.
    pub(crate) fn group_entry(
        &self,
        count_tag: i32,
        index: usize,
    ) -> Result<FieldMapRef<'a>, EngineError> {
        let index = i64::try_from(index).map_err(|_| EngineError::Call {
            operation: "group entry",
            message: "index overflow".into(),
        })?;
        // SAFETY: the map is live for `'a`; the entry is owned by it.
        let entry = unsafe { ffi::LmFix_groupEntry(self.raw, count_tag, index) }
            .ok_or_else(|| bridge_error("group entry"))?;
        Ok(FieldMapRef::new(entry))
    }
}

/// Persisted engine-store identity and in-memory sequence state of a live session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionState {
    /// Store creation time in whole seconds; it changes exactly when the store is reset.
    pub(crate) creation_unix_seconds: i64,
    pub(crate) next_sender: u64,
    pub(crate) next_target: u64,
}

/// Read the state of a registered session. Valid from any thread while the engine object that
/// owns the session is alive.
pub(crate) fn session_state(session: &SessionKey) -> Result<SessionState, EngineError> {
    let id = session.to_engine()?;
    session_state_raw(id.0)
}

fn session_state_raw(id: ffi::FixSessionID_t) -> Result<SessionState, EngineError> {
    let mut state = SessionState {
        creation_unix_seconds: 0,
        next_sender: 0,
        next_target: 0,
    };
    // SAFETY: `id` is live and the outputs are valid writable locations.
    let result = unsafe {
        ffi::LmFix_sessionState(
            id,
            &raw mut state.creation_unix_seconds,
            &raw mut state.next_sender,
            &raw mut state.next_target,
        )
    };
    if result != 0 {
        return Err(bridge_error("session state"));
    }
    Ok(state)
}

/// Set the next outbound sequence number of a registered session.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn set_next_sender(session: &SessionKey, next_sender: u64) -> Result<(), EngineError> {
    let id = session.to_engine()?;
    // SAFETY: `id` is live for the call.
    if unsafe { ffi::LmFix_sessionSetNextSender(id.0, next_sender) } != 0 {
        return Err(bridge_error("session set sender"));
    }
    Ok(())
}

/// Reset a registered session: Logout, disconnect, and a new store with sequence numbers 1.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn reset_session(session: &SessionKey) -> Result<(), EngineError> {
    let id = session.to_engine()?;
    // SAFETY: `id` is live for the call; the session pointer is owned by the engine socket.
    let session =
        unsafe { ffi::FixSession_lookup(id.0) }.ok_or_else(|| engine_error("session lookup"))?;
    // SAFETY: the session is live while its engine socket is.
    if unsafe { ffi::FixSession_reset(session) } != 0 {
        return Err(engine_error("session reset"));
    }
    Ok(())
}
