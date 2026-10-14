//! FIX data dictionaries and the dictionary pair a session parses with.

use std::ffi::{c_char, c_void};
use std::marker::PhantomData;

use quickfix_ffi as ffi;

use super::{
    append_bytes, bridge_error, c_string, engine_error, EngineError, OwnedMessage, Section,
};

/// The dictionaries of one session. Below FIXT one dictionary describes every message; under
/// FIXT.1.1 the transport dictionary describes the header, trailer, and session messages, and
/// the application dictionary describes application messages and their groups.
pub(crate) struct SessionDictionaries {
    transport: Option<DataDictionary>,
    application: DataDictionary,
}

impl SessionDictionaries {
    pub(crate) fn single(dictionary: DataDictionary) -> Self {
        Self {
            transport: None,
            application: dictionary,
        }
    }

    pub(crate) fn fixt(transport: DataDictionary, application: DataDictionary) -> Self {
        Self {
            transport: Some(transport),
            application,
        }
    }

    fn session(&self) -> &DataDictionary {
        self.transport.as_ref().unwrap_or(&self.application)
    }

    pub(crate) fn field_type(&self, tag: i32) -> Result<Option<FieldType>, EngineError> {
        match (self.application.field_type(tag)?, &self.transport) {
            (Some(field_type), _) => Ok(Some(field_type)),
            (None, Some(transport)) => transport.field_type(tag),
            (None, None) => Ok(None),
        }
    }

    pub(crate) fn has_message(&self, msg_type: &str) -> Result<bool, EngineError> {
        self.application.has_message(msg_type)
    }

    /// Where `tag` may appear in messages of `msg_type`: the session dictionary decides header
    /// and trailer, the application dictionary the body.
    pub(crate) fn placement(
        &self,
        msg_type: &str,
        tag: i32,
    ) -> Result<Option<Section>, EngineError> {
        let Some(transport) = &self.transport else {
            return self.application.placement(msg_type, tag);
        };
        match transport.placement(msg_type, tag)? {
            Some(section @ (Section::Header | Section::Trailer)) => Ok(Some(section)),
            _ => self.application.placement(msg_type, tag),
        }
    }

    pub(crate) fn group(
        &self,
        msg_type: &str,
        count_tag: i32,
    ) -> Result<Option<GroupDictionary<'_>>, EngineError> {
        self.application.group(msg_type, count_tag)
    }

    /// Parse one serialized message with the session's dictionaries.
    pub(crate) fn parse(&self, bytes: &[u8]) -> Result<OwnedMessage, EngineError> {
        // SAFETY: `bytes` is valid for `len` bytes during the call and both dictionaries are
        // live; the engine copies the bytes.
        let raw = unsafe {
            ffi::LmFix_messageParse(
                bytes.as_ptr().cast::<c_char>(),
                bytes.len(),
                self.session().raw,
                self.application.raw,
            )
        }
        .ok_or_else(|| bridge_error("message parse"))?;
        Ok(OwnedMessage { raw })
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

    /// `type.major.minor` of the dictionary's root element; the service pack is not part of it.
    pub(crate) fn version(&self) -> Result<String, EngineError> {
        let mut bytes = Vec::new();
        // SAFETY: `bytes` outlives the call and the sink only appends to it.
        let result = unsafe {
            ffi::LmFix_dictionaryVersion(
                self.raw,
                std::ptr::from_mut(&mut bytes).cast::<c_void>(),
                append_bytes,
            )
        };
        if result != 0 {
            return Err(bridge_error("dictionary version"));
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
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
    pub(crate) fn has_field(self, tag: i32) -> Result<bool, EngineError> {
        // SAFETY: the group dictionary is live for `'a` and only read.
        match unsafe { ffi::LmFix_dictionaryGroupHasField(self.raw, tag) } {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(bridge_error("dictionary group field")),
        }
    }

    /// Nested group `count_tag` inside this group.
    pub(crate) fn group(
        self,
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
