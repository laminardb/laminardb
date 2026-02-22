/// Generates `FromStr` and optionally `Display` impls for simple string enums.
///
/// # Forms
///
/// - `str_enum!(Enum, norm, ErrType, "msg", ...)` — both `Display` + `FromStr`
/// - `str_enum!(fromstr Enum, norm, ErrType, "msg", ...)` — `FromStr` only
///
/// # Normalization modes
///
/// - `lowercase`: `to_lowercase().replace('-', "_")`
/// - `uppercase`: `to_uppercase().replace('-', "_")`
/// - `lowercase_nodash`: `to_lowercase()`
/// - `lowercase_udash`: `to_lowercase().replace('_', "-")`
#[allow(unused_macros)]
macro_rules! str_enum {
    // ── Full form: Display + FromStr ──
    ($enum_name:ident, $norm:ident, $err_kind:ident, $err_msg:literal,
        $( $variant:ident => $display:literal $(, $alias:literal)* );+ $(;)?
    ) => {
        impl std::fmt::Display for $enum_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let s = match self {
                    $( Self::$variant => $display, )+
                };
                f.write_str(s)
            }
        }
        str_enum!(fromstr $enum_name, $norm, $err_kind, $err_msg,
            $( $variant => $display $(, $alias)* );+);
    };

    // ── FromStr-only form ──
    (fromstr $enum_name:ident, $norm:ident, $err_kind:ident, $err_msg:literal,
        $( $variant:ident => $canonical:literal $(, $alias:literal)* );+ $(;)?
    ) => {
        str_enum!(@fromstr_impl $enum_name, $norm, $err_kind, $err_msg,
            $( $variant => $canonical $(, $alias)* );+);
    };

    // ── Internal: ConnectorError variant ──
    (@fromstr_impl $enum_name:ident, $norm:ident, ConnectorError, $err_msg:literal,
        $( $variant:ident => $canonical:literal $(, $alias:literal)* );+ $(;)?
    ) => {
        impl std::str::FromStr for $enum_name {
            type Err = crate::error::ConnectorError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let normalized = str_enum!(@normalize $norm s);
                match normalized.as_str() {
                    $( $canonical $(| $alias)* => Ok(Self::$variant), )+
                    other => Err(crate::error::ConnectorError::ConfigurationError(
                        format!("{}: '{}'", $err_msg, other),
                    )),
                }
            }
        }
    };

    // ── Internal: String variant ──
    (@fromstr_impl $enum_name:ident, $norm:ident, String, $err_msg:literal,
        $( $variant:ident => $canonical:literal $(, $alias:literal)* );+ $(;)?
    ) => {
        impl std::str::FromStr for $enum_name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let normalized = str_enum!(@normalize $norm s);
                match normalized.as_str() {
                    $( $canonical $(| $alias)* => Ok(Self::$variant), )+
                    other => Err(format!("{}: '{}'", $err_msg, other)),
                }
            }
        }
    };

    // ── Normalization helpers ──
    (@normalize lowercase $s:ident) => { $s.to_lowercase().replace('-', "_") };
    (@normalize uppercase $s:ident) => { $s.to_uppercase().replace('-', "_") };
    (@normalize lowercase_nodash $s:ident) => { $s.to_lowercase() };
    (@normalize lowercase_udash $s:ident) => { $s.to_lowercase().replace('_', "-") };
}
