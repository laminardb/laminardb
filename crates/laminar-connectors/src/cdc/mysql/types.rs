//! MySQL type to Arrow type mapping.
//!
//! Maps MySQL column types to Arrow `DataType` for CDC record conversion.

// Using wildcard import for local mysql_type constants is cleaner for this domain-specific code.
#![allow(clippy::wildcard_imports)]
// Multiple match arms returning the same type (e.g., TIMESTAMP | TIMESTAMP2 -> same) is intentional.
#![allow(clippy::match_same_arms)]

use arrow_schema::DataType;

/// MySQL column type constants.
///
/// These match the field type values from MySQL's protocol.
/// See: <https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html>
pub mod mysql_type {
    /// DECIMAL, NUMERIC
    pub const DECIMAL: u8 = 0x00;
    /// TINYINT
    pub const TINY: u8 = 0x01;
    /// SMALLINT
    pub const SHORT: u8 = 0x02;
    /// INT
    pub const LONG: u8 = 0x03;
    /// FLOAT
    pub const FLOAT: u8 = 0x04;
    /// DOUBLE
    pub const DOUBLE: u8 = 0x05;
    /// NULL
    pub const NULL: u8 = 0x06;
    /// TIMESTAMP
    pub const TIMESTAMP: u8 = 0x07;
    /// BIGINT
    pub const LONGLONG: u8 = 0x08;
    /// MEDIUMINT
    pub const INT24: u8 = 0x09;
    /// DATE
    pub const DATE: u8 = 0x0A;
    /// TIME
    pub const TIME: u8 = 0x0B;
    /// DATETIME
    pub const DATETIME: u8 = 0x0C;
    /// YEAR
    pub const YEAR: u8 = 0x0D;
    /// Internal: NEWDATE
    pub const NEWDATE: u8 = 0x0E;
    /// VARCHAR
    pub const VARCHAR: u8 = 0x0F;
    /// BIT
    pub const BIT: u8 = 0x10;
    /// TIMESTAMP2 (fractional seconds)
    pub const TIMESTAMP2: u8 = 0x11;
    /// DATETIME2 (fractional seconds)
    pub const DATETIME2: u8 = 0x12;
    /// TIME2 (fractional seconds)
    pub const TIME2: u8 = 0x13;
    /// JSON (MySQL 5.7+)
    pub const JSON: u8 = 0xF5;
    /// DECIMAL (new)
    pub const NEWDECIMAL: u8 = 0xF6;
    /// ENUM
    pub const ENUM: u8 = 0xF7;
    /// SET
    pub const SET: u8 = 0xF8;
    /// TINYBLOB, TINYTEXT
    pub const TINY_BLOB: u8 = 0xF9;
    /// MEDIUMBLOB, MEDIUMTEXT
    pub const MEDIUM_BLOB: u8 = 0xFA;
    /// LONGBLOB, LONGTEXT
    pub const LONG_BLOB: u8 = 0xFB;
    /// BLOB, TEXT
    pub const BLOB: u8 = 0xFC;
    /// VARCHAR, VARBINARY
    pub const VAR_STRING: u8 = 0xFD;
    /// CHAR, BINARY
    pub const STRING: u8 = 0xFE;
    /// GEOMETRY
    pub const GEOMETRY: u8 = 0xFF;
}

/// MySQL column metadata from TABLE_MAP_EVENT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlColumn {
    /// Column name.
    pub name: String,
    /// MySQL type ID.
    pub type_id: u8,
    /// Column metadata (varies by type).
    pub metadata: u16,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Whether the column is unsigned (for integer types).
    pub unsigned: bool,
}

impl MySqlColumn {
    /// Creates a new MySQL column descriptor.
    #[must_use]
    pub fn new(name: String, type_id: u8, metadata: u16, nullable: bool, unsigned: bool) -> Self {
        Self {
            name,
            type_id,
            metadata,
            nullable,
            unsigned,
        }
    }

    /// Converts MySQL type to Arrow `DataType`.
    #[must_use]
    pub fn to_arrow_type(&self) -> DataType {
        mysql_type_to_arrow(self.type_id, self.unsigned, self.metadata)
    }

    /// Returns the display name of the MySQL type.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        mysql_type_name(self.type_id)
    }
}

/// Converts a MySQL type ID to an Arrow `DataType`.
#[must_use]
pub fn mysql_type_to_arrow(type_id: u8, unsigned: bool, _metadata: u16) -> DataType {
    use mysql_type::*;

    match type_id {
        // Integers
        TINY => {
            if unsigned {
                DataType::UInt8
            } else {
                DataType::Int8
            }
        }
        SHORT | YEAR => {
            if unsigned {
                DataType::UInt16
            } else {
                DataType::Int16
            }
        }
        LONG | INT24 => {
            if unsigned {
                DataType::UInt32
            } else {
                DataType::Int32
            }
        }
        LONGLONG => {
            if unsigned {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        }

        // Floating point
        FLOAT => DataType::Float32,
        DOUBLE => DataType::Float64,

        // Decimal - map to string for precision preservation
        DECIMAL | NEWDECIMAL => DataType::Utf8,

        // Date/Time types
        DATE | NEWDATE => DataType::Date32,
        TIME | TIME2 => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        TIMESTAMP | TIMESTAMP2 => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        }
        DATETIME | DATETIME2 => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),

        // String types
        VARCHAR | VAR_STRING | STRING | ENUM | SET => DataType::Utf8,

        // Binary types
        BIT => DataType::Binary,
        TINY_BLOB | MEDIUM_BLOB | LONG_BLOB | BLOB => DataType::LargeBinary,

        // JSON
        JSON => DataType::Utf8, // JSON stored as string

        // Geometry - binary
        GEOMETRY => DataType::LargeBinary,

        // NULL
        NULL => DataType::Null,

        // Unknown - fall back to binary
        _ => DataType::Binary,
    }
}

/// Returns the human-readable name of a MySQL type.
#[must_use]
pub fn mysql_type_name(type_id: u8) -> &'static str {
    use mysql_type::*;

    match type_id {
        DECIMAL => "DECIMAL",
        TINY => "TINYINT",
        SHORT => "SMALLINT",
        LONG => "INT",
        FLOAT => "FLOAT",
        DOUBLE => "DOUBLE",
        NULL => "NULL",
        TIMESTAMP => "TIMESTAMP",
        LONGLONG => "BIGINT",
        INT24 => "MEDIUMINT",
        DATE => "DATE",
        TIME => "TIME",
        DATETIME => "DATETIME",
        YEAR => "YEAR",
        NEWDATE => "DATE",
        VARCHAR => "VARCHAR",
        BIT => "BIT",
        TIMESTAMP2 => "TIMESTAMP",
        DATETIME2 => "DATETIME",
        TIME2 => "TIME",
        JSON => "JSON",
        NEWDECIMAL => "DECIMAL",
        ENUM => "ENUM",
        SET => "SET",
        TINY_BLOB => "TINYBLOB",
        MEDIUM_BLOB => "MEDIUMBLOB",
        LONG_BLOB => "LONGBLOB",
        BLOB => "BLOB",
        VAR_STRING => "VARCHAR",
        STRING => "CHAR",
        GEOMETRY => "GEOMETRY",
        _ => "UNKNOWN",
    }
}

/// SQL type string for DDL generation.
#[must_use]
pub fn mysql_type_to_sql(type_id: u8, unsigned: bool, metadata: u16) -> String {
    use mysql_type::*;

    let base = match type_id {
        TINY => "TINYINT",
        SHORT => "SMALLINT",
        LONG => "INT",
        LONGLONG => "BIGINT",
        INT24 => "MEDIUMINT",
        FLOAT => "FLOAT",
        DOUBLE => "DOUBLE",
        DECIMAL | NEWDECIMAL => {
            // metadata contains precision and scale
            let precision = (metadata >> 8) as u8;
            let scale = (metadata & 0xFF) as u8;
            return format!("DECIMAL({precision},{scale})");
        }
        DATE | NEWDATE => "DATE",
        TIME | TIME2 => "TIME",
        TIMESTAMP | TIMESTAMP2 => "TIMESTAMP",
        DATETIME | DATETIME2 => "DATETIME",
        YEAR => "YEAR",
        VARCHAR | VAR_STRING => {
            return format!("VARCHAR({metadata})");
        }
        STRING => {
            return format!("CHAR({metadata})");
        }
        BIT => {
            return format!("BIT({metadata})");
        }
        TINY_BLOB => "TINYBLOB",
        MEDIUM_BLOB => "MEDIUMBLOB",
        LONG_BLOB => "LONGBLOB",
        BLOB => "BLOB",
        JSON => "JSON",
        ENUM => "ENUM",
        SET => "SET",
        GEOMETRY => "GEOMETRY",
        NULL => "NULL",
        _ => "UNKNOWN",
    };

    if unsigned && matches!(type_id, TINY | SHORT | LONG | LONGLONG | INT24) {
        format!("{base} UNSIGNED")
    } else {
        base.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_types() {
        assert_eq!(
            mysql_type_to_arrow(mysql_type::TINY, false, 0),
            DataType::Int8
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::TINY, true, 0),
            DataType::UInt8
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::SHORT, false, 0),
            DataType::Int16
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::LONG, false, 0),
            DataType::Int32
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::LONGLONG, false, 0),
            DataType::Int64
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::LONGLONG, true, 0),
            DataType::UInt64
        );
    }

    #[test]
    fn test_float_types() {
        assert_eq!(
            mysql_type_to_arrow(mysql_type::FLOAT, false, 0),
            DataType::Float32
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::DOUBLE, false, 0),
            DataType::Float64
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(
            mysql_type_to_arrow(mysql_type::VARCHAR, false, 255),
            DataType::Utf8
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::STRING, false, 50),
            DataType::Utf8
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::JSON, false, 0),
            DataType::Utf8
        );
    }

    #[test]
    fn test_datetime_types() {
        assert_eq!(
            mysql_type_to_arrow(mysql_type::DATE, false, 0),
            DataType::Date32
        );
        assert!(matches!(
            mysql_type_to_arrow(mysql_type::TIMESTAMP, false, 0),
            DataType::Timestamp(_, Some(_))
        ));
        assert!(matches!(
            mysql_type_to_arrow(mysql_type::DATETIME, false, 0),
            DataType::Timestamp(_, None)
        ));
    }

    #[test]
    fn test_binary_types() {
        assert_eq!(
            mysql_type_to_arrow(mysql_type::BLOB, false, 0),
            DataType::LargeBinary
        );
        assert_eq!(
            mysql_type_to_arrow(mysql_type::BIT, false, 8),
            DataType::Binary
        );
    }

    #[test]
    fn test_type_names() {
        assert_eq!(mysql_type_name(mysql_type::TINY), "TINYINT");
        assert_eq!(mysql_type_name(mysql_type::VARCHAR), "VARCHAR");
        assert_eq!(mysql_type_name(mysql_type::JSON), "JSON");
        assert_eq!(mysql_type_name(mysql_type::BLOB), "BLOB");
    }

    #[test]
    fn test_mysql_column() {
        let col = MySqlColumn::new("id".to_string(), mysql_type::LONGLONG, 0, false, false);
        assert_eq!(col.name, "id");
        assert_eq!(col.to_arrow_type(), DataType::Int64);
        assert_eq!(col.type_name(), "BIGINT");
    }

    #[test]
    fn test_type_to_sql() {
        assert_eq!(mysql_type_to_sql(mysql_type::TINY, false, 0), "TINYINT");
        assert_eq!(
            mysql_type_to_sql(mysql_type::TINY, true, 0),
            "TINYINT UNSIGNED"
        );
        assert_eq!(
            mysql_type_to_sql(mysql_type::VARCHAR, false, 255),
            "VARCHAR(255)"
        );
        // DECIMAL with precision 10, scale 2
        let metadata = (10u16 << 8) | 2u16;
        assert_eq!(
            mysql_type_to_sql(mysql_type::NEWDECIMAL, false, metadata),
            "DECIMAL(10,2)"
        );
    }
}
