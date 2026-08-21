//! Canonical Arrow IPC schema, framing, and buffer-layout validation.

use super::IpcRestorePreflight;
use crate::error::DbError;

#[derive(Clone, Copy)]
enum IpcPhysicalShape {
    Null,
    Bits,
    Fixed(usize),
    Variable(usize),
    View,
}

#[derive(Clone, Copy)]
struct IpcDictionaryShape {
    id: i64,
    index_width: usize,
}

#[derive(Clone, Copy)]
struct IpcFieldShape {
    value: IpcPhysicalShape,
    dictionary: Option<IpcDictionaryShape>,
}

fn invalid_ipc_shape(context: std::fmt::Arguments<'_>, detail: &str) -> DbError {
    DbError::Pipeline(format!("{context} IPC {detail}"))
}

// COMPAT: Keep the complete Arrow physical-type admission table in one exhaustive match. Splitting
// it would make it harder to audit which persisted IPC types fail closed and which exact buffer
// width each admitted type owns.
fn ipc_field_shape(
    field: arrow_ipc::Field<'_>,
    context: std::fmt::Arguments<'_>,
) -> Result<IpcFieldShape, DbError> {
    if field
        .children()
        .is_some_and(|children| !children.is_empty())
        || field
            .custom_metadata()
            .is_some_and(|metadata| !metadata.is_empty())
    {
        return Err(invalid_ipc_shape(
            context,
            "contains a nested or metadata-bearing field",
        ));
    }

    let missing_type = || invalid_ipc_shape(context, "field type header is missing");
    let value = match field.type_type() {
        arrow_ipc::Type::Null => {
            field.type_as_null().ok_or_else(missing_type)?;
            IpcPhysicalShape::Null
        }
        arrow_ipc::Type::Bool => {
            field.type_as_bool().ok_or_else(missing_type)?;
            IpcPhysicalShape::Bits
        }
        arrow_ipc::Type::Int => {
            let width = field.type_as_int().ok_or_else(missing_type)?.bitWidth();
            match width {
                8 => IpcPhysicalShape::Fixed(1),
                16 => IpcPhysicalShape::Fixed(2),
                32 => IpcPhysicalShape::Fixed(4),
                64 => IpcPhysicalShape::Fixed(8),
                _ => return Err(invalid_ipc_shape(context, "integer width is unsupported")),
            }
        }
        arrow_ipc::Type::FloatingPoint => {
            let float = field.type_as_floating_point().ok_or_else(missing_type)?;
            let width = match float.precision() {
                arrow_ipc::Precision::HALF => 2,
                arrow_ipc::Precision::SINGLE => 4,
                arrow_ipc::Precision::DOUBLE => 8,
                _ => {
                    return Err(invalid_ipc_shape(
                        context,
                        "floating-point precision is unsupported",
                    ));
                }
            };
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Binary => {
            field.type_as_binary().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(4)
        }
        arrow_ipc::Type::Utf8 => {
            field.type_as_utf_8().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(4)
        }
        arrow_ipc::Type::LargeBinary => {
            field.type_as_large_binary().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(8)
        }
        arrow_ipc::Type::LargeUtf8 => {
            field.type_as_large_utf_8().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(8)
        }
        arrow_ipc::Type::BinaryView => {
            field.type_as_binary_view().ok_or_else(missing_type)?;
            IpcPhysicalShape::View
        }
        arrow_ipc::Type::Utf8View => {
            field.type_as_utf_8_view().ok_or_else(missing_type)?;
            IpcPhysicalShape::View
        }
        arrow_ipc::Type::Decimal => {
            let decimal = field.type_as_decimal().ok_or_else(missing_type)?;
            let precision = u8::try_from(decimal.precision())
                .map_err(|_| invalid_ipc_shape(context, "decimal precision is invalid"))?;
            let scale = i8::try_from(decimal.scale())
                .map_err(|_| invalid_ipc_shape(context, "decimal scale is invalid"))?;
            let (width, maximum) = match decimal.bitWidth() {
                32 => (4, 9),
                64 => (8, 18),
                128 => (16, 38),
                256 => (32, 76),
                _ => return Err(invalid_ipc_shape(context, "decimal width is unsupported")),
            };
            if precision == 0
                || precision > maximum
                || scale > maximum.cast_signed()
                || (scale > 0 && scale.cast_unsigned() > precision)
            {
                return Err(invalid_ipc_shape(
                    context,
                    "decimal precision or scale is invalid",
                ));
            }
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Date => {
            let date = field.type_as_date().ok_or_else(missing_type)?;
            IpcPhysicalShape::Fixed(match date.unit() {
                arrow_ipc::DateUnit::DAY => 4,
                arrow_ipc::DateUnit::MILLISECOND => 8,
                _ => return Err(invalid_ipc_shape(context, "date unit is unsupported")),
            })
        }
        arrow_ipc::Type::Time => {
            let time = field.type_as_time().ok_or_else(missing_type)?;
            let width = match (time.bitWidth(), time.unit()) {
                (32, arrow_ipc::TimeUnit::SECOND | arrow_ipc::TimeUnit::MILLISECOND) => 4,
                (64, arrow_ipc::TimeUnit::MICROSECOND | arrow_ipc::TimeUnit::NANOSECOND) => 8,
                _ => return Err(invalid_ipc_shape(context, "time shape is unsupported")),
            };
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Timestamp => {
            let timestamp = field.type_as_timestamp().ok_or_else(missing_type)?;
            if !matches!(
                timestamp.unit(),
                arrow_ipc::TimeUnit::SECOND
                    | arrow_ipc::TimeUnit::MILLISECOND
                    | arrow_ipc::TimeUnit::MICROSECOND
                    | arrow_ipc::TimeUnit::NANOSECOND
            ) {
                return Err(invalid_ipc_shape(context, "timestamp unit is unsupported"));
            }
            IpcPhysicalShape::Fixed(8)
        }
        arrow_ipc::Type::Interval => {
            let interval = field.type_as_interval().ok_or_else(missing_type)?;
            IpcPhysicalShape::Fixed(match interval.unit() {
                arrow_ipc::IntervalUnit::YEAR_MONTH => 4,
                arrow_ipc::IntervalUnit::DAY_TIME => 8,
                arrow_ipc::IntervalUnit::MONTH_DAY_NANO => 16,
                _ => return Err(invalid_ipc_shape(context, "interval unit is unsupported")),
            })
        }
        arrow_ipc::Type::FixedSizeBinary => {
            let width = usize::try_from(
                field
                    .type_as_fixed_size_binary()
                    .ok_or_else(missing_type)?
                    .byteWidth(),
            )
            .map_err(|_| invalid_ipc_shape(context, "fixed-size binary width is invalid"))?;
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Duration => {
            let duration = field.type_as_duration().ok_or_else(missing_type)?;
            if !matches!(
                duration.unit(),
                arrow_ipc::TimeUnit::SECOND
                    | arrow_ipc::TimeUnit::MILLISECOND
                    | arrow_ipc::TimeUnit::MICROSECOND
                    | arrow_ipc::TimeUnit::NANOSECOND
            ) {
                return Err(invalid_ipc_shape(context, "duration unit is unsupported"));
            }
            IpcPhysicalShape::Fixed(8)
        }
        _ => {
            return Err(invalid_ipc_shape(
                context,
                "contains a nested or unsupported field",
            ));
        }
    };

    let dictionary = field
        .dictionary()
        .map(|dictionary| {
            if dictionary.id() < 0
                || dictionary.dictionaryKind() != arrow_ipc::DictionaryKind::DenseArray
            {
                return Err(invalid_ipc_shape(
                    context,
                    "dictionary encoding is unsupported",
                ));
            }
            let index = dictionary
                .indexType()
                .ok_or_else(|| invalid_ipc_shape(context, "dictionary index type is missing"))?;
            let index_width = match index.bitWidth() {
                8 => 1,
                16 => 2,
                32 => 4,
                64 => 8,
                _ => {
                    return Err(invalid_ipc_shape(
                        context,
                        "dictionary index width is unsupported",
                    ));
                }
            };
            Ok(IpcDictionaryShape {
                id: dictionary.id(),
                index_width,
            })
        })
        .transpose()?;

    Ok(IpcFieldShape { value, dictionary })
}

fn validate_ipc_schema(
    schema: arrow_ipc::Schema<'_>,
    minimum_columns: usize,
    maximum_columns: usize,
    context: std::fmt::Arguments<'_>,
) -> Result<(usize, usize), DbError> {
    if schema.endianness() != arrow_ipc::Endianness::Little {
        return Err(invalid_ipc_shape(
            context,
            "schema endianness is unsupported",
        ));
    }
    if schema
        .custom_metadata()
        .is_some_and(|metadata| !metadata.is_empty())
        || schema
            .features()
            .is_some_and(|features| !features.is_empty())
    {
        return Err(invalid_ipc_shape(
            context,
            "schema metadata or features are unsupported",
        ));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid_ipc_shape(context, "schema fields are missing"))?;
    let columns = fields.len();
    if columns < minimum_columns || columns > maximum_columns {
        return Err(DbError::Pipeline(format!(
            "{context} IPC has {columns} columns; expected {minimum_columns}..={maximum_columns}"
        )));
    }

    let mut dictionary_count = 0usize;
    for index in 0..columns {
        let shape = ipc_field_shape(fields.get(index), context)?;
        if let Some(dictionary) = shape.dictionary {
            for previous in 0..index {
                if ipc_field_shape(fields.get(previous), context)?
                    .dictionary
                    .is_some_and(|candidate| candidate.id == dictionary.id)
                {
                    return Err(invalid_ipc_shape(
                        context,
                        "contains duplicate dictionary ids",
                    ));
                }
            }
            dictionary_count = dictionary_count.checked_add(1).ok_or_else(|| {
                invalid_ipc_shape(context, "dictionary-count accounting overflow")
            })?;
        }
    }
    Ok((columns, dictionary_count))
}

fn nth_ipc_dictionary_field<'a>(
    schema: arrow_ipc::Schema<'a>,
    ordinal: usize,
    context: std::fmt::Arguments<'_>,
) -> Result<Option<arrow_ipc::Field<'a>>, DbError> {
    let fields = schema
        .fields()
        .ok_or_else(|| invalid_ipc_shape(context, "schema fields are missing"))?;
    let mut seen = 0usize;
    for field in fields {
        if ipc_field_shape(field, context)?.dictionary.is_some() {
            if seen == ordinal {
                return Ok(Some(field));
            }
            seen += 1;
        }
    }
    Ok(None)
}

fn align_ipc_body_offset(value: usize) -> Option<usize> {
    value.checked_add(63).map(|value| value & !63)
}

// WHY: `buffer_index`, `variadic_index`, and `shared_payload_bytes` advance through one canonical
// Arrow body roster. Keeping their transitions together makes missing, overlapping, or surplus
// buffers auditable and prevents helper boundaries from hiding cursor ownership.
fn validate_ipc_batch_layout<F>(
    batch: arrow_ipc::RecordBatch<'_>,
    expected_rows: usize,
    body_len: usize,
    columns: usize,
    mut shape_at: F,
    context: std::fmt::Arguments<'_>,
) -> Result<usize, DbError>
where
    F: FnMut(usize) -> Result<IpcPhysicalShape, DbError>,
{
    if batch.compression().is_some() {
        return Err(invalid_ipc_shape(context, "compression is unsupported"));
    }
    let rows = usize::try_from(batch.length())
        .map_err(|_| invalid_ipc_shape(context, "batch length is negative or too large"))?;
    if rows != expected_rows {
        return Err(DbError::Pipeline(format!(
            "{context} IPC has {rows} rows; expected {expected_rows}"
        )));
    }
    let nodes = batch
        .nodes()
        .ok_or_else(|| invalid_ipc_shape(context, "field nodes are missing"))?;
    if nodes.len() != columns {
        return Err(invalid_ipc_shape(
            context,
            "field-node count does not match the schema",
        ));
    }
    let buffers = batch
        .buffers()
        .ok_or_else(|| invalid_ipc_shape(context, "buffer descriptors are missing"))?;

    let mut cursor = 0usize;
    for buffer in buffers {
        let offset = usize::try_from(buffer.offset())
            .map_err(|_| invalid_ipc_shape(context, "buffer offset is invalid"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid_ipc_shape(context, "buffer length is invalid"))?;
        if offset != cursor {
            return Err(invalid_ipc_shape(
                context,
                "buffers overlap or are not canonically aligned",
            ));
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid_ipc_shape(context, "buffer extent overflows"))?;
        if end > body_len {
            return Err(invalid_ipc_shape(
                context,
                "buffer exceeds the message body",
            ));
        }
        cursor = align_ipc_body_offset(end)
            .ok_or_else(|| invalid_ipc_shape(context, "buffer alignment overflows"))?;
    }
    if cursor != body_len {
        return Err(invalid_ipc_shape(
            context,
            "buffer descriptors do not cover the canonical message body",
        ));
    }

    let mut view_count = 0usize;
    for index in 0..columns {
        if matches!(shape_at(index)?, IpcPhysicalShape::View) {
            view_count += 1;
        }
    }
    let variadic = batch.variadicBufferCounts();
    if view_count == 0 {
        if variadic.is_some_and(|counts| !counts.is_empty()) {
            return Err(invalid_ipc_shape(
                context,
                "has variadic counts without View fields",
            ));
        }
    } else if variadic.is_none_or(|counts| counts.len() != view_count) {
        return Err(invalid_ipc_shape(
            context,
            "View variadic-count roster is missing or malformed",
        ));
    }

    let validity_len = rows
        .checked_add(7)
        .ok_or_else(|| invalid_ipc_shape(context, "validity-buffer length overflows"))?
        / 8;
    let buffer_len = |index: usize| -> Result<usize, DbError> {
        if index >= buffers.len() {
            return Err(invalid_ipc_shape(
                context,
                "buffer count does not match the schema",
            ));
        }
        usize::try_from(buffers.get(index).length())
            .map_err(|_| invalid_ipc_shape(context, "buffer length is invalid"))
    };
    let expect_len = |index: usize, expected: usize| -> Result<(), DbError> {
        if buffer_len(index)? != expected {
            return Err(invalid_ipc_shape(
                context,
                "buffer length does not match the canonical field shape",
            ));
        }
        Ok(())
    };

    let mut buffer_index = 0usize;
    let mut variadic_index = 0usize;
    let mut shared_payload_bytes = 0usize;
    for column in 0..columns {
        let node = nodes.get(column);
        let node_rows = usize::try_from(node.length())
            .map_err(|_| invalid_ipc_shape(context, "field-node length is invalid"))?;
        let null_count = usize::try_from(node.null_count())
            .map_err(|_| invalid_ipc_shape(context, "field-node null count is invalid"))?;
        if node_rows != rows || null_count > rows {
            return Err(invalid_ipc_shape(
                context,
                "field-node length or null count is invalid",
            ));
        }

        match shape_at(column)? {
            IpcPhysicalShape::Null => {
                if null_count != rows {
                    return Err(invalid_ipc_shape(
                        context,
                        "Null field node has an invalid null count",
                    ));
                }
            }
            shape => {
                expect_len(buffer_index, validity_len)?;
                buffer_index += 1;
                match shape {
                    IpcPhysicalShape::Bits => {
                        expect_len(buffer_index, validity_len)?;
                        buffer_index += 1;
                    }
                    IpcPhysicalShape::Fixed(width) => {
                        let length = rows.checked_mul(width).ok_or_else(|| {
                            invalid_ipc_shape(context, "fixed-width buffer length overflows")
                        })?;
                        expect_len(buffer_index, length)?;
                        buffer_index += 1;
                    }
                    IpcPhysicalShape::Variable(offset_width) => {
                        // Arrow's canonical V5 writer emits a zero-length offsets buffer for an
                        // empty variable-width array; nonempty arrays retain the terminal offset.
                        let length = if rows == 0 {
                            0
                        } else {
                            rows.checked_add(1)
                                .and_then(|rows| rows.checked_mul(offset_width))
                                .ok_or_else(|| {
                                    invalid_ipc_shape(context, "offset-buffer length overflows")
                                })?
                        };
                        expect_len(buffer_index, length)?;
                        buffer_index += 2;
                        if buffer_index > buffers.len() {
                            return Err(invalid_ipc_shape(
                                context,
                                "variable field buffers are missing",
                            ));
                        }
                    }
                    IpcPhysicalShape::View => {
                        let length = rows.checked_mul(16).ok_or_else(|| {
                            invalid_ipc_shape(context, "View-buffer length overflows")
                        })?;
                        expect_len(buffer_index, length)?;
                        buffer_index += 1;
                        let counts = variadic.expect("View fields require variadic counts");
                        let count = usize::try_from(counts.get(variadic_index)).map_err(|_| {
                            invalid_ipc_shape(context, "View variadic count is invalid")
                        })?;
                        variadic_index += 1;
                        let end = buffer_index.checked_add(count).ok_or_else(|| {
                            invalid_ipc_shape(context, "View buffer count overflows")
                        })?;
                        if end > buffers.len() {
                            return Err(invalid_ipc_shape(
                                context,
                                "View variadic count exceeds the buffer roster",
                            ));
                        }
                        while buffer_index < end {
                            shared_payload_bytes = shared_payload_bytes
                                .checked_add(buffer_len(buffer_index)?)
                                .ok_or_else(|| {
                                    invalid_ipc_shape(context, "View payload accounting overflows")
                                })?;
                            buffer_index += 1;
                        }
                    }
                    IpcPhysicalShape::Null => unreachable!(),
                }
            }
        }
    }
    if buffer_index != buffers.len() || variadic_index != view_count {
        return Err(invalid_ipc_shape(
            context,
            "buffer count does not match the schema",
        ));
    }
    Ok(shared_payload_bytes)
}

// COMPAT: Canonical stream acceptance depends on visible schema -> dictionaries -> record-batch ->
// terminator ordering. The local state below deliberately stays chronological so a new message
// kind cannot be accepted without updating the same fail-closed protocol.
pub(super) fn preflight_scalar_ipc_restore(
    bytes: &[u8],
    expected_rows: usize,
    minimum_columns: usize,
    maximum_columns: usize,
    context: std::fmt::Arguments<'_>,
) -> Result<IpcRestorePreflight, DbError> {
    const CONTINUATION: u32 = u32::MAX;

    if bytes.is_empty() || minimum_columns == 0 || minimum_columns > maximum_columns {
        return Err(DbError::Pipeline(format!(
            "{context} IPC stream is empty or has an invalid expected shape"
        )));
    }
    let mut offset = 0usize;
    let mut schema = None;
    let mut columns = None;
    let mut dictionary_count = 0usize;
    let mut next_dictionary = 0usize;
    let mut rows = None;
    let mut dictionary_rows = 0usize;
    let mut dictionary_body_bytes = 0usize;
    let mut shared_payload_bytes = 0usize;
    loop {
        let prefix_end = offset
            .checked_add(4)
            .ok_or_else(|| invalid_ipc_shape(context, "framing overflows"))?;
        let prefix = bytes
            .get(offset..prefix_end)
            .ok_or_else(|| invalid_ipc_shape(context, "frame is truncated"))?;
        offset = prefix_end;
        if u32::from_le_bytes(prefix.try_into().expect("four-byte prefix")) != CONTINUATION {
            return Err(invalid_ipc_shape(
                context,
                "stream does not use canonical V5 continuation framing",
            ));
        }
        let length_end = offset
            .checked_add(4)
            .ok_or_else(|| invalid_ipc_shape(context, "framing overflows"))?;
        let length = bytes
            .get(offset..length_end)
            .ok_or_else(|| invalid_ipc_shape(context, "continuation is truncated"))?;
        offset = length_end;
        let metadata_len = usize::try_from(u32::from_le_bytes(
            length.try_into().expect("four-byte length"),
        ))
        .map_err(|_| invalid_ipc_shape(context, "metadata length exceeds usize"))?;
        if metadata_len == 0 {
            if offset != bytes.len()
                || schema.is_none()
                || rows.is_none()
                || next_dictionary != dictionary_count
            {
                return Err(invalid_ipc_shape(context, "stream is non-canonical"));
            }
            break;
        }
        if metadata_len
            .checked_add(8)
            .is_none_or(|length| length % 64 != 0)
        {
            return Err(invalid_ipc_shape(
                context,
                "metadata is not canonically aligned",
            ));
        }
        let metadata_end = offset
            .checked_add(metadata_len)
            .ok_or_else(|| invalid_ipc_shape(context, "metadata framing overflows"))?;
        let metadata = bytes
            .get(offset..metadata_end)
            .ok_or_else(|| invalid_ipc_shape(context, "metadata is truncated"))?;
        offset = metadata_end;
        let message = arrow_ipc::root_as_message(metadata).map_err(|error| {
            DbError::Pipeline(format!("{context} IPC metadata is invalid: {error}"))
        })?;
        if message.version() != arrow_ipc::MetadataVersion::V5 {
            return Err(invalid_ipc_shape(context, "metadata version is not V5"));
        }
        let body_len = usize::try_from(message.bodyLength())
            .map_err(|_| invalid_ipc_shape(context, "body length is negative or too large"))?;
        if body_len % 64 != 0 {
            return Err(invalid_ipc_shape(
                context,
                "body is not canonically aligned",
            ));
        }
        let body_end = offset
            .checked_add(body_len)
            .ok_or_else(|| invalid_ipc_shape(context, "body framing overflows"))?;
        if body_end > bytes.len() {
            return Err(invalid_ipc_shape(context, "body is truncated"));
        }

        match message.header_type() {
            arrow_ipc::MessageHeader::Schema if schema.is_none() && rows.is_none() => {
                if body_len != 0 {
                    return Err(invalid_ipc_shape(context, "schema message has a body"));
                }
                let parsed = message
                    .header_as_schema()
                    .ok_or_else(|| invalid_ipc_shape(context, "schema header is missing"))?;
                let (field_count, dictionaries) =
                    validate_ipc_schema(parsed, minimum_columns, maximum_columns, context)?;
                schema = Some(parsed);
                columns = Some(field_count);
                dictionary_count = dictionaries;
            }
            arrow_ipc::MessageHeader::DictionaryBatch if schema.is_some() && rows.is_none() => {
                let parsed_schema = schema.expect("schema is present");
                let field = nth_ipc_dictionary_field(parsed_schema, next_dictionary, context)?
                    .ok_or_else(|| {
                        invalid_ipc_shape(context, "has an unexpected dictionary batch")
                    })?;
                let field_shape = ipc_field_shape(field, context)?;
                let expected_dictionary = field_shape
                    .dictionary
                    .expect("dictionary field has dictionary encoding");
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                    invalid_ipc_shape(context, "dictionary-batch header is missing")
                })?;
                if dictionary.id() != expected_dictionary.id || dictionary.isDelta() {
                    return Err(invalid_ipc_shape(
                        context,
                        "dictionary roster, order, or replacement mode is non-canonical",
                    ));
                }
                let data = dictionary.data().ok_or_else(|| {
                    invalid_ipc_shape(context, "dictionary-batch data is missing")
                })?;
                let batch_rows = usize::try_from(data.length()).map_err(|_| {
                    invalid_ipc_shape(context, "dictionary-batch length is invalid")
                })?;
                let shared = validate_ipc_batch_layout(
                    data,
                    batch_rows,
                    body_len,
                    1,
                    |_| Ok(field_shape.value),
                    context,
                )?;
                shared_payload_bytes =
                    shared_payload_bytes.checked_add(shared).ok_or_else(|| {
                        invalid_ipc_shape(context, "shared-buffer accounting overflows")
                    })?;
                dictionary_body_bytes =
                    dictionary_body_bytes.checked_add(body_len).ok_or_else(|| {
                        invalid_ipc_shape(context, "dictionary body accounting overflows")
                    })?;
                dictionary_rows = dictionary_rows.checked_add(batch_rows).ok_or_else(|| {
                    invalid_ipc_shape(context, "dictionary row accounting overflows")
                })?;
                next_dictionary += 1;
            }
            arrow_ipc::MessageHeader::RecordBatch
                if schema.is_some() && rows.is_none() && next_dictionary == dictionary_count =>
            {
                let batch = message
                    .header_as_record_batch()
                    .ok_or_else(|| invalid_ipc_shape(context, "record-batch header is missing"))?;
                let parsed_schema = schema.expect("schema is present");
                let fields = parsed_schema.fields().expect("validated schema has fields");
                let shared = validate_ipc_batch_layout(
                    batch,
                    expected_rows,
                    body_len,
                    columns.expect("schema column count is present"),
                    |index| {
                        let shape = ipc_field_shape(fields.get(index), context)?;
                        Ok(shape.dictionary.map_or(shape.value, |dictionary| {
                            IpcPhysicalShape::Fixed(dictionary.index_width)
                        }))
                    },
                    context,
                )?;
                shared_payload_bytes =
                    shared_payload_bytes.checked_add(shared).ok_or_else(|| {
                        invalid_ipc_shape(context, "shared-buffer accounting overflows")
                    })?;
                rows = Some(expected_rows);
            }
            _ => {
                return Err(invalid_ipc_shape(context, "message order is non-canonical"));
            }
        }
        offset = body_end;
    }
    Ok(IpcRestorePreflight {
        rows: rows.expect("canonical stream has one record batch"),
        columns: columns.expect("canonical stream has one schema"),
        dictionary_rows,
        dictionary_body_bytes,
        shared_payload_bytes,
    })
}
