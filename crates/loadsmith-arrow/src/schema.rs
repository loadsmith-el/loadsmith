//! Exact conversion between Arrow schemas and the protocol's field-type
//! vocabulary ([`loadsmith_protocol::FieldType`]).
//!
//! Arrow → protocol is total: every Arrow type in the vocabulary maps to its
//! exact protocol type (unit, time zone, precision, nullability, children),
//! and the few Arrow types outside it (`union`, `run_end_encoded`,
//! `list_view`, `large_list_view`) map to [`FieldType::Unknown`] with that
//! name. Field metadata is not carried.
//!
//! Protocol → Arrow is the inverse for every known type; an unknown type has
//! no Arrow equivalent.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, IntervalUnit as AIntervalUnit, Schema, TimeUnit as ATimeUnit};
use loadsmith_protocol::{Field as ProtocolField, FieldType, IntervalUnit, TimeUnit};

/// The protocol fields describing `schema`, exactly.
pub fn protocol_fields_from_schema(schema: &Schema) -> Vec<ProtocolField> {
    schema.fields().iter().map(|f| protocol_field_from_arrow(f)).collect()
}

/// The protocol form of one Arrow field.
pub fn protocol_field_from_arrow(field: &Field) -> ProtocolField {
    ProtocolField {
        name: field.name().clone(),
        field_type: protocol_type_from_arrow(field.data_type()),
        nullable: field.is_nullable(),
    }
}

fn unit_from_arrow(unit: &ATimeUnit) -> TimeUnit {
    match unit {
        ATimeUnit::Second => TimeUnit::Second,
        ATimeUnit::Millisecond => TimeUnit::Millisecond,
        ATimeUnit::Microsecond => TimeUnit::Microsecond,
        ATimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn unit_to_arrow(unit: TimeUnit) -> ATimeUnit {
    match unit {
        TimeUnit::Second => ATimeUnit::Second,
        TimeUnit::Millisecond => ATimeUnit::Millisecond,
        TimeUnit::Microsecond => ATimeUnit::Microsecond,
        TimeUnit::Nanosecond => ATimeUnit::Nanosecond,
    }
}

fn boxed(field: &Field) -> Box<ProtocolField> {
    Box::new(protocol_field_from_arrow(field))
}

/// The protocol type of an Arrow data type (see the module docs).
pub fn protocol_type_from_arrow(dt: &DataType) -> FieldType {
    match dt {
        DataType::Null => FieldType::Null,
        DataType::Boolean => FieldType::Bool,
        DataType::Int8 => FieldType::Int8,
        DataType::Int16 => FieldType::Int16,
        DataType::Int32 => FieldType::Int32,
        DataType::Int64 => FieldType::Int64,
        DataType::UInt8 => FieldType::UInt8,
        DataType::UInt16 => FieldType::UInt16,
        DataType::UInt32 => FieldType::UInt32,
        DataType::UInt64 => FieldType::UInt64,
        DataType::Float16 => FieldType::Float16,
        DataType::Float32 => FieldType::Float32,
        DataType::Float64 => FieldType::Float64,
        DataType::Decimal128(p, s) => FieldType::Decimal128 { precision: *p, scale: *s },
        DataType::Decimal256(p, s) => FieldType::Decimal256 { precision: *p, scale: *s },
        DataType::Utf8 => FieldType::Utf8,
        DataType::LargeUtf8 => FieldType::LargeUtf8,
        DataType::Utf8View => FieldType::Utf8View,
        DataType::Binary => FieldType::Binary,
        DataType::LargeBinary => FieldType::LargeBinary,
        DataType::BinaryView => FieldType::BinaryView,
        DataType::FixedSizeBinary(w) => FieldType::FixedSizeBinary { byte_width: *w },
        DataType::Date32 => FieldType::Date32,
        DataType::Date64 => FieldType::Date64,
        DataType::Time32(u) => FieldType::Time32 { unit: unit_from_arrow(u) },
        DataType::Time64(u) => FieldType::Time64 { unit: unit_from_arrow(u) },
        DataType::Timestamp(u, tz) => FieldType::Timestamp {
            unit: unit_from_arrow(u),
            timezone: tz.as_ref().map(|t| t.to_string()),
        },
        DataType::Duration(u) => FieldType::Duration { unit: unit_from_arrow(u) },
        DataType::Interval(u) => FieldType::Interval {
            unit: match u {
                AIntervalUnit::YearMonth => IntervalUnit::YearMonth,
                AIntervalUnit::DayTime => IntervalUnit::DayTime,
                AIntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
            },
        },
        DataType::List(item) => FieldType::List { item: boxed(item) },
        DataType::LargeList(item) => FieldType::LargeList { item: boxed(item) },
        DataType::FixedSizeList(item, size) => FieldType::FixedSizeList { item: boxed(item), size: *size },
        DataType::Struct(fields) => FieldType::Struct {
            fields: fields.iter().map(|f| protocol_field_from_arrow(f)).collect(),
        },
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => FieldType::Map {
                key: boxed(&kv[0]),
                value: boxed(&kv[1]),
                keys_sorted: *sorted,
            },
            _ => FieldType::Unknown("map".into()),
        },
        DataType::Dictionary(index, value) => FieldType::Dictionary {
            index: Box::new(protocol_type_from_arrow(index)),
            value: Box::new(protocol_type_from_arrow(value)),
        },
        DataType::Union(..) => FieldType::Unknown("union".into()),
        DataType::RunEndEncoded(..) => FieldType::Unknown("run_end_encoded".into()),
        DataType::ListView(_) => FieldType::Unknown("list_view".into()),
        DataType::LargeListView(_) => FieldType::Unknown("large_list_view".into()),
    }
}

/// The Arrow field for a protocol field; `None` if its type (or a child's)
/// is unknown.
pub fn arrow_field_from_protocol(field: &ProtocolField) -> Option<Field> {
    Some(Field::new(field.name.as_str(), arrow_type_from_protocol(&field.field_type)?, field.nullable))
}

fn arrow_child(field: &ProtocolField) -> Option<Arc<Field>> {
    arrow_field_from_protocol(field).map(Arc::new)
}

/// The Arrow data type for a protocol type; `None` for an unknown type.
pub fn arrow_type_from_protocol(ft: &FieldType) -> Option<DataType> {
    Some(match ft {
        FieldType::Null => DataType::Null,
        FieldType::Bool => DataType::Boolean,
        FieldType::Int8 => DataType::Int8,
        FieldType::Int16 => DataType::Int16,
        FieldType::Int32 => DataType::Int32,
        FieldType::Int64 => DataType::Int64,
        FieldType::UInt8 => DataType::UInt8,
        FieldType::UInt16 => DataType::UInt16,
        FieldType::UInt32 => DataType::UInt32,
        FieldType::UInt64 => DataType::UInt64,
        FieldType::Float16 => DataType::Float16,
        FieldType::Float32 => DataType::Float32,
        FieldType::Float64 => DataType::Float64,
        FieldType::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
        FieldType::Decimal256 { precision, scale } => DataType::Decimal256(*precision, *scale),
        FieldType::Utf8 => DataType::Utf8,
        FieldType::LargeUtf8 => DataType::LargeUtf8,
        FieldType::Utf8View => DataType::Utf8View,
        FieldType::Binary => DataType::Binary,
        FieldType::LargeBinary => DataType::LargeBinary,
        FieldType::BinaryView => DataType::BinaryView,
        FieldType::FixedSizeBinary { byte_width } => DataType::FixedSizeBinary(*byte_width),
        FieldType::Date32 => DataType::Date32,
        FieldType::Date64 => DataType::Date64,
        FieldType::Time32 { unit } => DataType::Time32(unit_to_arrow(*unit)),
        FieldType::Time64 { unit } => DataType::Time64(unit_to_arrow(*unit)),
        FieldType::Timestamp { unit, timezone } => {
            DataType::Timestamp(unit_to_arrow(*unit), timezone.as_deref().map(Arc::from))
        }
        FieldType::Duration { unit } => DataType::Duration(unit_to_arrow(*unit)),
        FieldType::Interval { unit } => DataType::Interval(match unit {
            IntervalUnit::YearMonth => AIntervalUnit::YearMonth,
            IntervalUnit::DayTime => AIntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => AIntervalUnit::MonthDayNano,
        }),
        FieldType::List { item } => DataType::List(arrow_child(item)?),
        FieldType::LargeList { item } => DataType::LargeList(arrow_child(item)?),
        FieldType::FixedSizeList { item, size } => DataType::FixedSizeList(arrow_child(item)?, *size),
        FieldType::Struct { fields } => DataType::Struct(
            fields.iter().map(arrow_field_from_protocol).collect::<Option<Vec<_>>>()?.into(),
        ),
        FieldType::Map { key, value, keys_sorted } => {
            let entries = Fields::from(vec![arrow_field_from_protocol(key)?, arrow_field_from_protocol(value)?]);
            DataType::Map(Arc::new(Field::new("entries", DataType::Struct(entries), false)), *keys_sorted)
        }
        FieldType::Dictionary { index, value } => DataType::Dictionary(
            Box::new(arrow_type_from_protocol(index)?),
            Box::new(arrow_type_from_protocol(value)?),
        ),
        FieldType::Unknown(_) => return None,
    })
}

/// Converts protocol field definitions to an Arrow schema. A field whose type
/// is unknown has no Arrow equivalent and is typed `Null` (the IPC stream
/// header, not the `schema` message, is the authoritative schema).
pub fn schema_from_protocol_fields(fields: &[ProtocolField]) -> Schema {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            arrow_field_from_protocol(f)
                .unwrap_or_else(|| Field::new(f.name.as_str(), DataType::Null, true))
        })
        .collect();
    Schema::new(arrow_fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> Schema {
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let kv = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]);
        Schema::new(vec![
            Field::new("null", DataType::Null, true),
            Field::new("b", DataType::Boolean, false),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, false),
            Field::new("u8", DataType::UInt8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f16", DataType::Float16, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("dec", DataType::Decimal128(38, 9), true),
            Field::new("dec256", DataType::Decimal256(76, -2), true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeUtf8, true),
            Field::new("vs", DataType::Utf8View, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("lbin", DataType::LargeBinary, true),
            Field::new("vbin", DataType::BinaryView, true),
            Field::new("fsb", DataType::FixedSizeBinary(16), true),
            Field::new("d32", DataType::Date32, true),
            Field::new("d64", DataType::Date64, true),
            Field::new("t32", DataType::Time32(ATimeUnit::Millisecond), true),
            Field::new("t64", DataType::Time64(ATimeUnit::Nanosecond), true),
            Field::new("ts", DataType::Timestamp(ATimeUnit::Microsecond, None), true),
            Field::new("tstz", DataType::Timestamp(ATimeUnit::Second, Some("UTC".into())), true),
            Field::new("dur", DataType::Duration(ATimeUnit::Millisecond), true),
            Field::new("iv", DataType::Interval(AIntervalUnit::DayTime), true),
            Field::new("list", DataType::List(item.clone()), true),
            Field::new("llist", DataType::LargeList(item.clone()), true),
            Field::new("fsl", DataType::FixedSizeList(item.clone(), 3), true),
            Field::new(
                "st",
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)])),
                true,
            ),
            Field::new(
                "map",
                DataType::Map(Arc::new(Field::new("entries", DataType::Struct(kv), false)), false),
                true,
            ),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ])
    }

    #[test]
    fn every_vocabulary_type_round_trips_exactly() {
        let schema = sample_schema();
        let fields = protocol_fields_from_schema(&schema);
        assert!(fields.iter().all(|f| !f.field_type.is_unknown()), "{fields:?}");
        let back = schema_from_protocol_fields(&fields);
        assert_eq!(back, schema);
    }

    #[test]
    fn units_timezones_and_nullability_are_exact() {
        let f = protocol_field_from_arrow(&Field::new(
            "ts",
            DataType::Timestamp(ATimeUnit::Nanosecond, Some("+02:00".into())),
            false,
        ));
        assert_eq!(
            f.field_type,
            FieldType::Timestamp { unit: TimeUnit::Nanosecond, timezone: Some("+02:00".into()) }
        );
        assert!(!f.nullable);
        // A list is a list, not utf8 (the old SDK fallback).
        let l = protocol_type_from_arrow(&DataType::List(Arc::new(Field::new("item", DataType::Int64, true))));
        assert!(matches!(l, FieldType::List { .. }));
    }

    #[test]
    fn types_outside_the_vocabulary_are_unknown() {
        let t = protocol_type_from_arrow(&DataType::ListView(Arc::new(Field::new("item", DataType::Int64, true))));
        assert_eq!(t, FieldType::Unknown("list_view".into()));
        assert!(arrow_type_from_protocol(&t).is_none());
        let s = schema_from_protocol_fields(&[ProtocolField::new("x", FieldType::Unknown("geo".into()))]);
        assert_eq!(s.field(0).data_type(), &DataType::Null);
    }
}
