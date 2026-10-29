//! The field-type vocabulary of the `schema` message (protocol v1).
//!
//! A field is a JSON object. `name` and `type` are always present; `type` is a
//! string from the vocabulary below, and the type's parameters sit beside it
//! in the same object. `nullable` is optional and defaults to `true`:
//!
//! ```json
//! {"name": "id",      "type": "int64", "nullable": false}
//! {"name": "amount",  "type": "decimal128", "precision": 38, "scale": 9, "nullable": true}
//! {"name": "seen_at", "type": "timestamp", "unit": "us", "timezone": "UTC", "nullable": true}
//! {"name": "tags",    "type": "list", "item": {"name": "item", "type": "utf8", "nullable": true}, "nullable": true}
//! ```
//!
//! A child that is a field (a list item, a struct member, a map key or value)
//! is a full field object; a child that is only a type (a dictionary's index
//! and value) is a *type object*: a field object without `name` and
//! `nullable`.
//!
//! **Tolerant reader.** The `schema` message is informational: the
//! authoritative schema is the Arrow IPC stream header on fd3. So a receiver
//! never fails on a field it cannot interpret. A `type` it does not know, or a
//! known type whose parameters are missing or invalid, is read as
//! [`FieldType::Unknown`] carrying the raw type string. Unknown extra keys are
//! ignored. Only a field without a string `name` or `type` is malformed.
//!
//! **Older plugins.** Every type string sent by an older SDK stays valid; the
//! one renamed type, `timestamp_ms`, is read as `timestamp` with unit `ms` and
//! no time zone.

use serde::de::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

/// One column (or a nested child field) of a schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    /// Whether the column may hold nulls. Absent on the wire ⇒ `true`.
    pub nullable: bool,
}

impl Field {
    /// A nullable field.
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self { name: name.into(), field_type, nullable: true }
    }

    /// The same field with `nullable` set.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

/// Time resolution of `time32`, `time64`, `timestamp` and `duration`.
/// Wire values: `s`, `ms`, `us`, `ns`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnit {
    pub fn as_str(self) -> &'static str {
        match self {
            TimeUnit::Second => "s",
            TimeUnit::Millisecond => "ms",
            TimeUnit::Microsecond => "us",
            TimeUnit::Nanosecond => "ns",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "s" => TimeUnit::Second,
            "ms" => TimeUnit::Millisecond,
            "us" => TimeUnit::Microsecond,
            "ns" => TimeUnit::Nanosecond,
            _ => return None,
        })
    }
}

/// Layout of an `interval`. Wire values: `year_month`, `day_time`,
/// `month_day_nano`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

impl IntervalUnit {
    pub fn as_str(self) -> &'static str {
        match self {
            IntervalUnit::YearMonth => "year_month",
            IntervalUnit::DayTime => "day_time",
            IntervalUnit::MonthDayNano => "month_day_nano",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "year_month" => IntervalUnit::YearMonth,
            "day_time" => IntervalUnit::DayTime,
            "month_day_nano" => IntervalUnit::MonthDayNano,
            _ => return None,
        })
    }
}

/// The type of a field: Arrow's logical types, named in `snake_case`.
///
/// | `type` | Parameters |
/// |---|---|
/// | `null`, `bool` | — |
/// | `int8` `int16` `int32` `int64` `uint8` `uint16` `uint32` `uint64` | — |
/// | `float16` `float32` `float64` | — |
/// | `decimal128`, `decimal256` | `precision` (1–38 / 1–76), `scale` (may be negative) |
/// | `utf8` `large_utf8` `utf8_view` | — |
/// | `binary` `large_binary` `binary_view` | — |
/// | `fixed_size_binary` | `byte_width` |
/// | `date32` (days), `date64` (ms) | — |
/// | `time32` | `unit`: `s` or `ms` |
/// | `time64` | `unit`: `us` or `ns` |
/// | `timestamp` | `unit`, optional `timezone` (IANA name or `±HH:MM`; absent ⇒ no zone) |
/// | `duration` | `unit` |
/// | `interval` | `unit`: `year_month`, `day_time`, `month_day_nano` |
/// | `list`, `large_list` | `item` (field) |
/// | `fixed_size_list` | `item` (field), `size` |
/// | `struct` | `fields` (array of fields) |
/// | `map` | `key` (field), `value` (field), `keys_sorted` (bool, default false) |
/// | `dictionary` | `index` (type object, an integer type), `value` (type object) |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Decimal32 { precision: u8, scale: i8 },
    Decimal64 { precision: u8, scale: i8 },
    Decimal128 { precision: u8, scale: i8 },
    Decimal256 { precision: u8, scale: i8 },
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    LargeBinary,
    BinaryView,
    FixedSizeBinary { byte_width: i32 },
    Date32,
    Date64,
    Time32 { unit: TimeUnit },
    Time64 { unit: TimeUnit },
    Timestamp { unit: TimeUnit, timezone: Option<String> },
    Duration { unit: TimeUnit },
    Interval { unit: IntervalUnit },
    List { item: Box<Field> },
    LargeList { item: Box<Field> },
    FixedSizeList { item: Box<Field>, size: i32 },
    Struct { fields: Vec<Field> },
    Map { key: Box<Field>, value: Box<Field>, keys_sorted: bool },
    Dictionary { index: Box<FieldType>, value: Box<FieldType> },
    /// A type this build cannot interpret: a type string from a newer
    /// vocabulary (or an Arrow type outside it, such as `union`), or a known
    /// type with missing or invalid parameters. Carries the raw `type` string.
    /// Serializes back as just `{"type": <name>}`.
    Unknown(String),
}

impl FieldType {
    /// Millisecond timestamp without a time zone — what `timestamp_ms` meant.
    pub fn timestamp_ms() -> Self {
        FieldType::Timestamp { unit: TimeUnit::Millisecond, timezone: None }
    }

    /// The wire `type` string.
    pub fn type_name(&self) -> &str {
        match self {
            FieldType::Null => "null",
            FieldType::Bool => "bool",
            FieldType::Int8 => "int8",
            FieldType::Int16 => "int16",
            FieldType::Int32 => "int32",
            FieldType::Int64 => "int64",
            FieldType::UInt8 => "uint8",
            FieldType::UInt16 => "uint16",
            FieldType::UInt32 => "uint32",
            FieldType::UInt64 => "uint64",
            FieldType::Float16 => "float16",
            FieldType::Float32 => "float32",
            FieldType::Float64 => "float64",
            FieldType::Decimal32 { .. } => "decimal32",
            FieldType::Decimal64 { .. } => "decimal64",
            FieldType::Decimal128 { .. } => "decimal128",
            FieldType::Decimal256 { .. } => "decimal256",
            FieldType::Utf8 => "utf8",
            FieldType::LargeUtf8 => "large_utf8",
            FieldType::Utf8View => "utf8_view",
            FieldType::Binary => "binary",
            FieldType::LargeBinary => "large_binary",
            FieldType::BinaryView => "binary_view",
            FieldType::FixedSizeBinary { .. } => "fixed_size_binary",
            FieldType::Date32 => "date32",
            FieldType::Date64 => "date64",
            FieldType::Time32 { .. } => "time32",
            FieldType::Time64 { .. } => "time64",
            FieldType::Timestamp { .. } => "timestamp",
            FieldType::Duration { .. } => "duration",
            FieldType::Interval { .. } => "interval",
            FieldType::List { .. } => "list",
            FieldType::LargeList { .. } => "large_list",
            FieldType::FixedSizeList { .. } => "fixed_size_list",
            FieldType::Struct { .. } => "struct",
            FieldType::Map { .. } => "map",
            FieldType::Dictionary { .. } => "dictionary",
            FieldType::Unknown(name) => name,
        }
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, FieldType::Unknown(_))
    }

    /// Writes this type's `type` key and parameters into a map being
    /// serialized, in a fixed, readable order.
    fn serialize_entries<M: SerializeMap>(&self, map: &mut M) -> Result<(), M::Error> {
        map.serialize_entry("type", self.type_name())?;
        match self {
            FieldType::Decimal32 { precision, scale }
            | FieldType::Decimal64 { precision, scale }
            | FieldType::Decimal128 { precision, scale }
            | FieldType::Decimal256 { precision, scale } => {
                map.serialize_entry("precision", precision)?;
                map.serialize_entry("scale", scale)?;
            }
            FieldType::FixedSizeBinary { byte_width } => map.serialize_entry("byte_width", byte_width)?,
            FieldType::Time32 { unit } | FieldType::Time64 { unit } | FieldType::Duration { unit } => {
                map.serialize_entry("unit", unit.as_str())?;
            }
            FieldType::Timestamp { unit, timezone } => {
                map.serialize_entry("unit", unit.as_str())?;
                if let Some(tz) = timezone {
                    map.serialize_entry("timezone", tz)?;
                }
            }
            FieldType::Interval { unit } => map.serialize_entry("unit", unit.as_str())?,
            FieldType::List { item } | FieldType::LargeList { item } => {
                map.serialize_entry("item", item.as_ref())?;
            }
            FieldType::FixedSizeList { item, size } => {
                map.serialize_entry("item", item.as_ref())?;
                map.serialize_entry("size", size)?;
            }
            FieldType::Struct { fields } => map.serialize_entry("fields", fields)?,
            FieldType::Map { key, value, keys_sorted } => {
                map.serialize_entry("key", key.as_ref())?;
                map.serialize_entry("value", value.as_ref())?;
                map.serialize_entry("keys_sorted", keys_sorted)?;
            }
            FieldType::Dictionary { index, value } => {
                map.serialize_entry("index", &TypeObject(index))?;
                map.serialize_entry("value", &TypeObject(value))?;
            }
            _ => {}
        }
        Ok(())
    }

    /// This type as a type object (`{"type": …, params}`).
    pub fn to_type_json(&self) -> Value {
        serde_json::to_value(TypeObject(self)).unwrap_or(Value::Null)
    }

    /// Reads a type from the object that holds its `type` key. `Err` only when
    /// `type` is missing or not a string; anything else it cannot interpret
    /// becomes [`FieldType::Unknown`].
    pub fn from_type_json(obj: &Map<String, Value>) -> Result<Self, String> {
        let name = match obj.get("type") {
            Some(Value::String(s)) => s.as_str(),
            Some(_) => return Err("field `type` is not a string".into()),
            None => return Err("missing field `type`".into()),
        };
        Ok(parse_type(name, obj).unwrap_or_else(|| FieldType::Unknown(name.to_string())))
    }
}

fn get_u64(obj: &Map<String, Value>, key: &str) -> Option<u64> {
    obj.get(key).and_then(Value::as_u64)
}

fn get_i64(obj: &Map<String, Value>, key: &str) -> Option<i64> {
    obj.get(key).and_then(Value::as_i64)
}

fn get_unit(obj: &Map<String, Value>) -> Option<TimeUnit> {
    obj.get("unit").and_then(Value::as_str).and_then(TimeUnit::parse)
}

fn get_field(obj: &Map<String, Value>, key: &str) -> Option<Box<Field>> {
    Field::from_json(obj.get(key)?).ok().map(Box::new)
}

fn get_type(obj: &Map<String, Value>, key: &str) -> Option<Box<FieldType>> {
    FieldType::from_type_json(obj.get(key)?.as_object()?).ok().map(Box::new)
}

/// `None` ⇒ unknown type name or unusable parameters.
fn parse_type(name: &str, obj: &Map<String, Value>) -> Option<FieldType> {
    let decimal = |max: u64| -> Option<(u8, i8)> {
        let precision = get_u64(obj, "precision").filter(|p| (1..=max).contains(p))?;
        let scale = i8::try_from(get_i64(obj, "scale").unwrap_or(0)).ok()?;
        Some((precision as u8, scale))
    };
    Some(match name {
        "null" => FieldType::Null,
        "bool" => FieldType::Bool,
        "int8" => FieldType::Int8,
        "int16" => FieldType::Int16,
        "int32" => FieldType::Int32,
        "int64" => FieldType::Int64,
        "uint8" => FieldType::UInt8,
        "uint16" => FieldType::UInt16,
        "uint32" => FieldType::UInt32,
        "uint64" => FieldType::UInt64,
        "float16" => FieldType::Float16,
        "float32" => FieldType::Float32,
        "float64" => FieldType::Float64,
        "decimal128" => {
            let (precision, scale) = decimal(38)?;
            FieldType::Decimal128 { precision, scale }
        }
        "decimal32" => {
            let (precision, scale) = decimal(9)?;
            FieldType::Decimal32 { precision, scale }
        }
        "decimal64" => {
            let (precision, scale) = decimal(18)?;
            FieldType::Decimal64 { precision, scale }
        }
        "decimal256" => {
            let (precision, scale) = decimal(76)?;
            FieldType::Decimal256 { precision, scale }
        }
        "utf8" => FieldType::Utf8,
        "large_utf8" => FieldType::LargeUtf8,
        "utf8_view" => FieldType::Utf8View,
        "binary" => FieldType::Binary,
        "large_binary" => FieldType::LargeBinary,
        "binary_view" => FieldType::BinaryView,
        "fixed_size_binary" => FieldType::FixedSizeBinary {
            byte_width: i32::try_from(get_u64(obj, "byte_width")?).ok()?,
        },
        "date32" => FieldType::Date32,
        "date64" => FieldType::Date64,
        "time32" => match get_unit(obj)? {
            unit @ (TimeUnit::Second | TimeUnit::Millisecond) => FieldType::Time32 { unit },
            _ => return None,
        },
        "time64" => match get_unit(obj)? {
            unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond) => FieldType::Time64 { unit },
            _ => return None,
        },
        "timestamp" => FieldType::Timestamp {
            unit: get_unit(obj)?,
            timezone: match obj.get("timezone") {
                None | Some(Value::Null) => None,
                Some(Value::String(tz)) => Some(tz.clone()),
                Some(_) => return None,
            },
        },
        // Protocol ≤ 0.2 name, kept for older plugins.
        "timestamp_ms" => FieldType::timestamp_ms(),
        "duration" => FieldType::Duration { unit: get_unit(obj)? },
        "interval" => FieldType::Interval {
            unit: obj.get("unit").and_then(Value::as_str).and_then(IntervalUnit::parse)?,
        },
        "list" => FieldType::List { item: get_field(obj, "item")? },
        "large_list" => FieldType::LargeList { item: get_field(obj, "item")? },
        "fixed_size_list" => FieldType::FixedSizeList {
            item: get_field(obj, "item")?,
            size: i32::try_from(get_u64(obj, "size")?).ok()?,
        },
        "struct" => FieldType::Struct {
            fields: obj
                .get("fields")?
                .as_array()?
                .iter()
                .map(Field::from_json)
                .collect::<Result<_, _>>()
                .ok()?,
        },
        "map" => FieldType::Map {
            key: get_field(obj, "key")?,
            value: get_field(obj, "value")?,
            keys_sorted: obj.get("keys_sorted").and_then(Value::as_bool).unwrap_or(false),
        },
        "dictionary" => FieldType::Dictionary {
            index: get_type(obj, "index")?,
            value: get_type(obj, "value")?,
        },
        _ => return None,
    })
}

impl Field {
    /// The wire form: `{"name", "type", params…, "nullable"}`.
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Reads a field object (tolerantly; see the module docs).
    pub fn from_json(value: &Value) -> Result<Self, String> {
        let obj = value.as_object().ok_or("a field must be a JSON object")?;
        let name = match obj.get("name") {
            Some(Value::String(s)) => s.clone(),
            Some(_) => return Err("field `name` is not a string".into()),
            None => return Err("missing field `name`".into()),
        };
        let field_type = FieldType::from_type_json(obj)?;
        let nullable = obj.get("nullable").and_then(Value::as_bool).unwrap_or(true);
        Ok(Field { name, field_type, nullable })
    }
}

impl Serialize for Field {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        map.serialize_entry("name", &self.name)?;
        self.field_type.serialize_entries(&mut map)?;
        map.serialize_entry("nullable", &self.nullable)?;
        map.end()
    }
}

/// Serializes a [`FieldType`] alone, as a type object.
struct TypeObject<'a>(&'a FieldType);

impl Serialize for TypeObject<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        self.0.serialize_entries(&mut map)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        Field::from_json(&value).map_err(D::Error::custom)
    }
}

/// Compact, human-readable form used in the run log, e.g. `int64`,
/// `timestamp(us, UTC)`, `decimal128(38, 9)`, `list<utf8>`,
/// `struct<a: int32, b: utf8?>`. A nullable child is marked `?`; an unknown
/// type is shown as `<name> (unknown)`.
impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let child = |field: &Field| {
            format!("{}{}", field.field_type, if field.nullable { "?" } else { "" })
        };
        match self {
            FieldType::Decimal32 { precision, scale }
            | FieldType::Decimal64 { precision, scale }
            | FieldType::Decimal128 { precision, scale }
            | FieldType::Decimal256 { precision, scale } => {
                write!(f, "{}({precision}, {scale})", self.type_name())
            }
            FieldType::FixedSizeBinary { byte_width } => write!(f, "fixed_size_binary({byte_width})"),
            FieldType::Time32 { unit } | FieldType::Time64 { unit } | FieldType::Duration { unit } => {
                write!(f, "{}({})", self.type_name(), unit.as_str())
            }
            FieldType::Timestamp { unit, timezone: None } => write!(f, "timestamp({})", unit.as_str()),
            FieldType::Timestamp { unit, timezone: Some(tz) } => {
                write!(f, "timestamp({}, {tz})", unit.as_str())
            }
            FieldType::Interval { unit } => write!(f, "interval({})", unit.as_str()),
            FieldType::List { item } | FieldType::LargeList { item } => {
                write!(f, "{}<{}>", self.type_name(), child(item))
            }
            FieldType::FixedSizeList { item, size } => {
                write!(f, "fixed_size_list<{}, {size}>", child(item))
            }
            FieldType::Struct { fields } => {
                let inner: Vec<String> =
                    fields.iter().map(|c| format!("{}: {}", c.name, child(c))).collect();
                write!(f, "struct<{}>", inner.join(", "))
            }
            FieldType::Map { key, value, .. } => write!(f, "map<{}, {}>", child(key), child(value)),
            FieldType::Dictionary { index, value } => write!(f, "dictionary<{index}, {value}>"),
            FieldType::Unknown(name) => write!(f, "{name} (unknown)"),
            other => f.write_str(other.type_name()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(json: &str) -> Field {
        serde_json::from_str(json).expect("parse")
    }

    #[test]
    fn legacy_type_strings_still_parse() {
        for (s, t) in [
            ("int32", FieldType::Int32),
            ("int64", FieldType::Int64),
            ("float32", FieldType::Float32),
            ("float64", FieldType::Float64),
            ("utf8", FieldType::Utf8),
            ("bool", FieldType::Bool),
            ("date32", FieldType::Date32),
            ("binary", FieldType::Binary),
            ("timestamp_ms", FieldType::timestamp_ms()),
        ] {
            let f = parse(&format!(r#"{{"name":"c","type":"{s}"}}"#));
            assert_eq!(f.field_type, t, "{s}");
            assert!(f.nullable, "nullable defaults to true");
        }
    }

    #[test]
    fn rich_types_round_trip() {
        let item = Field::new("item", FieldType::Utf8);
        let types = vec![
            FieldType::Null,
            FieldType::UInt64,
            FieldType::Float16,
            FieldType::Decimal32 { precision: 9, scale: 2 },
            FieldType::Decimal64 { precision: 18, scale: 4 },
            FieldType::Decimal128 { precision: 38, scale: 9 },
            FieldType::Decimal256 { precision: 76, scale: -2 },
            FieldType::LargeUtf8,
            FieldType::Utf8View,
            FieldType::LargeBinary,
            FieldType::BinaryView,
            FieldType::FixedSizeBinary { byte_width: 16 },
            FieldType::Date64,
            FieldType::Time32 { unit: TimeUnit::Second },
            FieldType::Time64 { unit: TimeUnit::Nanosecond },
            FieldType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
            FieldType::Timestamp { unit: TimeUnit::Nanosecond, timezone: None },
            FieldType::Duration { unit: TimeUnit::Millisecond },
            FieldType::Interval { unit: IntervalUnit::MonthDayNano },
            FieldType::List { item: Box::new(item.clone()) },
            FieldType::LargeList { item: Box::new(item.clone().with_nullable(false)) },
            FieldType::FixedSizeList { item: Box::new(item.clone()), size: 3 },
            FieldType::Struct {
                fields: vec![Field::new("a", FieldType::Int32), Field::new("b", FieldType::Utf8)],
            },
            FieldType::Map {
                key: Box::new(Field::new("key", FieldType::Utf8).with_nullable(false)),
                value: Box::new(Field::new("value", FieldType::Int64)),
                keys_sorted: false,
            },
            FieldType::Dictionary {
                index: Box::new(FieldType::Int32),
                value: Box::new(FieldType::Utf8),
            },
        ];
        for t in types {
            let f = Field::new("c", t).with_nullable(false);
            let json = serde_json::to_string(&f).unwrap();
            let back: Field = serde_json::from_str(&json).unwrap();
            assert_eq!(back, f, "{json}");
        }
    }

    #[test]
    fn wire_shape_is_flat() {
        let f = Field::new(
            "seen_at",
            FieldType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
        );
        assert_eq!(
            serde_json::to_string(&f).unwrap(),
            r#"{"name":"seen_at","type":"timestamp","unit":"us","timezone":"UTC","nullable":true}"#
        );
    }

    #[test]
    fn unknown_types_and_bad_parameters_are_tolerated() {
        let f = parse(r#"{"name":"g","type":"geometry","srid":4326}"#);
        assert_eq!(f.field_type, FieldType::Unknown("geometry".into()));
        // A known type with missing or invalid parameters.
        for json in [
            r#"{"name":"d","type":"decimal128"}"#,
            r#"{"name":"d","type":"decimal128","precision":99,"scale":2}"#,
            r#"{"name":"t","type":"timestamp","unit":"fortnight"}"#,
            r#"{"name":"t","type":"time32","unit":"ns"}"#,
            r#"{"name":"l","type":"list"}"#,
            r#"{"name":"l","type":"list","item":{"type":"int64"}}"#,
        ] {
            assert!(parse(json).field_type.is_unknown(), "{json}");
        }
        // An unknown child makes the parent's child unknown, not the parent.
        let f = parse(r#"{"name":"l","type":"list","item":{"name":"item","type":"hyperloglog"}}"#);
        match f.field_type {
            FieldType::List { item } => assert!(item.field_type.is_unknown()),
            other => panic!("{other:?}"),
        }
        // Unknown types serialize back with their name only.
        assert_eq!(
            serde_json::to_value(Field::new("g", FieldType::Unknown("geometry".into()))).unwrap(),
            json!({"name":"g","type":"geometry","nullable":true})
        );
        // Unknown extra keys on a known type are ignored.
        assert_eq!(parse(r#"{"name":"i","type":"int64","comment":"x"}"#).field_type, FieldType::Int64);
    }

    #[test]
    fn only_a_missing_name_or_type_is_malformed() {
        assert!(serde_json::from_str::<Field>(r#"{"type":"int64"}"#).is_err());
        assert!(serde_json::from_str::<Field>(r#"{"name":"x"}"#).is_err());
        assert!(serde_json::from_str::<Field>(r#"{"name":"x","type":5}"#).is_err());
        assert!(serde_json::from_str::<Field>(r#""int64""#).is_err());
    }

    #[test]
    fn display_is_compact() {
        let t = FieldType::Struct {
            fields: vec![
                Field::new("a", FieldType::Int32).with_nullable(false),
                Field::new("b", FieldType::List { item: Box::new(Field::new("item", FieldType::Utf8)) }),
            ],
        };
        assert_eq!(t.to_string(), "struct<a: int32, b: list<utf8?>?>");
        assert_eq!(
            FieldType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }
                .to_string(),
            "timestamp(us, UTC)"
        );
        assert_eq!(FieldType::Decimal128 { precision: 10, scale: 2 }.to_string(), "decimal128(10, 2)");
        assert_eq!(FieldType::Unknown("geo".into()).to_string(), "geo (unknown)");
    }
}
