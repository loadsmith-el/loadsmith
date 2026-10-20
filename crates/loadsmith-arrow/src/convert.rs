//! Arrow → JSON rows (used by the jsonl destination).
//!
//! Total and panic-free over every Arrow type: each value maps to JSON as
//! below; a type outside the protocol vocabulary (`union`,
//! `run_end_encoded`, `list_view`, `large_list_view`) becomes `null`.
//!
//! | Arrow | JSON |
//! |---|---|
//! | any null slot, `Null` | `null` |
//! | `Boolean` | `true` / `false` |
//! | `Int*`, `UInt*` | number (exact; `uint64` above 2^53 may lose precision in some JSON readers) |
//! | `Float16/32/64` | number; **NaN and ±Infinity become `null`** (JSON has no such numbers) |
//! | `Decimal128/256` | **string** with the scale applied, e.g. `"123.45"` (exact; JSON numbers would lose digits) |
//! | `Utf8`, `LargeUtf8`, `Utf8View` | string |
//! | `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary` | standard base64 string (with padding) |
//! | `Date32` | integer days since 1970-01-01 |
//! | `Date64` | integer milliseconds since the epoch (the stored value) |
//! | `Timestamp(unit, tz)` | integer **milliseconds** since the epoch, whatever the unit; `us`/`ns` are floored to the millisecond; the time zone does not change the instant |
//! | `Time32`, `Time64` | integer milliseconds since midnight (floored) |
//! | `Duration` | integer milliseconds (floored) |
//! | `Interval(YearMonth)` | `{"months": n}` |
//! | `Interval(DayTime)` | `{"days": d, "milliseconds": ms}` |
//! | `Interval(MonthDayNano)` | `{"months": m, "days": d, "nanoseconds": ns}` |
//! | `List`, `LargeList`, `FixedSizeList` | array of the converted items |
//! | `Struct` | object keyed by child name |
//! | `Map` | object; a non-string key is written as its JSON text (e.g. `"42"`) |
//! | `Dictionary` | the decoded value |
//!
//! A value that cannot be represented (a timestamp in seconds whose
//! millisecond form overflows `i64`) is `null`.

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrowPrimitiveType, RecordBatch};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use serde_json::{Map, Number, Value};

/// Converts each row in a RecordBatch to a JSON object.
pub fn record_batch_to_json_rows(batch: &RecordBatch) -> Vec<Value> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let columns: Vec<Vec<Value>> = batch.columns().iter().map(|c| column_values(c.as_ref())).collect();
    let mut rows: Vec<Map<String, Value>> = (0..num_rows).map(|_| Map::new()).collect();
    for (field, values) in schema.fields().iter().zip(columns) {
        for (row, value) in rows.iter_mut().zip(values) {
            row.insert(field.name().clone(), value);
        }
    }
    rows.into_iter().map(Value::Object).collect()
}

/// One JSON value per slot of `arr` (`arr.len()` values).
pub fn column_values(arr: &dyn Array) -> Vec<Value> {
    let len = arr.len();
    let mut out = convert(arr).unwrap_or_else(|| vec![Value::Null; len]);
    out.resize(len, Value::Null);
    // Parent-level nulls win over whatever the child data holds.
    if arr.null_count() > 0 {
        for (i, v) in out.iter_mut().enumerate() {
            if arr.is_null(i) {
                *v = Value::Null;
            }
        }
    }
    out
}

fn prim<T, F>(arr: &dyn Array, f: F) -> Option<Vec<Value>>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> Value,
{
    let a = arr.as_primitive_opt::<T>()?;
    Some(a.values().iter().map(|v| f(*v)).collect())
}

fn float(v: f64) -> Value {
    Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
}

fn int(v: Option<i64>) -> Value {
    v.map(Value::from).unwrap_or(Value::Null)
}

/// `v` in `unit` → milliseconds, floored.
fn to_ms(v: i64, unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => v.checked_mul(1000),
        TimeUnit::Millisecond => Some(v),
        TimeUnit::Microsecond => Some(v.div_euclid(1000)),
        TimeUnit::Nanosecond => Some(v.div_euclid(1_000_000)),
    }
}

fn base64_values<'a>(it: impl Iterator<Item = Option<&'a [u8]>>) -> Vec<Value> {
    it.map(|b| b.map(|b| Value::String(base64_encode(b))).unwrap_or(Value::Null)).collect()
}

fn string_values<'a>(it: impl Iterator<Item = Option<&'a str>>) -> Vec<Value> {
    it.map(|s| s.map(|s| Value::String(s.to_string())).unwrap_or(Value::Null)).collect()
}

/// Splits converted child values into one JSON array per offset window.
fn windows(child: &[Value], offsets: impl Iterator<Item = (usize, usize)>) -> Vec<Value> {
    offsets
        .map(|(start, end)| match child.get(start..end) {
            Some(items) => Value::Array(items.to_vec()),
            None => Value::Null,
        })
        .collect()
}

fn offset_pairs<O: Copy + TryInto<usize>>(offsets: &[O]) -> impl Iterator<Item = (usize, usize)> + '_ {
    offsets.windows(2).map(|w| {
        let start = w[0].try_into().unwrap_or(0);
        let end = w[1].try_into().unwrap_or(0);
        (start, end.max(start))
    })
}

fn convert(arr: &dyn Array) -> Option<Vec<Value>> {
    let dt = arr.data_type();
    Some(match dt {
        DataType::Null => vec![Value::Null; arr.len()],
        DataType::Boolean => arr.as_boolean_opt()?.iter().map(|b| b.map(Value::Bool).unwrap_or(Value::Null)).collect(),
        DataType::Int8 => prim::<Int8Type, _>(arr, Value::from)?,
        DataType::Int16 => prim::<Int16Type, _>(arr, Value::from)?,
        DataType::Int32 => prim::<Int32Type, _>(arr, Value::from)?,
        DataType::Int64 => prim::<Int64Type, _>(arr, Value::from)?,
        DataType::UInt8 => prim::<UInt8Type, _>(arr, Value::from)?,
        DataType::UInt16 => prim::<UInt16Type, _>(arr, Value::from)?,
        DataType::UInt32 => prim::<UInt32Type, _>(arr, Value::from)?,
        DataType::UInt64 => prim::<UInt64Type, _>(arr, Value::from)?,
        DataType::Float16 => prim::<Float16Type, _>(arr, |v| float(v.to_f64()))?,
        DataType::Float32 => prim::<Float32Type, _>(arr, |v| float(f64::from(v)))?,
        DataType::Float64 => prim::<Float64Type, _>(arr, float)?,
        DataType::Decimal128(..) => {
            let a = arr.as_primitive_opt::<Decimal128Type>()?;
            (0..a.len()).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Decimal256(..) => {
            let a = arr.as_primitive_opt::<Decimal256Type>()?;
            (0..a.len()).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Utf8 => string_values(arr.as_string_opt::<i32>()?.iter()),
        DataType::LargeUtf8 => string_values(arr.as_string_opt::<i64>()?.iter()),
        DataType::Utf8View => string_values(arr.as_string_view_opt()?.iter()),
        DataType::Binary => base64_values(arr.as_binary_opt::<i32>()?.iter()),
        DataType::LargeBinary => base64_values(arr.as_binary_opt::<i64>()?.iter()),
        DataType::BinaryView => base64_values(arr.as_binary_view_opt()?.iter()),
        DataType::FixedSizeBinary(_) => base64_values(arr.as_fixed_size_binary_opt()?.iter()),
        DataType::Date32 => prim::<Date32Type, _>(arr, Value::from)?,
        DataType::Date64 => prim::<Date64Type, _>(arr, Value::from)?,
        DataType::Timestamp(unit, _) => {
            let f = |v: i64| int(to_ms(v, unit));
            match unit {
                TimeUnit::Second => prim::<TimestampSecondType, _>(arr, f)?,
                TimeUnit::Millisecond => prim::<TimestampMillisecondType, _>(arr, f)?,
                TimeUnit::Microsecond => prim::<TimestampMicrosecondType, _>(arr, f)?,
                TimeUnit::Nanosecond => prim::<TimestampNanosecondType, _>(arr, f)?,
            }
        }
        DataType::Time32(unit) => {
            let f = |v: i32| int(to_ms(i64::from(v), unit));
            match unit {
                TimeUnit::Second => prim::<Time32SecondType, _>(arr, f)?,
                TimeUnit::Millisecond => prim::<Time32MillisecondType, _>(arr, f)?,
                _ => return None,
            }
        }
        DataType::Time64(unit) => {
            let f = |v: i64| int(to_ms(v, unit));
            match unit {
                TimeUnit::Microsecond => prim::<Time64MicrosecondType, _>(arr, f)?,
                TimeUnit::Nanosecond => prim::<Time64NanosecondType, _>(arr, f)?,
                _ => return None,
            }
        }
        DataType::Duration(unit) => {
            let f = |v: i64| int(to_ms(v, unit));
            match unit {
                TimeUnit::Second => prim::<DurationSecondType, _>(arr, f)?,
                TimeUnit::Millisecond => prim::<DurationMillisecondType, _>(arr, f)?,
                TimeUnit::Microsecond => prim::<DurationMicrosecondType, _>(arr, f)?,
                TimeUnit::Nanosecond => prim::<DurationNanosecondType, _>(arr, f)?,
            }
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            prim::<IntervalYearMonthType, _>(arr, |m| serde_json::json!({ "months": m }))?
        }
        DataType::Interval(IntervalUnit::DayTime) => prim::<IntervalDayTimeType, _>(arr, |v| {
            serde_json::json!({ "days": v.days, "milliseconds": v.milliseconds })
        })?,
        DataType::Interval(IntervalUnit::MonthDayNano) => prim::<IntervalMonthDayNanoType, _>(arr, |v| {
            serde_json::json!({ "months": v.months, "days": v.days, "nanoseconds": v.nanoseconds })
        })?,
        DataType::List(_) => {
            let a = arr.as_list_opt::<i32>()?;
            windows(&column_values(a.values().as_ref()), offset_pairs(a.value_offsets()))
        }
        DataType::LargeList(_) => {
            let a = arr.as_list_opt::<i64>()?;
            windows(&column_values(a.values().as_ref()), offset_pairs(a.value_offsets()))
        }
        DataType::FixedSizeList(_, size) => {
            let a = arr.as_fixed_size_list_opt()?;
            let size = usize::try_from(*size).ok()?;
            // The child array covers the whole (unsliced) parent: start at
            // this array's offset.
            let child = column_values(a.values().as_ref());
            let first = a.offset();
            windows(&child, (0..a.len()).map(|i| ((first + i) * size, (first + i + 1) * size)))
        }
        DataType::Struct(_) => {
            let a = arr.as_struct_opt()?;
            let names = a.column_names();
            let children: Vec<Vec<Value>> = a.columns().iter().map(|c| column_values(c.as_ref())).collect();
            (0..a.len())
                .map(|row| {
                    let mut obj = Map::new();
                    for (name, values) in names.iter().zip(&children) {
                        obj.insert((*name).to_string(), values.get(row).cloned().unwrap_or(Value::Null));
                    }
                    Value::Object(obj)
                })
                .collect()
        }
        DataType::Map(..) => {
            let a = arr.as_map_opt()?;
            let keys = column_values(a.keys().as_ref());
            let values = column_values(a.values().as_ref());
            offset_pairs(a.value_offsets())
                .map(|(start, end)| {
                    let mut obj = Map::new();
                    for i in start..end {
                        let key = match keys.get(i) {
                            Some(Value::String(s)) => s.clone(),
                            Some(other) => other.to_string(),
                            None => continue,
                        };
                        obj.insert(key, values.get(i).cloned().unwrap_or(Value::Null));
                    }
                    Value::Object(obj)
                })
                .collect()
        }
        DataType::Dictionary(..) => {
            let a = arr.as_any_dictionary_opt()?;
            let values = column_values(a.values().as_ref());
            a.normalized_keys()
                .into_iter()
                .map(|k| values.get(k).cloned().unwrap_or(Value::Null))
                .collect()
        }
        DataType::Union(..)
        | DataType::RunEndEncoded(..)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => return None,
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        out.push(if chunk.len() > 1 { CHARS[((b1 & 0xf) << 2) | (b2 >> 6)] as char } else { '=' });
        out.push(if chunk.len() > 2 { CHARS[b2 & 0x3f] as char } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow_array::*;
    use arrow_schema::{Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    /// Converts a single column and returns its values.
    fn values(arr: ArrayRef) -> Vec<Value> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", arr.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        record_batch_to_json_rows(&batch).into_iter().map(|mut r| r["c"].take()).collect()
    }

    #[test]
    fn converts_basic_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("nome", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("ativo", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Float64Array::from(vec![9.5, 8.0])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();

        let rows = record_batch_to_json_rows(&batch);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["nome"], "alice");
        assert_eq!(rows[0]["score"], 9.5);
        assert_eq!(rows[1]["ativo"], false);
    }

    #[test]
    fn null_values_become_json_null() {
        assert_eq!(values(Arc::new(Int64Array::from(vec![None::<i64>]))), [Value::Null]);
        assert_eq!(values(Arc::new(NullArray::new(2))), [Value::Null, Value::Null]);
    }

    #[test]
    fn integers_of_every_width() {
        assert_eq!(values(Arc::new(Int8Array::from(vec![-8]))), [json!(-8)]);
        assert_eq!(values(Arc::new(Int16Array::from(vec![-16]))), [json!(-16)]);
        assert_eq!(values(Arc::new(Int32Array::from(vec![-32]))), [json!(-32)]);
        assert_eq!(values(Arc::new(UInt8Array::from(vec![255]))), [json!(255)]);
        assert_eq!(values(Arc::new(UInt16Array::from(vec![65535]))), [json!(65535)]);
        assert_eq!(values(Arc::new(UInt32Array::from(vec![u32::MAX]))), [json!(u32::MAX)]);
        assert_eq!(values(Arc::new(UInt64Array::from(vec![u64::MAX]))), [json!(u64::MAX)]);
    }

    #[test]
    fn floats_and_non_finite_values() {
        assert_eq!(
            values(Arc::new(Float64Array::from(vec![1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY]))),
            [json!(1.5), Value::Null, Value::Null, Value::Null]
        );
        assert_eq!(values(Arc::new(Float32Array::from(vec![0.25, f32::NAN]))), [json!(0.25), Value::Null]);
        assert_eq!(
            values(Arc::new(Float16Array::from(vec![<Float16Type as ArrowPrimitiveType>::Native::from_f32(2.5)]))),
            [json!(2.5)]
        );
    }

    #[test]
    fn decimals_are_exact_strings() {
        let d = Decimal128Array::from(vec![Some(12345), Some(-5), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(values(Arc::new(d)), [json!("123.45"), json!("-0.05"), Value::Null]);
        let big = Decimal256Array::from(vec![arrow::datatypes::i256::from_i128(1_000_000_000_000_000_000_000_i128)])
            .with_precision_and_scale(76, 3)
            .unwrap();
        assert_eq!(values(Arc::new(big)), [json!("1000000000000000000.000")]);
    }

    #[test]
    fn strings_and_binaries() {
        assert_eq!(values(Arc::new(LargeStringArray::from(vec!["x"]))), [json!("x")]);
        assert_eq!(values(Arc::new(StringViewArray::from(vec!["a view string longer than twelve"]))), [
            json!("a view string longer than twelve")
        ]);
        assert_eq!(values(Arc::new(BinaryArray::from(vec![b"hi".as_ref()]))), [json!("aGk=")]);
        assert_eq!(values(Arc::new(LargeBinaryArray::from(vec![b"abc".as_ref()]))), [json!("YWJj")]);
        assert_eq!(values(Arc::new(BinaryViewArray::from(vec![b"a".as_ref()]))), [json!("YQ==")]);
        let fsb = FixedSizeBinaryArray::try_from_iter(vec![vec![0u8, 1, 2]].into_iter()).unwrap();
        assert_eq!(values(Arc::new(fsb)), [json!("AAEC")]);
    }

    #[test]
    fn dates_keep_their_existing_form() {
        assert_eq!(values(Arc::new(Date32Array::from(vec![19000]))), [json!(19000)]);
        assert_eq!(values(Arc::new(Date64Array::from(vec![86_400_000]))), [json!(86_400_000)]);
    }

    #[test]
    fn timestamps_of_every_unit_are_epoch_ms() {
        let ms = 1_700_000_000_123_i64;
        assert_eq!(values(Arc::new(TimestampMillisecondArray::from(vec![ms]))), [json!(ms)]);
        assert_eq!(values(Arc::new(TimestampSecondArray::from(vec![1_700_000_000]))), [json!(1_700_000_000_000_i64)]);
        assert_eq!(
            values(Arc::new(TimestampMicrosecondArray::from(vec![ms * 1000 + 999]).with_timezone("UTC"))),
            [json!(ms)]
        );
        assert_eq!(values(Arc::new(TimestampNanosecondArray::from(vec![ms * 1_000_000 + 1]))), [json!(ms)]);
        // Floored, not truncated toward zero, before the epoch.
        assert_eq!(values(Arc::new(TimestampMicrosecondArray::from(vec![-1]))), [json!(-1)]);
        // Overflow is null, never a panic.
        assert_eq!(values(Arc::new(TimestampSecondArray::from(vec![i64::MAX]))), [Value::Null]);
    }

    #[test]
    fn times_and_durations_are_ms() {
        assert_eq!(values(Arc::new(Time32SecondArray::from(vec![3600]))), [json!(3_600_000)]);
        assert_eq!(values(Arc::new(Time32MillisecondArray::from(vec![1500]))), [json!(1500)]);
        assert_eq!(values(Arc::new(Time64MicrosecondArray::from(vec![1_500_999]))), [json!(1500)]);
        assert_eq!(values(Arc::new(Time64NanosecondArray::from(vec![2_000_000]))), [json!(2)]);
        assert_eq!(values(Arc::new(DurationSecondArray::from(vec![2]))), [json!(2000)]);
        assert_eq!(values(Arc::new(DurationNanosecondArray::from(vec![3_000_000]))), [json!(3)]);
    }

    #[test]
    fn intervals_are_objects() {
        assert_eq!(values(Arc::new(IntervalYearMonthArray::from(vec![14]))), [json!({"months": 14})]);
        assert_eq!(
            values(Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(2, 500)]))),
            [json!({"days": 2, "milliseconds": 500})]
        );
        assert_eq!(
            values(Arc::new(IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(1, 2, 3)]))),
            [json!({"months": 1, "days": 2, "nanoseconds": 3})]
        );
    }

    #[test]
    fn lists_are_arrays() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
            None,
            Some(vec![]),
        ]);
        assert_eq!(values(Arc::new(list)), [json!([1, null, 3]), Value::Null, json!([])]);
        let large = LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(7)])]);
        assert_eq!(values(Arc::new(large)), [json!([7])]);
        let fixed = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)])],
            2,
        );
        assert_eq!(values(Arc::new(fixed.clone())), [json!([1, 2]), json!([3, 4])]);
        // A sliced fixed-size list starts at its own offset.
        assert_eq!(values(Arc::new(fixed.slice(1, 1))), [json!([3, 4])]);
        // A sliced variable list too.
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)]), Some(vec![Some(2)])]);
        assert_eq!(values(Arc::new(list.slice(1, 1))), [json!([2])]);
    }

    #[test]
    fn structs_are_objects() {
        let st = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
            ),
        ]);
        assert_eq!(values(Arc::new(st)), [json!({"a": 1, "b": "x"}), json!({"a": 2, "b": null})]);
    }

    #[test]
    fn maps_are_objects() {
        let mut b = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        b.keys().append_value("k1");
        b.values().append_value(1);
        b.keys().append_value("k2");
        b.values().append_null();
        b.append(true).unwrap();
        b.append(false).unwrap();
        assert_eq!(values(Arc::new(b.finish())), [json!({"k1": 1, "k2": null}), Value::Null]);
    }

    #[test]
    fn dictionaries_are_decoded() {
        let dict: DictionaryArray<Int32Type> = vec![Some("a"), None, Some("b"), Some("a")].into_iter().collect();
        assert_eq!(values(Arc::new(dict)), [json!("a"), Value::Null, json!("b"), json!("a")]);
    }

    #[test]
    fn types_outside_the_vocabulary_are_null_not_a_panic() {
        let run_ends = Int32Array::from(vec![2]);
        let ree = RunArray::<Int32Type>::try_new(&run_ends, &Int32Array::from(vec![5])).unwrap();
        assert_eq!(values(Arc::new(ree)), [Value::Null, Value::Null]);
    }
}
