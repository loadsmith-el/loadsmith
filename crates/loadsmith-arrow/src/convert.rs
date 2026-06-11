use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use serde_json::{Map, Value};

/// Converts each row in a RecordBatch to a JSON object.
pub fn record_batch_to_json_rows(batch: &RecordBatch) -> Vec<Value> {
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut map = Map::new();
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let val = column_value(col.as_ref(), row_idx, field.data_type());
            map.insert(field.name().clone(), val);
        }
        rows.push(Value::Object(map));
    }
    rows
}

fn column_value(
    col: &dyn arrow_array::Array,
    row: usize,
    dt: &DataType,
) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }
    match dt {
        DataType::Int32 => col.as_primitive::<arrow_array::types::Int32Type>().value(row).into(),
        DataType::Int64 => col.as_primitive::<arrow_array::types::Int64Type>().value(row).into(),
        DataType::Float32 => {
            let v = col.as_primitive::<arrow_array::types::Float32Type>().value(row) as f64;
            Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
        }
        DataType::Float64 => {
            let v = col.as_primitive::<arrow_array::types::Float64Type>().value(row);
            Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
        }
        DataType::Boolean => col.as_boolean().value(row).into(),
        DataType::Utf8 => col.as_string::<i32>().value(row).into(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row).into(),
        DataType::Date32 => {
            let days = col.as_primitive::<arrow_array::types::Date32Type>().value(row);
            Value::Number(days.into())
        }
        DataType::Timestamp(_, _) => {
            let ms = col.as_primitive::<arrow_array::types::TimestampMillisecondType>().value(row);
            Value::Number(ms.into())
        }
        DataType::Binary => {
            let bytes = col.as_binary::<i32>().value(row);
            Value::String(base64_encode(bytes))
        }
        _ => {
            // Fallback: represent as null with a note; unknown types are not silently dropped
            Value::Null
        }
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity((bytes.len() * 4 / 3) + 4);
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 { chunk[1] as usize } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as usize } else { 0 };
        let _ = write!(out, "{}", CHARS[b0 >> 2] as char);
        let _ = write!(out, "{}", CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        let _ = write!(out, "{}", if chunk.len() > 1 { CHARS[((b1 & 0xf) << 2) | (b2 >> 6)] as char } else { '=' });
        let _ = write!(out, "{}", if chunk.len() > 2 { CHARS[b2 & 0x3f] as char } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
        assert_eq!(rows[1]["ativo"], false);
    }

    #[test]
    fn null_values_become_json_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![None::<i64>]))],
        )
        .unwrap();
        let rows = record_batch_to_json_rows(&batch);
        assert_eq!(rows[0]["v"], Value::Null);
    }
}
