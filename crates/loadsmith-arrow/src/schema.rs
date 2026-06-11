use arrow_schema::{DataType, Field, Schema, TimeUnit};
use loadsmith_protocol::{Field as ProtocolField, FieldType};

/// Converts protocol field definitions to an Arrow schema.
pub fn schema_from_protocol_fields(fields: &[ProtocolField]) -> Schema {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| Field::new(f.name.as_str(), protocol_type_to_arrow(&f.field_type), true))
        .collect();
    Schema::new(arrow_fields)
}

pub fn protocol_type_to_arrow(ft: &FieldType) -> DataType {
    match ft {
        FieldType::Int32 => DataType::Int32,
        FieldType::Int64 => DataType::Int64,
        FieldType::Float32 => DataType::Float32,
        FieldType::Float64 => DataType::Float64,
        FieldType::Utf8 => DataType::Utf8,
        FieldType::Bool => DataType::Boolean,
        FieldType::Date32 => DataType::Date32,
        FieldType::TimestampMs => DataType::Timestamp(TimeUnit::Millisecond, None),
        FieldType::Binary => DataType::Binary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadsmith_protocol::Field as PF;

    #[test]
    fn converts_all_field_types() {
        let fields = vec![
            PF { name: "id".into(), field_type: FieldType::Int64 },
            PF { name: "nome".into(), field_type: FieldType::Utf8 },
            PF { name: "score".into(), field_type: FieldType::Float64 },
            PF { name: "ativo".into(), field_type: FieldType::Bool },
            PF { name: "data".into(), field_type: FieldType::Date32 },
            PF { name: "ts".into(), field_type: FieldType::TimestampMs },
        ];
        let schema = schema_from_protocol_fields(&fields);
        assert_eq!(schema.fields().len(), 6);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Boolean);
    }
}
