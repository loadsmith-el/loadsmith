pub mod schema;
pub mod ipc;
pub mod convert;

pub use schema::{
    arrow_field_from_protocol, arrow_type_from_protocol, protocol_field_from_arrow,
    protocol_fields_from_schema, protocol_type_from_arrow, schema_from_protocol_fields,
};
pub use ipc::{IpcWriter, IpcReader};
pub use convert::record_batch_to_json_rows;

/// The bytes of data a batch holds: the sum of each column's buffers as
/// sliced for this batch. Unlike `RecordBatch::get_array_memory_size`, it
/// does not count a buffer shared by several columns (an IPC message body)
/// once per column. Used for the `*.bytes` telemetry metrics.
pub fn batch_bytes(batch: &arrow_array::RecordBatch) -> u64 {
    use arrow_array::Array;
    batch.columns().iter().map(|c| c.to_data().get_slice_memory_size().unwrap_or(0) as u64).sum()
}
