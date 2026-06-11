use std::io::{Read, Write};
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct IpcWriter<W: Write> {
    inner: StreamWriter<W>,
}

impl<W: Write> IpcWriter<W> {
    pub fn new(writer: W, schema: &Schema) -> Result<Self, IpcError> {
        let inner = StreamWriter::try_new(writer, schema)?;
        Ok(Self { inner })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), IpcError> {
        self.inner.write(batch)?;
        Ok(())
    }

    /// Writes the EOS marker and flushes.
    pub fn finish(mut self) -> Result<(), IpcError> {
        self.inner.finish()?;
        Ok(())
    }
}

pub struct IpcReader<R: Read> {
    inner: StreamReader<R>,
}

impl<R: Read> IpcReader<R> {
    pub fn new(reader: R) -> Result<Self, IpcError> {
        let inner = StreamReader::try_new(reader, None)?;
        Ok(Self { inner })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    /// Returns `Ok(None)` when the stream ends.
    pub fn read_batch(&mut self) -> Result<Option<RecordBatch>, IpcError> {
        match self.inner.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(IpcError::Arrow(e)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("nome", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    #[test]
    fn write_read_roundtrip() {
        let (schema, batch) = make_batch();

        let mut buf = Vec::new();
        let mut writer = IpcWriter::new(&mut buf, &schema).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        let mut reader = IpcReader::new(buf.as_slice()).unwrap();
        let read_batch = reader.read_batch().unwrap().expect("should have a batch");
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);

        let ids = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(2), 3);

        assert!(reader.read_batch().unwrap().is_none());
    }

    #[test]
    fn multiple_batches_roundtrip() {
        let (schema, batch) = make_batch();

        let mut buf = Vec::new();
        let mut writer = IpcWriter::new(&mut buf, &schema).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        let mut reader = IpcReader::new(buf.as_slice()).unwrap();
        assert!(reader.read_batch().unwrap().is_some());
        assert!(reader.read_batch().unwrap().is_some());
        assert!(reader.read_batch().unwrap().is_none());
    }
}
