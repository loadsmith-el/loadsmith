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

/// Reads an Arrow IPC stream and tells a **clean end** (the explicit
/// end-of-stream marker) from a **truncated** one (the underlying reader hit
/// EOF first).
///
/// arrow-rs's `StreamReader` reports both as "no more batches": EOF at a
/// message boundary is valid per the IPC format. For Loadsmith that ambiguity
/// is a data-loss bug, because a writer that crashed between two batches looks
/// exactly like one that finished. Loadsmith writers always end with the
/// marker ([`IpcWriter::finish`]), so after [`read_batch`](Self::read_batch)
/// returns `Ok(None)`, [`ended_cleanly`](Self::ended_cleanly) is `true` only if
/// the marker was read.
pub struct IpcReader<R: Read> {
    inner: StreamReader<EofTracker<R>>,
}

impl<R: Read> IpcReader<R> {
    pub fn new(reader: R) -> Result<Self, IpcError> {
        let inner = StreamReader::try_new(EofTracker { inner: reader, hit_eof: false }, None)?;
        Ok(Self { inner })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    /// Returns `Ok(None)` when the stream ends — cleanly or not; see
    /// [`ended_cleanly`](Self::ended_cleanly).
    pub fn read_batch(&mut self) -> Result<Option<RecordBatch>, IpcError> {
        match self.inner.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(IpcError::Arrow(e)),
            None => Ok(None),
        }
    }

    /// After `read_batch` returned `Ok(None)`: `true` if the stream ended with
    /// the end-of-stream marker, `false` if the writer's side closed (or died)
    /// without writing it. Meaningless before the stream has ended.
    pub fn ended_cleanly(&self) -> bool {
        self.inner.is_finished() && !self.inner.get_ref().hit_eof
    }
}

/// Records whether the wrapped reader ever reported EOF. `StreamReader` stops
/// reading right after the end-of-stream marker, so it only sees EOF when the
/// marker is missing.
struct EofTracker<R> {
    inner: R,
    hit_eof: bool,
}

impl<R: Read> Read for EofTracker<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n == 0 && !buf.is_empty() {
            self.hit_eof = true;
        }
        Ok(n)
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
        assert!(reader.ended_cleanly());
    }

    #[test]
    fn missing_end_of_stream_marker_is_not_clean() {
        let (schema, batch) = make_batch();

        // A writer that died between two batches: complete messages, no marker.
        let mut buf = Vec::new();
        let mut writer = IpcWriter::new(&mut buf, &schema).unwrap();
        writer.write_batch(&batch).unwrap();
        drop(writer);

        let mut reader = IpcReader::new(buf.as_slice()).unwrap();
        assert!(reader.read_batch().unwrap().is_some());
        assert!(reader.read_batch().unwrap().is_none());
        assert!(!reader.ended_cleanly());
    }

    #[test]
    fn empty_stream_with_marker_is_clean() {
        let (schema, _) = make_batch();
        let mut buf = Vec::new();
        IpcWriter::new(&mut buf, &schema).unwrap().finish().unwrap();
        let mut reader = IpcReader::new(buf.as_slice()).unwrap();
        assert!(reader.read_batch().unwrap().is_none());
        assert!(reader.ended_cleanly());
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
