//! The data-plane pump: loadsmith sits between source and destination.
//!
//! Architecture: the source writes an Arrow IPC stream into one pipe (read end
//! held by core), and the destination reads an Arrow IPC stream from another
//! pipe (write end held by core). The pump copies every RecordBatch from the
//! source stream to the destination stream, counting rows/batches as it goes.
//!
//! Neither plugin knows the other exists — data always flows through the
//! control plane (loadsmith), which is what lets us observe and report progress.

use std::fs::File;

use anyhow::Result;
use loadsmith_arrow::{IpcReader, IpcWriter};

/// Totals observed by the pump as data flowed through it.
#[derive(Debug, Default, Clone, Copy)]
pub struct PumpStats {
    pub rows: u64,
    pub batches: u64,
}

/// Copies the Arrow IPC stream from `read_in` (source) to `write_out`
/// (destination), invoking `on_progress(rows, batches)` at exponentially
/// growing batch intervals (1, 2, 4, 8, …) so logs never flood.
///
/// This is synchronous, blocking I/O — intended to run on a blocking thread
/// (e.g. `tokio::task::spawn_blocking`).
pub fn pump<F>(read_in: File, write_out: File, mut on_progress: F) -> Result<PumpStats>
where
    F: FnMut(u64, u64),
{
    let mut reader = IpcReader::new(read_in).map_err(|e| anyhow::anyhow!("data plane reader: {e}"))?;
    let schema = reader.schema();
    let mut writer = IpcWriter::new(write_out, schema.as_ref())
        .map_err(|e| anyhow::anyhow!("data plane writer: {e}"))?;

    let mut stats = PumpStats::default();
    let mut next_log: u64 = 1;

    while let Some(batch) = reader.read_batch().map_err(|e| anyhow::anyhow!("read batch: {e}"))? {
        stats.rows += batch.num_rows() as u64;
        stats.batches += 1;
        writer.write_batch(&batch).map_err(|e| anyhow::anyhow!("write batch: {e}"))?;

        if stats.batches == next_log {
            on_progress(stats.rows, stats.batches);
            next_log *= 2;
        }
    }

    // Emit a final progress tick if the last batch didn't land on a power of two,
    // so the displayed total always matches reality.
    if stats.batches > 0 && stats.batches != next_log / 2 {
        on_progress(stats.rows, stats.batches);
    }

    writer.finish().map_err(|e| anyhow::anyhow!("data plane finish: {e}"))?;
    Ok(stats)
}
