# Data Flow

## The critical invariant

**Data flows through the core. Always.**

A source plugin never connects to a destination plugin directly. There is no
shared pipe between them. Every Arrow batch emitted by the source passes through
the core's pump before reaching the destination:

```
source plugin                core (loadsmith)               destination plugin
─────────────────────────────────────────────────────────────────────────────
emit RecordBatch ──[fd3]──►  read batch                        (waiting)
                             count rows
                             report progress
                             write batch ──[fd3]──►            receive RecordBatch
                                                               write to sink
```

This is not an optimization target. This invariant is what makes progress
reporting, row counting, future transforms, schema validation, and metrics
possible. Eliminating it by wiring the source's write end directly to the
destination's read end bypasses the entire control plane and was a real bug in
an early prototype.

## Two pipes, one pump

The runner creates two independent OS pipes before spawning any plugin:

```
in_pipe:   (in_read_raw,  in_write_raw)   source writes here
out_pipe:  (out_read_raw, out_write_raw)  destination reads from here
```

When spawning the source, `in_write_raw` is dup2'd to fd3 in the child. When
spawning the destination, `out_read_raw` is dup2'd to fd3 in the child. The core
retains `in_read_raw` and `out_write_raw` for the pump.

```
                      fd3 = in_write_raw
source plugin ──────────────────────────► in_pipe
                                              │
                              core pump reads │ from in_read_raw
                              core pump writes│ to out_write_raw
                                              ▼
destination plugin ◄────────────────────── out_pipe
                      fd3 = out_read_raw
```

## The pump

The pump (`crates/loadsmith-core/src/pump.rs`) is a synchronous blocking
function — it runs on `tokio::task::spawn_blocking` to avoid blocking the async
runtime while doing Arrow I/O.

```rust
pub fn pump<F>(read_in: File, write_out: File, mut on_progress: F) -> Result<PumpStats>
where
    F: FnMut(u64, u64),
```

It reads the source's IPC stream from `read_in`, writes each `RecordBatch` to
`write_out`, and calls `on_progress(total_rows, total_batches)` at exponentially
increasing intervals.

### Progress at doubling intervals

The pump reports progress at batch 1, 2, 4, 8, 16, 32, … — each time the batch
count reaches the next power of two. If the run ends on a batch count that is not
a power of two, one final progress call is made.

This strategy avoids flooding the output for large runs (imagine 10,000 batches)
while still giving dense feedback at the start when the operator wants to confirm
that data is flowing. It is the same approach used by Embulk.

```
batch   1  →   2,000 rows    (next_log: 2)
batch   2  →   4,000 rows    (next_log: 4)
batch   4  →   8,000 rows    (next_log: 8)
batch   8  →  16,000 rows    (next_log: 16)
batch  16  →  24,500 rows    ← final (24,500 / 2,000 = ~12.25 batches, not exact power)
```

### PumpStats

The pump returns:

```rust
pub struct PumpStats {
    pub rows: u64,    // total rows copied
    pub batches: u64, // total batches copied
}
```

These values populate the `Summary` at the end of the run.

## Concurrency model

The pump is synchronous. While it runs on `spawn_blocking`, the async runtime
handles:

1. **Draining source fd4** — `tokio::spawn(drain_events(src_event, "source"))`.
   If not drained concurrently, a plugin writing many log events fills the pipe
   and blocks. While the plugin is blocked, it cannot write more data. The pump
   waits for more data. Deadlock.

2. **Draining destination fd4** — same reasoning.

3. **Awaiting `Finished` messages** — after the pump completes, the core reads
   the terminal `Finished` message from each plugin on the control channel.

The full sequence after spawning both plugins is:

```
tokio::spawn  drain_source_events    ─────────────────────────────────────────────► (async, concurrently)
tokio::spawn  drain_destination_events ──────────────────────────────────────────► (async, concurrently)
spawn_blocking  pump                ────────────────────────────────────────────►  (blocks until done)
await pump result
await Finished from source control channel
await Finished from destination control channel
```

## What the `Finished` message carries

When the source's IPC stream ends (writer drops → EOF on the read side → IPC
reader returns `Ok(None)`), the pump closes its write end of `out_pipe`. The
destination sees EOF on its fd3 and calls `finalize()`. Then both plugins send
`Finished`:

**Source `Finished`** (success):
```json
{"type": "finished", "status": "success", "rows_read": 100000, "batches_read": 50}
```

**Destination `Finished`** (success):
```json
{"type": "finished", "status": "success", "rows_written": 100000, "batches_written": 50}
```

The core validates that both finished successfully, then prints the summary.
