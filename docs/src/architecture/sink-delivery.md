# Sink Delivery

A **sink** is a fourth plugin kind that delivers finalized files to a remote
target (S3, GCS, sftp, …). It exists to keep *format* and *location* separate:
without it, "Parquet on S3" would mean an `s3_parquet` plugin, then `s3_csv`,
`gcs_parquet`, … — an N×M explosion of format-times-location plugins.

## Two kinds of destination

There are two fundamentally different ways a pipeline can end:

- **Native sinks** — databases and warehouses (Postgres, MySQL, Snowflake). They
  consume Arrow batches and write them through a native protocol. The connection
  *is* the location: no separate "format", no separate "where". These are
  monolithic `destination` plugins.
- **File / object output** — the data is serialized to a byte format (Parquet,
  CSV, JSONL) and then placed somewhere (local disk, S3, GCS). Here format and
  location are genuinely orthogonal.

Loadsmith handles the second case by splitting it across **time**, not across a
new data plane:

1. A file-output **destination** (e.g. `parquet`) writes files to a local
   staging directory — exactly as it always has.
2. A **sink** (e.g. `local-copy`, later `s3`) delivers each finalized file to its
   remote target.

This is a *staging* model: write locally, then ship. It keeps the parquet writer
untouched (it owns file rollover and the footer, which a live byte-stream split
across processes could not), makes retries cheap (re-deliver without
re-extracting), and gives all-or-nothing visibility at the remote (nothing
appears there until the local file is complete).

## Configuration

`sink` is a top-level block, a sibling of `source` and `destination` — separate
because it is a separate lifecycle phase, not a different data consumer:

```yaml
source:
  type: postgres
  config: { ... }

destination:
  type: parquet
  config:
    prefix: events
    compression: zstd
    # path: optional staging dir. Omitted ⇒ the core allocates scratch.
    #       Point it at a mounted volume (e.g. EBS) for very large outputs.

sink:                 # optional; only valid with a file-output destination
  type: local-copy    # the shipped sink; an `s3` sink is planned, not yet available
  config:
    dest: /mnt/out
```

A database destination has no `sink` (and no format/location split):

```yaml
destination:
  type: postgres
  config: { host: ..., dbname: ..., user: ..., password: ..., target_table: vendas }
```

### Path resolution and cleanup

| `destination.config.path` | `sink` | Behaviour |
|---|---|---|
| given | any | files staged there (e.g. an EBS mount) |
| omitted | present | core allocates a scratch dir, removed after delivery |
| omitted | absent | error — nowhere to write |

A core-allocated scratch dir is removed only after the sink has acknowledged
**every** object; a user-given `path` is left untouched. Staged files are never
deleted before their delivery is acked — that is what makes resume possible.

## Runtime coupling

The destination and sink are decoupled in config and coupled only at runtime,
brokered by the core:

```
postgres ─Arrow─▶ [core pump] ─Arrow─▶ parquet ─(writes to staging dir)
                                          │
                                          └─fd4 object_ready{path}─▶ [core] ─▶ [sink supervisor]
                                                                                    │ deliver_object{path}
                                                                                    ▼
                                                                                  sink ─▶ remote
```

The destination announces each finalized file as an **`object_ready`** event on
the **event plane (fd4)** — the same channel that already carries progress, and
which the core already drains concurrently during the pump. That is what makes
per-file delivery overlap extraction with no new data plane and no race on the
control channel: as each chunk rolls over and closes, the sink starts shipping it
while the pump keeps filling the next one. The sink config carries only the
*remote* coordinates; the *local* paths arrive at runtime.

A sink may only be attached to a destination that advertises the
`object_output` capability. Attaching one to a database destination is a
configuration error.

## The sink can outlive source and destination

The path channel into the supervisor is unbounded by design. A slow sink simply
accumulates staged files on disk and delivers them at its own pace — disk space
is not treated as a scarce resource. The run is complete only when the
supervisor has drained its queue, which may be *after* source and destination
have already exited. (Unbounded is also what keeps fd4 always-drained, avoiding
the pump deadlock described in [The Three Planes](./three-planes.md).)

## Crash recovery: the core owns the ledger

The sink is **stateless**; the core owns the delivery state. This is the same
principle as the rest of Loadsmith — the core is the sole owner of state,
plugins are memoryless tools.

The supervisor keeps a ledger:

- `emitted` — every `object_ready` path seen.
- `delivered` — every path the sink has acknowledged with `object_delivered`.

If the sink hangs (no `pong` to a periodic `ping`), dies (control EOF), or
reports an error, the supervisor tears it down, **respawns it, and re-sends every
object not yet acknowledged**, in order. Because re-delivery may repeat a path
that was partially delivered, sinks must make `deliver` **idempotent**
(`local-copy` overwrites; an S3 sink overwrites the object key). A restart cap
guards against an endlessly-failing sink (e.g. bad credentials), after which the
run fails with a clear error.

This in-process recovery (the core survives, the sink restarts) is distinct from
resuming a run that failed as a whole, which the state's
[resume record](./incremental-state.md#resuming-a-failed-run) does.

## Delivered means durable: the incremental watermark

With a sink, a batch is durable when the file holding it **reached the
target**, not when the destination wrote it locally. A destination that knows
which batches each file holds says so in `object_ready.batches` (`parquet`
does); the ledger then tracks the **delivered prefix** — the highest batch
whose file, and every earlier file, the sink acknowledged — and the state
supervisor uses it as the durable frontier of the incremental watermark:

- the watermark advances file by file during the run (throttled by
  `state.checkpoint_interval`);
- if the sink fails for good, the run fails with the watermark at the last
  delivered file, and the next run reads only what did not reach the target
  (plus the rows tied with the boundary value, see
  [Incremental State](./incremental-state.md)).

A destination that announces no batch ranges keeps the all-or-nothing rule:
the watermark is persisted only once the sink delivered every object.

**Stopping.** On a graceful stop (SIGTERM, or an abort elsewhere in the run)
the supervisor hands the sink nothing new and waits, up to
`cancel_grace_seconds`, for the delivery in flight to be acknowledged: an
object that reached the target counts as delivered, and the next run does not
read its rows again. A crash (SIGKILL) cannot wait: the object being uploaded
may land unacknowledged. The next run continues the crashed one (its
[resume record](./incremental-state.md#resuming-a-failed-run)): the same run
id, and file numbers from the first undelivered one, so its file replaces the
unacknowledged object instead of adding its rows a second time. After a crash
the state lock stays leased until its lease runs out: the next run waits up to
15 s for it, then goes on.

Each run's objects land under names of their own: both sinks' default `key` is
`{run_date}/{run_id}/{file}` (`local-copy` since 0.3.0; before, it named files
by file name alone, and a later run's file replaced an earlier run's).
