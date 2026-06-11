# Incremental State

Loadsmith can resume a load from where the last run stopped, instead of always
re-reading everything. The mechanism is small and rests on one principle:

> **The core is the sole owner of state.** Plugins are memoryless. A source
> reports its high *watermark* as an opaque value; a destination reports *which
> batches are durable*. The core persists the watermark and hands it back on the
> next run. The core never interprets the watermark — it is an opaque scalar,
> exactly like a plugin's `config` block.

## The two keys (don't confuse them)

- **Cursor column** (source side, `source.config.incremental.cursor_column`): an
  ordered, monotonically-advancing column — ideally an `updated_at` timestamp —
  that the source orders by and resumes after. It is *how to resume*.
- **Merge key** (destination side, e.g. the postgres destination's `merge_key`):
  the row identity used to deduplicate on write. It is *how to stay idempotent*.

They may be the same column or different ones.

## How a run flows

```
core: load watermark W from the state backend (under a lock)
core → source: start { resume: { cursor_value: W } }
source: WHERE cursor > W ORDER BY cursor        (the source builds the query)
   ── per batch on fd4 ──
source → core:  checkpoint { batch_seq: N, cursor_value: Vn }
dest   → core:  committed  { batch_seq: M }      (M = durably committed so far)
core: safe watermark = Vn for the greatest N ≤ M  → persist (throttled)
core: final flush of the safe watermark, release the lock
```

The **batch ordinal** is the join key: the data plane is one serial Arrow IPC
stream, so batch *K* is the same batch for source, pump, and destination, and all
three count it in lockstep.

## The durability gate (why resume is gap-free)

The core persists watermark `Vn` **only after** the destination's `committed`
confirms the batch that produced it is durable. So on resume from `Vn`:
everything `≤ Vn` is durably at the destination (no data loss), and the source
rereads strictly `> Vn`. The only overlap is the boundary, which an idempotent
destination absorbs.

This makes the guarantee **at-least-once**. Exactly-once needs an idempotent
destination — the postgres destination's `staged_merge` mode (a `MERGE` by
`merge_key`) is exactly that: re-applying the delta is harmless, so the
crash-window between "destination committed" and "core persisted the watermark"
is closed for free.

## Commit granularity = resume granularity

A destination advertises how durable it is:

| Capability | When durable | Watermark cadence | Guarantee |
|---|---|---|---|
| *(none)* | at `finalize` | end of run | at-least-once |
| `checkpointed_commit` | incremental flush | intra-run | at-least-once |
| `staged_merge` | atomic swap at end | end of run | exactly-once effective |

All three flow through the same `committed` mechanism — a destination that only
becomes durable at the end simply emits a single final `committed` covering every
batch, which the core treats as end-of-run persistence. `checkpoint_interval` in
the `state:` block throttles how often the core writes the state file.

## Ordering invariants for plugin-internal parallelism

The mechanism above relies on the data plane being **one serial, in-cursor-order
stream** — `batch_seq` only works as a join key because source, pump, and
destination all see the same sequence. A plugin is free to parallelize its own
I/O internally (multiple DB connections, concurrent fetches/writes), as long as
it preserves that serial order at the protocol boundary.

**Source side.** A source may fan out fetches across connections/partitions
internally, but must **reorder results back into cursor order** before returning
them from `next_batch()` — and therefore before calling `events.checkpoint()`.
The core never sees out-of-order batches or a non-monotonic watermark.

**Destination side.** `committed { batch_seq }` must report the **highest
contiguous prefix** that is durable — the largest `N` such that batches `1..=N`
are *all* durable — not simply "the most recently finished batch". If a
destination completes batches 5, 3, 2 (in that order) while 1 and 4 are still in
flight, it must not yet emit `committed { 5 }`; nothing has advanced past 0 until
batch 1 lands. This is the same out-of-order buffering as the source side, just
on the acknowledgement path (a cumulative ack, like TCP).

**Idempotency at every durable layer, not just the last one.** If a destination
parallelizes writes via independent commits (e.g., multiple connections each
committing their own batch into a *shared, persistent* staging table), a batch
can become durable in the database **before** it contributes to the contiguous
prefix above — and therefore before the core ever learns about it. On crash, the
core's watermark hasn't advanced, so the source replays that batch. If the
intermediate write isn't itself idempotent, the replay produces duplicate rows in
staging, which a key-based final swap (e.g. `MERGE`) can't always reconcile (most
engines reject a single `MERGE`/`ON CONFLICT` statement matching the same target
row twice). So every write step that can independently become durable — not just
the final swap — must be idempotent by the merge key (e.g. staging writes use
`INSERT ... ON CONFLICT (merge_key) DO UPDATE`, not a plain `INSERT`/`COPY`).
With that, replaying already-durable work on resume is always safe: at worst it's
wasted work, never a duplicate.

## Backend & locking

State lives behind a `StateBackend` trait. The shipped backend is **local**: a
JSON state file written atomically (temp file + `rename`), with a sibling
`<path>.lock` created via `O_EXCL`. A lock held by a *live* process fails the
second run fast (so two concurrent runs of one pipeline can't corrupt the
watermark); a lock left by a *dead* process is stolen with a warning. A remote
`s3` backend (conditional PUT for a distributed lock) slots in behind the same
trait later.

The state document also records a **schema fingerprint**; if the source shape
changed since the recorded run, `on_schema_change` decides whether to refuse
(default) or continue.

## Operating it

```bash
loadsmith state show pipeline.yaml   # print the persisted watermark
loadsmith state rm   pipeline.yaml   # clear it — the next run is a full read
```

See the [Pipeline YAML reference](../reference/pipeline-yaml.md) for the `state:`
block, the source `incremental` block, and the postgres destination modes.
