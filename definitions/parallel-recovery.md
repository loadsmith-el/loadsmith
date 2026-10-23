# Parallelism and recovery — design plan

Status: draft for review (2026-10-23). Inputs: the scaling bench (1M–25M rows,
`loadsmith-bench/RESULTS.md` "At scale"), the parallelism investigation
(`loadsmith-bench/parallelism-investigation.md`), the 2026-10-20 mid-run restart
evaluation, and `docs/src/architecture/incremental-state.md`.

## 0. Goals

1. **Speed at volume.** ≥2× wall time on the 25M-row full loads (Postgres and
   Oracle → Parquet → S3) against 1.0.0, without giving up flat memory.
   Today Loadsmith runs them on ~1 core (pg 0.9, Oracle 1.6) and leads Embulk
   by only ~1.2×.
2. **Resume where it stopped.** A run that fails mid-way (database drop, plugin
   crash, S3 outage, node restart) continues from the last *delivered* point,
   for incremental **and** full loads, without duplicates or gaps, and lands in
   the same output prefix as if it had never failed.
3. **No protocol v2.** Everything below is an additive v1 change (new optional
   fields, new optional messages, new capabilities), per the tolerant-reader
   rule in `docs/src/reference/stability.md`.

Non-goals: a multi-threaded pump or several data planes (rejected in
`ROADMAP.md`, and the core is not the bottleneck); parallel writers for the
database destinations (one transaction = one connection; out of scope here).

## 1. The idea that ties the three together

Recovery and parallelism meet in one place: **which part of the input is
durably at the target**. Today that is answered once, at the end of the run,
and answered wrong when a sink is configured (B1 below). The plan makes it a
continuously advancing, exact quantity, the **delivered prefix**, and uses it
for both:

- **Batches stay one ordered stream** (`batch_seq`), so the join between a
  source's `checkpoint` and the target's durability keeps working.
- **Every output object declares the batch range it holds**
  (`object_ready.batches = {first, last}`). Parquet already writes batches into
  one file at a time, in order, so ranges are contiguous.
- **Durable = delivered.** With a sink, a batch is durable when the object that
  holds it has been delivered; without one, when the destination has closed
  (fsync + rename) that object. The core computes the highest contiguous
  delivered `batch_seq` and gates the watermark on it, exactly as it gates on
  `committed` today.
- **Partitions are the unit of both parallelism and full-load resume.** A
  partitioned full load reads N key/ctid/ROWID ranges at once; its watermark is
  the set of partitions whose every batch is delivered. That value grows with
  `batch_seq`, so the existing "watermark of the highest durable seq" rule
  stays correct with no protocol change.
- **Column-parallel Parquet** keeps one file at a time in batch order, so it
  speeds the destination up without disturbing batch ranges.

## 2. Bug B1 — fix first (data loss on incremental + sink)

`loadsmith-core/src/runner.rs:622-660`: after the drains end, the state
supervisor does its final watermark flush **before** the sink task is awaited.
The Parquet destination reports `committed` at `finalize`, i.e. when the last
file is written locally, not delivered. If the sink then fails on that file
(or on any queued object), the run fails **with the watermark already
advanced**: the next incremental run starts after rows that never reached S3.
This hits the client flow (Oracle/Postgres incremental → Parquet → S3).

Fix (engine patch, 1.0.1): when a sink is configured, the watermark is
persisted only after the sink supervisor reports every object delivered; on a
sink failure, nothing advances. Prove it with a lab chaos case (sink fails on
the last object; run 2 must deliver those rows).

## 3. Architecture

### 3.1 Data plane (unchanged contract)

One serial Arrow IPC stream, `batch_seq` 1..N, the core in the middle. All
parallelism is inside plugins; anything parallel reorders back (or, for
unordered full loads, is order-free by construction, see 3.5) before the
protocol boundary.

### 3.2 Durability by object ranges

- **Protocol (additive):** `object_ready` gains an optional
  `batches: {first, last}`; the destination declares the capability
  `object_batch_ranges`. Older destinations omit it and keep today's
  end-of-run behavior.
- **Core:** the sink supervisor already keeps the ledger (`emitted`,
  `delivered`). It now also maps object → range and feeds the state supervisor
  a `durable_through` = highest contiguous `batch_seq` whose objects are all
  delivered. Without a sink, the destination reports `durable_through` itself
  (SDK `DestinationPlugin::durable_through`, already in the SDK) as files close.
- **Parquet:** emit the range per chunk; implement `durable_through` for the
  no-sink case; never re-delete a dataset on resume (3.4).
- Result: `checkpoint_interval` finally means something for the client flow;
  the watermark advances every delivered file (~every 64 MiB).

### 3.3 Tie-safe watermarks

A checkpoint must never sit between two rows with the same cursor value: a
resume reads `> watermark` and would skip the tied rows of the next batch
(found 2026-10-20). Rule: a source's checkpoint value is the last cursor value
it knows is **complete**, i.e. followed by a strictly greater value. Sources
with a fetch-ahead (all of them after 3.6) can peek the next row. Rows of the
boundary value are re-read on resume (at-least-once, as documented). SDK
helper so every source does it the same way.

### 3.4 Resume record (state format v3)

The state file gains an optional `resume` object, separate from the
incremental `cursor_value`:

```json
"resume": {
  "run_id": "4242-1781078400000",
  "config_hash": "…",
  "token": { "partitions_done": [0, 1, 4] },
  "next_object_seq": 9,
  "attempt": 2
}
```

- Written whenever the delivered prefix advances during a run; removed when the
  run succeeds.
- **A resumed run reuses `run_id`**, so its objects land under the same
  `<date>/<run_id>/` prefix and continue the object numbering
  (`next_object_seq`): one complete dataset, as if nothing had failed.
- Refused (fresh run, with a warning) when `config_hash` differs, i.e. the
  pipeline changed.
- Incremental: the token is unnecessary (the cursor is the token); the record
  only carries the run identity and object numbering.
- Full load, unpartitioned: no safe resume point inside one cursor without an
  ordered key; the run restarts from zero (the same prefix, objects replaced).
  With `parallel.split_by` (3.5): resume skips finished partitions.
- Migration: v2 files read as "no resume"; v3 is written only when a run first
  records one.

### 3.5 Partitioned reads (opt-in)

`source.config.parallel: {workers: N, split_by: ctid | rowid | <column>}`.

- **Postgres:** one exported snapshot (`pg_export_snapshot()` + `SET
  TRANSACTION SNAPSHOT`), N connections over `ctid` ranges (PG14+ TID range
  scan) or ranges of an indexed column.
- **Oracle:** `AS OF SCN <scn>` for consistency; ROWID ranges from the extent
  map (or `DBMS_PARALLEL_EXECUTE` chunks when the grant exists).
- **Full load:** batches from all partitions go out first-come; the watermark is
  the set of finished partitions.
- **Incremental:** the cursor range is cut into many small ordered chunks;
  workers take them in order and a bounded reorder window emits batches in
  cursor order (the documented source rule).
- **Resume of a partitioned full load is consistent by default.** Oracle resumes
  at the same SCN while undo still covers it; when the original snapshot cannot
  be reused (Postgres: an exported snapshot dies with its transaction; Oracle:
  `ORA-01555`), the full load restarts from zero with a clear warning. Opt-in
  `parallel.resume_snapshot: new` reads the remaining partitions at a new
  snapshot (the output then mixes two points in time); it is accepted only with
  a key `split_by` (never `ctid`/`rowid`, where an UPDATE moves a row between
  partitions: duplicated or lost).
- Only for `query:` shapes that can be split (a table or a simple
  `SELECT … FROM t [WHERE …]`); anything else errors at configure with the
  reason.

### 3.6 Single-node CPU work (no semantic change)

From the investigation (§1.5, O2–O4), all byte-identical output:

- **Oracle source:** native cells (no number → String → number round trip),
  ownership instead of clones, per-column builders filled on the fetch thread.
- **Postgres source:** a fetch task one or two batches ahead; allocation-free
  decimal and timestamp parsing.
- **Both sources:** Arrow columns built in parallel (rayon over the 34 columns).
- **SDK:** the source's IPC writer on its own thread. *(Done 2026-10-23.)*
- **Core:** 1 MiB pipes; forward IPC messages raw instead of decode + re-encode.
  *(Deferred 2026-10-23: F0 measured the core at 0.7 CPU-s of a 17 s run; not
  worth the complexity until a profile says otherwise.)*
- **Parquet:** column-parallel encoding (`get_column_writers` +
  `ArrowColumnChunk`, already in the pinned parquet 54.3.1); same row groups,
  same rollover, same bytes.
- **S3 sink (optional, slow links):** concurrent multipart parts.

### 3.7 Automatic recovery

`pipeline.recovery: {max_attempts: 1, backoff: 30s, resume: auto}`. The default
(`max_attempts: 1`) is today's behavior. With more attempts the core retries
the whole pipeline in-process after a failure, resuming from the record (3.4),
with exponential backoff. `loadsmith run --no-resume` forces a fresh run;
`loadsmith state show` prints the resume record. Hot restart of a single
source/destination mid-stream stays out of scope: the whole-pipeline retry with
an exact resume point gives the same result with far less machinery.

### 3.8 Flow control: the core grants, plugins obey

Principle: **the core controls the flow; a plugin only does its job.** Today
the only flow control is the pipe: a slow destination fills the pipe and the
source's `write` blocks. That bounds a plugin that does one thing at a time. It
does not bound a plugin with workers or read-ahead (3.5, 3.6): each worker
would keep fetching into memory while the pipe is full, and a source that reads
10× faster than the destination writes would pull the table into RAM.

- **Credits (additive protocol):** the core sends `credit {batch_seq}` to the
  source on the control channel, with the initial grant in `start.credit`; a
  source with the capability `flow_credit` may have fetched at most up to that
  batch ordinal — across all its workers and read-ahead buffers. Grants are
  cumulative and never shrink (a lost or late one never takes credit back).
  The core grants after each batch the pump hands to the destination ("me dá
  mais três"), so a destination that falls behind stops the grants ("peraí").
- **The SDK enforces it**, not each plugin: the SDK never calls `next_batch`
  past the grant, advertises `flow_credit` itself, and hands the plugin a
  `FlowGate` (`attach_flow`) whose `wait_for(seq)` every internal fetch (a
  fetch task, a partition worker) awaits before fetching batch `seq`.
- **The core decides the window**: `pipeline.flow: {max_inflight_batches: 8,
  memory_budget_mib: 256}`. It starts at 2 batches and is resized after each
  batch to what the memory budget holds at the largest batch seen, within
  `max_inflight_batches`. The run logs the pump's time on each side
  (`reading_s`, `writing_s`) and the peak window. *(Implemented 2026-10-23.
  AIMD was dropped: the pump's read/write timings mix waiting with transfer
  through the pipes, so they cannot tell "source starved of credit" from "data
  in transit"; a memory-sized window bounds RAM, which is the requirement.)*
- **Partition workers follow credits.** `parallel.workers` is a ceiling; with no
  credit, workers wait idle instead of buffering.
- **Destination side** needs no grant: it pulls from fd3, and the column-parallel
  Parquet encoder works on one row group at a time.
- **Older plugins** without `flow_credit` get no credits (the core sends them
  only to a source that advertised the capability) and keep today's pipe
  backpressure, which bounds a source that fetches one batch at a time.

## 4. Invariants every piece must keep

| Piece | Must guarantee |
|---|---|
| Sources (ordered mode) | batches in cursor order; tie-safe checkpoint values; stop on `cancel`/dropped `next_batch`; never more in flight than the core's credit |
| Sources (partitioned full load) | every row in exactly one partition; one snapshot per attempt; partition set grows with `batch_seq` |
| Parquet | one open file at a time, batches in order; contiguous `batches` range per object; deterministic names from `next_object_seq` |
| Sink | idempotent `deliver` (already); ledger keyed by object |
| Core | watermark/resume persisted only at the delivered prefix; resume refused on config drift |

## 5. Phases

| Phase | Content | Repos | Release |
|---|---|---|---|
| F0 | Baseline per-thread CPU + null-destination ceiling. **B1 fix** + chaos lab case | engine, lab, bench | engine 1.0.1 |
| F1 | **Flow control (3.8)** first, then single-thread wins + pipelining (3.6 minus column-parallel) on top of it: oracle, postgres, SDK, core pipes/raw forward | plugins, engine | oracle 0.6, postgres 0.5, engine 1.1 |
| F2 | Column-parallel Arrow build + column-parallel Parquet | plugins | parquet 0.3, sources |
| F3 | Object ranges + delivered-prefix durability + tie-safe watermarks + resume record v3 + `recovery:` | engine, SDK, parquet, sources | engine 1.1 / 1.2 |
| F4 | Partitioned reads + partition-set resume | postgres, oracle | minor releases |
| F5 | Bench rerun (scale, network, chaos/resume scenario vs Embulk), RESULTS.md, client document | bench | — |

F1/F2 (performance, plugin internals) and F3 (durability, core/SDK) touch
different layers and can be developed in parallel worktrees after F0. F4 needs
both: it multiplies the data rate (needs F2) and its resume needs F3.

### F0 results (2026-10-23)

B1 fixed and released in **1.0.1** (lab case
`catalog/postgres-to-parquet-localcopy-outage`: run 2 read 0 rows on 1.0.0,
100,000 with the fix).

Baseline, 5M rows of `events_scale`, Loadsmith 1.0.0 + oracle 0.5.2, CPU
seconds per thread sampled from `/proc` (DB = the database container's CPU):

| Run | Wall | Source threads | Destination | DB |
|---|---|---|---|---|
| pg → null | 16.0 s | main 10.3 s (0.65 core) | — | 5.8 s |
| pg → Parquet → S3 | 19.3 s | main 10.7 s | 7.3 s (0.43) | 6.4 s |
| oracle → null | 18.2 s | **`oracle-fetch` 15.4 s (0.85)**, main 6.1 s | — | 3.9 s |
| oracle → Parquet → S3 | 21.0 s | `oracle-fetch` 15.6 s, main 6.2 s | 7.4 s (0.39) | 3.9 s |

- **Postgres:** source CPU + DB CPU ≈ wall (10.3 + 5.8 ≈ 16.0): the FETCH loop
  is strictly serial, never overlapped. Fetch-ahead alone should bring the
  source to max(10.3, 5.8); column-parallel decode and Parquet then leave the
  database (~6 s) as the floor: ~3× on the Parquet run.
- **Oracle:** the fetch thread is saturated (TNS decode + per-cell `String`),
  and the main thread spends 6.1 s parsing those strings back. The database
  needs only 3.9 s. Native cells (W1–W4) and parallel conversion target ≥2×.

## 6. Validation

- **Byte-identical output** for F1/F2: compare Parquet against 1.0.0 on the same
  seed; type-coverage lab cases.
- **Chaos lab cases** (lab-only fault plugin/feature, never in release
  binaries) with a row-level expectation (no duplicate, no gap): kill the
  source at batch N, kill the destination, fail the sink on object K and on the
  last object, kill the engine (SIGKILL) mid-run; then resume and compare with
  the source table. Incremental with ties on the cursor across a batch boundary.
- **Bench:** `scale` 5M/25M on the scrambled tables (targets: ≤50% wall of
  1.0.0, peak memory within +30%), `netsuite` hybrid/slow, and a new scenario:
  kill at 60% and resume, Loadsmith vs Embulk (Embulk restarts from zero).

## 7. Open decisions

1. ~~Resumed partitioned full load at a new snapshot?~~ Decided 2026-10-23:
   consistent by default (same SCN or restart), `resume_snapshot: new` opt-in
   with a key `split_by` only (3.5).
2. ~~Default `recovery.max_attempts`: keep 1 (opt-in) or default to 3?~~
   Decided 2026-10-23: default 1 (off); retry is opt-in.
3. Object names on resume continue the numbering in the same prefix — any
   consumer that expects one prefix per attempt instead?
4. ~~B1 as an immediate 1.0.1 before the rest~~ Done: released as 1.0.1.
