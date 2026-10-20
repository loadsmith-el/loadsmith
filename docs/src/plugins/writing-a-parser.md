# Writing a Parser

Some data does not come from a system that speaks rows. It comes as **files**:
CSV exports in a directory, spreadsheets on a mounted bucket, JSON dumps an
upstream job leaves behind. Loadsmith splits that job in two plugins:

- a **raw source** finds (or downloads) the files and announces each one;
- a **parser** turns each file into Arrow batches.

```yaml
source:   { type: file, config: { path: /mnt/s3/inbox, glob: "*.csv" } }
parser:   { type: csv,  config: { delimiter: ",", header: true } }
destination: { type: postgres, config: { … } }
```

```
raw source ──fd4 object_ready{path}──▶ core ──fd0 parse_object{path}──▶ parser ──fd3──▶ core pump ──▶ destination
```

The core stays in the middle, as always. A raw source never talks to the
parser, and the parser's Arrow stream goes through the same pump, timeouts and
commit point as a regular source's. One `csv` parser serves every raw source
(local files, S3, SFTP), and one `file` source serves every format.

## The lifecycle

**Raw source** (kind `source`, capability `object_output`, no fd3):
handshake → capabilities → `configure` → `start` → `ready`, then one
`object_ready{path}` event on fd4 per **complete** local file, then
`finished`.

**Parser** (kind `parser`, capability `parse`, writes fd3): handshake →
capabilities → `configure` → `start`, then one `parse_object{path}` per file
on stdin. It sends `schema` right after `start` when its config declares the
columns, otherwise after it opened the first file (the core keeps sending
`parse_object` meanwhile). It writes each file's batches to fd3 and reports
`object_parsed{path, rows, batches, last_batch_seq}` on fd4. **Stdin EOF**
means "no more files": the parser ends the IPC stream with its end-of-stream
marker and sends `finished{rows_read, batches_read}`.

The core orders it like this:

1. handshake and configure the raw source, the parser, the destination;
2. capability gates (below);
3. `start` the parser, then the raw source (it answers `ready`);
4. hand the parser every announced file until its `schema` arrives
   (`timeouts.start_seconds`), then wait for its IPC stream header;
5. `start` the destination, then the pump — files keep flowing to the parser;
6. once the raw source's fd4 reached EOF (every `object_ready` read) **and**
   it sent `finished` with success, close the parser's stdin;
7. **commit point:** end the destination's input only after the parser's
   stream ended with its marker, the parser sent `finished` success, **and**
   the raw source did too.

## What the core guarantees

- **No partial load after an upstream failure.** The parser's stdin is closed
  gracefully only after the raw source succeeded. If the raw source fails,
  crashes or times out half-way, the parser gets `cancel` (never EOF), so it
  never ends its stream; the destination gets `cancel` and rolls back. Files
  already parsed are not committed.
- **Any failure aborts all three.** A parser crash, a `finished` error, a
  protocol violation or a timeout aborts the run with the
  [abort rules](../protocol/reference.md#abort-rules): `error` to the culprit,
  `cancel` to the others, grace period, kill.
- **Each path once.** The core sends `parse_object` once per path, in the
  order the raw source announced them. (A parser should still treat a repeated
  path as already handled: `parse_object` is idempotent per path.)
- **A parser that finishes early is rejected.** `finished` with success
  before the core closed the parser's stdin is a protocol violation: more
  files could still come.
- **Scratch space.** A raw source that downloads its files advertises
  `scratch_dir`: the core then adds `scratch_dir: <fresh directory>` to its
  config (unless the user set one) and removes that directory after a
  **successful** run. After a failure the downloads are kept for inspection.
  The core never deletes files it did not allocate (a local directory, a
  mounted bucket read in place).

Capability gates (checked after every handshake, before anything starts):

| Situation | Result |
|---|---|
| `parser:` block, source without `object_output` | configuration error, exit `3` |
| source with `object_output`, no `parser:` block | configuration error, exit `3` |
| parser that does not advertise `parse` | incompatible plugin, exit `4` |
| parser binary missing | plugin not found, exit `4` |
| `parser:` together with `state:` | validation error, exit `3` (incremental loads through a parser are deferred to 1.x) |

## The `ParserPlugin` trait

```rust
#[async_trait]
pub trait ParserPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Defaults to ["parse"], which the core requires.
    fn capabilities(&self) -> Vec<String> { vec!["parse".into()] }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

    /// The stream's schema when the config fixes it (e.g. `columns:`).
    fn declared_schema(&self) -> Option<Schema> { None }

    /// Opens one file; returns its schema (declared or inferred).
    async fn open(&mut self, path: &Path) -> Result<Schema>;

    /// The next batch of the file opened last; `None` at its end.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    async fn cancel(&mut self);
}
```

| Method | When called | What to do |
|---|---|---|
| `plugin_name()` | During handshake | Return `"loadsmith-parser-{type}"` |
| `capabilities()` | During handshake | Keep the default `["parse"]` |
| `configure(config)` | After `configure` | Parse and validate the options (delimiter, encoding, header, declared columns). `Err` rejects the config (exit `3`) |
| `declared_schema()` | Once, after `start` | Return the schema the config declares, or `None` to infer it from the first file |
| `open(path)` | Once per file, in arrival order | Open the file and return its schema. Infer types from a bounded sample (the first N rows), never by reading the whole file |
| `next_batch()` | Until it returns `None` | Return the next batch of the open file. Stream: hold one batch, not the file |
| `cancel()` | On `cancel`/`error` after `start`, or after your own failure | Close what you hold |

`run_parser(plugin)` drives everything else:

- **One schema per stream.** Without a declared schema, the first file's
  schema is the stream's; every later file must match it — the same columns,
  names and types, in order — or the run fails with `SCHEMA_MISMATCH` (for
  example *b.csv: column 2 (amount) is utf8, the stream has int64*). A column
  may be non-nullable in one file where the stream's is nullable. Batches are
  relabelled with the stream's schema before they are written.
- **Zero files.** With a declared schema, stdin EOF before any file is a
  valid, empty load (the stream has a header and no batch). Without one it is
  `NO_INPUT`: there is nothing to describe the stream with.
- **Control channel.** `open` and `next_batch` run while the SDK reads stdin:
  `cancel` interrupts them (the in-flight future is dropped before `cancel()`
  runs), paths that arrive meanwhile are queued, `ping` and `resend` are
  answered.
- **Events.** `progress` after every batch, `object_parsed` after every file.
- **Error codes** in `finished`: `OPEN_FAILED` (`open` failed), `PARSE_FAILED`
  (`next_batch` failed), `SCHEMA_MISMATCH`, `NO_INPUT`, `IPC_WRITE_FAILED`,
  `CONFIGURE_FAILED`.

```rust
#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_parser(CsvParser::default()).await
}
```

## The `ObjectSourcePlugin` trait (raw sources)

```rust
#[async_trait]
pub trait ObjectSourcePlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Defaults to ["object_output"]. Add "scratch_dir" to get a
    /// core-allocated download directory in the config.
    fn capabilities(&self) -> Vec<String> { vec!["object_output".into()] }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

    /// The next complete local file; `None` when there are no more.
    async fn next_object(&mut self) -> Result<Option<PathBuf>>;

    async fn cancel(&mut self);
}
```

`run_object_source(plugin)` answers `start` with `ready`, sends an
`object_ready` event for every path `next_object` returns, and `finished`
once it returns `None` (an error is `finished` error `OBJECT_FAILED`). Return
a path only once the file is complete: the parser may open it at once. List
files in a deterministic order (sorted), so reruns parse them in the same
order. `cancel` interrupts a `next_object` in flight (a long download).

A raw source reports no row counts: the summary's rows read come from the
parser.

## What a canonical `file` source and `csv` parser need

- **`file` source** (`ObjectSourcePlugin`): `path` + `glob` config; list the
  matching files sorted; return each path in place (no copy, no
  `scratch_dir`); fail on an unreadable directory, not on an empty match (the
  parser decides what zero files means).
- **`csv` parser** (`ParserPlugin`): stream the file (never load it whole);
  infer types from the first `infer_rows` rows, or take declared `columns:`
  (then return them from `declared_schema`); `delimiter`, `encoding`,
  `header` options; infer columns as nullable unless declared otherwise, so
  files with and without empty cells share one schema.

## Not in 1.0

- **Incremental loads through a parser** (`parser:` + `state:`). The
  `object_parsed.last_batch_seq` field already ties each file to the batch
  ordinals, so per-file checkpoints can be added in 1.x without a new message.
- **Backpressure on the raw source.** It announces files as fast as it finds
  them; a downloading source should bound how far it runs ahead of the parser
  on its own.
