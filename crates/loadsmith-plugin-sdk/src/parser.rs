//! The parser runtime: turns the local files a raw source produced into one
//! Arrow IPC stream on fd3.
//!
//! Lifecycle (driven by the core): handshake → capabilities (`parse`) →
//! `configure` → `start`. Then one `parse_object{path}` per file on stdin.
//! The parser answers `start` with `schema`: at once when the config declares
//! the schema ([`ParserPlugin::declared_schema`]), otherwise after it opened
//! the first file. Each file's batches go to fd3, followed by an
//! informational `object_parsed` event on fd4. Stdin EOF means "no more
//! files": the SDK ends the IPC stream (the end-of-stream marker) and sends
//! `finished{rows_read, batches_read}`.
//!
//! The stream has **one** schema. A later file whose schema differs fails the
//! run with `SCHEMA_MISMATCH`; zero files without a declared schema fail it
//! with `NO_INPUT`. `cancel`/`error` from the core stop the parser at any
//! point, also in the middle of `open` or `next_batch`.

use std::collections::{HashSet, VecDeque};
use std::io::Write;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_arrow::IpcWriter;
use loadsmith_protocol::{capabilities, Finished, Message, PluginKind};
use tokio::io::AsyncWrite;

use crate::context::EventSender;
use crate::config::PluginConfig;
use crate::control::{answer_configure, conclude, handshake, recv_configure, recv_start, unexpected, Control, Flow, Inbound};

/// `finished.code` when a file's schema differs from the stream's.
pub const SCHEMA_MISMATCH: &str = "SCHEMA_MISMATCH";
/// `finished.code` when stdin closed before any file arrived and the config
/// declares no schema (there is nothing to describe the stream with).
pub const NO_INPUT: &str = "NO_INPUT";

/// Implemented by parser plugins. The SDK drives the conversation, owns the
/// Arrow stream and enforces its single schema; implementors read one file
/// at a time.
#[async_trait]
pub trait ParserPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake. Defaults to `parse`,
    /// which the core requires.
    fn capabilities(&self) -> Vec<String> {
        vec![capabilities::PARSE.into()]
    }

    /// Called after `configure`. Validate the config and prepare. Return
    /// `Err` to send `configure_ack { error }` and exit.
    async fn configure(&mut self, config: PluginConfig) -> Result<()>;

    /// The stream's schema when the config fixes it (for example a
    /// `columns:` list). Called once, after `start`. With it, the SDK sends
    /// `schema` at once, every file must match it, and zero files is a valid,
    /// empty load. Without it (the default) the first file's schema is the
    /// stream's, and zero files is a `NO_INPUT` error.
    fn declared_schema(&self) -> Option<Schema> {
        None
    }

    /// Opens one file and returns its schema (declared or inferred). Its
    /// batches are then read with [`next_batch`](Self::next_batch) until it
    /// returns `None`. Called once per distinct path, in arrival order.
    async fn open(&mut self, path: &Path) -> Result<Schema>;

    /// The next batch of the file opened last; `Ok(None)` at its end. Each
    /// batch must have the schema `open` returned.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Called when the run will not complete, once `start` was received: the
    /// core sent `cancel` or `error`, the core is gone, or this parser
    /// failed. Release what you hold (open files). An in-flight `open` or
    /// `next_batch` future may have been dropped before this runs.
    async fn cancel(&mut self);
}

/// Entry point for parser plugin binaries. Never returns normally.
///
/// Exit code: `0` after success or a `cancel`, `1` after a failure, an
/// `error` from the core, or a lost core.
pub async fn run_parser<P: ParserPlugin + 'static>(mut plugin: P) -> ! {
    let mut ctl = Control::stdio();
    let mut started = false;
    // fd4 = event out; fd3 = the Arrow IPC stream, opened once the schema is
    // known.
    let event_fd = unsafe { std::fs::File::from_raw_fd(4) };
    let mut events = EventSender::new(tokio::fs::File::from_std(event_fd));
    let data = || Ok(unsafe { std::fs::File::from_raw_fd(3) });
    let result = drive_parser(&mut plugin, &mut ctl, &mut events, &mut started, data).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
    std::process::exit(code);
}

/// The paths received so far: queued, already seen (`parse_object` is
/// idempotent per path), and whether stdin reached EOF.
#[derive(Default)]
struct Inbox {
    queue: VecDeque<PathBuf>,
    seen: HashSet<PathBuf>,
    eof: bool,
}

impl Inbox {
    /// Handles one inbound control line.
    async fn take<W: AsyncWrite + Unpin>(&mut self, ctl: &mut Control<W>, inbound: Inbound) -> Result<(), Flow> {
        match inbound {
            Inbound::Eof => self.eof = true,
            inbound => match ctl.process(inbound).await? {
                None => {}
                Some(Message::ParseObject(p)) => {
                    let path = PathBuf::from(p.path);
                    if self.seen.insert(path.clone()) {
                        self.queue.push_back(path);
                    } else {
                        tracing::debug!(path = %path.display(), "ignoring a repeated parse_object");
                    }
                }
                Some(other) => return Err(unexpected("parse_object", &other)),
            },
        }
        Ok(())
    }

    /// Waits for the next control line (there is no work in flight).
    async fn wait<W: AsyncWrite + Unpin>(&mut self, ctl: &mut Control<W>) -> Result<(), Flow> {
        let inbound = ctl.next_inbound().await;
        self.take(ctl, inbound).await
    }
}

/// Runs `work` while reading the control channel, so `cancel` stops the
/// parser mid-file and paths that arrive meanwhile are queued.
async fn with_control<W, T>(
    ctl: &mut Control<W>,
    inbox: &mut Inbox,
    work: impl std::future::Future<Output = T>,
) -> Result<T, Flow>
where
    W: AsyncWrite + Unpin,
{
    tokio::pin!(work);
    loop {
        tokio::select! {
            biased;
            inbound = ctl.next_inbound(), if !inbox.eof => inbox.take(ctl, inbound).await?,
            out = &mut work => return Ok(out),
        }
    }
}

/// The open Arrow stream on fd3, with its one schema.
struct Stream<D: Write> {
    schema: Arc<Schema>,
    writer: IpcWriter<D>,
}

impl<D: Write> Stream<D> {
    /// Sends `schema` and writes the IPC header: the core reads the message,
    /// then the header (the authoritative schema).
    async fn open<W: AsyncWrite + Unpin>(
        ctl: &mut Control<W>,
        data: &mut Option<impl FnOnce() -> std::io::Result<D>>,
        schema: Schema,
    ) -> Result<Self, Flow> {
        let fields = loadsmith_arrow::protocol_fields_from_schema(&schema);
        ctl.send(Message::Schema(loadsmith_protocol::Schema { fields })).await?;
        let open = data.take().expect("the stream is opened once");
        let sink = open().map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("open fd3: {e}")))?;
        let writer = IpcWriter::new(sink, &schema)
            .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("{e}")))?;
        Ok(Self { schema: Arc::new(schema), writer })
    }

    /// Fails with `SCHEMA_MISMATCH` unless `file` fits the stream: the same
    /// columns, names and types in the same order. A column may be
    /// non-nullable in the file where the stream's is nullable.
    fn check(&self, file: &Schema, path: &Path) -> Result<(), Flow> {
        let stream = self.schema.fields();
        let theirs = file.fields();
        let problem = if stream.len() != theirs.len() {
            Some(format!("{} columns, the stream has {}", theirs.len(), stream.len()))
        } else {
            stream.iter().zip(theirs.iter()).enumerate().find_map(|(i, (s, f))| {
                let n = i + 1;
                if s.name() != f.name() {
                    Some(format!("column {n} is named `{}`, the stream's is `{}`", f.name(), s.name()))
                } else if s.data_type() != f.data_type() {
                    Some(format!(
                        "column {n} (`{}`) is {}, the stream has {}",
                        f.name(),
                        type_label(f.data_type()),
                        type_label(s.data_type())
                    ))
                } else if f.is_nullable() && !s.is_nullable() {
                    Some(format!("column {n} (`{}`) is nullable, the stream's is not", f.name()))
                } else {
                    None
                }
            })
        };
        match problem {
            None => Ok(()),
            Some(p) => Err(mismatch(path, p)),
        }
    }

    /// The batch, relabelled with the stream's schema (a file's schema may
    /// differ in nullability only).
    fn conform(&self, batch: RecordBatch, path: &Path) -> Result<RecordBatch, Flow> {
        if batch.schema() == self.schema {
            return Ok(batch);
        }
        RecordBatch::try_new(Arc::clone(&self.schema), batch.columns().to_vec())
            .map_err(|e| mismatch(path, format!("a batch does not fit the stream's schema: {e}")))
    }
}

fn mismatch(path: &Path, problem: String) -> Flow {
    Flow::fatal(SCHEMA_MISMATCH, anyhow::anyhow!("{}: {problem}", path.display()))
}

fn type_label(dt: &arrow_schema::DataType) -> String {
    loadsmith_arrow::protocol_type_from_arrow(dt).to_string()
}

pub(crate) async fn drive_parser<P, W, E, D, F>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    events: &mut EventSender<E>,
    started: &mut bool,
    data: F,
) -> Result<(), Flow>
where
    P: ParserPlugin,
    W: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
    D: Write,
    F: FnOnce() -> std::io::Result<D>,
{
    // ── Handshake + capabilities ─────────────────────────────────────────
    handshake(ctl, PluginKind::Parser, plugin.plugin_name(), plugin.plugin_version(), plugin.capabilities())
        .await?;

    // ── Configure ────────────────────────────────────────────────────────
    let (config, warnings) = recv_configure(ctl).await?;
    let outcome = plugin.configure(config).await;
    answer_configure(ctl, events, &warnings, outcome).await?;

    // ── Start ─────────────────────────────────────────────────────────────
    recv_start(ctl).await?;
    *started = true;

    let mut data = Some(data);
    let mut stream: Option<Stream<D>> = None;
    if let Some(schema) = plugin.declared_schema() {
        stream = Some(Stream::open(ctl, &mut data, schema).await?);
    }

    // ── Parse loop ───────────────────────────────────────────────────────
    // One file at a time, in arrival order; paths that arrive meanwhile are
    // queued. EOF on stdin: finish what is queued, then end the stream.
    let mut inbox = Inbox::default();
    let (mut rows_total, mut batches_total) = (0u64, 0u64);
    loop {
        let Some(path) = inbox.queue.pop_front() else {
            if inbox.eof {
                break;
            }
            inbox.wait(ctl).await?;
            continue;
        };

        let schema = with_control(ctl, &mut inbox, plugin.open(&path))
            .await?
            .map_err(|e| Flow::fatal("OPEN_FAILED", e.context(format!("open {}", path.display()))))?;
        match &stream {
            Some(s) => s.check(&schema, &path)?,
            None => stream = Some(Stream::open(ctl, &mut data, schema).await?),
        }
        let out = stream.as_mut().expect("the stream is open");

        let (mut rows, mut batches) = (0u64, 0u64);
        loop {
            match with_control(ctl, &mut inbox, plugin.next_batch()).await? {
                Ok(Some(batch)) => {
                    let batch = out.conform(batch, &path)?;
                    rows += batch.num_rows() as u64;
                    batches += 1;
                    out.writer
                        .write_batch(&batch)
                        .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("{e}")))?;
                    events.progress_source(rows_total + rows, batches_total + batches).await;
                }
                Ok(None) => break,
                Err(e) => {
                    events.error_msg(format!("parse error in {}: {e:#}", path.display())).await;
                    return Err(Flow::fatal("PARSE_FAILED", e.context(format!("parse {}", path.display()))));
                }
            }
        }
        rows_total += rows;
        batches_total += batches;
        let last_batch_seq = if batches > 0 { batches_total } else { 0 };
        events.object_parsed(path.to_string_lossy().into_owned(), rows, batches, last_batch_seq).await;
    }

    let Some(out) = stream else {
        return Err(Flow::fatal(
            NO_INPUT,
            anyhow::anyhow!("no file to parse arrived, and the config declares no schema"),
        ));
    };
    // The end-of-stream marker: written only after the last file parsed.
    out.writer
        .finish()
        .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("IPC finish: {e}")))?;
    ctl.send(Message::Finished(Finished::success_source(rows_total, batches_total))).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    /// fd3 in memory.
    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A "file" is its path: `a.3` has 3 rows of column `id`; `wide.*` has
    /// columns `id` and `x`; `bad.*` fails to open; `slow.*` never opens.
    #[derive(Default)]
    struct Fake {
        declared: bool,
        opened: Vec<String>,
        left: u64,
        wide: bool,
    }

    fn schema(wide: bool) -> Schema {
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        if wide {
            fields.push(Field::new("x", DataType::Int64, false));
        }
        Schema::new(fields)
    }

    #[async_trait]
    impl ParserPlugin for Fake {
        fn plugin_name(&self) -> &str {
            "loadsmith-parser-fake"
        }
        fn plugin_version(&self) -> &str {
            "0.0.0"
        }
        async fn configure(&mut self, config: crate::PluginConfig) -> Result<()> {
            self.declared = config.get("declared").is_some();
            Ok(())
        }
        fn declared_schema(&self) -> Option<Schema> {
            self.declared.then(|| schema(false))
        }
        async fn open(&mut self, path: &Path) -> Result<Schema> {
            let name = path.to_string_lossy().into_owned();
            self.opened.push(name.clone());
            let (stem, n) = name.split_once('.').unwrap_or((name.as_str(), "0"));
            match stem {
                "bad" => anyhow::bail!("unreadable"),
                "slow" => std::future::pending::<()>().await,
                _ => {}
            }
            self.wide = stem == "wide";
            self.left = n.parse().unwrap_or(0);
            Ok(schema(self.wide))
        }
        async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            if self.left == 0 {
                return Ok(None);
            }
            self.left -= 1;
            let mut cols: Vec<arrow_array::ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
            if self.wide {
                cols.push(Arc::new(Int64Array::from(vec![2])));
            }
            Ok(Some(RecordBatch::try_new(Arc::new(schema(self.wide)), cols)?))
        }
        async fn cancel(&mut self) {}
    }

    struct Core {
        to_plugin: DuplexStream,
        from_plugin: BufReader<DuplexStream>,
    }

    impl Core {
        async fn send(&mut self, line: &str) {
            self.to_plugin.write_all(format!("{line}\n").as_bytes()).await.unwrap();
        }
        async fn parse(&mut self, path: &str) {
            self.send(&format!(r#"{{"type":"parse_object","path":"{path}"}}"#)).await;
        }
        async fn recv(&mut self) -> String {
            let mut s = String::new();
            self.from_plugin.read_line(&mut s).await.unwrap();
            s.trim_end().to_string()
        }
        async fn start(&mut self, config: &str) {
            self.send(r#"{"type":"handshake"}"#).await;
            assert!(self.recv().await.contains(r#""kind":"parser""#));
            self.send(r#"{"type":"set_protocol_version","protocol_version":1}"#).await;
            self.send(r#"{"type":"capabilities_request"}"#).await;
            assert_eq!(self.recv().await, r#"{"type":"capabilities_response","supports":["parse"]}"#);
            self.send(&format!(r#"{{"type":"configure","config":{config}}}"#)).await;
            assert_eq!(self.recv().await, r#"{"type":"configure_ack","status":"ok"}"#);
            self.send(r#"{"type":"start"}"#).await;
        }
    }

    struct Outcome {
        ok: bool,
        code: i32,
        opened: Vec<String>,
        data: Vec<u8>,
        events: String,
    }

    fn spawn() -> (Core, tokio::task::JoinHandle<Outcome>) {
        let (core_in, plugin_in) = tokio::io::duplex(1 << 16);
        let (plugin_out, core_out) = tokio::io::duplex(1 << 16);
        let task = tokio::spawn(async move {
            let mut ctl = Control::new(BufReader::new(plugin_in), plugin_out);
            let (ev_w, mut ev_r) = tokio::io::duplex(1 << 20);
            let mut events = EventSender::new(ev_w);
            let mut plugin = Fake::default();
            let mut started = false;
            let buf = Buf::default();
            let sink = buf.clone();
            let result =
                drive_parser(&mut plugin, &mut ctl, &mut events, &mut started, move || Ok(sink)).await;
            let ok = result.is_ok();
            let code = conclude(&mut ctl, result, started, || async {}).await;
            drop(events);
            let mut ev = String::new();
            tokio::io::AsyncReadExt::read_to_string(&mut ev_r, &mut ev).await.unwrap();
            let data = buf.0.lock().unwrap().clone();
            Outcome { ok, code, opened: plugin.opened, data, events: ev }
        });
        (Core { to_plugin: core_in, from_plugin: BufReader::new(core_out) }, task)
    }

    fn read_stream(data: &[u8]) -> (usize, bool) {
        let mut r = loadsmith_arrow::IpcReader::new(data).unwrap();
        let mut n = 0;
        while r.read_batch().unwrap().is_some() {
            n += 1;
        }
        (n, r.ended_cleanly())
    }

    #[tokio::test]
    async fn parses_every_file_once_then_ends_the_stream_on_eof() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.parse("a.2").await;
        assert!(core.recv().await.starts_with(r#"{"type":"schema","fields":[{"name":"id""#));
        core.parse("b.3").await;
        core.parse("a.2").await; // repeated: ignored
        core.to_plugin.shutdown().await.unwrap();
        assert_eq!(core.recv().await, r#"{"type":"finished","status":"success","rows_read":5,"batches_read":5}"#);
        let out = task.await.unwrap();
        assert!(out.ok);
        assert_eq!(out.opened, ["a.2", "b.3"]);
        assert_eq!(read_stream(&out.data), (5, true));
        assert!(out.events.contains(r#"{"type":"object_parsed","path":"a.2","rows":2,"batches":2,"last_batch_seq":2}"#));
        assert!(out.events.contains(r#"{"type":"object_parsed","path":"b.3","rows":3,"batches":3,"last_batch_seq":5}"#));
    }

    #[tokio::test]
    async fn a_file_with_another_schema_is_a_mismatch() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.parse("a.1").await;
        core.parse("wide.1").await;
        core.recv().await; // schema
        let finished = core.recv().await;
        assert!(finished.contains(r#""code":"SCHEMA_MISMATCH""#), "{finished}");
        assert!(finished.contains("wide.1: 2 columns, the stream has 1"), "{finished}");
        let out = task.await.unwrap();
        assert_eq!((out.ok, out.code), (false, 1));
        // The stream is left without its end-of-stream marker.
        assert!(!read_stream(&out.data).1);
    }

    #[tokio::test]
    async fn no_file_and_no_declared_schema_is_no_input() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.to_plugin.shutdown().await.unwrap();
        let finished = core.recv().await;
        assert!(finished.contains(r#""status":"error","code":"NO_INPUT""#), "{finished}");
        let out = task.await.unwrap();
        assert!(out.data.is_empty(), "no stream is opened");
    }

    #[tokio::test]
    async fn a_declared_schema_is_sent_at_start_and_zero_files_is_empty() {
        let (mut core, task) = spawn();
        core.start(r#"{"declared":true}"#).await;
        assert!(core.recv().await.starts_with(r#"{"type":"schema""#));
        core.to_plugin.shutdown().await.unwrap();
        assert_eq!(core.recv().await, r#"{"type":"finished","status":"success","rows_read":0,"batches_read":0}"#);
        let out = task.await.unwrap();
        assert!(out.ok);
        assert_eq!(read_stream(&out.data), (0, true));
    }

    #[tokio::test]
    async fn cancel_interrupts_a_slow_open() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.parse("slow.1").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        core.send(r#"{"type":"cancel","reason":"the source failed"}"#).await;
        assert_eq!(core.recv().await, r#"{"type":"finished","status":"cancelled"}"#);
        let out = tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
        assert_eq!((out.ok, out.code), (false, 0));
    }

    #[tokio::test]
    async fn an_unopenable_file_fails_the_parser() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.parse("bad.1").await;
        let finished = core.recv().await;
        assert!(finished.contains(r#""code":"OPEN_FAILED""#) && finished.contains("unreadable"), "{finished}");
        assert_eq!(task.await.unwrap().code, 1);
    }
}
