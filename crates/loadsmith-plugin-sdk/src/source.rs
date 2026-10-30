use std::os::unix::io::FromRawFd;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_protocol::{capabilities, Finished, Message, PluginKind};
use std::future::Future;
use tokio::io::AsyncWrite;

use crate::context::EventSender;
use crate::telemetry::{self, phase_span};
use std::time::Instant;
use tracing::Instrument;

use crate::config::PluginConfig;
use crate::control::{answer_configure, conclude, handshake, recv_configure, recv_start, Control, Flow};
use crate::flow::{FlowGate, FlowGrant};

/// What a source does with a failed run's resume token
/// ([`SourcePlugin::resume_at`]), and the line the run's log shows for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resumption {
    /// Continue where the failed run stopped; logged as info.
    Continue(String),
    /// Read everything again, and why; logged as a warning.
    StartOver(String),
}

/// Implemented by source plugins. The SDK drives the full lifecycle;
/// implementors only provide data and metadata.
#[async_trait]
pub trait SourcePlugin: Send {
    /// The JSON Schema of this plugin's `config:` block, printed by
    /// `<binary> --config-schema` and published with the release: usually
    /// `Some(config_schema::schema_for::<MyConfig>())`. Default: none.
    fn config_schema() -> Option<serde_json::Value>
    where
        Self: Sized,
    {
        None
    }

    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake. Defaults to
    /// `["batch_read", "schema_inference"]`. An incremental source should also
    /// advertise `incremental_state`. The SDK adds `flow_credit` itself: it
    /// enforces the core's credits for every source.
    fn capabilities(&self) -> Vec<String> {
        vec!["batch_read".into(), "schema_inference".into()]
    }

    /// Called after `start`, before `schema()`/`next_batch()`. Carries the
    /// opaque watermark persisted by the previous run (`None` ⇒ full read). An
    /// incremental source resumes reading after this value; non-incremental
    /// sources leave the default no-op.
    async fn resume_from(&mut self, _cursor_value: Option<serde_json::Value>) {}

    /// Called after `resume_from` when the run resumes after a failure and
    /// this source reported a [`current_resume_token`] before it: the last
    /// token that was durable. [`Resumption::Continue`] continues from it
    /// (e.g. skips the partitions it lists), [`Resumption::StartOver`] reads
    /// everything again (the token can no longer be honoured); either way
    /// the message is logged to the run. The core keeps the run's earlier
    /// output only after a `Continue`. The default reads everything again.
    ///
    /// [`current_resume_token`]: SourcePlugin::current_resume_token
    async fn resume_at(&mut self, _token: serde_json::Value) -> Resumption {
        Resumption::StartOver("this source does not continue a failed run".into())
    }

    /// Called after `start`, before `resume_from`. The core decides how far a
    /// source may read ahead of the destination; the SDK never calls
    /// `next_batch` past that grant. A source that fetches ahead on its own
    /// (a fetch task, partition workers) keeps the gate and calls
    /// [`FlowGate::wait_for`] before fetching each batch, so its buffers stay
    /// within the grant too. A source that only fetches inside `next_batch`
    /// leaves the default no-op.
    fn attach_flow(&mut self, _gate: FlowGate) {}

    /// Called after `configure`. Plugin validates its config and prepares.
    /// Return `Err` to send `configure_ack { error }` and exit.
    async fn configure(&mut self, config: PluginConfig) -> Result<()>;

    /// Return the Arrow schema of the data this source will produce.
    async fn schema(&mut self) -> Result<Schema>;

    /// Produce the next batch. Return `Ok(None)` when exhausted.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// The high watermark of the cursor column produced so far, as an opaque
    /// scalar the core will persist. Consulted by the SDK after each batch, and
    /// once more after `next_batch` returned `Ok(None)`. While the stream runs,
    /// report only a value no later row can repeat ([`TieSafeCursor`]), since
    /// a resume reads strictly after it; at the end, the last value.
    /// `None` (the default) ⇒ this source has no watermark to report.
    ///
    /// [`TieSafeCursor`]: crate::TieSafeCursor
    fn current_watermark(&self) -> Option<serde_json::Value> {
        None
    }

    /// Where a resumed run could continue, given everything produced so far
    /// were durable — e.g. the partitions whose every batch has been returned
    /// by `next_batch`. Consulted like [`current_watermark`]; sent with the
    /// checkpoint and handed back to [`resume_at`] if the run fails and
    /// resumes. `None` (the default) ⇒ nothing to resume from.
    ///
    /// [`current_watermark`]: SourcePlugin::current_watermark
    /// [`resume_at`]: SourcePlugin::resume_at
    fn current_resume_token(&self) -> Option<serde_json::Value> {
        None
    }

    /// Called when the run will not complete, once `start` was received: the
    /// core sent `cancel` or `error`, the core is gone, or this source failed.
    /// Stop producing and release what you hold (cursors, connections).
    ///
    /// The SDK waits for `next_batch` and for the core at the same time, so on
    /// `cancel` an in-flight `next_batch` future may have been **dropped**
    /// before this runs; `cancel` must cope with that (for example, by dropping
    /// a connection that was mid-query). Never called before `start`.
    async fn cancel(&mut self);
}

type DataWriter = loadsmith_arrow::IpcWriter<std::fs::File>;

/// The writer thread: writes every queued batch to fd3, in order, and hands
/// the stream back (for its end marker) once the queue is closed.
fn write_stream(
    mut writer: DataWriter,
    mut batches: tokio::sync::mpsc::Receiver<RecordBatch>,
) -> Result<DataWriter, String> {
    while let Some(batch) = batches.blocking_recv() {
        writer.write_batch(&batch).map_err(|e| e.to_string())?;
    }
    Ok(writer)
}

async fn join_writer(writer: std::thread::JoinHandle<Result<DataWriter, String>>) -> anyhow::Result<DataWriter> {
    match tokio::task::spawn_blocking(move || writer.join()).await {
        Ok(Ok(Ok(w))) => Ok(w),
        Ok(Ok(Err(e))) => Err(anyhow::anyhow!("{e}")),
        _ => Err(anyhow::anyhow!("the data writer thread panicked")),
    }
}

async fn writer_failure(writer: std::thread::JoinHandle<Result<DataWriter, String>>) -> Flow {
    let e = join_writer(writer).await.err().unwrap_or_else(|| anyhow::anyhow!("the data writer stopped"));
    Flow::fatal("IPC_WRITE_FAILED", e)
}

/// What the last checkpoint carried.
#[derive(Default)]
struct Reported {
    watermark: Option<serde_json::Value>,
    token: Option<serde_json::Value>,
}

impl Reported {
    /// Sends a checkpoint for `batch_seq` when the watermark or the resume
    /// token changed since the last one.
    async fn report<E: AsyncWrite + Unpin>(
        &mut self,
        events: &mut EventSender<E>,
        batch_seq: u64,
        watermark: Option<serde_json::Value>,
        token: Option<serde_json::Value>,
    ) {
        let changed = (watermark.is_some() && watermark != self.watermark) || (token.is_some() && token != self.token);
        if !changed {
            return;
        }
        let cursor = watermark.clone().or_else(|| self.watermark.clone()).unwrap_or(serde_json::Value::Null);
        events.checkpoint_with(batch_seq, cursor, token.clone().or_else(|| self.token.clone())).await;
        self.watermark = watermark.or(self.watermark.take());
        self.token = token.or(self.token.take());
    }
}

/// Runs `work` while answering the core, so a `cancel` stops a source even
/// while it is busy producing (some sources buffer their whole result before
/// the first batch) or waiting for credit. Only a stopping message drops
/// `work`; `ping`, `resend` and `credit` are handled without touching it.
async fn attending<W: AsyncWrite + Unpin, T>(
    ctl: &mut Control<W>,
    grant: &FlowGrant,
    work: impl Future<Output = T>,
) -> Result<T, Flow> {
    tokio::pin!(work);
    loop {
        tokio::select! {
            biased;
            inbound = ctl.next_inbound() => {
                match ctl.process(inbound).await? {
                    None => {}
                    Some(Message::Credit(c)) => grant.grant(c.batch_seq),
                    Some(other) => {
                        tracing::warn!("ignoring unexpected control message mid-stream: {other:?}");
                    }
                }
            }
            result = &mut work => return Ok(result),
        }
    }
}

/// Entry point for source plugin binaries. Never returns normally.
///
/// Exit code: `0` after success or a `cancel`, `1` after a failure, an
/// `error` from the core, or a lost core.
pub async fn run_source<P: SourcePlugin + 'static>(mut plugin: P) -> ! {
    crate::config_schema::serve_flag(P::config_schema, &[]);
    telemetry::install();
    // fd0 = stdin (control in), fd1 = stdout (control out), fd4 = event out
    let mut ctl = Control::stdio();
    let mut events = EventSender::fd4();
    let mut started = false;
    let result = drive_source(&mut plugin, &mut ctl, &mut events, &mut started).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
    events.close().await;
    std::process::exit(code);
}

async fn drive_source<P: SourcePlugin, W: AsyncWrite + Unpin, E: AsyncWrite + Unpin>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    events: &mut EventSender<E>,
    started: &mut bool,
) -> Result<(), Flow> {
    // ── Handshake + capabilities ─────────────────────────────────────────
    let mut supports = plugin.capabilities();
    if !supports.iter().any(|c| c == capabilities::FLOW_CREDIT) {
        supports.push(capabilities::FLOW_CREDIT.into());
    }
    handshake(ctl, PluginKind::Source, plugin.plugin_name(), plugin.plugin_version(), supports).await?;

    // ── Configure ────────────────────────────────────────────────────────
    let (config, warnings) = recv_configure(ctl).await?;
    let span = phase_span!("configure");
    let outcome = plugin.configure(config).instrument(span.clone()).await;
    telemetry::outcome(&span, &outcome);
    drop(span);
    answer_configure(ctl, events, &warnings, outcome).await?;

    // ── Start ─────────────────────────────────────────────────────────────
    let params = recv_start(ctl).await?;
    *started = true;
    let span = phase_span!("start");
    let (grant, mut gate) = FlowGrant::new(params.credit);
    plugin.attach_flow(gate.clone());
    plugin.resume_from(params.resume.map(|r| r.cursor_value)).instrument(span.clone()).await;
    let resumed = match params.resume_token {
        Some(token) => Some(match plugin.resume_at(token).instrument(span.clone()).await {
            Resumption::Continue(how) => {
                events.info(how).await;
                true
            }
            Resumption::StartOver(why) => {
                events.warn(why).await;
                false
            }
        }),
        None => None,
    };

    // ── Schema ────────────────────────────────────────────────────────────
    let schema = plugin.schema().instrument(span.clone()).await;
    telemetry::outcome(&span, &schema);
    drop(span);
    let schema = schema.map_err(|e| Flow::fatal("SCHEMA_FAILED", e))?;
    // The exact protocol form of the Arrow schema (types, units, time zones,
    // precision, nullability, children). The IPC header written next carries
    // the same schema and is what the core treats as authoritative.
    let protocol_fields = loadsmith_arrow::protocol_fields_from_schema(&schema);
    ctl.send(Message::Schema(loadsmith_protocol::Schema { fields: protocol_fields, resumed })).await?;

    // ── Data loop (Arrow IPC on fd3) ──────────────────────────────────────
    let data_fd = unsafe { std::fs::File::from_raw_fd(3) };
    let ipc_writer = loadsmith_arrow::IpcWriter::new(data_fd, &schema)
        .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("{e}")))?;
    // Encoding and writing fd3 run on their own thread, so the source fetches
    // the next batch meanwhile. One batch waits in the queue; the core's
    // credit counts it like any other fetched batch.
    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(1);
    let writer = std::thread::Builder::new()
        .name("ipc-writer".into())
        .spawn(move || write_stream(ipc_writer, batch_rx))
        .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("spawning the writer: {e}")))?;

    let mut rows_total: u64 = 0;
    let mut batches_total: u64 = 0;
    // The last watermark and resume token sent: unchanged, they are not sent
    // again.
    let mut reported = Reported::default();

    loop {
        // The core's grant first: never ask for a batch past it.
        attending(ctl, &grant, gate.wait_for(batches_total + 1)).await?;
        let span = telemetry::batch_span(batches_total + 1, "read");
        let began = Instant::now();
        let next = attending(ctl, &grant, plugin.next_batch().instrument(span.clone())).await;
        match next? {
            Ok(Some(batch)) => {
                let bytes = telemetry::batch_bytes(&batch);
                telemetry::batch_done(&span, "read", batch.num_rows() as u64, bytes, began.elapsed());
                drop(span);
                rows_total += batch.num_rows() as u64;
                batches_total += 1;
                if attending(ctl, &grant, batch_tx.send(batch)).await?.is_err() {
                    // The writer stopped: its error says why.
                    return Err(writer_failure(writer).await);
                }
                events.progress_source(rows_total, batches_total).await;
                // Report the high watermark for this batch ordinal. The core
                // pairs it with the destination's durable-commit ack before
                // persisting, so emitting per batch is safe (and cheap on fd4).
                reported.report(events, batches_total, plugin.current_watermark(), plugin.current_resume_token()).await;
            }
            Ok(None) => {
                // The stream is over: the source may now report a value it
                // held back (the last one, which no row can follow).
                if batches_total > 0 {
                    reported.report(events, batches_total, plugin.current_watermark(), plugin.current_resume_token()).await;
                }
                break;
            }
            Err(e) => {
                telemetry::fail(&span, &e);
                drop(span);
                events.error_msg(format!("batch error: {e:#}")).await;
                return Err(Flow::fatal("BATCH_FAILED", e));
            }
        }
    }

    // The end-of-stream marker tells the core the stream is complete. It is
    // written only after the last batch succeeded; a source that dies before
    // this point leaves a stream the core recognises as truncated.
    drop(batch_tx);
    let ipc_writer = match join_writer(writer).await {
        Ok(w) => w,
        Err(e) => return Err(Flow::fatal("IPC_WRITE_FAILED", e)),
    };
    let span = phase_span!("finish");
    let finished = span.in_scope(|| ipc_writer.finish());
    drop(span);
    finished.map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("IPC finish: {e}")))?;

    events.close().await;
    ctl.send(Message::Finished(Finished::success_source(rows_total, batches_total))).await?;
    Ok(())
}
