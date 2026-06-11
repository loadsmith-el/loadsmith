use std::os::unix::io::FromRawFd;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_protocol::{
    ConfigureAck, Finished, HandshakeAck, Message, PluginKind, SetProtocolVersion,
};
use loadsmith_transport::{ControlReader, ControlWriter};
use tokio::io::{AsyncWriteExt, BufReader};

use crate::context::EventSender;

const PROTOCOL_VERSION: u32 = 1;

/// Implemented by source plugins. The SDK drives the full lifecycle;
/// implementors only provide data and metadata.
#[async_trait]
pub trait SourcePlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake. Defaults to
    /// `["batch_read", "schema_inference"]`. An incremental source should also
    /// advertise `incremental_state`.
    fn capabilities(&self) -> Vec<String> {
        vec!["batch_read".into(), "schema_inference".into()]
    }

    /// Called after `start`, before `schema()`/`next_batch()`. Carries the
    /// opaque watermark persisted by the previous run (`None` ⇒ full read). An
    /// incremental source resumes reading after this value; non-incremental
    /// sources leave the default no-op.
    async fn resume_from(&mut self, _cursor_value: Option<serde_json::Value>) {}

    /// Called after `configure`. Plugin validates its config and prepares.
    /// Return `Err` to send `configure_ack { error }` and exit.
    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

    /// Return the Arrow schema of the data this source will produce.
    async fn schema(&mut self) -> Result<Schema>;

    /// Produce the next batch. Return `Ok(None)` when exhausted.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// The high watermark of the cursor column produced so far, as an opaque
    /// scalar the core will persist. Consulted by the SDK after each batch.
    /// `None` (the default) ⇒ this source has no watermark to report.
    fn current_watermark(&self) -> Option<serde_json::Value> {
        None
    }

    /// Called when core sends `cancel`. Plugin should abort promptly.
    async fn cancel(&mut self);
}

/// Entry point for source plugin binaries. Never returns normally.
pub async fn run_source<P: SourcePlugin + 'static>(mut plugin: P) -> ! {
    if let Err(e) = run_source_inner(&mut plugin).await {
        eprintln!("[plugin-sdk] fatal error: {e:#}");
        std::process::exit(1);
    }
    std::process::exit(0);
}

async fn run_source_inner<P: SourcePlugin>(plugin: &mut P) -> Result<()> {
    // fd0 = stdin (control in), fd1 = stdout (control out), fd4 = event out
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let event_fd = unsafe { std::fs::File::from_raw_fd(4) };
    let event_async = tokio::fs::File::from_std(event_fd);

    let mut reader = ControlReader::new(BufReader::new(stdin));
    let mut writer = ControlWriter::new(stdout);
    let mut events = EventSender::new(event_async);

    // ── Handshake ────────────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before handshake"))?;
    anyhow::ensure!(matches!(msg, Message::Handshake), "expected Handshake, got {msg:?}");

    writer
        .send(&Message::HandshakeAck(HandshakeAck {
            protocol_supported_versions: vec![PROTOCOL_VERSION],
            plugin_name: plugin.plugin_name().to_string(),
            plugin_version: plugin.plugin_version().to_string(),
            kind: PluginKind::Source,
        }))
        .await?;

    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF after handshake_ack"))?;
    let chosen_version = match msg {
        Message::SetProtocolVersion(SetProtocolVersion { protocol_version }) => protocol_version,
        other => anyhow::bail!("expected set_protocol_version, got {other:?}"),
    };
    anyhow::ensure!(
        chosen_version == PROTOCOL_VERSION,
        "unsupported protocol version {chosen_version}"
    );

    // ── Capabilities ─────────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before capabilities"))?;
    anyhow::ensure!(
        matches!(msg, Message::CapabilitiesRequest),
        "expected capabilities_request"
    );
    writer
        .send(&Message::CapabilitiesResponse(
            loadsmith_protocol::CapabilitiesResponse { supports: plugin.capabilities() },
        ))
        .await?;

    // ── Configure ────────────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before configure"))?;
    let config = match msg {
        Message::Configure(c) => c.config,
        other => anyhow::bail!("expected configure, got {other:?}"),
    };

    match plugin.configure(config).await {
        Ok(()) => writer.send(&Message::ConfigureAck(ConfigureAck::ok())).await?,
        Err(e) => {
            writer
                .send(&Message::ConfigureAck(ConfigureAck::error(
                    "CONFIGURE_FAILED",
                    e.to_string(),
                )))
                .await?;
            anyhow::bail!("configure rejected: {e}");
        }
    }

    // ── Start ─────────────────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before start"))?;
    let resume = match msg {
        Message::Start(params) => params.resume.map(|r| r.cursor_value),
        other => anyhow::bail!("expected start, got {other:?}"),
    };
    plugin.resume_from(resume).await;

    // ── Schema ────────────────────────────────────────────────────────────
    let schema = plugin.schema().await?;
    let protocol_fields = schema_to_protocol_fields(&schema);
    writer.send(&Message::Schema(loadsmith_protocol::Schema { fields: protocol_fields })).await?;

    // ── Data loop (Arrow IPC on fd3) ──────────────────────────────────────
    let data_fd = unsafe { std::fs::File::from_raw_fd(3) };
    let mut ipc_writer =
        loadsmith_arrow::IpcWriter::new(data_fd, &schema).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut rows_total: u64 = 0;
    let mut batches_total: u64 = 0;

    loop {
        // Check for cancel on stdin before reading next batch
        // (non-blocking peek — if nothing is there we continue)
        match plugin.next_batch().await {
            Ok(Some(batch)) => {
                rows_total += batch.num_rows() as u64;
                batches_total += 1;
                ipc_writer.write_batch(&batch).map_err(|e| anyhow::anyhow!("{e}"))?;
                events.progress_source(rows_total, batches_total).await;
                // Report the high watermark for this batch ordinal. The core
                // pairs it with the destination's durable-commit ack before
                // persisting, so emitting per batch is safe (and cheap on fd4).
                if let Some(watermark) = plugin.current_watermark() {
                    events.checkpoint(batches_total, watermark).await;
                }
            }
            Ok(None) => break,
            Err(e) => {
                events.error_msg(format!("batch error: {e}")).await;
                writer
                    .send(&Message::Finished(Finished::error("BATCH_FAILED", e.to_string())))
                    .await?;
                return Ok(());
            }
        }
    }

    ipc_writer.finish().map_err(|e| anyhow::anyhow!("IPC finish: {e}"))?;

    writer
        .send(&Message::Finished(Finished::success_source(rows_total, batches_total)))
        .await?;

    let mut stdout = tokio::io::stdout();
    stdout.flush().await?;
    Ok(())
}

fn schema_to_protocol_fields(schema: &Schema) -> Vec<loadsmith_protocol::Field> {
    use arrow_schema::DataType;
    use loadsmith_protocol::{Field, FieldType};

    schema
        .fields()
        .iter()
        .map(|f| Field {
            name: f.name().clone(),
            field_type: match f.data_type() {
                DataType::Int32 => FieldType::Int32,
                DataType::Int64 => FieldType::Int64,
                DataType::Float32 => FieldType::Float32,
                DataType::Float64 => FieldType::Float64,
                DataType::Utf8 | DataType::LargeUtf8 => FieldType::Utf8,
                DataType::Boolean => FieldType::Bool,
                DataType::Date32 => FieldType::Date32,
                DataType::Timestamp(_, _) => FieldType::TimestampMs,
                DataType::Binary | DataType::LargeBinary => FieldType::Binary,
                _ => FieldType::Utf8,
            },
        })
        .collect()
}
