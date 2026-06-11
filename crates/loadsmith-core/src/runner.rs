use std::os::unix::io::IntoRawFd;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use loadsmith_config::{validate_pipeline, ConfigError, PipelineConfig};
use loadsmith_protocol::PluginKind;
use loadsmith_transport::{ControlReader, ControlWriter};
use tokio::io::BufReader;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use loadsmith_config::SchemaChangePolicy;

use crate::{
    discovery::find_plugin_binary,
    error::CoreError,
    events::{drain_events, EventForward},
    lifecycle::{await_finished, run_handshake_and_configure, start_destination, start_source},
    pump::pump,
    reporter::Reporter,
    sink_supervisor::run_sink_supervisor,
    spawner::{create_data_pipe, spawn_plugin},
    state::{new_run_id, open_backend, schema_fingerprint},
    state_supervisor::{run_state_supervisor, StateRun},
    summary::{Summary, SummaryStatus},
};

/// Capability a destination must advertise for a sink to be attachable: it
/// stages files locally and announces each via `ObjectReady`.
const CAP_OBJECT_OUTPUT: &str = "object_output";

/// Capability a source must advertise for a `state:` block to drive incremental
/// loads: it accepts a resume cursor and reports watermarks.
const CAP_INCREMENTAL_STATE: &str = "incremental_state";

/// Options for running a pipeline.
pub struct RunOpts {
    pub plugin_dir: PathBuf,
    pub dry_run: bool,
    pub print_resolved_config: bool,
    /// Emit ANSI colour in the human report on stdout.
    pub color: bool,
    /// Version string of the loadsmith binary, shown in the report.
    pub version: String,
}

impl RunOpts {
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugin_dir: plugin_dir.into(),
            dry_run: false,
            print_resolved_config: false,
            color: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Runs the pipeline described by `config` to completion.
pub async fn run_pipeline(config: PipelineConfig, opts: RunOpts) -> Result<Summary, CoreError> {
    validate_pipeline(&config)?;

    if opts.dry_run {
        println!("Dry run — pipeline validated. Not executing.");
        return Ok(Summary {
            pipeline_name: config.pipeline.name.clone(),
            status: SummaryStatus::Success,
            ..Default::default()
        });
    }

    // ── Incremental state setup ───────────────────────────────────────────
    // With a `state:` block we lock the pipeline's state (failing fast if a
    // concurrent run holds it), load the prior watermark to resume from, and
    // prepare the channels the event drains feed to the state supervisor.
    let state_enabled = config.state.is_some();
    let (mut state_backend, mut state_guard, prior_state) = match &config.state {
        Some(cfg) => {
            let backend = open_backend(cfg)?;
            let guard = backend.lock(&config.pipeline.name)?;
            let prior = backend.load(&config.pipeline.name)?;
            (Some(backend), Some(guard), prior)
        }
        None => (None, None, None),
    };
    let resume_value = prior_state.as_ref().map(|d| d.cursor_value.clone());
    let (checkpoint_tx, checkpoint_rx) = maybe_channel(state_enabled);
    let (committed_tx, committed_rx) = maybe_channel(state_enabled);

    // Print the header first — before any log line — so the version and route
    // always lead the output, then the run log follows underneath it.
    let reporter = Reporter::new(opts.color);
    let sink_type = config.sink.as_ref().map(|s| s.plugin_type.clone());
    reporter.header(
        &opts.version,
        &config.source.plugin_type,
        &config.destination.plugin_type,
        sink_type.as_deref(),
    );

    let source_bin = find_plugin_binary("source", &config.source.plugin_type, &opts.plugin_dir)?;
    let dest_bin =
        find_plugin_binary("destination", &config.destination.plugin_type, &opts.plugin_dir)?;
    let sink_bin = match &config.sink {
        Some(s) => Some(find_plugin_binary("sink", &s.plugin_type, &opts.plugin_dir)?),
        None => None,
    };

    tracing::info!(source = ?source_bin, destination = ?dest_bin, sink = ?sink_bin, "spawning plugins");

    let source_config = yaml_value_to_json(&config.source.config)?;
    let mut dest_config = yaml_value_to_json(&config.destination.config)?;
    let sink_config = match &config.sink {
        Some(s) => Some(yaml_value_to_json(&s.config)?),
        None => None,
    };

    // When a sink is attached and the destination wasn't given an explicit
    // staging `path`, allocate a scratch directory and inject it. A
    // core-allocated scratch is removed after delivery; a user-given path
    // (e.g. a mounted EBS volume) is left untouched.
    let mut scratch_cleanup: Option<PathBuf> = None;
    if config.sink.is_some() {
        let has_path = dest_config.get("path").and_then(|v| v.as_str()).is_some();
        if !has_path {
            let scratch = allocate_scratch_dir(&config.pipeline.name)?;
            tracing::info!(path = %scratch.display(), "allocated staging scratch dir");
            if !dest_config.is_object() {
                dest_config = serde_json::Value::Object(serde_json::Map::new());
            }
            dest_config["path"] = serde_json::Value::String(scratch.to_string_lossy().into_owned());
            scratch_cleanup = Some(scratch);
        }
    }

    // Unbounded by design: the destination's event drain forwards ObjectReady
    // paths here and must never block (a full fd4 would deadlock the pump).
    let (object_tx, object_rx) = if config.sink.is_some() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Two pipes, with loadsmith in the middle of the data plane:
    //   source ──IN──▶ core (pump) ──OUT──▶ destination
    // Neither plugin sees the other; everything flows through the control plane.
    let (in_read, in_write) = create_data_pipe()
        .map_err(|e| CoreError::PluginProcess(format!("create data pipe (in): {e}")))?;
    let (out_read, out_write) = create_data_pipe()
        .map_err(|e| CoreError::PluginProcess(format!("create data pipe (out): {e}")))?;

    // Source's fd3 is the write end of the IN pipe; core keeps the read end.
    let mut src = spawn_plugin(&source_bin, Some(in_write.into_raw_fd()))
        .map_err(|e| CoreError::PluginProcess(format!("spawn source: {e}")))?;
    // Destination's fd3 is the read end of the OUT pipe; core keeps the write end.
    let mut dst = spawn_plugin(&dest_bin, Some(out_read.into_raw_fd()))
        .map_err(|e| CoreError::PluginProcess(format!("spawn destination: {e}")))?;

    // Take the event channels (fd4) out so we can drain them concurrently —
    // a plugin blocked writing progress to a full fd4 would stall the pump.
    // The destination's drain also forwards ObjectReady paths to the sink
    // supervisor (when a sink is attached).
    let src_event = tokio::fs::File::from_std(src.event_fd_take());
    let dst_event = tokio::fs::File::from_std(dst.event_fd_take());
    let src_events = tokio::spawn(async move {
        drain_events(
            ControlReader::new(BufReader::new(src_event)),
            "source",
            EventForward { checkpoint_tx, ..Default::default() },
        )
        .await;
    });
    let dst_events = tokio::spawn(async move {
        drain_events(
            ControlReader::new(BufReader::new(dst_event)),
            "destination",
            EventForward { object_tx, committed_tx, ..Default::default() },
        )
        .await;
    });

    let mut src_reader = ControlReader::new(BufReader::new(&mut src.control_out));
    let mut src_writer = ControlWriter::new(&mut src.control_in);
    let mut dst_reader = ControlReader::new(BufReader::new(&mut dst.control_out));
    let mut dst_writer = ControlWriter::new(&mut dst.control_in);

    let src_info =
        run_handshake_and_configure(&mut src_reader, &mut src_writer, source_config, PluginKind::Source)
            .await?;
    let dst_info = run_handshake_and_configure(
        &mut dst_reader,
        &mut dst_writer,
        dest_config,
        PluginKind::Destination,
    )
    .await?;

    // Capability gate: a `state:` block requires a source that supports
    // incremental loads (accepts a resume cursor, reports watermarks).
    if state_enabled && !src_info.supports.iter().any(|c| c == CAP_INCREMENTAL_STATE) {
        return Err(CoreError::Config(ConfigError::Validation(format!(
            "source '{}' does not support incremental state (missing \
             '{CAP_INCREMENTAL_STATE}' capability), but a state: block was configured",
            config.source.plugin_type
        ))));
    }

    // Capability gate: a sink may only be attached to a destination that stages
    // files (advertises `object_output`). Databases / `null` don't — fail clearly.
    if config.sink.is_some() && !dst_info.supports.iter().any(|c| c == CAP_OBJECT_OUTPUT) {
        return Err(CoreError::Config(ConfigError::Validation(format!(
            "destination '{}' does not produce objects (missing '{CAP_OBJECT_OUTPUT}' capability), \
             so a sink cannot be attached",
            config.destination.plugin_type
        ))));
    }

    // Launch the sink supervisor now (before the pump) so it is ready to deliver
    // files the instant the destination starts finalizing them. It consumes the
    // ObjectReady paths forwarded by the destination's event drain.
    let sink_task = match (sink_bin, sink_config, object_rx) {
        (Some(bin), Some(cfg), Some(rx)) => {
            Some(tokio::spawn(run_sink_supervisor(bin, cfg, rx)))
        }
        _ => None,
    };

    let start_time = Instant::now();

    // Start source (returns its schema) and destination (signals ready). The
    // resume cursor (if any) tells an incremental source where to pick up.
    let schema = start_source(&mut src_reader, &mut src_writer, resume_value).await?;
    reporter.schema(&schema);

    // Schema-drift guard: if the source shape changed since the recorded state,
    // a blind resume could corrupt the load. Refuse (default) or warn.
    let schema_hash = schema_fingerprint(&schema);
    if let (Some(prior), Some(state_cfg)) = (&prior_state, &config.state) {
        if prior.schema_hash != schema_hash {
            match state_cfg.on_schema_change {
                SchemaChangePolicy::Refuse => {
                    return Err(CoreError::State(format!(
                        "source schema changed since last run (recorded {}, now {}); \
                         set state.on_schema_change: continue to override",
                        prior.schema_hash, schema_hash
                    )));
                }
                SchemaChangePolicy::Continue => {
                    tracing::warn!(
                        recorded = %prior.schema_hash,
                        current = %schema_hash,
                        "source schema changed since last run — continuing per policy"
                    );
                }
            }
        }
    }

    // Spawn the state supervisor (before the pump) so it persists watermarks as
    // checkpoints/commits flow on fd4 during the run. Fed by the event drains.
    let state_task = match (state_backend.take(), state_guard.take(), checkpoint_rx, committed_rx) {
        (Some(backend), Some(guard), Some(ck_rx), Some(cm_rx)) => {
            let run = StateRun {
                pipeline: config.pipeline.name.clone(),
                schema_hash,
                run_id: new_run_id(),
                checkpoint_interval: config
                    .state
                    .as_ref()
                    .map(|c| c.checkpoint_interval)
                    .unwrap_or(0),
            };
            Some(tokio::spawn(run_state_supervisor(backend, guard, run, ck_rx, cm_rx)))
        }
        _ => None,
    };

    start_destination(&mut dst_reader, &mut dst_writer).await?;

    // Run the data pump on a blocking thread: it copies every batch from the
    // source's IN stream to the destination's OUT stream, reporting progress.
    tracing::info!("data pump started");
    let pump_handle = tokio::task::spawn_blocking(move || {
        pump(in_read, out_write, move |rows, batches| reporter.progress(rows, batches))
    });

    // Heartbeat: log every `heartbeat_seconds` while the pump runs so the
    // operator knows the pipeline is alive during long transfers. 0 disables it.
    let heartbeat_secs = config.pipeline.heartbeat_seconds;
    let heartbeat = (heartbeat_secs > 0).then(|| {
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                tracing::info!("pipeline still running…");
            }
        })
    });

    let pump_result = pump_handle.await;
    if let Some(h) = heartbeat {
        h.abort(); // pump finished — stop the heartbeat log
    }
    if let Ok(Ok(ref stats)) = pump_result {
        tracing::info!(rows = stats.rows, batches = stats.batches, "data pump completed");
    }

    let src_finished = await_finished(&mut src_reader).await?;
    let dst_finished = await_finished(&mut dst_reader).await?;

    let _ = src.child.wait().await;
    let _ = dst.child.wait().await;
    let _ = src_events.await;
    // Draining the destination's events to completion drops the ObjectReady
    // sender, which closes the supervisor's input channel — so this must finish
    // before awaiting the sink.
    let _ = dst_events.await;

    // Both drains are done ⇒ the checkpoint/committed channels are closed and
    // the state supervisor performs its final watermark flush and releases the
    // pipeline state lock.
    let state_ok = match state_task {
        Some(task) => match task.await {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                tracing::error!("incremental state persistence failed: {e}");
                false
            }
            Err(e) => {
                tracing::error!("state supervisor panicked: {e}");
                false
            }
        },
        None => true,
    };

    // The sink can outlive source/destination: it keeps delivering its queue at
    // its own pace. The run is only done once the supervisor drains it.
    let (sink_name, objects_delivered, sink_ok) = match sink_task {
        Some(task) => match task.await {
            Ok(Ok(outcome)) => {
                tracing::info!(objects = outcome.objects_delivered, "sink delivery complete");
                (Some(outcome.name), Some(outcome.objects_delivered), true)
            }
            Ok(Err(e)) => {
                tracing::error!("sink delivery failed: {e}");
                (None, None, false)
            }
            Err(e) => {
                tracing::error!("sink supervisor panicked: {e}");
                (None, None, false)
            }
        },
        None => (None, None, true),
    };

    let duration = start_time.elapsed();

    // Remove a core-allocated scratch dir only after the sink ack'd everything;
    // a user-provided staging path is left untouched.
    if let Some(scratch) = &scratch_cleanup {
        if sink_ok {
            if let Err(e) = std::fs::remove_dir_all(scratch) {
                tracing::warn!(path = %scratch.display(), "could not remove scratch dir: {e}");
            } else {
                tracing::debug!(path = %scratch.display(), "removed staging scratch dir");
            }
        } else {
            tracing::warn!(
                path = %scratch.display(),
                "sink did not complete — leaving staged files in scratch dir for inspection"
            );
        }
    }

    // Surface a pump failure as a pipeline error.
    let pump_ok = matches!(&pump_result, Ok(Ok(_)));
    if let Ok(Err(e)) = &pump_result {
        tracing::error!("data plane pump failed: {e:#}");
    } else if let Err(e) = &pump_result {
        tracing::error!("data plane pump panicked: {e}");
    }

    tracing::debug!(
        source = %src_info.name,
        destination = %dst_info.name,
        "pipeline finished"
    );

    let status = if pump_ok
        && sink_ok
        && state_ok
        && src_finished.status == loadsmith_protocol::FinishedStatus::Success
        && dst_finished.status == loadsmith_protocol::FinishedStatus::Success
    {
        SummaryStatus::Success
    } else {
        SummaryStatus::Error
    };

    let summary = Summary {
        pipeline_name: config.pipeline.name.clone(),
        status,
        loadsmith_version: opts.version.clone(),
        source_name: src_info.name,
        destination_name: dst_info.name,
        sink_name,
        rows_read: src_finished.rows_read.unwrap_or(0),
        rows_written: dst_finished.rows_written.unwrap_or(0),
        batches_read: src_finished.batches_read.unwrap_or(0),
        batches_written: dst_finished.batches_written.unwrap_or(0),
        objects_delivered,
        duration,
    };

    summary.print();
    Ok(summary)
}

/// Creates an unbounded channel only when `enabled`; otherwise both ends are
/// `None`. Used to wire the state supervisor's inputs only when a `state:` block
/// is present.
fn maybe_channel<T>(enabled: bool) -> (Option<UnboundedSender<T>>, Option<UnboundedReceiver<T>>) {
    if enabled {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    }
}

fn yaml_value_to_json(val: &serde_yaml::Value) -> Result<serde_json::Value, CoreError> {
    // serde_yaml::Value implements Serialize, so we can go through serde_json.
    serde_json::to_value(val).map_err(|e| CoreError::Protocol(format!("yaml->json: {e}")))
}

/// Creates a fresh staging directory for a destination whose output a sink will
/// deliver. Named with the pipeline and pid so concurrent runs don't collide.
fn allocate_scratch_dir(pipeline_name: &str) -> Result<PathBuf, CoreError> {
    let slug: String = pipeline_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let dir = std::env::temp_dir().join(format!("loadsmith-stage-{slug}-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
