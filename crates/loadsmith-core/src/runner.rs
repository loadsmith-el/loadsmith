//! Runs one pipeline: spawns the plugins, drives their lifecycle, pumps the
//! data plane, and enforces the abort rules of protocol v1.
//!
//! **Abort rules** (normative; see the protocol reference):
//!
//! 1. The core ends the destination's fd3 (writes the IPC end-of-stream
//!    marker) only after the source's stream ended with its own marker **and**
//!    the source sent `finished` with status `success`. This is the commit
//!    point: until then a destination must not finalize.
//! 2. On any failure — a plugin crash, exit, `finished` with an error, a
//!    protocol violation, a timeout, a stalled data plane, an operator signal
//!    — the core sends `cancel` to every live plugin (`error` to the one at
//!    fault), waits the grace period, and then kills each plugin's process
//!    group. The destination's fd3 stays open until the destination is gone,
//!    so it can never observe a clean end of input after an upstream failure.
//! 3. Both plugins' outcomes are always collected, and the summary reports the
//!    run's status with the root cause (the first failure observed).

use std::os::unix::io::IntoRawFd;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use loadsmith_config::{validate_pipeline, ConfigError, PipelineConfig, Timeouts};
use loadsmith_protocol::{error_codes, FinishedStatus, Message, PluginKind};
use loadsmith_transport::ControlReader;
use tokio::io::BufReader;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;

use crate::{
    discovery::find_plugin_binary,
    error::CoreError,
    events::{drain_events, EventForward},
    lifecycle::{run_handshake_and_configure, start_and_await_ready, start_source},
    peer::{describe_exit, finished_failure, Event, Failure, Peer},
    pump::{pump, PumpControl, PumpOutcome, PumpOutput},
    reporter::Reporter,
    shutdown::{Halt, Shutdown},
    sink_supervisor::run_sink_supervisor,
    spawner::{create_data_pipe, spawn_plugin},
    state::{check_schema_drift, new_run_id, open_backend, schema_fingerprint},
    state_supervisor::{run_state_supervisor, StateRun},
    summary::{Summary, SummaryStatus},
};

/// Capability a destination must advertise for a sink to be attachable: it
/// stages files locally and announces each via `ObjectReady`.
const CAP_OBJECT_OUTPUT: &str = "object_output";

/// Capability a source must advertise for a `state:` block to drive incremental
/// loads: it accepts a resume cursor and reports watermarks.
const CAP_INCREMENTAL_STATE: &str = "incremental_state";

/// How often the data-phase watchdog checks deadlines and the stall timer.
const WATCHDOG_TICK: Duration = Duration::from_millis(250);

/// Options for running a pipeline.
pub struct RunOpts {
    pub plugin_dir: PathBuf,
    pub dry_run: bool,
    pub print_resolved_config: bool,
    /// Emit ANSI colour in the human report on stdout.
    pub color: bool,
    /// Version string of the loadsmith binary, shown in the report.
    pub version: String,
    /// Fires on SIGINT/SIGTERM (see [`crate::shutdown::listen_for_signals`]);
    /// the run then aborts gracefully.
    pub shutdown: Shutdown,
}

impl RunOpts {
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugin_dir: plugin_dir.into(),
            dry_run: false,
            print_resolved_config: false,
            color: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
            shutdown: Shutdown::never(),
        }
    }
}

/// Runs the pipeline described by `config` to completion.
///
/// Returns `Err` when the run fails before data starts flowing (bad config, a
/// missing or incompatible plugin, a rejected config, the state lock, a
/// signal); the plugins are cancelled and reaped first. Once data flows, the
/// outcome — success, error, or cancelled by a signal — is a [`Summary`]
/// (already printed) with the root cause.
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

    let mut shutdown = opts.shutdown.clone();
    if let Some(halt) = shutdown.fired() {
        return Err(Failure::halted(halt).error);
    }
    let timeouts = config.pipeline.timeouts;
    let grace = Duration::from_secs(timeouts.cancel_grace_seconds);

    // ── Incremental state setup ───────────────────────────────────────────
    // With a `state:` block we lock the pipeline's state (failing fast if a
    // concurrent run holds it), load the prior watermark to resume from, and
    // prepare the channels the event drains feed to the state supervisor.
    let state_enabled = config.state.is_some();
    let (mut state_backend, mut state_guard, prior_state) = match &config.state {
        Some(cfg) => {
            let backend = open_backend(cfg)?;
            let mut guard = backend.lock(&config.pipeline.name)?;
            // Renew from the moment of acquisition: plugin spawn, handshake and
            // a slow first query all happen before the state supervisor exists,
            // and must not let the lease expire.
            guard.start_renewal();
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
    let mut src_sp = spawn_plugin(&source_bin, Some(in_write.into_raw_fd()))
        .map_err(|e| CoreError::PluginIncompatible(format!("spawn source {}: {e}", source_bin.display())))?;
    // Destination's fd3 is the read end of the OUT pipe; core keeps the write end.
    let mut dst_sp = spawn_plugin(&dest_bin, Some(out_read.into_raw_fd())).map_err(|e| {
        CoreError::PluginIncompatible(format!("spawn destination {}: {e}", dest_bin.display()))
    })?;

    // Take the event channels (fd4) out so we can drain them concurrently —
    // a plugin blocked writing progress to a full fd4 would stall the pump.
    // The destination's drain also forwards ObjectReady paths to the sink
    // supervisor (when a sink is attached).
    let src_event = tokio::fs::File::from_std(src_sp.event_fd_take());
    let dst_event = tokio::fs::File::from_std(dst_sp.event_fd_take());
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

    let mut src = Peer::new("source", src_sp);
    let mut dst = Peer::new("destination", dst_sp);

    // The sink stage is stopped through its own shutdown input: the runner
    // fires it when the run aborts (or forwards a signal).
    let (sink_trigger, sink_shutdown) = Shutdown::channel();
    let mut sink_task = None;
    let mut state_task = None;
    let start_time = std::time::Instant::now();

    // ── Setup: handshake, gates, start ────────────────────────────────────
    let setup = async {
        let src_info = run_handshake_and_configure(
            &mut src,
            source_config,
            PluginKind::Source,
            &timeouts,
            &mut shutdown,
        )
        .await?;
        let dst_info = run_handshake_and_configure(
            &mut dst,
            dest_config,
            PluginKind::Destination,
            &timeouts,
            &mut shutdown,
        )
        .await?;

        // Capability gate: a `state:` block requires a source that supports
        // incremental loads (accepts a resume cursor, reports watermarks).
        if state_enabled && !src_info.supports.iter().any(|c| c == CAP_INCREMENTAL_STATE) {
            return Err(Failure::new(CoreError::Config(ConfigError::Validation(format!(
                "source '{}' does not support incremental state (missing \
                 '{CAP_INCREMENTAL_STATE}' capability), but a state: block was configured",
                config.source.plugin_type
            )))));
        }

        // Capability gate: a sink may only be attached to a destination that stages
        // files (advertises `object_output`). Databases / `null` don't — fail clearly.
        if config.sink.is_some() && !dst_info.supports.iter().any(|c| c == CAP_OBJECT_OUTPUT) {
            return Err(Failure::new(CoreError::Config(ConfigError::Validation(format!(
                "destination '{}' does not produce objects (missing '{CAP_OBJECT_OUTPUT}' capability), \
                 so a sink cannot be attached",
                config.destination.plugin_type
            )))));
        }

        // Launch the sink supervisor now (before the pump) so it is ready to deliver
        // files the instant the destination starts finalizing them. It consumes the
        // ObjectReady paths forwarded by the destination's event drain.
        if let (Some(bin), Some(cfg), Some(rx)) = (sink_bin, sink_config, object_rx) {
            sink_task =
                Some(tokio::spawn(run_sink_supervisor(bin, cfg, rx, timeouts, sink_shutdown)));
        }

        // Start source (returns its schema) and destination (signals ready). The
        // resume cursor (if any) tells an incremental source where to pick up.
        let schema = start_source(&mut src, resume_value, &timeouts, &mut shutdown).await?;
        reporter.schema(&schema);

        // Schema-drift guard: if the source shape changed since the recorded state,
        // a blind resume could corrupt the load. Refuse (default) or warn.
        let schema_hash = schema_fingerprint(&schema);
        if let (Some(prior), Some(state_cfg)) = (&prior_state, &config.state) {
            check_schema_drift(prior, &schema_hash, state_cfg.on_schema_change)?;
        }

        // Spawn the state supervisor (before the pump) so it persists watermarks as
        // checkpoints/commits flow on fd4 during the run. Fed by the event drains.
        if let (Some(backend), Some(guard), Some(ck_rx), Some(cm_rx)) =
            (state_backend.take(), state_guard.take(), checkpoint_rx, committed_rx)
        {
            let run = StateRun {
                pipeline: config.pipeline.name.clone(),
                schema_hash,
                run_id: new_run_id(),
                checkpoint_interval: config.state.as_ref().map(|c| c.checkpoint_interval).unwrap_or(0),
            };
            state_task = Some(tokio::spawn(run_state_supervisor(backend, guard, run, ck_rx, cm_rx)));
        }

        start_and_await_ready(&mut dst, &timeouts, &mut shutdown).await?;
        tracing::info!("destination ready — pump starting");
        Ok((src_info, dst_info))
    }
    .await;

    let (src_info, dst_info) = match setup {
        Ok(infos) => infos,
        Err(failure) => {
            tracing::error!("aborting the run: {}", failure.reason());
            sink_trigger.trigger(halt_for(&failure));
            abort_plugins(&mut src, &mut dst, &failure, grace).await;
            let _ = src_events.await;
            let _ = dst_events.await;
            if let Some(task) = state_task {
                let _ = task.await;
            }
            if let Some(task) = sink_task {
                let _ = task.await;
            }
            // The destination is gone; only now close its fd3.
            drop(out_write);
            drop(in_read);
            return Err(failure.error);
        }
    };

    // ── Data phase ────────────────────────────────────────────────────────
    // Run the data pump on a blocking thread: it copies every batch from the
    // source's IN stream to the destination's OUT stream, reporting progress.
    tracing::info!("data pump started");
    let pump_control = Arc::new(PumpControl::default());
    let mut pump_join = {
        let control = Arc::clone(&pump_control);
        tokio::task::spawn_blocking(move || {
            pump(in_read, out_write, control, move |rows, batches| reporter.progress(rows, batches))
        })
    };

    // Heartbeat: log every `heartbeat_seconds` while the pump runs so the
    // operator knows the pipeline is alive during long transfers. 0 disables it.
    let heartbeat_secs = config.pipeline.heartbeat_seconds;
    let heartbeat = (heartbeat_secs > 0).then(|| {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_secs));
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                tracing::info!("pipeline still running…");
            }
        })
    });

    let supervised = supervise_data_phase(
        &mut src,
        &mut dst,
        &mut pump_join,
        &pump_control,
        &timeouts,
        &mut shutdown,
    )
    .await;
    if let Some(h) = heartbeat {
        h.abort(); // pump finished — stop the heartbeat log
    }
    let DataPhase { failure, mut pump_outcome } = supervised;

    match &failure {
        Some(failure) => {
            tracing::error!("aborting the run: {}", failure.reason());
            pump_control.abort.store(true, Ordering::Relaxed);
            sink_trigger.trigger(halt_for(failure));
            abort_plugins(&mut src, &mut dst, failure, grace).await;
        }
        None => {
            // Both plugins sent `finished`; let them exit.
            tokio::join!(src.reap(grace), dst.reap(grace));
        }
    }
    // The source is gone, so the pump has ended (it drains to EOF); and the
    // destination is gone, so dropping its stream can no longer be mistaken
    // for anything.
    if pump_outcome.is_none() {
        pump_outcome = pump_join.await.ok();
    }
    let pump_stats = pump_outcome.as_ref().map(|o| o.stats).unwrap_or_default();
    drop(pump_outcome);
    tracing::info!(rows = pump_stats.rows, batches = pump_stats.batches, "data pump completed");

    let _ = src_events.await;
    // Draining the destination's events to completion drops the ObjectReady
    // sender, which closes the supervisor's input channel — so this must finish
    // before awaiting the sink.
    let _ = dst_events.await;

    // Both drains are done ⇒ the checkpoint/committed channels are closed and
    // the state supervisor performs its final watermark flush and releases the
    // pipeline state lock. Only durably committed watermarks are ever
    // persisted, so this is safe after an abort too.
    let mut late_failure: Option<String> = None;
    if let Some(task) = state_task {
        match task.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::error!("incremental state persistence failed: {e}");
                late_failure.get_or_insert(format!("incremental state persistence failed: {e}"));
            }
            Err(e) => {
                tracing::error!("state supervisor panicked: {e}");
                late_failure.get_or_insert(format!("state supervisor panicked: {e}"));
            }
        }
    }

    // The sink can outlive source/destination: it keeps delivering its queue at
    // its own pace. The run is only done once the supervisor drains it.
    let (sink_name, objects_delivered, sink_ok) = match sink_task {
        Some(task) => match task.await {
            Ok(Ok(outcome)) => {
                tracing::info!(objects = outcome.objects_delivered, "sink delivery complete");
                (Some(outcome.name), Some(outcome.objects_delivered), true)
            }
            Ok(Err(e)) => {
                if failure.is_none() {
                    tracing::error!("sink delivery failed: {e}");
                    late_failure.get_or_insert(format!("sink delivery failed: {e}"));
                }
                (None, None, false)
            }
            Err(e) => {
                tracing::error!("sink supervisor panicked: {e}");
                late_failure.get_or_insert(format!("sink supervisor panicked: {e}"));
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

    tracing::debug!(
        source = %src_info.name,
        destination = %dst_info.name,
        "pipeline finished"
    );

    let (status, root_cause, signal) = match (&failure, late_failure) {
        (Some(f), _) => match f.error {
            CoreError::Interrupted(sig) => (SummaryStatus::Cancelled, Some(f.reason()), Some(sig)),
            _ => (SummaryStatus::Error, Some(f.reason()), None),
        },
        (None, Some(cause)) => (SummaryStatus::Error, Some(cause), None),
        (None, None) => (SummaryStatus::Success, None, None),
    };

    let src_finished = src.finished();
    let dst_finished = dst.finished();
    let summary = Summary {
        pipeline_name: config.pipeline.name.clone(),
        status,
        loadsmith_version: opts.version.clone(),
        source_name: src_info.name,
        destination_name: dst_info.name,
        sink_name,
        rows_read: src_finished.and_then(|f| f.rows_read).unwrap_or(pump_stats.rows),
        rows_written: dst_finished.and_then(|f| f.rows_written).unwrap_or(0),
        batches_read: src_finished.and_then(|f| f.batches_read).unwrap_or(pump_stats.batches),
        batches_written: dst_finished.and_then(|f| f.batches_written).unwrap_or(0),
        objects_delivered,
        duration,
        root_cause,
        signal,
    };

    summary.print();
    Ok(summary)
}

/// What the data phase ended with.
struct DataPhase {
    /// The first failure observed; `None` ⇒ both plugins finished successfully
    /// after the commit point.
    failure: Option<Failure>,
    pump_outcome: Option<PumpOutcome>,
}

/// Supervises the running pipeline until both plugins finished successfully
/// (after the commit point) or something failed. Never aborts anything
/// itself: it returns the first failure, and the caller runs the abort path.
async fn supervise_data_phase(
    src: &mut Peer,
    dst: &mut Peer,
    pump_join: &mut tokio::task::JoinHandle<PumpOutcome>,
    pump_control: &PumpControl,
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
) -> DataPhase {
    let mut failure: Option<Failure> = None;
    let mut pump_outcome: Option<PumpOutcome> = None;
    // The commit (end-of-stream marker to the destination) runs on a blocking
    // thread: the write can wait on a full pipe.
    let mut commit: Option<tokio::task::JoinHandle<anyhow::Result<()>>> = None;
    let mut committed = false;

    let finish_limit = Timeouts::limit(timeouts.finish_seconds);
    let finalize_limit = Timeouts::limit(timeouts.finalize_seconds);
    let stall_limit = Timeouts::limit(timeouts.stall_seconds);
    let first_batch_limit = stall_limit.and(Timeouts::limit(timeouts.first_batch_seconds));

    let pump_started = Instant::now();
    let mut last_batches = 0u64;
    let mut last_progress = pump_started;
    let mut source_end_deadline: Option<Instant> = None;
    let mut finalize_deadline: Option<Instant> = None;
    let mut watchdog = tokio::time::interval(WATCHDOG_TICK);

    loop {
        // ── Commit point ─────────────────────────────────────────────────
        if failure.is_none() && !committed && commit.is_none() {
            let source_ok = src.finished().is_some_and(|f| f.status == FinishedStatus::Success);
            if let (true, Some(out)) = (source_ok, pump_outcome.as_mut()) {
                if out.source_clean {
                    match std::mem::replace(&mut out.output, PumpOutput::Closed) {
                        PumpOutput::Open(writer) => {
                            let output = PumpOutput::Open(writer);
                            commit = Some(tokio::task::spawn_blocking(move || output.finish()));
                            // Measured from the commit, which itself waits
                            // on the destination draining its pipe.
                            finalize_deadline = finalize_limit.map(|d| Instant::now() + d);
                        }
                        _ => {
                            failure = Some(Failure::of(
                                "destination",
                                CoreError::PluginFailed(format!(
                                    "the destination stopped reading its input: {}",
                                    out.write_error.as_deref().unwrap_or("stream closed")
                                )),
                            ));
                        }
                    }
                }
            }
        }
        if failure.is_some() {
            break;
        }
        if committed && dst.finished().is_some() {
            break;
        }

        tokio::select! {
            biased;
            halt = shutdown.wait() => {
                failure = Some(Failure::halted(halt));
            }
            res = async { commit.as_mut().expect("guarded").await }, if commit.is_some() => {
                commit = None;
                match res {
                    Ok(Ok(())) => {
                        committed = true;
                        tracing::info!("source finished — input to the destination ended");
                    }
                    Ok(Err(e)) => {
                        failure = Some(Failure::of(
                            "destination",
                            CoreError::PluginFailed(format!("ending the destination's input failed: {e:#}")),
                        ));
                    }
                    Err(e) => {
                        failure = Some(Failure::new(CoreError::PluginProcess(format!("commit task: {e}"))));
                    }
                }
            }
            res = &mut *pump_join, if pump_outcome.is_none() => {
                match res {
                    Ok(out) => {
                        if let Some(e) = &out.read_error {
                            failure = Some(source_failure(src, format!("the source's data stream is unreadable: {e}")).await);
                        } else if !out.source_clean {
                            failure = Some(source_failure(
                                src,
                                "the source's data stream ended without an end-of-stream marker \
                                 (the source crashed or exited early)".into(),
                            ).await);
                        } else if src.finished().is_none() {
                            source_end_deadline = finish_limit.map(|d| Instant::now() + d);
                        }
                        pump_outcome = Some(out);
                    }
                    Err(e) => {
                        failure = Some(Failure::new(CoreError::PluginProcess(format!("data pump panicked: {e}"))));
                    }
                }
            }
            inbound = src.next_inbound() => {
                match src.process(inbound).await {
                    Err(f) => failure = Some(f),
                    Ok(Some(Event::Msg(Message::Finished(f)))) => {
                        if f.status != FinishedStatus::Success {
                            failure = Some(Failure::of("source", finished_failure("source", &f)));
                        } else if pump_outcome.is_none() {
                            source_end_deadline = finish_limit.map(|d| Instant::now() + d);
                        }
                    }
                    Ok(Some(Event::Eof)) => {
                        if src.finished().is_none() {
                            let status = src.exit_status_soon().await;
                            failure = Some(Failure::of(
                                "source",
                                CoreError::PluginFailed(format!(
                                    "source {} without sending finished",
                                    describe_exit(status)
                                )),
                            ));
                        }
                    }
                    Ok(Some(Event::Msg(other))) => ignore_mid_stream("source", &other),
                    Ok(Some(Event::ResendRequested)) | Ok(None) => {}
                }
            }
            inbound = dst.next_inbound() => {
                match dst.process(inbound).await {
                    Err(f) => failure = Some(f),
                    Ok(Some(Event::Msg(Message::Finished(f)))) => {
                        if f.status != FinishedStatus::Success {
                            failure = Some(Failure::of("destination", finished_failure("destination", &f)));
                        } else if !committed && commit.is_none() {
                            // (With the commit in flight, the destination may
                            // already have read the end marker and finished
                            // before the commit task reports back.)
                            failure = Some(Failure::by(
                                "destination",
                                error_codes::PROTOCOL_VIOLATION,
                                CoreError::Protocol(
                                    "destination reported success before its input ended".into(),
                                ),
                            ));
                        }
                    }
                    Ok(Some(Event::Eof)) => {
                        if dst.finished().is_none() {
                            let status = dst.exit_status_soon().await;
                            failure = Some(Failure::of(
                                "destination",
                                CoreError::PluginFailed(format!(
                                    "destination {} without sending finished",
                                    describe_exit(status)
                                )),
                            ));
                        }
                    }
                    Ok(Some(Event::Msg(other))) => ignore_mid_stream("destination", &other),
                    Ok(Some(Event::ResendRequested)) | Ok(None) => {}
                }
            }
            _ = watchdog.tick() => {
                let now = Instant::now();
                if pump_outcome.is_none() {
                    let batches = pump_control.batches.load(Ordering::Relaxed);
                    if batches != last_batches {
                        last_batches = batches;
                        last_progress = now;
                    }
                    if batches == 0 {
                        if let Some(limit) = first_batch_limit {
                            if now.duration_since(pump_started) >= limit {
                                failure = Some(Failure::new(CoreError::Timeout(format!(
                                    "data plane: no first batch within {}s (timeouts.first_batch_seconds)",
                                    limit.as_secs()
                                ))));
                            }
                        }
                    } else if let Some(limit) = stall_limit {
                        if now.duration_since(last_progress) >= limit {
                            failure = Some(Failure::new(CoreError::Timeout(format!(
                                "data plane stalled: no batch for {}s (timeouts.stall_seconds)",
                                limit.as_secs()
                            ))));
                        }
                    }
                }
                if !committed && source_end_deadline.is_some_and(|d| now >= d) {
                    failure = Some(Failure::by(
                        "source",
                        error_codes::TIMEOUT,
                        CoreError::Timeout(format!(
                            "source: did not complete within {}s after its data ended \
                             (timeouts.finish_seconds)",
                            timeouts.finish_seconds
                        )),
                    ));
                }
                if finalize_deadline.is_some_and(|d| now >= d) {
                    failure = Some(Failure::by(
                        "destination",
                        error_codes::TIMEOUT,
                        CoreError::Timeout(format!(
                            "destination: no finished within {}s after its input ended \
                             (timeouts.finalize_seconds)",
                            timeouts.finalize_seconds
                        )),
                    ));
                }
            }
        }
    }

    // A commit still in flight after a failure must not be left running.
    if let Some(task) = commit {
        task.abort();
    }
    DataPhase { failure, pump_outcome }
}

/// The failure for a broken source data stream. When the source's control
/// channel already explains it (a `finished` error, a crash), that is the
/// root cause; otherwise it is the stream itself.
async fn source_failure(src: &mut Peer, stream_problem: String) -> Failure {
    // The data stream and the control channel race: a failing source closes
    // fd3 right after (or before) its `finished`. Give fd1 a moment to explain.
    let deadline = Instant::now() + Duration::from_secs(1);
    while src.finished().is_none() && !src.at_eof() {
        let inbound = tokio::select! {
            inbound = src.next_inbound() => inbound,
            () = tokio::time::sleep_until(deadline) => break,
        };
        if matches!(src.process(inbound).await, Ok(Some(Event::Eof)) | Err(_)) {
            break;
        }
    }
    match src.finished() {
        Some(f) if f.status != FinishedStatus::Success => {
            Failure::of("source", finished_failure("source", f))
        }
        None if src.at_eof() => {
            let status = src.exit_status_soon().await;
            Failure::of(
                "source",
                CoreError::PluginFailed(format!(
                    "source {} without sending finished ({stream_problem})",
                    describe_exit(status)
                )),
            )
        }
        _ => Failure::of("source", CoreError::PluginFailed(stream_problem)),
    }
}

fn ignore_mid_stream(label: &str, msg: &Message) {
    match msg {
        Message::Log(_) | Message::Progress(_) | Message::Pong => {}
        other => tracing::warn!(plugin = label, "ignoring unexpected control message: {other:?}"),
    }
}

/// Sends `cancel` (or `error` to the plugin at fault) to every live plugin,
/// waits the grace period, then kills and reaps what is left.
async fn abort_plugins(src: &mut Peer, dst: &mut Peer, failure: &Failure, grace: Duration) {
    let reason = failure.reason();
    for peer in [&mut *src, &mut *dst] {
        let code = if failure.culprit == Some(peer.label()) { failure.code } else { None };
        peer.notify_abort(&reason, code).await;
    }
    tokio::join!(src.wind_down(grace), dst.wind_down(grace));
}

/// How a run failure is relayed to the sink stage.
fn halt_for(failure: &Failure) -> Halt {
    match failure.error {
        CoreError::Interrupted(sig) => Halt::Signal(sig),
        _ => Halt::Abort(failure.reason()),
    }
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
