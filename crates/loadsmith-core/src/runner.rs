//! Runs one pipeline: spawns the plugins, drives their lifecycle, pumps the
//! data plane, and enforces the abort rules of protocol v1.
//!
//! The Arrow stream comes from the **producer**: the source, or — with a
//! `parser:` — the parser, fed the files a raw source announces (see
//! [`crate::parser_feed`]).
//!
//! **Abort rules** (normative; see the protocol reference):
//!
//! 1. The core ends the destination's fd3 (writes the IPC end-of-stream
//!    marker) only after the producer's stream ended with its own marker
//!    **and** the producer sent `finished` with status `success` — and, with
//!    a parser, after the raw source too sent `finished` with `success`. This
//!    is the commit point: until then a destination must not finalize.
//! 2. On any failure — a plugin crash, exit, `finished` with an error, a
//!    protocol violation, a timeout, a stalled data plane, an operator signal
//!    — the core sends `cancel` to every live plugin (`error` to the one at
//!    fault), waits the grace period, and then kills each plugin's process
//!    group. The destination's fd3 stays open until the destination is gone,
//!    so it can never observe a clean end of input after an upstream failure.
//!    A parser's stdin is closed gracefully ("no more files") only after the
//!    raw source succeeded; after an upstream failure it gets `cancel`.
//! 3. Every plugin's outcome is always collected, and the summary reports the
//!    run's status with the root cause (the first failure observed).

use std::fs::File;
use std::os::unix::io::IntoRawFd;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use loadsmith_arrow::ipc::IpcError;
use loadsmith_arrow::IpcReader;
use loadsmith_config::{validate_pipeline, ConfigError, MaskList, PipelineConfig, Timeouts};
use loadsmith_protocol::{capabilities, error_codes, Credit, FinishedStatus, Message, PluginKind, UnknownKeys};
use loadsmith_transport::ControlReader;
use tokio::io::BufReader;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;
use tracing::Instrument;

use crate::{
    discovery::find_plugin_binary,
    error::CoreError,
    events::{drain_events, EventForward},
    lifecycle::{
        run_handshake_and_configure, run_handshake_and_configure_with, start_and_await_ready, start_source,
        PluginHandshake,
    },
    parser_feed::{await_parser_schema, Feed, FeedEvent},
    peer::{describe_exit, finished_failure, Event, Failure, Peer},
    pump::{pump, FlowWindow, PumpControl, PumpOutcome, PumpOutput},
    reporter::Reporter,
    shutdown::{Halt, Shutdown},
    sink_supervisor::{run_sink_supervisor, SinkLaunch},
    spawner::{create_data_pipe, spawn_plugin, StderrRoute},
    state::{check_schema_drift, new_run_id, open_backend, schema_fingerprint},
    state_supervisor::{run_state_supervisor, StateRun},
    summary::{FactsHandle, Summary, SummaryStatus},
};

/// Capability a destination must advertise for a sink to be attachable: it
/// stages files locally and announces each via `ObjectReady`. On a source it
/// marks a raw source, which requires a `parser:`.
const CAP_OBJECT_OUTPUT: &str = capabilities::OBJECT_OUTPUT;

/// Capability a source must advertise for a `state:` block to drive incremental
/// loads: it accepts a resume cursor and reports watermarks.
const CAP_INCREMENTAL_STATE: &str = capabilities::INCREMENTAL_STATE;

/// The config key a raw source advertising `scratch_dir` receives its
/// download directory in.
const SCRATCH_DIR_KEY: &str = "scratch_dir";

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
    /// Print the human report (header, summary box, dry-run notice) on
    /// stdout. Off when stdout carries a machine-readable summary instead.
    pub human_report: bool,
    /// Filled in as the run learns about its plugins and state, so the
    /// caller can report them even when the run fails during setup.
    pub facts: FactsHandle,
    /// Resolved template values (from the pipeline loader) to redact from
    /// plugin logs, plugin error texts, the abort log and the root cause.
    pub mask: MaskList,
    /// What every plugin does with unknown keys in its `config:` (sent in
    /// `configure`). `Error` unless the operator ran with
    /// `--ignore-unknown-keys`.
    pub unknown_keys: UnknownKeys,
    /// This run's id: in the persisted state and on the run's telemetry.
    pub run_id: String,
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
            human_report: true,
            facts: FactsHandle::default(),
            mask: MaskList::new(),
            unknown_keys: UnknownKeys::Error,
            run_id: new_run_id(),
        }
    }
}

/// Runs the pipeline described by `config` to completion.
///
/// Returns `Err` when the run fails before data starts flowing (bad config, a
/// missing or incompatible plugin, a rejected config, a failed capability
/// gate, the state lock, a signal); the plugins are cancelled and reaped
/// first. Once data flows, the outcome — success, error, or cancelled by a
/// signal — is a [`Summary`] (already printed) with the root cause.
pub async fn run_pipeline(config: PipelineConfig, opts: RunOpts) -> Result<Summary, CoreError> {
    validate_pipeline(&config)?;

    if opts.dry_run {
        if opts.human_report {
            println!("Dry run — pipeline validated. Not executing.");
        }
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
    // Sent to every data plugin and the sink in `configure`.
    let run = Some(loadsmith_protocol::RunInfo {
        id: opts.run_id.clone(),
        pipeline: config.pipeline.name.clone(),
        started_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0),
    });

    // ── Incremental state setup ───────────────────────────────────────────
    // With a `state:` block we lock the pipeline's state (failing fast if a
    // concurrent run holds it), load the prior watermark to resume from, and
    // prepare the channels the event drains feed to the state supervisor.
    let state_enabled = config.state.is_some();
    let (mut state_backend, mut state_guard, prior_state) = match &config.state {
        Some(cfg) => {
            let backend = open_backend(cfg)?;
            let mut guard = tracing::info_span!(target: "loadsmith::state", "lock.acquire", loadsmith.state.backend = %cfg.backend)
                .in_scope(|| backend.lock(&config.pipeline.name))?;
            // Renew from the moment of acquisition: plugin spawn, handshake and
            // a slow first query all happen before the state supervisor exists,
            // and must not let the lease expire.
            guard.start_renewal();
            let prior = tracing::info_span!(target: "loadsmith::state", "state.load", loadsmith.state.backend = %cfg.backend)
                .in_scope(|| backend.load(&config.pipeline.name))?;
            (Some(backend), Some(guard), prior)
        }
        None => (None, None, None),
    };
    let resume_value = prior_state.as_ref().map(|d| d.cursor_value.clone());
    let facts = opts.facts.clone();
    facts.update(|f| f.watermark_before = resume_value.clone());
    let (checkpoint_tx, checkpoint_rx) = maybe_channel(state_enabled);
    let (committed_tx, committed_rx) = maybe_channel(state_enabled);

    // Print the header first — before any log line — so the version and route
    // always lead the output, then the run log follows underneath it.
    let reporter = Reporter::new(opts.color);
    let parser_type = config.parser.as_ref().map(|p| p.plugin_type.clone());
    let sink_type = config.sink.as_ref().map(|s| s.plugin_type.clone());
    if opts.human_report {
        reporter.header(
            &opts.version,
            &config.source.plugin_type,
            parser_type.as_deref(),
            &config.destination.plugin_type,
            sink_type.as_deref(),
        );
    }

    let source_bin = find_plugin_binary(PluginKind::Source, &config.source.plugin_type, &opts.plugin_dir)?;
    facts.update(|f| f.source.path = Some(source_bin.clone()));
    let parser_bin = match &config.parser {
        Some(p) => Some(find_plugin_binary(PluginKind::Parser, &p.plugin_type, &opts.plugin_dir)?),
        None => None,
    };
    facts.update(|f| f.parser.path = parser_bin.clone());
    let dest_bin =
        find_plugin_binary(PluginKind::Destination, &config.destination.plugin_type, &opts.plugin_dir)?;
    facts.update(|f| f.destination.path = Some(dest_bin.clone()));
    let sink_bin = match &config.sink {
        Some(s) => Some(find_plugin_binary(PluginKind::Sink, &s.plugin_type, &opts.plugin_dir)?),
        None => None,
    };
    facts.update(|f| f.sink.path = sink_bin.clone());

    tracing::info!(
        source = ?source_bin,
        parser = ?parser_bin,
        destination = ?dest_bin,
        sink = ?sink_bin,
        "spawning plugins"
    );

    let source_config = yaml_value_to_json(&config.source.config)?;
    let parser_config = match &config.parser {
        Some(p) => Some(yaml_value_to_json(&p.config)?),
        None => None,
    };
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
            let scratch = allocate_scratch_dir("stage", &config.pipeline.name)?;
            tracing::info!(path = %scratch.display(), "allocated staging scratch dir");
            set_config_path(&mut dest_config, "path", &scratch);
            scratch_cleanup = Some(scratch);
        }
    }
    // A raw source that downloads its files (capability `scratch_dir`) gets
    // a core-allocated directory, unless its config names one. Allocated
    // during its configure (capabilities arrive first); removed only after a
    // successful run.
    let mut download_scratch: Option<PathBuf> = None;

    // Unbounded by design: the destination's event drain forwards ObjectReady
    // paths here and must never block (a full fd4 would deadlock the pump).
    let (object_tx, object_rx) = maybe_channel(config.sink.is_some());
    // The same for the raw source's files, which go to the parser.
    let (file_tx, file_rx) = maybe_channel(config.parser.is_some());

    // Two pipes, with loadsmith in the middle of the data plane:
    //   producer ──IN──▶ core (pump) ──OUT──▶ destination
    // The producer is the source, or the parser (a raw source has no fd3).
    // Neither side sees the other; everything flows through the control plane.
    let (in_read, in_write) = create_data_pipe()
        .map_err(|e| CoreError::PluginProcess(format!("create data pipe (in): {e}")))?;
    let (out_read, out_write) = create_data_pipe()
        .map_err(|e| CoreError::PluginProcess(format!("create data pipe (out): {e}")))?;

    // The producer's fd3 is the write end of the IN pipe; core keeps the read end.
    let mut in_write = Some(in_write);
    let src_fd3 = if parser_bin.is_some() { None } else { in_write.take().map(IntoRawFd::into_raw_fd) };
    let mut src_sp = spawn_plugin(&source_bin, src_fd3, StderrRoute::Forward(opts.mask.clone()))
        .map_err(|e| CoreError::PluginIncompatible(format!("spawn source {}: {e}", source_bin.display())))?;
    let mut prs_sp = match &parser_bin {
        Some(bin) => Some(
            spawn_plugin(bin, in_write.take().map(IntoRawFd::into_raw_fd), StderrRoute::Forward(opts.mask.clone()))
                .map_err(|e| CoreError::PluginIncompatible(format!("spawn parser {}: {e}", bin.display())))?,
        ),
        None => None,
    };
    // Destination's fd3 is the read end of the OUT pipe; core keeps the write end.
    let mut dst_sp = spawn_plugin(&dest_bin, Some(out_read.into_raw_fd()), StderrRoute::Forward(opts.mask.clone())).map_err(|e| {
        CoreError::PluginIncompatible(format!("spawn destination {}: {e}", dest_bin.display()))
    })?;

    // Take the event channels (fd4) out so we can drain them concurrently —
    // a plugin blocked writing progress to a full fd4 would stall the pump.
    // The destination's drain also forwards ObjectReady paths to the sink
    // supervisor (when a sink is attached); a raw source's drain forwards its
    // files to the parser feed. Each drain also re-emits its plugin's
    // telemetry through the plugin's scope.
    let src_event = tokio::fs::File::from_std(src_sp.event_fd_take());
    let dst_event = tokio::fs::File::from_std(dst_sp.event_fd_take());
    let prs_event = prs_sp.as_mut().map(|sp| tokio::fs::File::from_std(sp.event_fd_take()));

    let mut src = Peer::new(PluginKind::Source.as_str(), src_sp).with_mask(opts.mask.clone()).with_run(run.clone());
    let mut prs = prs_sp.map(|sp| {
        let mut peer = Peer::new(PluginKind::Parser.as_str(), sp).with_mask(opts.mask.clone()).with_run(run.clone());
        // The parser's stdin is owned by a writer task: one `parse_object`
        // per file must never stall the runner on a full pipe.
        peer.queue_input();
        peer
    });
    let mut dst = Peer::new(PluginKind::Destination.as_str(), dst_sp).with_mask(opts.mask.clone()).with_run(run.clone());

    let (src_mask, dst_mask) = (opts.mask.clone(), opts.mask.clone());
    let (src_scope, dst_scope) = (Some(src.scope().clone()), Some(dst.scope().clone()));
    let src_events = tokio::spawn(async move {
        drain_events(
            ControlReader::new(BufReader::new(src_event)),
            "source",
            EventForward { object_tx: file_tx, checkpoint_tx, mask: src_mask, scope: src_scope, ..Default::default() },
        )
        .await;
    });
    let prs_events = prs_event.map(|event| {
        let mask = opts.mask.clone();
        let scope = prs.as_ref().map(|p| p.scope().clone());
        tokio::spawn(async move {
            drain_events(
                ControlReader::new(BufReader::new(event)),
                "parser",
                EventForward { mask, scope, ..Default::default() },
            )
            .await;
        })
    });
    let dst_events = tokio::spawn(async move {
        drain_events(
            ControlReader::new(BufReader::new(dst_event)),
            "destination",
            EventForward { object_tx, committed_tx, mask: dst_mask, scope: dst_scope, ..Default::default() },
        )
        .await;
    });
    // With a parser, the source is a raw source: its files reach the parser
    // through the feed, and the parser is the producer.
    let mut feed: Option<Feed> = file_rx.map(|rx| Feed::new(rx, &timeouts));

    // Read the header of the producer's Arrow IPC stream (its exact schema)
    // on a blocking thread. It only completes once the producer writes it,
    // right after its `schema` message; setup waits for it before the drift
    // check.
    let mut source_stream: Option<StreamOpen> =
        Some(tokio::task::spawn_blocking(move || IpcReader::new(in_read)));

    // The sink stage is stopped through its own shutdown input: the runner
    // fires it when the run aborts (or forwards a signal).
    let (sink_trigger, sink_shutdown) = Shutdown::channel();
    let mut sink_task = None;
    let mut state_task = None;
    let mut delivery_gate = None;
    // Flow control, when the producer is a source that obeys credits: the
    // pump sizes the window, the data phase relays its grants.
    let mut flow_window: Option<FlowWindow> = None;
    let mut credit_rx: Option<tokio::sync::watch::Receiver<u64>> = None;
    let start_time = std::time::Instant::now();

    // ── Setup: handshake, gates, start ────────────────────────────────────
    let setup = async {
        let wants_scratch = config.parser.is_some();
        let pipeline_name = config.pipeline.name.clone();
        let scratch_slot = &mut download_scratch;
        let src_info = run_handshake_and_configure_with(
            &mut src,
            move |supports| {
                let mut source_config = source_config;
                let asks = supports.iter().any(|c| c == capabilities::SCRATCH_DIR);
                let given = source_config.get(SCRATCH_DIR_KEY).and_then(|v| v.as_str()).is_some();
                if wants_scratch && asks && !given {
                    let dir = allocate_scratch_dir("download", &pipeline_name).map_err(Failure::new)?;
                    tracing::info!(path = %dir.display(), "allocated download scratch dir for the source");
                    set_config_path(&mut source_config, SCRATCH_DIR_KEY, &dir);
                    *scratch_slot = Some(dir);
                }
                Ok(source_config)
            },
            opts.unknown_keys,
            PluginKind::Source,
            &timeouts,
            &mut shutdown,
        )
        .await?;
        facts.update(|f| {
            f.source.name = Some(src_info.name.clone());
            f.source.version = Some(src_info.version.clone());
        });
        let prs_info = match (prs.as_mut(), parser_config) {
            (Some(peer), Some(cfg)) => {
                let info =
                    run_handshake_and_configure(peer, cfg, opts.unknown_keys, PluginKind::Parser, &timeouts, &mut shutdown)
                        .await?;
                facts.update(|f| {
                    f.parser.name = Some(info.name.clone());
                    f.parser.version = Some(info.version.clone());
                });
                Some(info)
            }
            _ => None,
        };
        let dst_info = run_handshake_and_configure(
            &mut dst,
            dest_config,
            opts.unknown_keys,
            PluginKind::Destination,
            &timeouts,
            &mut shutdown,
        )
        .await?;
        facts.update(|f| {
            f.destination.name = Some(dst_info.name.clone());
            f.destination.version = Some(dst_info.version.clone());
        });

        // Capability gate: a raw source (it produces files, `object_output`)
        // and a `parser:` go together.
        check_parser_gates(&config, &src_info, prs_info.as_ref())?;

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
            // The delivery gate: the state supervisor persists nothing until
            // the sink reports every object delivered (see state_supervisor).
            let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
            delivery_gate = Some(gate_rx);
            let supervisor = run_sink_supervisor(
                SinkLaunch {
                    binary: bin,
                    config: cfg,
                    unknown_keys: opts.unknown_keys,
                    mask: opts.mask.clone(),
                    run: run.clone(),
                },
                rx,
                timeouts,
                sink_shutdown,
            );
            sink_task = Some(tokio::spawn(
                async move {
                    let outcome = supervisor.await;
                    let _ = gate_tx.send(outcome.is_ok());
                    outcome
                }
                .in_current_span(),
            ));
        }

        // Start the producer. A plain source answers `start` with its schema.
        // With a parser: start the parser (it answers with its schema, maybe
        // only after its first file), then the raw source (it answers
        // `ready` and starts announcing files), and hand the parser every
        // file until its schema arrives.
        let declared = match prs.as_mut() {
            None => {
                let mut credit = None;
                if src_info.supports.iter().any(|c| c == capabilities::FLOW_CREDIT) {
                    let (window, rx) = FlowWindow::new(&config.pipeline.flow);
                    credit = Some(window.granted());
                    flow_window = Some(window);
                    credit_rx = Some(rx);
                }
                start_source(&mut src, resume_value, credit, &timeouts, &mut shutdown).await?
            }
            Some(parser) => {
                tracing::debug!(plugin = "parser", "→ start");
                parser.send(Message::Start(Default::default())).await?;
                start_and_await_ready(&mut src, &timeouts, &mut shutdown).await?;
                let feed = feed.as_mut().expect("a parser has a feed");
                await_parser_schema(parser, &mut src, feed, &timeouts, &mut shutdown).await?
            }
        };

        // The Arrow IPC stream header is the authoritative schema: the `schema`
        // message is informational (and lossy from older SDKs).
        let producer = prs.as_mut().unwrap_or(&mut src);
        let reader = open_source_stream(producer, &mut source_stream, &timeouts, &mut shutdown).await?;
        let arrow_schema = reader.schema();
        let fields = loadsmith_arrow::protocol_fields_from_schema(&arrow_schema);
        warn_on_schema_mismatch(producer.label(), &declared.fields, &fields);
        reporter.schema(&fields);

        // Schema-drift guard: if the source shape changed since the recorded state,
        // a blind resume could corrupt the load. Refuse (default) or warn.
        let schema_hash = schema_fingerprint(&arrow_schema);
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
                run_id: opts.run_id.clone(),
                checkpoint_interval: config.state.as_ref().map(|c| c.checkpoint_interval).unwrap_or(0),
            };
            state_task = Some(tokio::spawn(
                run_state_supervisor(backend, guard, run, ck_rx, cm_rx, delivery_gate.take()).in_current_span(),
            ));

        }

        start_and_await_ready(&mut dst, &timeouts, &mut shutdown).await?;
        tracing::info!("destination ready — pump starting");
        Ok((src_info, prs_info, dst_info, reader))
    }
    .await;

    let (src_info, prs_info, dst_info, source_reader) = match setup {
        Ok(infos) => infos,
        Err(failure) => {
            tracing::error!("aborting the run: {}", opts.mask.apply(&failure.reason()));
            sink_trigger.trigger(halt_for(&failure));
            abort_plugins(&mut src, prs.as_mut(), &mut dst, &failure, grace).await;
            let _ = src_events.await;
            if let Some(task) = prs_events {
                let _ = task.await;
            }
            let _ = dst_events.await;
            if let Some(task) = state_task {
                let _ = task.await;
            }
            if let Some(task) = sink_task {
                let _ = task.await;
            }
            // The producer is gone, so a header read still in flight ends now.
            if let Some(task) = source_stream {
                let _ = task.await;
            }
            // The destination is gone; only now close its fd3.
            drop(out_write);
            if let Some(dir) = &download_scratch {
                release_download_scratch(dir, false);
            }
            return Err(failure.error);
        }
    };

    // ── Data phase ────────────────────────────────────────────────────────
    // Run the data pump on a blocking thread: it copies every batch from the
    // producer's IN stream to the destination's OUT stream, reporting progress.
    tracing::info!("data pump started");
    let pump_control = Arc::new(PumpControl::default());
    let mut pump_join = {
        let control = Arc::clone(&pump_control);
        // The pump's progress lines belong to the run's trace.
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                pump(source_reader, out_write, control, flow_window, move |rows, batches| {
                    reporter.progress(rows, batches)
                })
            })
        })
    };

    // Heartbeat: log every `heartbeat_seconds` while the pump runs so the
    // operator knows the pipeline is alive during long transfers. 0 disables it.
    let heartbeat_secs = config.pipeline.heartbeat_seconds;
    let heartbeat = (heartbeat_secs > 0).then(|| {
        tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_secs));
                interval.tick().await; // skip the immediate first tick
                loop {
                    interval.tick().await;
                    tracing::info!("pipeline still running…");
                }
            }
            .in_current_span(),
        )

    });

    // The producer is the parser (fed by the raw source) or the source.
    let (producer, upstream) = match (prs.as_mut(), feed.as_mut()) {
        (Some(parser), Some(f)) => (parser, Some(Upstream { raw: &mut src, feed: f })),
        _ => (&mut src, None),
    };
    let supervised =
        supervise_data_phase(
            producer,
            upstream,
            &mut dst,
            Pumping { join: &mut pump_join, control: &pump_control, credits: credit_rx },
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
            tracing::error!("aborting the run: {}", opts.mask.apply(&failure.reason()));
            pump_control.abort.store(true, Ordering::Relaxed);
            sink_trigger.trigger(halt_for(failure));
            abort_plugins(&mut src, prs.as_mut(), &mut dst, failure, grace).await;
        }
        None => {
            // Every plugin sent `finished`; let them exit.
            let reap_parser = async {
                if let Some(p) = prs.as_mut() {
                    p.reap(grace).await;
                }
            };
            tokio::join!(src.reap(grace), reap_parser, dst.reap(grace));
        }
    }
    // The producer is gone, so the pump has ended (it drains to EOF); and the
    // destination is gone, so dropping its stream can no longer be mistaken
    // for anything.
    if pump_outcome.is_none() {
        pump_outcome = pump_join.await.ok();
    }
    let pump_stats = pump_outcome.as_ref().map(|o| o.stats).unwrap_or_default();
    drop(pump_outcome);
    tracing::info!(rows = pump_stats.rows, batches = pump_stats.batches, "data pump completed");
    tracing::info!(
        reading_s = format!("{:.1}", pump_stats.reading.as_secs_f64()),
        writing_s = format!("{:.1}", pump_stats.writing.as_secs_f64()),
        peak_window = pump_stats.peak_window,
        "data plane: time in the source's stream vs the destination's (a slow side shows here)"
    );

    let _ = src_events.await;
    if let Some(task) = prs_events {
        let _ = task.await;
    }
    // Draining the destination's events to completion drops the ObjectReady
    // sender, which closes the supervisor's input channel — so this must finish
    // before awaiting the sink.
    let _ = dst_events.await;

    // Both drains are done ⇒ the checkpoint/committed channels are closed and
    // the state supervisor performs its final watermark flush and releases the
    // pipeline state lock. Only durably committed watermarks are ever
    // persisted, so this is safe after an abort too. With a sink, the flush
    // also waits for the sink's delivery gate, so this resolves only once the
    // sink is done.
    let mut late_failure: Option<String> = None;
    if let Some(task) = state_task {
        match task.await {
            Ok(Ok(persisted)) => facts.update(|f| f.watermark_persisted = persisted),
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
                facts.update(|f| {
                    f.sink.name = Some(outcome.name.clone());
                    f.sink.version = Some(outcome.version.clone());
                });
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
    // The raw source's downloads: removed only after the whole run succeeded.
    if let Some(dir) = &download_scratch {
        release_download_scratch(dir, failure.is_none() && late_failure.is_none());
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

    // Rows and batches read are the producer's: the parser's with a parser
    // (a raw source reports files, not rows).
    let produced = match prs.as_ref() {
        Some(parser) => parser.finished(),
        None => src.finished(),
    };
    let dst_finished = dst.finished();
    let summary = Summary {
        pipeline_name: config.pipeline.name.clone(),
        status,
        loadsmith_version: opts.version.clone(),
        source_name: src_info.name,
        parser_name: prs_info.map(|p| p.name),
        destination_name: dst_info.name,
        sink_name,
        rows_read: produced.and_then(|f| f.rows_read).unwrap_or(pump_stats.rows),
        rows_written: dst_finished.and_then(|f| f.rows_written).unwrap_or(0),
        batches_read: produced.and_then(|f| f.batches_read).unwrap_or(pump_stats.batches),
        batches_written: dst_finished.and_then(|f| f.batches_written).unwrap_or(0),
        objects_delivered,
        duration,
        root_cause: root_cause.map(|c| opts.mask.apply(&c)),
        signal,
    };

    if opts.human_report {
        summary.print();
    }
    Ok(summary)
}

/// The capability gates of the parser stage: a `parser:` needs a raw source
/// (`object_output`) and a parser advertising `parse`; a raw source needs a
/// `parser:`. A mismatched pair is a configuration error (exit 3); a parser
/// without `parse` is an incompatible plugin (exit 4).
fn check_parser_gates(
    config: &PipelineConfig,
    src: &PluginHandshake,
    parser: Option<&PluginHandshake>,
) -> Result<(), Failure> {
    let gate = |msg: String| Err(Failure::new(CoreError::Config(ConfigError::Validation(msg))));
    let raw_source = src.supports.iter().any(|c| c == CAP_OBJECT_OUTPUT);
    match (&config.parser, parser) {
        (Some(p), Some(info)) => {
            if !raw_source {
                return gate(format!(
                    "parser '{}' needs a source that produces files, but source '{}' streams \
                     Arrow (missing '{CAP_OBJECT_OUTPUT}' capability); remove the parser: block",
                    p.plugin_type, config.source.plugin_type
                ));
            }
            // Like a provider without `resolve`: the plugin itself cannot do
            // the job (exit 4), whatever the pipeline says.
            if !info.supports.iter().any(|c| c == capabilities::PARSE) {
                return Err(Failure::new(CoreError::PluginIncompatible(format!(
                    "parser '{}' ({}) does not parse files (missing '{}' capability)",
                    p.plugin_type,
                    info.name,
                    capabilities::PARSE
                ))));
            }
        }
        _ if raw_source => {
            return gate(format!(
                "source '{}' produces files, not Arrow ('{CAP_OBJECT_OUTPUT}' capability): add a \
                 parser: block to parse them",
                config.source.plugin_type
            ));
        }
        _ => {}
    }
    Ok(())
}

/// Removes the raw source's core-allocated download directory after a
/// successful run. After a failure only an empty one is removed; downloaded
/// files are kept for inspection.
fn release_download_scratch(dir: &std::path::Path, success: bool) {
    if success {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => tracing::debug!(path = %dir.display(), "removed download scratch dir"),
            Err(e) => tracing::warn!(path = %dir.display(), "could not remove download scratch dir: {e}"),
        }
    } else if std::fs::remove_dir(dir).is_err() && dir.exists() {
        tracing::warn!(
            path = %dir.display(),
            "the run did not succeed — leaving the source's downloaded files in the scratch dir"
        );
    }
}

/// Sets `key` to `path` in a plugin's config object (made an object if it
/// was not one).
fn set_config_path(config: &mut serde_json::Value, key: &str, path: &std::path::Path) {
    if !config.is_object() {
        *config = serde_json::Value::Object(serde_json::Map::new());
    }
    config[key] = serde_json::Value::String(path.to_string_lossy().into_owned());
}

/// The blocking read of the source's IPC stream header.
type StreamOpen = tokio::task::JoinHandle<Result<IpcReader<File>, IpcError>>;

/// Waits for the header of the producer's Arrow IPC stream, which a source
/// (or a parser) writes right after its `schema` message. Bounded by
/// `start_seconds` (measured from here) and interrupted by `shutdown`; the
/// producer's control channel is watched meanwhile, so a producer that gives
/// up is reported as such. `task` stays `Some` until the read completes, so
/// the abort path can reap it once the producer is gone.
async fn open_source_stream(
    src: &mut Peer,
    task: &mut Option<StreamOpen>,
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
) -> Result<IpcReader<File>, Failure> {
    let label = src.label();
    let limit = Timeouts::limit(timeouts.start_seconds);
    let deadline = limit.map(|d| Instant::now() + d);
    let handle = task.as_mut().expect("the source stream is opened once");
    let opened = loop {
        tokio::select! {
            biased;
            halt = shutdown.wait() => return Err(Failure::halted(halt)),
            res = &mut *handle => break res,
            inbound = src.next_inbound() => match src.process(inbound).await? {
                Some(Event::Msg(Message::Finished(f))) if f.status != FinishedStatus::Success => {
                    return Err(Failure::of(label, finished_failure(label, &f, src.mask())));
                }
                // A small producer can be done before the core read its
                // stream header. Its `finished` is kept (the peer records
                // it): the data phase checks it at the commit point.
                Some(Event::Msg(Message::Finished(_))) => {
                    tracing::debug!(plugin = label, "finished before its stream header was read; kept");
                }
                // EOF on fd1: the producer is exiting, and its fd3 closes with
                // it, which ends the header read below.
                Some(Event::Eof) | Some(Event::ResendRequested) | None => {}
                Some(Event::Msg(other)) => ignore_mid_stream(label, &other),
            },
            () = sleep_until(deadline) => {
                return Err(Failure::by(
                    label,
                    error_codes::TIMEOUT,
                    CoreError::Timeout(format!(
                        "{label}: no Arrow IPC stream header on fd3 within {}s after its schema \
                         (timeouts.start_seconds)",
                        timeouts.start_seconds
                    )),
                ));
            }
        }
    };
    *task = None;
    match opened {
        Ok(Ok(reader)) => Ok(reader),
        Ok(Err(e)) => Err(source_failure(src, format!("the {label}'s data stream has no valid header: {e}")).await),
        Err(e) => Err(Failure::new(CoreError::PluginProcess(format!("stream header task: {e}")))),
    }
}

/// Sleeps until `deadline`, or forever without one.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

/// Warns when a producer's `schema` message does not describe its Arrow
/// stream (a plugin built with an older SDK labels types lossily). The stream
/// wins.
fn warn_on_schema_mismatch(
    label: &str,
    declared: &[loadsmith_protocol::Field],
    actual: &[loadsmith_protocol::Field],
) {
    if declared == actual {
        return;
    }
    let detail = if declared.len() != actual.len() {
        format!("{} declared columns, {} in the stream", declared.len(), actual.len())
    } else {
        declared
            .iter()
            .zip(actual)
            .find(|(d, a)| d != a)
            .map(|(d, a)| {
                let show = |f: &loadsmith_protocol::Field| {
                    format!("{}: {}{}", f.name, f.field_type, if f.nullable { "" } else { " not null" })
                };
                format!("declared `{}`, stream has `{}`", show(d), show(a))
            })
            .unwrap_or_default()
    };
    tracing::warn!(
        "the {label}'s schema message does not match its Arrow stream ({detail}); using the \
         stream. A plugin built with a current SDK sends the exact schema"
    );
}

/// What the data phase ended with.
struct DataPhase {
    /// The first failure observed; `None` ⇒ every plugin finished
    /// successfully after the commit point.
    failure: Option<Failure>,
    pump_outcome: Option<PumpOutcome>,
}

/// With a parser: the raw source and the feed that hands its files to the
/// parser.
struct Upstream<'a> {
    raw: &'a mut Peer,
    feed: &'a mut Feed,
}

/// The next event upstream of the parser; never, without one.
async fn upstream_next(upstream: &mut Option<Upstream<'_>>) -> FeedEvent {
    match upstream {
        Some(up) => up.feed.next(up.raw).await,
        None => std::future::pending().await,
    }
}

/// The running pump, as the data phase watches it.
struct Pumping<'a> {
    join: &'a mut tokio::task::JoinHandle<PumpOutcome>,
    control: &'a PumpControl,
    /// The flow window's grants, to relay to the source (flow control only).
    credits: Option<tokio::sync::watch::Receiver<u64>>,
}

/// The next flow-control grant to relay to the source; `None` once the pump
/// is done (its window is gone). Never, without flow control.
async fn next_grant(credit_rx: &mut Option<tokio::sync::watch::Receiver<u64>>) -> Option<u64> {
    match credit_rx {
        Some(rx) => match rx.changed().await {
            Ok(()) => Some(*rx.borrow_and_update()),
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}

/// Supervises the running pipeline until every plugin finished successfully
/// (after the commit point) or something failed. Never aborts anything
/// itself: it returns the first failure, and the caller runs the abort path.
///
/// `src` is the producer of the Arrow stream: the source, or the parser.
/// With a parser, `upstream` carries the raw source: its files keep flowing
/// to the parser, the parser's input ends only after the raw source
/// succeeded, and the commit point needs both to have succeeded.
async fn supervise_data_phase(
    src: &mut Peer,
    mut upstream: Option<Upstream<'_>>,
    dst: &mut Peer,
    pumping: Pumping<'_>,
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
) -> DataPhase {
    let Pumping { join: pump_join, control: pump_control, credits: mut credit_rx } = pumping;
    let label = src.label();
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
        // ── The parser's input ───────────────────────────────────────────
        // A parser's `finished` may have arrived during setup (while the
        // core read its stream header), where nothing judged it: a success
        // before its input ended would end the load early. Checked before
        // the input is ended below, so it is exact.
        if let Some(up) = upstream.as_mut() {
            if failure.is_none()
                && !src.input_closed()
                && src.finished().is_some_and(|f| f.status == FinishedStatus::Success)
            {
                failure = Some(early_success(label));
            }
            // "No more files" only once every file was handed over and the
            // raw source succeeded.
            if failure.is_none() {
                up.feed.maybe_end_input(up.raw, src);
            }
        }

        // ── Commit point ─────────────────────────────────────────────────
        if failure.is_none() && !committed && commit.is_none() {
            // The producer succeeded, and with a parser so did the raw
            // source, after which (only) the parser's input was ended.
            let upstream_ok = upstream
                .as_ref()
                .is_none_or(|up| src.input_closed() && Feed::raw_succeeded(up.raw));
            let source_ok = upstream_ok && src.finished().is_some_and(|f| f.status == FinishedStatus::Success);
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
                        tracing::info!("{label} finished — input to the destination ended");
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
                            failure = Some(source_failure(src, format!("the {label}'s data stream is unreadable: {e}")).await);
                        } else if !out.source_clean {
                            failure = Some(source_failure(
                                src,
                                format!(
                                    "the {label}'s data stream ended without an end-of-stream marker \
                                     (the {label} crashed or exited early)"
                                ),
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
                            failure = Some(Failure::of(label, finished_failure(label, &f, src.mask())));
                        } else if upstream.is_some() && !src.input_closed() {
                            // A parser that finishes while files may still
                            // come would end the load early.
                            failure = Some(early_success(label));
                        } else if pump_outcome.is_none() {
                            source_end_deadline = finish_limit.map(|d| Instant::now() + d);
                        }
                    }
                    Ok(Some(Event::Eof)) => {
                        if src.finished().is_none() {
                            let status = src.exit_status_soon().await;
                            failure = Some(Failure::of(
                                label,
                                CoreError::PluginFailed(format!(
                                    "{label} {} without sending finished",
                                    describe_exit(status)
                                )),
                            ));
                        }
                    }
                    Ok(Some(Event::Msg(other))) => ignore_mid_stream(label, &other),
                    Ok(Some(Event::ResendRequested)) | Ok(None) => {}
                }
            }
            grant = next_grant(&mut credit_rx) => {
                match grant {
                    // A source that is gone fails through its own branches.
                    Some(batch_seq) => {
                        if let Err(e) = src.send(Message::Credit(Credit { batch_seq })).await {
                            tracing::debug!("credit not delivered to the {label}: {e}");
                        }
                    }
                    None => credit_rx = None,
                }
            }
            event = upstream_next(&mut upstream) => {
                if let Some(up) = upstream.as_mut() {
                    if let Err(f) = up.feed.handle(event, up.raw, src).await {
                        failure = Some(f);
                    }
                }
            }
            inbound = dst.next_inbound() => {
                match dst.process(inbound).await {
                    Err(f) => failure = Some(f),
                    Ok(Some(Event::Msg(Message::Finished(f)))) => {
                        if f.status != FinishedStatus::Success {
                            failure = Some(Failure::of("destination", finished_failure("destination", &f, dst.mask())));
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
                        label,
                        error_codes::TIMEOUT,
                        CoreError::Timeout(format!(
                            "{label}: did not complete within {}s after its data ended \
                             (timeouts.finish_seconds)",
                            timeouts.finish_seconds
                        )),
                    ));
                }
                if let Some(f) = upstream.as_ref().and_then(|up| up.feed.overdue(now)) {
                    failure = Some(f);
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

/// A parser that reported success before the core ended its input.
fn early_success(label: &'static str) -> Failure {
    Failure::by(
        label,
        error_codes::PROTOCOL_VIOLATION,
        CoreError::Protocol(format!("{label} reported success before its input ended")),
    )
}

/// The failure for a broken data stream of the producer (source or parser).
/// When its control channel already explains it (a `finished` error, a
/// crash), that is the root cause; otherwise it is the stream itself.
async fn source_failure(src: &mut Peer, stream_problem: String) -> Failure {
    let label = src.label();
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
            Failure::of(label, finished_failure(label, f, src.mask()))
        }
        None if src.at_eof() => {
            let status = src.exit_status_soon().await;
            Failure::of(
                label,
                CoreError::PluginFailed(format!(
                    "{label} {} without sending finished ({stream_problem})",
                    describe_exit(status)
                )),
            )
        }
        _ => Failure::of(label, CoreError::PluginFailed(stream_problem)),
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
async fn abort_plugins(
    src: &mut Peer,
    mut parser: Option<&mut Peer>,
    dst: &mut Peer,
    failure: &Failure,
    grace: Duration,
) {
    // Plugins echo the reason (to stderr): it must not carry a value.
    let reason = src.mask().apply(&failure.reason());
    let peers = std::iter::once(&mut *src).chain(parser.as_deref_mut()).chain(std::iter::once(&mut *dst));
    for peer in peers {
        let code = if failure.culprit == Some(peer.label()) { failure.code } else { None };
        peer.notify_abort(&reason, code).await;
    }
    let parser_down = async {
        if let Some(p) = parser {
            p.wind_down(grace).await;
        }
    };
    tokio::join!(src.wind_down(grace), parser_down, dst.wind_down(grace));
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

/// Creates a fresh scratch directory the core owns: `stage` for a
/// destination whose output a sink will deliver, `download` for a raw source
/// that downloads its files. Named with the purpose, the pipeline and the pid
/// so concurrent runs (and the two purposes) don't collide. The core removes
/// only directories it allocated.
fn allocate_scratch_dir(purpose: &str, pipeline_name: &str) -> Result<PathBuf, CoreError> {
    let slug: String = pipeline_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let dir = std::env::temp_dir().join(format!("loadsmith-{purpose}-{slug}-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
