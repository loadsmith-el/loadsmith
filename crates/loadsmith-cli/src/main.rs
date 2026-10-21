use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};
use loadsmith_config::MaskList;
use loadsmith_core::{discovery, run_pipeline, LoadOpts, Mode, RunOpts};
use tracing::Instrument;


mod download;
mod exit;
mod plugin_install;
mod run_summary;

use exit::{classify, Exit, RunOutcome, UsageError};

#[derive(Parser)]
#[command(
    name = "loadsmith",
    version,
    about = "Modern EL pipeline tool — plugin-first, Arrow IPC, declarative YAML"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, default_value = "info", help = "Log level: trace|debug|info|warn|error")]
    log_level: String,

    #[arg(long, global = true, help = "Disable ANSI colour in output (also honours NO_COLOR)")]
    no_color: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a pipeline from a YAML config file
    Run(RunArgs),
    /// Validate a pipeline config without running it (static; --resolve for full resolution)
    Validate(ValidateArgs),
    /// Manage installed plugins
    Plugin(PluginCmd),
    /// Inspect or reset a pipeline's incremental state
    State(StateCmd),
}

#[derive(Args)]
struct RunArgs {
    /// Path to the pipeline YAML file
    pipeline: PathBuf,

    #[arg(long, help = "Resolve and validate the config (providers included), but start no data plugin")]
    dry_run: bool,

    #[arg(long, help = "Resolve the config (providers included), print it with every resolved value masked, and exit")]
    print_resolved_config: bool,

    #[arg(long, help = "Override plugin directory (default: ~/.loadsmith/plugins)")]
    plugin_dir: Option<PathBuf>,

    /// What stdout carries: the human report, or one JSON run summary
    /// (logs stay on stderr either way)
    #[arg(long, value_enum, default_value_t = OutputFormat::Human, conflicts_with = "print_resolved_config")]
    output: OutputFormat,

    /// Also write the JSON run summary to this file (atomically), whatever
    /// --output says
    #[arg(long, value_name = "PATH", conflicts_with = "print_resolved_config")]
    summary_file: Option<PathBuf>,

    #[command(flatten)]
    strictness: Strictness,

    #[command(flatten)]
    telemetry: TelemetryArgs,
}

/// OpenTelemetry export, over the `telemetry:` block and the `OTEL_*`
/// variables.
#[derive(Args, Clone, Debug, Default)]
struct TelemetryArgs {
    /// OTLP endpoint for traces, metrics and logs; turns telemetry on.
    /// Overrides telemetry.endpoint and OTEL_EXPORTER_OTLP_ENDPOINT
    #[arg(long, value_name = "URL")]
    otel_endpoint: Option<String>,

    /// OTLP transport: grpc or http-protobuf (the default). Overrides
    /// telemetry.protocol and OTEL_EXPORTER_OTLP_PROTOCOL
    #[arg(long, value_name = "PROTOCOL", value_parser = ["grpc", "http-protobuf", "http/protobuf"])]
    otel_protocol: Option<String>,

    /// An export header (gRPC metadata) as KEY=VALUE; repeatable. Wins over
    /// telemetry.headers and OTEL_EXPORTER_OTLP_HEADERS. Values are masked
    #[arg(long = "otel-header", value_name = "KEY=VALUE")]
    otel_headers: Vec<String>,

    /// No telemetry for this run, whatever the file and the environment say
    #[arg(long)]
    no_telemetry: bool,
}

impl TelemetryArgs {
    fn flags(&self) -> Result<loadsmith_core::telemetry::TelemetryFlags, UsageError> {
        let headers = self
            .otel_headers
            .iter()
            .map(|h| match h.split_once('=') {
                Some((k, v)) if !k.trim().is_empty() => Ok((k.trim().to_string(), v.to_string())),
                _ => Err(UsageError(format!("--otel-header {h:?}: expected KEY=VALUE"))),
            })
            .collect::<Result<_, _>>()?;
        Ok(loadsmith_core::telemetry::TelemetryFlags {
            endpoint: self.otel_endpoint.clone(),
            protocol: self.otel_protocol.as_deref().and_then(loadsmith_config::OtlpProtocol::parse),
            headers,
            disabled: self.no_telemetry,
        })
    }
}

/// The run's telemetry settings: the flags over the `telemetry:` block over
/// the environment.
fn telemetry_settings(
    args: &TelemetryArgs,
    block: Option<&loadsmith_config::TelemetryConfig>,
) -> Result<Option<loadsmith_core::telemetry::TelemetrySettings>> {
    let flags = args.flags()?;
    let settings = loadsmith_core::telemetry::resolve(&flags, block, &|k| std::env::var(k).ok())
        .map_err(loadsmith_core::CoreError::Config)?;
    Ok(settings)
}

/// What the end of a run reports to the run's telemetry.
struct RunTelemetry {
    span: tracing::Span,
    /// The flush bound at exit: the export timeout, or the cancel grace
    /// when that is shorter and the run was interrupted.
    flush: std::time::Duration,
    grace: std::time::Duration,
}

/// The environment variable that turns `--ignore-unknown-keys` on.
const IGNORE_UNKNOWN_KEYS_ENV: &str = "LOADSMITH_IGNORE_UNKNOWN_KEYS";

/// How unknown keys in a pipeline file are treated.
#[derive(Args, Clone, Copy, Debug, Default)]
struct Strictness {
    /// Warn about unknown keys and ignore them instead of failing: in the
    /// pipeline file's own blocks and in every plugin's `config:`. Also
    /// turned on by LOADSMITH_IGNORE_UNKNOWN_KEYS=1; the flag wins over the
    /// variable (`--ignore-unknown-keys=false` keeps the default)
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    ignore_unknown_keys: Option<bool>,
}

impl Strictness {
    /// The policy: the flag if given, else the environment variable, else
    /// `error`.
    fn policy(self) -> Result<loadsmith_core::UnknownKeys, UsageError> {
        let ignore = match self.ignore_unknown_keys {
            Some(flag) => flag,
            None => env_switch(IGNORE_UNKNOWN_KEYS_ENV, std::env::var(IGNORE_UNKNOWN_KEYS_ENV).ok().as_deref())?,
        };
        Ok(if ignore { loadsmith_core::UnknownKeys::Warn } else { loadsmith_core::UnknownKeys::Error })
    }
}

/// A boolean environment variable: unset or empty is off; `1`/`true`/`yes`/
/// `on` and `0`/`false`/`no`/`off` (any case) are accepted; anything else is
/// a usage error.
fn env_switch(name: &str, value: Option<&str>) -> Result<bool, UsageError> {
    match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("0") | Some("false") | Some("no") | Some("off") => Ok(false),
        Some("1") | Some("true") | Some("yes") | Some("on") => Ok(true),
        Some(other) => Err(UsageError(format!("{name}={other}: expected 1/true/yes/on or 0/false/no/off"))),
    }
}

/// The format of `loadsmith run`'s stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    /// Header and summary box for people (default).
    Human,
    /// One JSON run-summary document, on a single line.
    Json,
}

#[derive(Args)]
struct ValidateArgs {
    /// Path to the pipeline YAML file
    pipeline: PathBuf,

    /// Also resolve every template: read env()/file() values and run the
    /// provider instances the secret() calls reference (the default check
    /// is static and evaluates nothing)
    #[arg(long)]
    resolve: bool,

    #[arg(long, help = "Override plugin directory (where --resolve finds provider binaries)")]
    plugin_dir: Option<PathBuf>,

    #[command(flatten)]
    strictness: Strictness,

    /// Checked like `run` does (static: the flags and the environment;
    /// with --resolve also the resolved telemetry: block); nothing is exported
    #[command(flatten)]
    telemetry: TelemetryArgs,
}

#[derive(Args)]
struct PluginCmd {
    #[command(subcommand)]
    action: PluginAction,
}

#[derive(Args)]
struct StateCmd {
    #[command(subcommand)]
    action: StateAction,
}

#[derive(Subcommand)]
enum StateAction {
    /// Show the persisted watermark for a pipeline
    Show(StateArgs),
    /// Remove the persisted state for a pipeline (next run starts fresh)
    Rm(StateArgs),
}

#[derive(Args)]
struct StateArgs {
    /// Path to the pipeline YAML file (its `state:` block locates the state)
    pipeline: PathBuf,

    #[command(flatten)]
    strictness: Strictness,
}

#[derive(Subcommand)]
enum PluginAction {
    /// List installed plugins
    List(PluginListArgs),
    /// Install a plugin from the index (by name), a manifest, or a local binary
    Install(PluginInstallArgs),
    /// Remove an installed plugin's binaries (by type name, e.g. `postgres`)
    Uninstall(PluginUninstallArgs),
}

#[derive(Args)]
struct PluginListArgs {
    #[arg(long, help = "Plugin directory to inspect")]
    plugin_dir: Option<PathBuf>,
}

#[derive(Args)]
struct PluginInstallArgs {
    /// Package to resolve from the official index, as `<name>[:<kind>][@<version>]`.
    /// A bare `<name>` (or `<name> --all`) installs all of the package's plugins;
    /// `<name>:<kind>` (or `--kind`) installs just one — e.g. `mysql:source`,
    /// `mysql:source@0.1.0`.
    name: Option<String>,

    /// Install only this plugin kind from the package
    /// (source|destination|sink|parser|provider). Alternative to the
    /// `<name>:<kind>` spec suffix.
    #[arg(long, conflicts_with_all = ["manifest", "binary", "all"])]
    kind: Option<String>,

    /// Install from a plugin manifest (loadsmith-plugin.yaml) — a local path
    /// or a file:// / http:// / https:// URL
    #[arg(short = 'f', long, conflicts_with_all = ["binary", "name"])]
    manifest: Option<String>,

    /// Install a single local plugin binary directly, no manifest
    #[arg(long, conflicts_with_all = ["manifest", "name"])]
    binary: Option<PathBuf>,

    /// Install all plugins: every package in the index, or — combined with a
    /// `<name>` — every plugin of that one package
    #[arg(long, conflicts_with_all = ["manifest", "binary"])]
    all: bool,

    #[arg(long, help = "Override the plugin index URL (for `install <name>` / --all)")]
    index: Option<String>,

    #[arg(long, help = "Plugin directory to install into")]
    plugin_dir: Option<PathBuf>,
}

#[derive(Args)]
struct PluginUninstallArgs {
    /// Package to remove, as `<name>[:<kind>]`. A bare `<name>` removes all of
    /// the package's binaries (e.g. `postgres` → `loadsmith-*-postgres`);
    /// `<name>:<kind>` (or `--kind`) removes just one.
    name: String,

    /// Remove only this plugin kind (alternative to the `<name>:<kind>` suffix)
    #[arg(long)]
    kind: Option<String>,

    #[arg(long, help = "Plugin directory to remove from")]
    plugin_dir: Option<PathBuf>,
}

// mimalloc: release binaries are static musl, whose default allocator is slow
// under the many small allocations of Arrow IPC framing.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let color = !cli.no_color && std::env::var_os("NO_COLOR").is_none();
    init_tracing(&cli.log_level, color);

    // Every value the pipeline loader resolves lands here, so nothing printed
    // below — the final error line included — can carry one.
    let mask = MaskList::new();
    match run(cli, color, mask.clone()).await {
        Ok(()) => ExitCode::from(Exit::Success.code()),
        Err(e) => {
            eprintln!("error: {}", mask.apply(&format!("{e:#}")));
            ExitCode::from(classify(&e).code())
        }
    }
}

async fn run(cli: Cli, color: bool, mask: MaskList) -> Result<()> {
    match cli.command {
        Commands::Run(args) => cmd_run(args, color, mask).await,
        Commands::Validate(args) => cmd_validate(args, mask).await,
        Commands::Plugin(cmd) => match cmd.action {
            PluginAction::List(args) => cmd_plugin_list(args),
            PluginAction::Install(args) => cmd_plugin_install(args),
            PluginAction::Uninstall(args) => cmd_plugin_uninstall(args),
        },
        Commands::State(cmd) => match cmd.action {
            StateAction::Show(args) => cmd_state_show(args, mask).await,
            StateAction::Rm(args) => cmd_state_rm(args, mask).await,
        },
    }
}

async fn cmd_run(args: RunArgs, color: bool, mask: MaskList) -> Result<()> {
    let json_stdout = args.output == OutputFormat::Json;
    let summary_file = args.summary_file.clone();
    let started = std::time::SystemTime::now();
    let facts = loadsmith_core::FactsHandle::default();
    let mut ctx = run_summary::RunContext { dry_run: args.dry_run, ..Default::default() };
    let mut telemetry: Option<RunTelemetry> = None;

    let result = execute_run(args, color, !json_stdout, &facts, &mut ctx, &mask, &mut telemetry).await;
    let finished = std::time::SystemTime::now();
    let (summary, outcome) = match result {
        Ok(Some(summary)) => {
            let outcome = run_outcome(&summary);
            (Some(summary), outcome)
        }
        Ok(None) => (None, Ok(())),
        Err(e) => (None, Err(e)),
    };
    let exit_code = match &outcome {
        Ok(()) => Exit::Success.code(),
        Err(e) => classify(e).code(),
    };

    // Close the run's trace and flush every signal (bounded) before the
    // summary goes out: the summary carries the trace id.
    if let Some(run) = telemetry.take() {
        let interrupted = summary.as_ref().is_some_and(|s| s.signal.is_some())
            || outcome.as_ref().err().is_some_and(|e| {
                e.chain().any(|c| matches!(c.downcast_ref(), Some(loadsmith_core::CoreError::Interrupted(_))))
            });
        let label = match (&outcome, interrupted) {
            (Ok(()), _) => "success",
            (Err(_), true) => "cancelled",
            (Err(_), false) => "error",
        };
        ctx.trace_id = loadsmith_core::telemetry::trace_id(&run.span);
        run.span.record("loadsmith.run.outcome", label);
        run.span.record("loadsmith.run.exit_code", i64::from(exit_code));
        if let Err(e) = &outcome {
            run.span.record("error", mask.apply(&format!("{e:#}")));
        }
        let elapsed = finished.duration_since(started).unwrap_or_default();
        loadsmith_core::telemetry::record_run(label, elapsed);
        drop(run.span);
        let bound = if interrupted { run.flush.min(run.grace) } else { run.flush };
        loadsmith_core::telemetry::shutdown(bound).await;
    }

    if json_stdout || summary_file.is_some() {
        let snapshot = facts.snapshot();
        let doc = run_summary::build(&run_summary::RunRecord {
            ctx: &ctx,
            facts: &snapshot,
            summary: summary.as_ref(),
            error: outcome.as_ref().err(),
            mask: &mask,
            exit_code,
            started,
            finished,
        });
        if let Some(path) = &summary_file {
            if let Err(e) = run_summary::write_file(path, &doc) {
                let err = anyhow::anyhow!("cannot write the summary file {}: {e}", path.display());
                match &outcome {
                    // A successful run whose summary was asked for and lost
                    // is a failure for whoever waits on that file.
                    Ok(()) => {
                        if json_stdout {
                            println!("{}", serde_json::to_string(&doc)?);
                        }
                        return Err(err);
                    }
                    Err(_) => eprintln!("error: {err:#}"),
                }
            }
        }
        if json_stdout {
            println!("{}", serde_json::to_string(&doc)?);
        }
    }
    outcome
}

/// Runs the pipeline. `Ok(None)`: `--print-resolved-config`, nothing ran.
async fn execute_run(
    args: RunArgs,
    color: bool,
    human_report: bool,
    facts: &loadsmith_core::FactsHandle,
    ctx: &mut run_summary::RunContext,
    mask: &MaskList,
    telemetry: &mut Option<RunTelemetry>,
) -> Result<Option<loadsmith_core::Summary>> {
    // Handle SIGINT/SIGTERM explicitly before anything else: in the images the
    // engine is PID 1, where an unhandled SIGTERM is silently ignored.
    let shutdown = loadsmith_core::listen_for_signals()?;
    let plugin_dir = args.plugin_dir.unwrap_or_else(discovery::default_plugin_dir);
    let unknown_keys = args.strictness.policy()?;

    // Static check first (nothing runs on a malformed file), so even a run
    // that fails resolving its providers reports the pipeline it was.
    let doc = loadsmith_core::read_document(&args.pipeline, unknown_keys)?;
    ctx.learn(doc.config());

    // Telemetry first (its block resolves without providers), so that the
    // providers run inside the traced run.
    let early = doc.resolve_subtrees(&["pipeline", "telemetry"], mask).map_err(loadsmith_core::CoreError::Config)?;
    let settings = telemetry_settings(&args.telemetry, early.telemetry.as_ref())?;
    let run_id = loadsmith_core::state::new_run_id();
    let identity = loadsmith_core::telemetry::RunIdentity {
        pipeline: early.pipeline.name.clone(),
        run_id: run_id.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    if let Some(s) = &settings {
        // Header values are secrets wherever they came from.
        for value in s.headers.values() {
            mask.add(value.clone());
        }
    }
    let span = match settings.clone() {
        Some(s) if !args.print_resolved_config => {
            let flush = s.timeout;
            loadsmith_core::telemetry::install(s, &identity, mask);
            let span = loadsmith_core::telemetry::run_span(&identity);
            *telemetry = Some(RunTelemetry {
                span: span.clone(),
                flush,
                grace: std::time::Duration::from_secs(early.pipeline.timeouts.cancel_grace_seconds),
            });
            span
        }
        _ => tracing::Span::none(),
    };

    async {
        // Full resolution — providers included — before the state lock and
        // before any data plugin starts. `--dry-run` and
        // `--print-resolved-config` resolve too: they show what would run.
        let load = LoadOpts { plugin_dir: plugin_dir.clone(), shutdown: shutdown.clone(), mask: mask.clone(), unknown_keys };
        let mut config = loadsmith_core::resolve_document(&doc, Mode::Full, &load).await?;
        ctx.learn(&config);

        if args.print_resolved_config {
            // The effective telemetry (flags, file and environment merged).
            if let Some(s) = &settings {
                config.telemetry = Some(s.as_config());
            }
            let yaml = serde_yaml::to_string(&config)?;
            println!("{}", mask.apply(&yaml));
            return Ok(None);
        }

        let opts = RunOpts {
            plugin_dir,
            dry_run: args.dry_run,
            print_resolved_config: args.print_resolved_config,
            color,
            version: env!("CARGO_PKG_VERSION").to_string(),
            shutdown,
            human_report,
            facts: facts.clone(),
            mask: mask.clone(),
            unknown_keys,
            run_id,
        };

        Ok(Some(run_pipeline(config, opts).await?))
    }
    .instrument(span)
    .await
}

/// A pipeline that ran but failed (a plugin error, a failed delivery…) must
/// exit non-zero, or schedulers and the lab read it as a success.
fn run_outcome(summary: &loadsmith_core::Summary) -> Result<()> {
    let pipeline = summary.pipeline_name.clone();
    match summary.status {
        loadsmith_core::SummaryStatus::Success => Ok(()),
        loadsmith_core::SummaryStatus::Cancelled => match summary.signal {
            Some(signal) => Err(RunOutcome::Cancelled { pipeline, signal }.into()),
            None => Err(RunOutcome::Failed { pipeline }.into()),
        },
        loadsmith_core::SummaryStatus::Error => Err(RunOutcome::Failed { pipeline }.into()),
    }
}

async fn cmd_validate(args: ValidateArgs, mask: MaskList) -> Result<()> {
    // Static by default: structure, template syntax, and every secret()
    // naming a declared provider instance. Nothing is evaluated or started.
    let unknown_keys = args.strictness.policy()?;
    let doc = loadsmith_core::read_document(&args.pipeline, unknown_keys)?;
    if !args.resolve {
        // The telemetry flags and environment; the block's values may hold
        // env()/file() templates, which only --resolve evaluates.
        telemetry_settings(&args.telemetry, None)?;
        println!(
            "Pipeline '{}' is valid (static check; --resolve also resolves env()/file() and \
             runs the providers).",
            doc.config().pipeline.name
        );
        return Ok(());
    }
    // --resolve: the full resolution `run` performs before it starts
    // anything, providers included.
    let plugin_dir = args.plugin_dir.unwrap_or_else(discovery::default_plugin_dir);
    let load = LoadOpts { plugin_dir, shutdown: loadsmith_core::listen_for_signals()?, mask, unknown_keys };
    let config = loadsmith_core::resolve_document(&doc, Mode::Full, &load).await?;
    telemetry_settings(&args.telemetry, config.telemetry.as_ref())?;
    let secrets = doc.secrets().len();
    println!(
        "Pipeline '{}' is valid (fully resolved; {secrets} secret() reference{} resolved).",
        config.pipeline.name,
        if secrets == 1 { "" } else { "s" }
    );
    Ok(())
}

fn cmd_plugin_list(args: PluginListArgs) -> Result<()> {
    let plugin_dir = args.plugin_dir.unwrap_or_else(discovery::default_plugin_dir);
    let plugins = discovery::list_plugins(&plugin_dir);

    if plugins.is_empty() {
        println!("No plugins found in {}", plugin_dir.display());
    } else {
        println!("Plugins in {}:", plugin_dir.display());
        for p in plugins {
            println!("  {p}");
        }
    }
    Ok(())
}

/// The pipeline with only `pipeline:` and `state:` resolved: the state
/// commands never start a provider, and other blocks' templates (database
/// credentials…) need not resolve.
async fn load_state_cfg(
    args: &StateArgs,
    mask: MaskList,
) -> Result<(loadsmith_config::PipelineConfig, loadsmith_config::StateConfig)> {
    let unknown_keys = args.strictness.policy()?;
    let load = LoadOpts { mask, unknown_keys, ..LoadOpts::new(discovery::default_plugin_dir()) };
    let (config, _) = loadsmith_core::load_pipeline(&args.pipeline, Mode::Builtins, &load).await?;
    let state = config.state.clone().ok_or_else(|| {
        loadsmith_config::ConfigError::Validation(format!(
            "pipeline '{}' has no state: block",
            config.pipeline.name
        ))
    })?;
    Ok((config, state))
}

async fn cmd_state_show(args: StateArgs, mask: MaskList) -> Result<()> {
    let (config, state_cfg) = load_state_cfg(&args, mask).await?;
    let backend = loadsmith_core::state::open_backend(&state_cfg)?;
    match backend.load(&config.pipeline.name)? {
        None => println!("No state recorded for pipeline '{}'.", config.pipeline.name),
        Some(doc) => {
            println!("Pipeline:     {}", doc.pipeline);
            if doc.is_legacy() {
                println!("Format:       legacy (pre-v1; upgraded on the next run's save)");
            } else if !doc.fingerprint_is_comparable() {
                println!(
                    "Format:       v{} (old schema fingerprint; upgraded to v{} on the next run's save)",
                    doc.version,
                    loadsmith_core::state::STATE_FORMAT_VERSION
                );
            } else {
                println!("Format:       v{}", doc.version);
            }
            println!("Cursor value: {}", doc.cursor_value);
            println!("Schema hash:  {}", doc.schema_hash);
            println!("Run id:       {}", doc.run_id);
            println!("Updated at:   {} (unix ms)", doc.updated_at_unix_ms);
        }
    }
    Ok(())
}

async fn cmd_state_rm(args: StateArgs, mask: MaskList) -> Result<()> {
    let (config, state_cfg) = load_state_cfg(&args, mask).await?;
    let backend = loadsmith_core::state::open_backend(&state_cfg)?;
    // Take the lock so we don't clear state out from under a running pipeline.
    let _guard = backend.lock(&config.pipeline.name)?;
    backend.clear(&config.pipeline.name)?;
    println!("Cleared state for pipeline '{}'.", config.pipeline.name);
    Ok(())
}

fn cmd_plugin_install(args: PluginInstallArgs) -> Result<()> {
    let plugin_dir = args.plugin_dir.unwrap_or_else(discovery::default_plugin_dir);

    if let Some(binary) = args.binary {
        // Direct local-binary install (no manifest).
        let dest = discovery::install_plugin(&binary, &plugin_dir)?;
        println!("Installed {} → {}", binary.display(), dest.display());
        return Ok(());
    }

    let index = args.index.as_deref().unwrap_or(plugin_install::DEFAULT_INDEX_URL);

    if args.kind.is_some() && args.name.is_none() && args.manifest.is_none() {
        return Err(UsageError(
            "--kind needs a package name (e.g. `install mysql --kind source`)".into(),
        )
        .into());
    }
    if let Some(k) = &args.kind {
        plugin_install::validate_kind(k)?;
    }

    // `--all` over the index skips packages this loadsmith can't use (see
    // `Incompatible`); naming a package explicitly still fails on them.
    let skip_incompatible = args.all && args.name.is_none();

    // Each job: a manifest (index URL / path) plus the kinds to install from it
    // (`None` ⇒ all of the package's binaries).
    let jobs: Vec<(String, Option<Vec<String>>)> = if args.all && args.name.is_none() {
        // The whole canonical set, every kind of each package.
        plugin_install::index_plugin_names(index)?
            .iter()
            .map(|name| plugin_install::resolve_from_index(name, index).map(|url| (url, None)))
            .collect::<Result<_>>()?
    } else if let Some(spec) = args.manifest {
        // A manifest path/URL, optionally narrowed by --kind.
        vec![(spec, args.kind.map(|k| vec![k]))]
    } else if let Some(name_spec) = args.name {
        let (pkg, spec_kind, version) = plugin_install::parse_install_spec(&name_spec)?;
        // Reconcile a `:kind` spec suffix with the --kind flag.
        let kind = match (spec_kind, args.kind) {
            (Some(a), Some(b)) if a != b => {
                return Err(UsageError(format!(
                    "conflicting kind: {a:?} (in spec) vs {b:?} (--kind)"
                ))
                .into())
            }
            (Some(a), _) => Some(a),
            (None, b) => b,
        };
        if args.all && kind.is_some() {
            return Err(UsageError(
                "--all installs every kind of the package; drop the kind selector".into(),
            )
            .into());
        }
        let kinds = if args.all { None } else { kind.map(|k| vec![k]) };
        let index_spec = match version {
            Some(v) => format!("{pkg}@{v}"),
            None => pkg,
        };
        vec![(plugin_install::resolve_from_index(&index_spec, index)?, kinds)]
    } else {
        return Err(UsageError(
            "nothing to install: pass a plugin name, --all, --manifest, or --binary".into(),
        )
        .into());
    };

    for (spec, kinds) in jobs {
        let kind_refs: Option<Vec<&str>> =
            kinds.as_ref().map(|v| v.iter().map(String::as_str).collect());
        let attempt = plugin_install::load_manifest(&spec).and_then(|manifest| {
            plugin_install::install_from_manifest(
                &manifest,
                &plugin_dir,
                loadsmith_core::lifecycle::SUPPORTED_VERSIONS,
                kind_refs.as_deref(),
            )
            .map(|installed| (manifest, installed))
        });
        let (manifest, installed) = match attempt {
            Ok(ok) => ok,
            Err(e) if skip_incompatible && e.downcast_ref::<plugin_install::Incompatible>().is_some() => {
                eprintln!("warning: skipping incompatible package: {e}");
                continue;
            }
            Err(e) => return Err(e),
        };
        println!(
            "Installed plugin '{}' v{} ({} binar{}) → {}",
            manifest.name,
            manifest.version,
            installed.len(),
            if installed.len() == 1 { "y" } else { "ies" },
            plugin_dir.display()
        );
        for p in installed {
            if let Some(name) = p.file_name().and_then(|f| f.to_str()) {
                println!("  {name}");
            }
        }
    }
    Ok(())
}

fn cmd_plugin_uninstall(args: PluginUninstallArgs) -> Result<()> {
    let plugin_dir = args.plugin_dir.unwrap_or_else(discovery::default_plugin_dir);
    let (pkg, spec_kind, version) = plugin_install::parse_install_spec(&args.name)?;
    if version.is_some() {
        return Err(UsageError(
            "uninstall takes no @version (one version is installed at a time)".into(),
        )
        .into());
    }
    if let Some(k) = &args.kind {
        plugin_install::validate_kind(k)?;
    }
    let kind = match (spec_kind, args.kind) {
        (Some(a), Some(b)) if a != b => {
            anyhow::bail!("conflicting kind: {a:?} (in spec) vs {b:?} (--kind)")
        }
        (Some(a), _) => Some(a),
        (None, b) => b,
    };
    let removed = plugin_install::uninstall(&pkg, kind.as_deref(), &plugin_dir)?;
    if removed.is_empty() {
        println!("No installed binaries found for plugin '{pkg}'.");
    } else {
        println!("Removed {} binar{}:", removed.len(), if removed.len() == 1 { "y" } else { "ies" });
        for p in removed {
            if let Some(name) = p.file_name().and_then(|f| f.to_str()) {
                println!("  {name}");
            }
        }
    }
    Ok(())
}

/// stderr logs (`--log-level`), plus the OpenTelemetry layers, dormant
/// until a run installs telemetry. Only events reach stderr: the engine's
/// spans exist for telemetry and never decorate the log lines.
fn init_tracing(level: &str, color: bool) {
    use tracing_subscriber::filter::{filter_fn, FilterExt};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, Layer};
    let stderr = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(color)
        .with_filter(EnvFilter::new(level).and(filter_fn(|meta| meta.is_event())));
    tracing_subscriber::registry().with(loadsmith_core::telemetry::layers()).with(stderr).init();
}
