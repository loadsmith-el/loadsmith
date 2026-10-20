use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};
use loadsmith_config::{parse_pipeline_yaml, MaskList};
use loadsmith_core::{discovery, run_pipeline, LoadOpts, Mode, RunOpts};

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
    /// Validate a pipeline config without running it
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

    #[arg(long, help = "Validate and print config, but do not execute")]
    dry_run: bool,

    #[arg(long, help = "Print resolved config (secrets masked) and exit")]
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

    #[arg(long, help = "Override plugin directory")]
    plugin_dir: Option<PathBuf>,
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

/// Reads the pipeline file; an unreadable file is a config error (exit 3).
fn read_pipeline(path: &std::path::Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| {
        anyhow::Error::new(loadsmith_config::ConfigError::Io(e))
            .context(format!("cannot read {}", path.display()))
    })
}

async fn run(cli: Cli, color: bool, mask: MaskList) -> Result<()> {
    match cli.command {
        Commands::Run(args) => cmd_run(args, color, mask).await,
        Commands::Validate(args) => cmd_validate(args),
        Commands::Plugin(cmd) => match cmd.action {
            PluginAction::List(args) => cmd_plugin_list(args),
            PluginAction::Install(args) => cmd_plugin_install(args),
            PluginAction::Uninstall(args) => cmd_plugin_uninstall(args),
        },
        Commands::State(cmd) => match cmd.action {
            StateAction::Show(args) => cmd_state_show(args),
            StateAction::Rm(args) => cmd_state_rm(args),
        },
    }
}

async fn cmd_run(args: RunArgs, color: bool, mask: MaskList) -> Result<()> {
    let json_stdout = args.output == OutputFormat::Json;
    let summary_file = args.summary_file.clone();
    let started = std::time::SystemTime::now();
    let facts = loadsmith_core::FactsHandle::default();
    let mut ctx = run_summary::RunContext { dry_run: args.dry_run, ..Default::default() };

    let result = execute_run(args, color, !json_stdout, &facts, &mut ctx, &mask).await;
    let finished = std::time::SystemTime::now();
    let (summary, outcome) = match result {
        Ok(Some(summary)) => {
            let outcome = run_outcome(&summary);
            (Some(summary), outcome)
        }
        Ok(None) => (None, Ok(())),
        Err(e) => (None, Err(e)),
    };

    if json_stdout || summary_file.is_some() {
        let exit_code = match &outcome {
            Ok(()) => Exit::Success.code(),
            Err(e) => classify(e).code(),
        };
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
) -> Result<Option<loadsmith_core::Summary>> {
    // Handle SIGINT/SIGTERM explicitly before anything else: in the images the
    // engine is PID 1, where an unhandled SIGTERM is silently ignored.
    let shutdown = loadsmith_core::listen_for_signals()?;
    let plugin_dir = args.plugin_dir.unwrap_or_else(discovery::default_plugin_dir);

    // Static check first (nothing runs on a malformed file), so even a run
    // that fails resolving its providers reports the pipeline it was.
    let doc = loadsmith_core::read_document(&args.pipeline)?;
    ctx.learn(doc.config());
    // Full resolution — providers included — before the state lock and
    // before any data plugin starts. `--dry-run` and
    // `--print-resolved-config` resolve too: they show what would run.
    let load = LoadOpts { plugin_dir: plugin_dir.clone(), shutdown: shutdown.clone(), mask: mask.clone() };
    let config = loadsmith_core::resolve_document(&doc, Mode::Full, &load).await?;
    ctx.learn(&config);

    if args.print_resolved_config {
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
    };

    Ok(Some(run_pipeline(config, opts).await?))
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

fn cmd_validate(args: ValidateArgs) -> Result<()> {
    let content = read_pipeline(&args.pipeline)?;

    let (config, _mask) = parse_pipeline_yaml(&content)?;
    loadsmith_config::validate_pipeline(&config)?;

    println!("Pipeline '{}' is valid.", config.pipeline.name);
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

fn load_state_cfg(pipeline: &std::path::Path) -> Result<(loadsmith_config::PipelineConfig, loadsmith_config::StateConfig)> {
    let content = read_pipeline(pipeline)?;
    let (config, _mask) = parse_pipeline_yaml(&content)?;
    let state = config.state.clone().ok_or_else(|| {
        loadsmith_config::ConfigError::Validation(format!(
            "pipeline '{}' has no state: block",
            config.pipeline.name
        ))
    })?;
    Ok((config, state))
}

fn cmd_state_show(args: StateArgs) -> Result<()> {
    let (config, state_cfg) = load_state_cfg(&args.pipeline)?;
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

fn cmd_state_rm(args: StateArgs) -> Result<()> {
    let (config, state_cfg) = load_state_cfg(&args.pipeline)?;
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
        let manifest = plugin_install::load_manifest(&spec)?;
        let kind_refs: Option<Vec<&str>> =
            kinds.as_ref().map(|v| v.iter().map(String::as_str).collect());
        let installed = plugin_install::install_from_manifest(
            &manifest,
            &plugin_dir,
            loadsmith_core::lifecycle::SUPPORTED_VERSIONS,
            kind_refs.as_deref(),
        )?;
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

fn init_tracing(level: &str, color: bool) {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(level))
        .with_writer(std::io::stderr)
        .with_ansi(color)
        .init();
}
