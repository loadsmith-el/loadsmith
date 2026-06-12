use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use loadsmith_config::parse_pipeline_yaml;
use loadsmith_core::{discovery, run_pipeline, RunOpts};

mod download;
mod plugin_install;

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
    /// Install a plugin from a manifest, a local binary, or (soon) the index
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
    /// Plugin name to resolve from the official index (not available yet —
    /// use --manifest or --binary)
    name: Option<String>,

    /// Install from a plugin manifest (loadsmith-plugin.yaml) — a local path
    /// or a file:// / http:// / https:// URL
    #[arg(short = 'f', long, conflicts_with_all = ["binary", "name"])]
    manifest: Option<String>,

    /// Install a single local plugin binary directly, no manifest
    #[arg(long, conflicts_with_all = ["manifest", "name"])]
    binary: Option<PathBuf>,

    /// Install every plugin in the index (the whole canonical set)
    #[arg(long, conflicts_with_all = ["manifest", "name", "binary"])]
    all: bool,

    #[arg(long, help = "Override the plugin index URL (for `install <name>` / --all)")]
    index: Option<String>,

    #[arg(long, help = "Plugin directory to install into")]
    plugin_dir: Option<PathBuf>,
}

#[derive(Args)]
struct PluginUninstallArgs {
    /// Plugin type name (e.g. `postgres` removes loadsmith-*-postgres)
    name: String,

    #[arg(long, help = "Plugin directory to remove from")]
    plugin_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let color = !cli.no_color && std::env::var_os("NO_COLOR").is_none();
    init_tracing(&cli.log_level, color);

    match run(cli, color).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli, color: bool) -> Result<()> {
    match cli.command {
        Commands::Run(args) => cmd_run(args, color).await,
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

async fn cmd_run(args: RunArgs, color: bool) -> Result<()> {
    let content = std::fs::read_to_string(&args.pipeline)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", args.pipeline.display()))?;

    let (config, mask) = parse_pipeline_yaml(&content)?;

    if args.print_resolved_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", mask.apply(&yaml));
        return Ok(());
    }

    let plugin_dir = args.plugin_dir.unwrap_or_else(discovery::default_plugin_dir);
    let opts = RunOpts {
        plugin_dir,
        dry_run: args.dry_run,
        print_resolved_config: args.print_resolved_config,
        color,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };

    run_pipeline(config, opts).await?;
    Ok(())
}

fn cmd_validate(args: ValidateArgs) -> Result<()> {
    let content = std::fs::read_to_string(&args.pipeline)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", args.pipeline.display()))?;

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
    let content = std::fs::read_to_string(pipeline)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", pipeline.display()))?;
    let (config, _mask) = parse_pipeline_yaml(&content)?;
    let state = config
        .state
        .clone()
        .ok_or_else(|| anyhow::anyhow!("pipeline '{}' has no state: block", config.pipeline.name))?;
    Ok((config, state))
}

fn cmd_state_show(args: StateArgs) -> Result<()> {
    let (config, state_cfg) = load_state_cfg(&args.pipeline)?;
    let backend = loadsmith_core::state::open_backend(&state_cfg)?;
    match backend.load(&config.pipeline.name)? {
        None => println!("No state recorded for pipeline '{}'.", config.pipeline.name),
        Some(doc) => {
            println!("Pipeline:     {}", doc.pipeline);
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

    // Resolve the manifest spec(s) to install: every plugin (--all), one named
    // plugin from the index, or a manifest path/URL.
    let specs: Vec<String> = if args.all {
        plugin_install::index_plugin_names(index)?
            .iter()
            .map(|name| plugin_install::resolve_from_index(name, index))
            .collect::<Result<_>>()?
    } else if let Some(spec) = args.manifest {
        vec![spec]
    } else if let Some(name) = args.name {
        vec![plugin_install::resolve_from_index(&name, index)?]
    } else {
        anyhow::bail!("nothing to install: pass a plugin name, --all, --manifest, or --binary")
    };

    for spec in specs {
        let manifest = plugin_install::load_manifest(&spec)?;
        let installed = plugin_install::install_from_manifest(
            &manifest,
            &plugin_dir,
            loadsmith_core::lifecycle::SUPPORTED_VERSIONS,
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
    let removed = plugin_install::uninstall(&args.name, &plugin_dir)?;
    if removed.is_empty() {
        println!("No installed binaries found for plugin '{}'.", args.name);
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
