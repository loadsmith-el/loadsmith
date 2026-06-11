use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use loadsmith_config::parse_pipeline_yaml;
use loadsmith_core::{discovery, run_pipeline, RunOpts};

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
    /// Install a plugin binary into the plugin directory
    Install(PluginInstallArgs),
}

#[derive(Args)]
struct PluginListArgs {
    #[arg(long, help = "Plugin directory to inspect")]
    plugin_dir: Option<PathBuf>,
}

#[derive(Args)]
struct PluginInstallArgs {
    /// Path to the plugin binary to install
    binary: PathBuf,

    #[arg(long, help = "Plugin directory to install into")]
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
    let dest = discovery::install_plugin(&args.binary, &plugin_dir)?;
    println!("Installed {} → {}", args.binary.display(), dest.display());
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
