//! The one way the engine turns a pipeline file into a [`PipelineConfig`].
//!
//! Every command reads its pipeline here, at the depth it needs
//! ([`Mode`]): `validate` statically, `state show|rm` with just the blocks
//! that locate the state, `run` (also `--dry-run` and
//! `--print-resolved-config`) and `validate --resolve` fully — which starts
//! the provider instances the file's `secret()` calls reference.

use std::path::{Path, PathBuf};

use loadsmith_config::{ConfigError, Document, MaskList, PipelineConfig, SecretValues};

use crate::error::CoreError;
use crate::providers::{jobs_for, resolve_providers};
use crate::shutdown::Shutdown;

/// How far to resolve a pipeline file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Structure, template syntax and `secret()` references only. Evaluates
    /// nothing: templates stay verbatim in the returned config.
    Static,
    /// [`Static`](Mode::Static), then `env()`/`file()` in `pipeline:` and
    /// `state:` — the blocks that never use `secret()`. No provider runs;
    /// other blocks stay verbatim. For the state commands.
    Builtins,
    /// Everything: `env()`/`file()` everywhere, and every `secret()` through
    /// its provider instance.
    Full,
}

/// What resolution needs besides the file.
#[derive(Debug, Clone)]
pub struct LoadOpts {
    /// Where provider binaries are discovered (as for every plugin).
    pub plugin_dir: PathBuf,
    /// Stops provider resolution (SIGINT/SIGTERM).
    pub shutdown: Shutdown,
    /// Receives every resolved value. A shared handle: pass the one the rest
    /// of the run masks with.
    pub mask: MaskList,
}

impl LoadOpts {
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        Self { plugin_dir: plugin_dir.into(), shutdown: Shutdown::never(), mask: MaskList::new() }
    }
}

/// Reads and statically checks a pipeline file ([`Mode::Static`]).
pub fn read_document(path: &Path) -> Result<Document, CoreError> {
    let content = std::fs::read_to_string(path)
        .map_err(|source| ConfigError::Read { path: path.display().to_string(), source })?;
    Ok(Document::parse(&content)?)
}

/// Resolves an already read document to `mode`.
pub async fn resolve_document(doc: &Document, mode: Mode, opts: &LoadOpts) -> Result<PipelineConfig, CoreError> {
    match mode {
        Mode::Static => Ok(doc.config().clone()),
        Mode::Builtins => Ok(doc.resolve_subtrees(&["pipeline", "state"], &opts.mask)?),
        Mode::Full => {
            // `pipeline:` (the timeouts) and `providers:` resolve without
            // providers; then the referenced instances run.
            let partial = doc.resolve_subtrees(&["pipeline", "providers"], &opts.mask)?;
            for unused in doc.unused_instances() {
                tracing::warn!(
                    instance = unused,
                    "provider instance '{unused}' is declared but no secret() uses it; not starting it"
                );
            }
            let jobs = jobs_for(doc.secrets(), &partial.providers);
            let values = if jobs.is_empty() {
                SecretValues::new()
            } else {
                resolve_providers(jobs, &opts.plugin_dir, &partial.pipeline.timeouts, &opts.mask, &opts.shutdown)
                    .await?
            };
            Ok(doc.resolve(&values, &opts.mask)?)
        }
    }
}

/// Reads `path` and resolves it to `mode`. Returns the config and the mask
/// holding every value resolved on the way (the same handle as `opts.mask`).
pub async fn load_pipeline(path: &Path, mode: Mode, opts: &LoadOpts) -> Result<(PipelineConfig, MaskList), CoreError> {
    let doc = read_document(path)?;
    let config = resolve_document(&doc, mode, opts).await?;
    Ok((config, opts.mask.clone()))
}
