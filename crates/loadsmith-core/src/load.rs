//! The one way the engine turns a pipeline file into a [`PipelineConfig`].
//!
//! Every command reads its pipeline here, at the depth it needs
//! ([`Mode`]): `validate` statically, `state show|rm` with just the blocks
//! that locate the state, `run` (also `--dry-run` and
//! `--print-resolved-config`) and `validate --resolve` fully — which starts
//! the provider instances the file's `secret()` calls reference.

use std::path::{Path, PathBuf};

use loadsmith_config::{ConfigError, Document, MaskList, PipelineConfig, SecretValues, UnknownKeys};

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
    /// Unknown keys in the file's own blocks, and in each provider
    /// instance's `config:` (sent in its `configure`): an error, or a warning
    /// per key (`--ignore-unknown-keys`).
    pub unknown_keys: UnknownKeys,
}

impl LoadOpts {
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugin_dir: plugin_dir.into(),
            shutdown: Shutdown::never(),
            mask: MaskList::new(),
            unknown_keys: UnknownKeys::Error,
        }
    }
}

/// Reads and statically checks a pipeline file ([`Mode::Static`]). Unknown
/// keys in the file's own blocks fail it under [`UnknownKeys::Error`]; under
/// [`UnknownKeys::Warn`] each is logged as a warning and ignored.
pub fn read_document(path: &Path, unknown_keys: UnknownKeys) -> Result<Document, CoreError> {
    let content = std::fs::read_to_string(path)
        .map_err(|error| ConfigError::Read { path: path.display().to_string(), error })?;
    let doc = Document::parse_with(&content, unknown_keys)?;
    for key in doc.ignored_keys() {
        tracing::warn!(key = %key, "ignoring unknown key `{key}` in the pipeline file");
    }
    Ok(doc)
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
            // An unset variable anywhere fails before a provider starts.
            doc.check_builtins(&opts.mask)?;
            for unused in doc.unused_instances() {
                tracing::warn!(
                    instance = unused,
                    "provider instance '{unused}' is declared but no secret() uses it; not starting it"
                );
            }
            let jobs = jobs_for(doc.secrets(), &partial.providers, opts.unknown_keys);
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
    let doc = read_document(path, opts.unknown_keys)?;
    let config = resolve_document(&doc, mode, opts).await?;
    Ok((config, opts.mask.clone()))
}
