//! The one way the engine turns a pipeline file into a [`PipelineConfig`].
//!
//! Every command reads its pipeline here, at the depth it needs
//! ([`Mode`]): `validate` statically, `state show|rm` with just the blocks
//! that locate the state, `run` (also `--dry-run` and
//! `--print-resolved-config`) and `validate --resolve` fully — which starts
//! the provider instances the file's `secret()` calls reference.

use std::fmt;
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

/// Where a pipeline file is read from: a local path, or an S3 object
/// (`s3://bucket/key`, read with the AWS credential chain; region and
/// endpoint from `AWS_REGION` and `AWS_ENDPOINT_URL_S3`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineSource {
    File(PathBuf),
    S3(loadsmith_aws::s3::S3Uri),
}

impl std::str::FromStr for PipelineSource {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match loadsmith_aws::s3::S3Uri::parse(text) {
            Some(uri) => uri.map(Self::S3).map_err(|e| e.to_string()),
            None => Ok(Self::File(PathBuf::from(text))),
        }
    }
}

impl fmt::Display for PipelineSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => write!(f, "{}", path.display()),
            Self::S3(uri) => write!(f, "{uri}"),
        }
    }
}

impl From<&Path> for PipelineSource {
    fn from(path: &Path) -> Self {
        Self::File(path.to_path_buf())
    }
}

impl From<&PathBuf> for PipelineSource {
    fn from(path: &PathBuf) -> Self {
        Self::File(path.clone())
    }
}

impl From<PathBuf> for PipelineSource {
    fn from(path: PathBuf) -> Self {
        Self::File(path)
    }
}

/// The text of the pipeline file at `source`.
async fn read_text(source: &PipelineSource) -> Result<String, CoreError> {
    let read_error = |error: std::io::Error| ConfigError::Read { path: source.to_string(), error };
    match source {
        PipelineSource::File(path) => Ok(std::fs::read_to_string(path).map_err(read_error)?),
        PipelineSource::S3(uri) => Ok(fetch_s3(uri, Default::default()).await.map_err(read_error)?),
    }
}

/// The text of the S3 object at `uri`; `base` sets everything but the bucket.
async fn fetch_s3(uri: &loadsmith_aws::s3::S3Uri, base: loadsmith_aws::s3::Settings) -> std::io::Result<String> {
    use std::io::{Error, ErrorKind};
    loadsmith_tls::install_provider();
    let settings = loadsmith_aws::s3::Settings { bucket: uri.bucket.clone(), ..base };
    let fetched = async { loadsmith_aws::s3::S3Client::new(settings).await?.get(&uri.key).await }
        .await
        .map_err(|e| Error::other(format!("{e:#}")))?;
    let object = fetched.ok_or_else(|| Error::new(ErrorKind::NotFound, "no such object"))?;
    String::from_utf8(object.body).map_err(|_| Error::new(ErrorKind::InvalidData, "not UTF-8"))
}

/// Reads and statically checks a pipeline file from `source` — a path or an
/// S3 object ([`PipelineSource`]); otherwise as [`read_document`].
pub async fn read_source(source: &PipelineSource, unknown_keys: UnknownKeys) -> Result<Document, CoreError> {
    parse_document(&read_text(source).await?, unknown_keys)
}

fn parse_document(content: &str, unknown_keys: UnknownKeys) -> Result<Document, CoreError> {
    let doc = Document::parse_with(content, unknown_keys)?;
    for key in doc.ignored_keys() {
        tracing::warn!(key = %key, "ignoring unknown key `{key}` in the pipeline file");
    }
    Ok(doc)
}

/// Reads and statically checks a pipeline file ([`Mode::Static`]). Unknown
/// keys in the file's own blocks fail it under [`UnknownKeys::Error`]; under
/// [`UnknownKeys::Warn`] each is logged as a warning and ignored.
pub fn read_document(path: &Path, unknown_keys: UnknownKeys) -> Result<Document, CoreError> {
    let content = std::fs::read_to_string(path)
        .map_err(|error| ConfigError::Read { path: path.display().to_string(), error })?;
    parse_document(&content, unknown_keys)
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

/// Reads the pipeline at `source` (a path or an S3 object) and resolves it
/// to `mode`. Returns the config and the mask
/// holding every value resolved on the way (the same handle as `opts.mask`).
pub async fn load_pipeline(
    source: impl Into<PipelineSource>,
    mode: Mode,
    opts: &LoadOpts,
) -> Result<(PipelineConfig, MaskList), CoreError> {
    let doc = read_source(&source.into(), opts.unknown_keys).await?;
    let config = resolve_document(&doc, mode, opts).await?;
    Ok((config, opts.mask.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    #[test]
    fn sources_are_paths_or_s3_objects() {
        assert_eq!("p/x.yaml".parse::<PipelineSource>().unwrap(), PipelineSource::File("p/x.yaml".into()));
        let s3 = "s3://lake/pipelines/x.yaml".parse::<PipelineSource>().unwrap();
        assert_eq!(s3.to_string(), "s3://lake/pipelines/x.yaml");
        assert!("s3://lake".parse::<PipelineSource>().unwrap_err().contains("s3://<bucket>/<key>"));
    }

    #[tokio::test]
    async fn a_pipeline_is_read_from_s3() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/lake/pipelines/x.yaml").header_exists("authorization");
                then.status(200).body("pipeline: { name: x }\n");
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/lake/missing.yaml");
                then.status(404);
            })
            .await;
        let base = || loadsmith_aws::s3::Settings {
            endpoint: Some(server.base_url()),
            region: Some("us-east-1".into()),
            access_key_id: Some("AKID".into()),
            secret_access_key: Some("s".into()),
            max_attempts: Some(1),
            ..Default::default()
        };
        let uri = |key: &str| loadsmith_aws::s3::S3Uri { bucket: "lake".into(), key: key.into() };
        assert_eq!(fetch_s3(&uri("pipelines/x.yaml"), base()).await.unwrap(), "pipeline: { name: x }\n");
        let e = fetch_s3(&uri("missing.yaml"), base()).await.unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
    }
}
