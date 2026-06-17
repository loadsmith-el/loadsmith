//! Manifest-based plugin install: resolve a `loadsmith-plugin.yaml`, verify the
//! artifact, and place its binaries in the plugin dir so the core's
//! name-based discovery finds them. The runtime protocol handshake still does
//! the authoritative version check; this just fails *early* (before download)
//! on an incompatible manifest.

use std::io::Read;
use std::path::{Path, PathBuf};

use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use loadsmith_plugin_manifest::{current_platform, PluginManifest};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::download;

/// The canonical plugin index `loadsmith install <name>` reads by default.
pub const DEFAULT_INDEX_URL: &str =
    "https://raw.githubusercontent.com/loadsmith-el/loadsmith-canonical-plugins/main/index.json";

#[derive(Debug, Deserialize)]
struct Index {
    #[serde(default)]
    plugins: HashMap<String, IndexEntry>,
}

#[derive(Debug, Deserialize)]
struct IndexEntry {
    latest: String,
    #[serde(default)]
    versions: HashMap<String, String>,
}

/// All plugin names listed in the index (sorted) — for `install --all`.
pub fn index_plugin_names(index_url: &str) -> Result<Vec<String>> {
    let bytes = download::fetch(index_url).with_context(|| format!("fetching index {index_url}"))?;
    let index: Index = serde_json::from_slice(&bytes).context("parsing plugin index")?;
    let mut names: Vec<String> = index.plugins.into_keys().collect();
    names.sort();
    Ok(names)
}

/// Parse an install spec `<name>[:<kind>][@<version>]` into its parts.
///
/// The `@version` suffix (if any) is split off first, then a `:kind` suffix, so
/// `mysql:source@0.1.0` resolves to (`mysql`, `source`, `0.1.0`). `kind`, when
/// present, is validated against [`KNOWN_KINDS`](loadsmith_plugin_manifest::KNOWN_KINDS).
pub fn parse_install_spec(spec: &str) -> Result<(String, Option<String>, Option<String>)> {
    let (left, version) = match spec.rsplit_once('@') {
        Some((l, v)) => (l, Some(v.to_string())),
        None => (spec, None),
    };
    let (name, kind) = match left.split_once(':') {
        Some((n, k)) => (n, Some(k.to_string())),
        None => (left, None),
    };
    if name.is_empty() {
        bail!("invalid plugin spec {spec:?}: empty package name");
    }
    if let Some(k) = &kind {
        let known = loadsmith_plugin_manifest::KNOWN_KINDS;
        if !known.contains(&k.as_str()) {
            bail!("unknown plugin kind {k:?} (expected one of {known:?})");
        }
    }
    Ok((name.to_string(), kind, version))
}

/// Validate a plugin kind string (e.g. a `--kind` flag value) against the known
/// kinds. The `:kind` spec suffix is already validated by [`parse_install_spec`].
pub fn validate_kind(kind: &str) -> Result<()> {
    let known = loadsmith_plugin_manifest::KNOWN_KINDS;
    if !known.contains(&kind) {
        bail!("unknown plugin kind {kind:?} (expected one of {known:?})");
    }
    Ok(())
}

/// Resolve a `name[@version]` spec against the index, returning the published
/// manifest URL. `index_url` defaults to [`DEFAULT_INDEX_URL`].
pub fn resolve_from_index(spec: &str, index_url: &str) -> Result<String> {
    let (name, version) = match spec.split_once('@') {
        Some((n, v)) => (n, Some(v)),
        None => (spec, None),
    };

    let bytes = download::fetch(index_url).with_context(|| format!("fetching index {index_url}"))?;
    let index: Index = serde_json::from_slice(&bytes).context("parsing plugin index")?;

    let entry = index
        .plugins
        .get(name)
        .ok_or_else(|| anyhow!("plugin '{name}' not found in the index ({index_url})"))?;

    let version = version.unwrap_or(&entry.latest);
    entry.versions.get(version).cloned().ok_or_else(|| {
        let mut available: Vec<&String> = entry.versions.keys().collect();
        available.sort();
        anyhow!("plugin '{name}' has no version {version:?} (available: {available:?})")
    })
}

/// Load a manifest from a local path or a `file`/`http`/`https` URL.
pub fn load_manifest(spec: &str) -> Result<PluginManifest> {
    let text = if spec.contains("://") {
        let bytes = download::fetch(spec).with_context(|| format!("fetching manifest {spec}"))?;
        String::from_utf8(bytes).context("manifest is not valid UTF-8")?
    } else {
        std::fs::read_to_string(spec).with_context(|| format!("reading manifest {spec}"))?
    };
    PluginManifest::parse(&text).map_err(|e| anyhow!("invalid manifest {spec}: {e}"))
}

/// Resolve the host artifact, download + verify it, and install the promised
/// binaries into `plugin_dir`. Returns the installed paths.
///
/// `wanted_kinds` filters which of the package's binaries to place: `None`
/// installs every binary the manifest provides; `Some(kinds)` installs only the
/// matching ones (how `install <pkg>:<kind>` / `--kind` pull a single plugin
/// from the package's shared tarball).
pub fn install_from_manifest(
    manifest: &PluginManifest,
    plugin_dir: &Path,
    supported_protocols: &[u32],
    wanted_kinds: Option<&[&str]>,
) -> Result<Vec<PathBuf>> {
    if !manifest.protocol_compatible_with(supported_protocols) {
        bail!(
            "plugin '{}' targets protocol {:?}, but this loadsmith supports {:?} \
             — no compatible protocol version",
            manifest.name,
            manifest.protocol,
            supported_protocols
        );
    }

    let (os, arch) = current_platform();
    let artifact = manifest.artifact_for(&os, &arch).ok_or_else(|| {
        anyhow!(
            "plugin '{}' has no artifact for this platform ({os}/{arch})",
            manifest.name
        )
    })?;

    let bytes = download::fetch(&artifact.url)
        .with_context(|| format!("downloading {}", artifact.url))?;

    let got = hex_sha256(&bytes);
    if !got.eq_ignore_ascii_case(&artifact.sha256) {
        bail!(
            "checksum mismatch for {}\n  expected {}\n  got      {got}",
            artifact.url,
            artifact.sha256
        );
    }

    let wanted = manifest.bins_for_kinds(wanted_kinds)?;
    let installed = extract_binaries(&bytes, &wanted, plugin_dir)?;

    for bin in &wanted {
        let found = installed
            .iter()
            .any(|p| p.file_name().and_then(|f| f.to_str()) == Some(*bin));
        if !found {
            bail!("artifact for '{}' did not contain expected binary {bin:?}", manifest.name);
        }
    }
    Ok(installed)
}

/// Remove installed binaries belonging to a package. With `kind = None`, removes
/// every `loadsmith-<kind>-<name>` for the package; with `kind = Some(k)`, removes
/// only `loadsmith-<k>-<name>`. Returns the removed paths.
pub fn uninstall(name: &str, kind: Option<&str>, plugin_dir: &Path) -> Result<Vec<PathBuf>> {
    let suffix = format!("-{name}");
    let exact = kind.map(|k| format!("loadsmith-{k}-{name}"));
    let mut removed = Vec::new();
    let entries = match std::fs::read_dir(plugin_dir) {
        Ok(e) => e,
        Err(_) => return Ok(removed),
    };
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let fname = fname.to_string_lossy();
        let matches = match &exact {
            Some(e) => fname == e.as_str(),
            None => fname.starts_with("loadsmith-") && fname.ends_with(&suffix),
        };
        if matches {
            let path = entry.path();
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
            removed.push(path);
        }
    }
    Ok(removed)
}

/// Extract the `wanted` binaries (matched by file name) from a `.tar.gz` into
/// `plugin_dir`, made executable.
fn extract_binaries(targz: &[u8], wanted: &[&str], plugin_dir: &Path) -> Result<Vec<PathBuf>> {
    std::fs::create_dir_all(plugin_dir)?;
    let gz = flate2::read::GzDecoder::new(targz);
    let mut archive = tar::Archive::new(gz);
    let mut installed = Vec::new();
    for entry in archive.entries().context("reading tar archive")? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let Some(fname) = path.file_name().and_then(|f| f.to_str()) else {
            continue;
        };
        if !wanted.contains(&fname) {
            continue;
        }
        let dest = plugin_dir.join(fname);
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        std::fs::write(&dest, &buf).with_context(|| format!("writing {}", dest.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;
        }
        installed.push(dest);
    }
    Ok(installed)
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Build a gzipped tar holding `(name, contents)` files at the top level.
    fn make_targz(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut tar_buf = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_buf);
            for (name, data) in files {
                let mut header = tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                builder.append_data(&mut header, name, *data).unwrap();
            }
            builder.finish().unwrap();
        }
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&tar_buf).unwrap();
        gz.finish().unwrap()
    }

    #[test]
    fn extracts_only_wanted_binaries() {
        let targz = make_targz(&[
            ("loadsmith-source-postgres", b"BINSRC"),
            ("loadsmith-destination-postgres", b"BINDST"),
            ("README.md", b"ignore me"),
        ]);
        let dir = tempdir();
        let installed =
            extract_binaries(&targz, &["loadsmith-source-postgres", "loadsmith-destination-postgres"], &dir)
                .unwrap();
        assert_eq!(installed.len(), 2);
        assert!(dir.join("loadsmith-source-postgres").exists());
        assert!(!dir.join("README.md").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn uninstall_removes_matching() {
        let dir = tempdir();
        for f in ["loadsmith-source-postgres", "loadsmith-destination-postgres", "loadsmith-destination-jsonl"] {
            std::fs::write(dir.join(f), b"x").unwrap();
        }
        let removed = uninstall("postgres", None, &dir).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(dir.join("loadsmith-destination-jsonl").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn uninstall_by_kind_removes_only_that_binary() {
        let dir = tempdir();
        for f in ["loadsmith-source-postgres", "loadsmith-destination-postgres"] {
            std::fs::write(dir.join(f), b"x").unwrap();
        }
        let removed = uninstall("postgres", Some("source"), &dir).unwrap();
        assert_eq!(removed.len(), 1);
        assert!(!dir.join("loadsmith-source-postgres").exists());
        assert!(dir.join("loadsmith-destination-postgres").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parses_install_specs() {
        assert_eq!(
            parse_install_spec("mysql").unwrap(),
            ("mysql".into(), None, None)
        );
        assert_eq!(
            parse_install_spec("mysql:source").unwrap(),
            ("mysql".into(), Some("source".into()), None)
        );
        assert_eq!(
            parse_install_spec("mysql@0.1.0").unwrap(),
            ("mysql".into(), None, Some("0.1.0".into()))
        );
        assert_eq!(
            parse_install_spec("mysql:source@0.1.0").unwrap(),
            ("mysql".into(), Some("source".into()), Some("0.1.0".into()))
        );
        // local-copy / config-provider are valid kinds with hyphens in the name.
        assert_eq!(
            parse_install_spec("local-copy:sink").unwrap(),
            ("local-copy".into(), Some("sink".into()), None)
        );
        assert!(parse_install_spec("mysql:bogus").is_err());
        assert!(parse_install_spec(":source").is_err());
    }

    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        // Unique per call so parallel tests don't share (and clobber) a dir.
        let p = std::env::temp_dir().join(format!(
            "loadsmith-pi-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
