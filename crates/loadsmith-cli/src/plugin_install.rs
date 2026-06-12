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
pub fn install_from_manifest(
    manifest: &PluginManifest,
    plugin_dir: &Path,
    supported_protocols: &[u32],
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

    let wanted: Vec<&str> = manifest.provides.iter().map(|p| p.bin.as_str()).collect();
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

/// Remove every installed binary belonging to a plugin type, i.e. files named
/// `loadsmith-<kind>-<name>` in the plugin dir. Returns the removed paths.
pub fn uninstall(name: &str, plugin_dir: &Path) -> Result<Vec<PathBuf>> {
    let suffix = format!("-{name}");
    let mut removed = Vec::new();
    let entries = match std::fs::read_dir(plugin_dir) {
        Ok(e) => e,
        Err(_) => return Ok(removed),
    };
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let fname = fname.to_string_lossy();
        if fname.starts_with("loadsmith-") && fname.ends_with(&suffix) {
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
        let removed = uninstall("postgres", &dir).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(dir.join("loadsmith-destination-jsonl").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn tempdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("loadsmith-pi-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
