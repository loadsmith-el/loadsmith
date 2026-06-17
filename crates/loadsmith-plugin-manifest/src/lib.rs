//! The Loadsmith plugin manifest (`loadsmith-plugin.yaml`) — the stable contract
//! behind `loadsmith plugin install`.
//!
//! A manifest describes one installable **plugin package** (a "connector" when
//! it provides more than one role): its identity, the protocol range it targets,
//! the binaries it provides (each with a kind), and how to obtain them. The
//! `runtime` is deliberately extensible (`binary` now; `oci`/`command` later for
//! non-Rust plugins) — only `binary` is implemented today.
//!
//! ```yaml
//! apiVersion: loadsmith.dev/plugin/v1
//! name: postgres
//! version: 1.0.0
//! summary: PostgreSQL source and destination
//! protocol: "^1"
//! provides:
//!   - { kind: source,      bin: loadsmith-source-postgres }
//!   - { kind: destination, bin: loadsmith-destination-postgres }
//! runtime:
//!   type: binary
//!   artifacts:
//!     - { os: linux, arch: amd64, url: "...", sha256: "..." }
//!     - { os: linux, arch: arm64, url: "...", sha256: "..." }
//! ```

use semver::{Version, VersionReq};
use serde::Deserialize;

/// The only `apiVersion` understood today.
pub const API_VERSION: &str = "loadsmith.dev/plugin/v1";

/// Plugin kinds, matching the core's `loadsmith-{kind}-{type}` discovery
/// convention. `kind` in a manifest is validated against this set; `bin` is the
/// authoritative binary filename (so the manifest, not a derivation, decides
/// what gets installed).
pub const KNOWN_KINDS: &[&str] = &["source", "destination", "sink", "parser", "config-provider"];

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("failed to parse plugin manifest: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("unsupported apiVersion {got:?} (expected {expected:?})")]
    ApiVersion { got: String, expected: &'static str },
    #[error("invalid plugin version {0:?}: {1}")]
    Version(String, semver::Error),
    #[error("invalid protocol range {0:?}: {1}")]
    Protocol(String, semver::Error),
    #[error("manifest field {0} must not be empty")]
    Empty(&'static str),
    #[error("unknown plugin kind {kind:?} (expected one of {KNOWN_KINDS:?})")]
    UnknownKind { kind: String },
    #[error("invalid sha256 {0:?}: expected 64 hex characters")]
    Sha256(String),
    #[error("package {name:?} provides no {requested} plugin (it provides: {available})")]
    KindNotProvided {
        name: String,
        requested: String,
        available: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginManifest {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub summary: Option<String>,
    /// Semver range over the protocol version the plugin targets, e.g. `"^1"`.
    pub protocol: String,
    pub provides: Vec<Provide>,
    pub runtime: Runtime,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Provide {
    pub kind: String,
    /// The binary filename to place in the plugin dir (e.g. `loadsmith-source-postgres`).
    pub bin: String,
}

/// How to obtain/run the plugin. Tagged by `type`; only `binary` is implemented.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Runtime {
    /// Native per-platform binaries downloaded and placed on disk.
    Binary { artifacts: Vec<Artifact> },
}

#[derive(Debug, Clone, Deserialize)]
pub struct Artifact {
    /// OS in `std::env::consts::OS` terms (e.g. `linux`).
    pub os: String,
    /// Arch in Docker terms (`amd64`, `arm64`) — see [`current_platform`].
    pub arch: String,
    pub url: String,
    /// Lowercase hex sha256 of the downloaded artifact.
    pub sha256: String,
}

impl PluginManifest {
    /// Parse a manifest from YAML (JSON is a subset, so this accepts both) and
    /// validate it.
    pub fn parse(s: &str) -> Result<Self, ManifestError> {
        let manifest: PluginManifest = serde_yaml::from_str(s)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Structural validation beyond what serde enforces.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.api_version != API_VERSION {
            return Err(ManifestError::ApiVersion {
                got: self.api_version.clone(),
                expected: API_VERSION,
            });
        }
        if self.name.is_empty() {
            return Err(ManifestError::Empty("name"));
        }
        Version::parse(&self.version)
            .map_err(|e| ManifestError::Version(self.version.clone(), e))?;
        self.protocol_req()?;

        if self.provides.is_empty() {
            return Err(ManifestError::Empty("provides"));
        }
        for p in &self.provides {
            if !KNOWN_KINDS.contains(&p.kind.as_str()) {
                return Err(ManifestError::UnknownKind { kind: p.kind.clone() });
            }
            if p.bin.is_empty() {
                return Err(ManifestError::Empty("provides[].bin"));
            }
        }

        match &self.runtime {
            Runtime::Binary { artifacts } => {
                if artifacts.is_empty() {
                    return Err(ManifestError::Empty("runtime.artifacts"));
                }
                for a in artifacts {
                    if a.os.is_empty() || a.arch.is_empty() || a.url.is_empty() {
                        return Err(ManifestError::Empty("runtime.artifacts[] os/arch/url"));
                    }
                    if !is_sha256(&a.sha256) {
                        return Err(ManifestError::Sha256(a.sha256.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    /// The parsed protocol requirement (e.g. `"^1"` → `>=1, <2`).
    pub fn protocol_req(&self) -> Result<VersionReq, ManifestError> {
        VersionReq::parse(&self.protocol)
            .map_err(|e| ManifestError::Protocol(self.protocol.clone(), e))
    }

    /// True if the plugin's protocol range admits the given protocol version
    /// (the core's versions are plain `u32`, mapped to `N.0.0`).
    pub fn protocol_matches(&self, version: u32) -> bool {
        match self.protocol_req() {
            Ok(req) => req.matches(&Version::new(version as u64, 0, 0)),
            Err(_) => false,
        }
    }

    /// True if any of the core's `supported` protocol versions is admitted.
    pub fn protocol_compatible_with(&self, supported: &[u32]) -> bool {
        supported.iter().any(|v| self.protocol_matches(*v))
    }

    /// The artifact matching a host `(os, arch)` (Docker-style arch), if any.
    pub fn artifact_for(&self, os: &str, arch: &str) -> Option<&Artifact> {
        match &self.runtime {
            Runtime::Binary { artifacts } => {
                artifacts.iter().find(|a| a.os == os && a.arch == arch)
            }
        }
    }

    /// The binary names to install, optionally filtered to `kinds`.
    ///
    /// `None` ⇒ every binary the package provides. `Some(kinds)` ⇒ only the
    /// provides whose `kind` is in `kinds` — this is how `install <pkg>:<kind>`
    /// / `--kind` install a single plugin from a package's shared tarball. Errors
    /// when the filter matches nothing (the package provides no such kind).
    pub fn bins_for_kinds(&self, kinds: Option<&[&str]>) -> Result<Vec<&str>, ManifestError> {
        let bins: Vec<&str> = match kinds {
            None => self.provides.iter().map(|p| p.bin.as_str()).collect(),
            Some(kinds) => self
                .provides
                .iter()
                .filter(|p| kinds.contains(&p.kind.as_str()))
                .map(|p| p.bin.as_str())
                .collect(),
        };
        if bins.is_empty() {
            return Err(ManifestError::KindNotProvided {
                name: self.name.clone(),
                requested: kinds.map(|k| k.join("/")).unwrap_or_default(),
                available: self
                    .provides
                    .iter()
                    .map(|p| p.kind.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        Ok(bins)
    }
}

/// The host platform as `(os, arch)` in the manifest's terms: OS via
/// `std::env::consts::OS` (`linux`, …), arch normalized to Docker spelling
/// (`x86_64` → `amd64`, `aarch64` → `arm64`).
pub fn current_platform() -> (String, String) {
    let os = std::env::consts::OS.to_string();
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
    .to_string();
    (os, arch)
}

fn is_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK: &str = r#"
apiVersion: loadsmith.dev/plugin/v1
name: postgres
version: 1.0.0
summary: PostgreSQL source and destination
protocol: "^1"
provides:
  - { kind: source,      bin: loadsmith-source-postgres }
  - { kind: destination, bin: loadsmith-destination-postgres }
runtime:
  type: binary
  artifacts:
    - { os: linux, arch: amd64, url: "https://example/pg-amd64.tar.gz", sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
    - { os: linux, arch: arm64, url: "https://example/pg-arm64.tar.gz", sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }
"#;

    #[test]
    fn parses_and_validates() {
        let m = PluginManifest::parse(OK).unwrap();
        assert_eq!(m.name, "postgres");
        assert_eq!(m.provides.len(), 2);
        assert_eq!(m.provides[0].bin, "loadsmith-source-postgres");
    }

    #[test]
    fn selects_artifact_by_platform() {
        let m = PluginManifest::parse(OK).unwrap();
        assert_eq!(m.artifact_for("linux", "arm64").unwrap().arch, "arm64");
        assert!(m.artifact_for("linux", "riscv64").is_none());
        assert!(m.artifact_for("windows", "amd64").is_none());
    }

    #[test]
    fn protocol_compat() {
        let m = PluginManifest::parse(OK).unwrap();
        assert!(m.protocol_matches(1));
        assert!(!m.protocol_matches(2)); // ^1 excludes 2
        assert!(m.protocol_compatible_with(&[1]));
        assert!(!m.protocol_compatible_with(&[2, 3]));
    }

    #[test]
    fn rejects_wrong_api_version() {
        let s = OK.replace("loadsmith.dev/plugin/v1", "loadsmith.dev/plugin/v2");
        assert!(matches!(
            PluginManifest::parse(&s),
            Err(ManifestError::ApiVersion { .. })
        ));
    }

    #[test]
    fn rejects_unknown_kind() {
        let s = OK.replace("kind: source", "kind: sourcerer");
        assert!(matches!(
            PluginManifest::parse(&s),
            Err(ManifestError::UnknownKind { .. })
        ));
    }

    #[test]
    fn bins_for_kinds_filters() {
        let m = PluginManifest::parse(OK).unwrap();
        // None ⇒ every provided binary.
        assert_eq!(
            m.bins_for_kinds(None).unwrap(),
            vec!["loadsmith-source-postgres", "loadsmith-destination-postgres"]
        );
        // A single kind ⇒ just that binary.
        assert_eq!(
            m.bins_for_kinds(Some(&["source"])).unwrap(),
            vec!["loadsmith-source-postgres"]
        );
        // A kind the package doesn't provide ⇒ a clear error.
        assert!(matches!(
            m.bins_for_kinds(Some(&["sink"])),
            Err(ManifestError::KindNotProvided { .. })
        ));
    }

    #[test]
    fn rejects_bad_sha256() {
        let s = OK.replace(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "deadbeef",
        );
        assert!(matches!(
            PluginManifest::parse(&s),
            Err(ManifestError::Sha256(_))
        ));
    }
}
