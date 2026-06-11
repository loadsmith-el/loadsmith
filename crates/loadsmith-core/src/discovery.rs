use std::path::{Path, PathBuf};

use crate::error::CoreError;

/// Default plugin directory.
pub fn default_plugin_dir() -> PathBuf {
    dirs_home().join(".loadsmith").join("plugins")
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

/// Resolves the binary path for a plugin by kind and type name.
/// E.g. kind="source", plugin_type="postgres" → `<dir>/loadsmith-source-postgres`
pub fn find_plugin_binary(
    kind: &str,
    plugin_type: &str,
    plugin_dir: &Path,
) -> Result<PathBuf, CoreError> {
    let name = format!("loadsmith-{kind}-{plugin_type}");
    let path = plugin_dir.join(&name);
    if path.exists() {
        return Ok(path);
    }
    // Also check PATH as fallback for development
    if let Ok(found) = which_on_path(&name) {
        return Ok(found);
    }
    Err(CoreError::PluginNotFound(format!(
        "binary '{}' not found in {} or PATH",
        name,
        plugin_dir.display()
    )))
}

fn which_on_path(name: &str) -> Result<PathBuf, ()> {
    let path_var = std::env::var("PATH").map_err(|_| ())?;
    for dir in path_var.split(':') {
        let candidate = PathBuf::from(dir).join(name);
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(())
}

/// Lists all plugin binaries in the plugin directory.
pub fn list_plugins(plugin_dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(plugin_dir) else {
        return vec![];
    };
    entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("loadsmith-") {
                Some(name)
            } else {
                None
            }
        })
        .collect()
}

/// Copies a binary into the plugin directory.
pub fn install_plugin(source: &Path, plugin_dir: &Path) -> Result<PathBuf, CoreError> {
    std::fs::create_dir_all(plugin_dir)?;
    let name = source
        .file_name()
        .ok_or_else(|| CoreError::PluginNotFound("source has no filename".into()))?;
    let dest = plugin_dir.join(name);
    std::fs::copy(source, &dest)?;
    // Make executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&dest)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dest, perms)?;
    }
    Ok(dest)
}
