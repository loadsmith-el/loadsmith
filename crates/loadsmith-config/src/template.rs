/// Internal expression parser for `{{ expr }}` template syntax.
///
/// Supported in v0.1.0:
///   {{ env('VAR') }}          — reads an environment variable
///   {{ file('/path').field }} — reads a field from a local file (stub)
///
/// Secrets returned by `env()` are added to the MaskList.
use crate::{error::ConfigError, mask::MaskList};

pub struct ResolveResult {
    pub value: String,
}

/// Resolves all `{{ expr }}` occurrences in `input`.
/// Appends resolved secret values to `mask`.
pub fn resolve_string(input: &str, mask: &mut MaskList) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(input.len());
    let mut pos = 0;
    let bytes = input.as_bytes();

    while pos < bytes.len() {
        // Find next `{{`
        if pos + 1 < bytes.len() && bytes[pos] == b'{' && bytes[pos + 1] == b'{' {
            let expr_start = pos + 2;
            // Find closing `}}`
            let close = find_close(bytes, expr_start).ok_or_else(|| {
                ConfigError::Template(format!(
                    "unclosed '{{{{' in template: {}",
                    &input[pos..]
                ))
            })?;
            let expr = input[expr_start..close].trim();
            let resolved = eval_expr(expr, mask)?;
            out.push_str(&resolved);
            pos = close + 2; // skip past `}}`
        } else {
            out.push(bytes[pos] as char);
            pos += 1;
        }
    }
    Ok(out)
}

fn find_close(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'}' && bytes[i + 1] == b'}' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Evaluates a single template expression.
fn eval_expr(expr: &str, mask: &mut MaskList) -> Result<String, ConfigError> {
    // env('VAR')
    if let Some(inner) = strip_call(expr, "env") {
        let var_name = unquote(inner)?;
        let val = std::env::var(&var_name).map_err(|_| {
            ConfigError::Template(format!("environment variable '{var_name}' not set"))
        })?;
        mask.add(val.clone());
        return Ok(val);
    }

    // file('/path') — for v0.1.0 we only support file content as raw string.
    // Dot-field access (file('/path').field) is a future extension.
    if let Some(inner) = strip_call(expr, "file") {
        let path = unquote(inner)?;
        let content = std::fs::read_to_string(&path)
            .map_err(|e| ConfigError::Template(format!("cannot read file '{path}': {e}")))?;
        // File content is not automatically treated as a secret.
        return Ok(content.trim_end().to_string());
    }

    // aws.sm(...) — stub, not available in v0.1.0
    if expr.starts_with("aws.sm") {
        return Err(ConfigError::Template(
            "aws.sm provider not available in v0.1.0; use env() or file()".into(),
        ));
    }

    Err(ConfigError::Template(format!("unknown template expression: '{expr}'")))
}

/// Strips a function-call prefix and returns the argument part, or None.
/// E.g. `strip_call("env('VAR')", "env")` → Some("'VAR'")
fn strip_call<'a>(expr: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}(");
    let s = expr.trim();
    if s.starts_with(prefix.as_str()) && s.ends_with(')') {
        Some(&s[prefix.len()..s.len() - 1])
    } else {
        None
    }
}

/// Removes surrounding single or double quotes from a string literal.
fn unquote(s: &str) -> Result<String, ConfigError> {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\''))
        || (s.starts_with('"') && s.ends_with('"'))
    {
        Ok(s[1..s.len() - 1].to_string())
    } else {
        Err(ConfigError::Template(format!("expected a quoted string, got: {s}")))
    }
}

/// Recursively resolves templates in a serde_yaml::Value tree.
pub fn resolve_value(val: serde_yaml::Value, mask: &mut MaskList) -> Result<serde_yaml::Value, ConfigError> {
    match val {
        serde_yaml::Value::String(s) => {
            let resolved = resolve_string(&s, mask)?;
            Ok(serde_yaml::Value::String(resolved))
        }
        serde_yaml::Value::Mapping(map) => {
            let mut new_map = serde_yaml::Mapping::new();
            for (k, v) in map {
                new_map.insert(k, resolve_value(v, mask)?);
            }
            Ok(serde_yaml::Value::Mapping(new_map))
        }
        serde_yaml::Value::Sequence(seq) => {
            let resolved: Result<Vec<_>, _> =
                seq.into_iter().map(|v| resolve_value(v, mask)).collect();
            Ok(serde_yaml::Value::Sequence(resolved?))
        }
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_env_var() {
        std::env::set_var("LS_TEST_VAR", "hello_world");
        let mut mask = MaskList::new();
        let result = resolve_string("prefix_{{ env('LS_TEST_VAR') }}_suffix", &mut mask).unwrap();
        assert_eq!(result, "prefix_hello_world_suffix");
        // The resolved value should be masked
        assert_eq!(mask.apply("hello_world"), "***");
    }

    #[test]
    fn missing_env_var_returns_error() {
        let mut mask = MaskList::new();
        let result = resolve_string("{{ env('LS_DEFINITELY_NOT_SET_XYZ') }}", &mut mask);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not set"));
    }

    #[test]
    fn string_without_template_passthrough() {
        let mut mask = MaskList::new();
        let result = resolve_string("no templates here", &mut mask).unwrap();
        assert_eq!(result, "no templates here");
    }

    #[test]
    fn unclosed_brace_returns_error() {
        let mut mask = MaskList::new();
        let result = resolve_string("{{ env('VAR')", &mut mask);
        assert!(result.is_err());
    }

    #[test]
    fn aws_sm_returns_stub_error() {
        let mut mask = MaskList::new();
        let result = resolve_string("{{ aws.sm('/path').field }}", &mut mask);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not available"));
    }
}
