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
    // Copy literal text as whole `&str` slices: the delimiters are ASCII, so
    // every split point is a char boundary and multi-byte UTF-8 (São, Zürich,
    // 東京) passes through untouched.
    let mut out = String::with_capacity(input.len());
    let mut rest = input;

    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        let close = after_open.find("}}").ok_or_else(|| {
            ConfigError::Template(format!("unclosed '{{{{' in template: {}", &rest[open..]))
        })?;
        let expr = after_open[..close].trim();
        out.push_str(&eval_expr(expr, mask)?);
        rest = &after_open[close + 2..]; // skip past `}}`
    }
    out.push_str(rest);
    Ok(out)
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

    /// Regression: literal text was copied byte by byte as `u8 as char`, so
    /// every non-ASCII character was mangled (São → SÃ£o), including inside
    /// queries sent to plugins.
    #[test]
    fn non_ascii_outside_templates_is_preserved() {
        let mut mask = MaskList::new();
        let input = "SELECT * FROM cidades WHERE nome = 'São Paulo' -- Zürich, 東京, 🚚";
        assert_eq!(resolve_string(input, &mut mask).unwrap(), input);
    }

    #[test]
    fn non_ascii_around_and_inside_templates_is_preserved() {
        std::env::set_var("LS_TEST_UTF8_VAR", "Conceição");
        let mut mask = MaskList::new();
        let result = resolve_string(
            "ação={{ env('LS_TEST_UTF8_VAR') }}·{{env(\"LS_TEST_UTF8_VAR\")}}é",
            &mut mask,
        )
        .unwrap();
        assert_eq!(result, "ação=Conceição·Conceiçãoé");
    }

    #[test]
    fn non_ascii_in_file_template_is_preserved() {
        let dir = std::env::temp_dir().join(format!("ls-tpl-utf8-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("v.txt");
        std::fs::write(&path, "Florianópolis\n").unwrap();
        let mut mask = MaskList::new();
        let input = format!("cidade: {{{{ file('{}') }}}} — ok", path.display());
        let result = resolve_string(&input, &mut mask);
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(result.unwrap(), "cidade: Florianópolis — ok");
    }

    #[test]
    fn single_braces_are_literal() {
        let mut mask = MaskList::new();
        assert_eq!(resolve_string("{ \"a\": \"ç\" }", &mut mask).unwrap(), "{ \"a\": \"ç\" }");
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
