//! The `{{ expr }}` template syntax of pipeline files.
//!
//! Three functions, each taking string literals (single or double quotes, no
//! escapes):
//!
//! - `{{ env('VAR') }}` — an environment variable of the engine process;
//! - `{{ file('/path') }}` — a local file's content (trailing whitespace
//!   trimmed);
//! - `{{ secret('instance', 'ref') }}` — a value resolved by the provider
//!   instance declared as `providers.<instance>`; `ref` is opaque to the core.
//!
//! Every resolved value is recorded in the [`MaskList`]. Resolution happens in
//! two passes over the YAML value tree: [`scan_value`] parses every template
//! (syntax errors surface before anything runs) and collects the `secret()`
//! calls; [`substitute_value`] then replaces each call with its value.
use std::collections::HashMap;

use crate::{error::ConfigError, mask::MaskList};

/// One `secret()` call: which provider instance resolves which reference.
/// Neither part is secret (the reference names a value, it is not one).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SecretRef {
    pub instance: String,
    pub reference: String,
}

impl std::fmt::Display for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "secret('{}', '{}')", self.instance, self.reference)
    }
}

/// Provider-resolved values, by call.
pub type SecretValues = HashMap<SecretRef, String>;

/// A parsed template expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Env(String),
    File(String),
    Secret(SecretRef),
}

/// A piece of a template string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Piece<'a> {
    Literal(&'a str),
    Expr(Expr),
}

/// Splits `input` into literal text and parsed `{{ }}` expressions.
pub fn parse_template(input: &str) -> Result<Vec<Piece<'_>>, ConfigError> {
    // Copy literal text as whole `&str` slices: the delimiters are ASCII, so
    // every split point is a char boundary and multi-byte UTF-8 (São, Zürich,
    // 東京) passes through untouched.
    let mut pieces = Vec::new();
    let mut rest = input;
    while let Some(open) = rest.find("{{") {
        if open > 0 {
            pieces.push(Piece::Literal(&rest[..open]));
        }
        let after_open = &rest[open + 2..];
        let close = after_open.find("}}").ok_or_else(|| {
            ConfigError::Template(format!("unclosed '{{{{' in template: {}", &rest[open..]))
        })?;
        pieces.push(Piece::Expr(parse_expr(after_open[..close].trim())?));
        rest = &after_open[close + 2..];
    }
    if !rest.is_empty() {
        pieces.push(Piece::Literal(rest));
    }
    Ok(pieces)
}

/// Parses one expression (the text between `{{` and `}}`).
pub fn parse_expr(expr: &str) -> Result<Expr, ConfigError> {
    let unknown = || {
        let hint = if expr.starts_with("aws.") {
            " (the aws.sm() form was retired: declare a provider under providers: and use \
             secret('<instance>', '<ref>'))"
        } else {
            ""
        };
        ConfigError::Template(format!(
            "unknown template expression: '{expr}'{hint}; expected env('VAR'), file('/path') \
             or secret('<instance>', '<ref>')"
        ))
    };
    let Some(paren) = expr.find('(') else { return Err(unknown()) };
    let name = expr[..paren].trim();
    let Some(args) = expr[paren + 1..].trim_end().strip_suffix(')') else {
        return Err(unknown());
    };
    let arity = match name {
        "env" | "file" => 1,
        "secret" => 2,
        _ => return Err(unknown()),
    };
    let args = parse_args(args).map_err(|why| {
        ConfigError::Template(format!("invalid arguments in '{expr}': {why}"))
    })?;
    if args.len() != arity {
        return Err(ConfigError::Template(format!(
            "{name}() takes {arity} string argument{}, got {} in '{expr}'",
            if arity == 1 { "" } else { "s" },
            args.len()
        )));
    }
    let mut args = args.into_iter();
    let mut next = || args.next().unwrap_or_default();
    Ok(match name {
        "env" => Expr::Env(next()),
        "file" => Expr::File(next()),
        _ => {
            let instance = next();
            let reference = next();
            if instance.is_empty() || reference.is_empty() {
                return Err(ConfigError::Template(format!(
                    "secret() needs a non-empty instance and reference in '{expr}'"
                )));
            }
            Expr::Secret(SecretRef { instance, reference })
        }
    })
}

/// A comma-separated list of quoted string literals.
fn parse_args(s: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut rest = s.trim();
    if rest.is_empty() {
        return Ok(out);
    }
    loop {
        let quote = rest.chars().next().ok_or("expected a quoted string")?;
        if quote != '\'' && quote != '"' {
            return Err(format!("expected a quoted string, got: {rest}"));
        }
        let body = &rest[1..];
        let end = body.find(quote).ok_or_else(|| format!("unterminated string: {rest}"))?;
        out.push(body[..end].to_string());
        rest = body[end + 1..].trim_start();
        if rest.is_empty() {
            return Ok(out);
        }
        rest = rest
            .strip_prefix(',')
            .ok_or_else(|| format!("expected ',' between arguments, got: {rest}"))?
            .trim_start();
    }
}

/// How `secret()` calls are answered during substitution.
#[derive(Clone, Copy)]
pub enum Secrets<'a> {
    /// No provider values: a `secret()` call is an error.
    None,
    /// Provider values; a call missing from the map is an error.
    Values(&'a SecretValues),
}

/// Evaluates one expression. The value is recorded in `mask`.
fn eval(expr: &Expr, secrets: Secrets<'_>, mask: &MaskList) -> Result<String, ConfigError> {
    let value = match expr {
        Expr::Env(var) => std::env::var(var).map_err(|_| {
            ConfigError::Template(format!("environment variable '{var}' not set"))
        })?,
        Expr::File(path) => std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Template(format!("cannot read file '{path}': {e}")))?
            .trim_end()
            .to_string(),
        Expr::Secret(call) => match secrets {
            Secrets::Values(values) => values.get(call).cloned().ok_or_else(|| {
                ConfigError::Template(format!("{call} was not resolved"))
            })?,
            Secrets::None => {
                return Err(ConfigError::Template(format!(
                    "{call} needs provider resolution, which is not available here"
                )))
            }
        },
    };
    mask.add(value.clone());
    Ok(value)
}

/// Resolves every `{{ }}` in `input` with `env()`/`file()` only; a
/// `secret()` call is an error. Values are recorded in `mask`.
pub fn resolve_string(input: &str, mask: &MaskList) -> Result<String, ConfigError> {
    substitute_string(input, Secrets::None, mask)
}

/// Resolves every `{{ }}` in `input`. Values are recorded in `mask`.
pub fn substitute_string(input: &str, secrets: Secrets<'_>, mask: &MaskList) -> Result<String, ConfigError> {
    let pieces = parse_template(input)?;
    if !pieces.iter().any(|p| matches!(p, Piece::Expr(_))) {
        return Ok(input.to_string());
    }
    let mut out = String::with_capacity(input.len());
    for piece in pieces {
        match piece {
            Piece::Literal(s) => out.push_str(s),
            Piece::Expr(e) => out.push_str(&eval(&e, secrets, mask)?),
        }
    }
    Ok(out)
}

/// Recursively resolves templates in a value tree with `env()`/`file()`
/// only. Mapping keys are never templates.
pub fn resolve_value(val: serde_yaml::Value, mask: &MaskList) -> Result<serde_yaml::Value, ConfigError> {
    substitute_value(val, Secrets::None, mask)
}

/// Recursively resolves templates in a value tree. Mapping keys are never
/// templates.
pub fn substitute_value(
    val: serde_yaml::Value,
    secrets: Secrets<'_>,
    mask: &MaskList,
) -> Result<serde_yaml::Value, ConfigError> {
    use serde_yaml::Value;
    match val {
        Value::String(s) => Ok(Value::String(substitute_string(&s, secrets, mask)?)),
        Value::Mapping(map) => {
            let mut out = serde_yaml::Mapping::new();
            for (k, v) in map {
                out.insert(k, substitute_value(v, secrets, mask)?);
            }
            Ok(Value::Mapping(out))
        }
        Value::Sequence(seq) => Ok(Value::Sequence(
            seq.into_iter().map(|v| substitute_value(v, secrets, mask)).collect::<Result<_, _>>()?,
        )),
        Value::Tagged(t) => {
            let t = *t;
            Ok(Value::Tagged(Box::new(serde_yaml::value::TaggedValue {
                tag: t.tag,
                value: substitute_value(t.value, secrets, mask)?,
            })))
        }
        other => Ok(other),
    }
}

/// One `secret()` call found by [`scan_value`], with where it is.
#[derive(Debug, Clone)]
pub struct FoundSecret {
    pub call: SecretRef,
    /// Dotted location, e.g. `source.config.password`.
    pub at: String,
}

/// Parses every template in a value tree (so syntax errors surface without
/// evaluating anything) and returns the `secret()` calls in document order.
/// `at` is the location of `val` (dotted; empty for the document root).
pub fn scan_value(val: &serde_yaml::Value, at: &str, found: &mut Vec<FoundSecret>) -> Result<(), ConfigError> {
    use serde_yaml::Value;
    match val {
        Value::String(s) => {
            let pieces = parse_template(s).map_err(|e| locate(e, at))?;
            for p in pieces {
                if let Piece::Expr(Expr::Secret(call)) = p {
                    found.push(FoundSecret { call, at: at.to_string() });
                }
            }
        }
        Value::Mapping(map) => {
            for (k, v) in map {
                let key = match k {
                    Value::String(s) => s.clone(),
                    other => serde_yaml::to_string(other).unwrap_or_default().trim().to_string(),
                };
                let child = if at.is_empty() { key } else { format!("{at}.{key}") };
                scan_value(v, &child, found)?;
            }
        }
        Value::Sequence(seq) => {
            for (i, v) in seq.iter().enumerate() {
                scan_value(v, &format!("{at}[{i}]"), found)?;
            }
        }
        Value::Tagged(t) => scan_value(&t.value, at, found)?,
        _ => {}
    }
    Ok(())
}

fn locate(e: ConfigError, at: &str) -> ConfigError {
    match e {
        ConfigError::Template(m) if !at.is_empty() => ConfigError::Template(format!("{at}: {m}")),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_env_var() {
        std::env::set_var("LS_TEST_VAR", "hello_world");
        let mask = MaskList::new();
        let result = resolve_string("prefix_{{ env('LS_TEST_VAR') }}_suffix", &mask).unwrap();
        assert_eq!(result, "prefix_hello_world_suffix");
        // The resolved value should be masked
        assert_eq!(mask.apply("hello_world"), "***");
    }

    #[test]
    fn missing_env_var_returns_error() {
        let mask = MaskList::new();
        let result = resolve_string("{{ env('LS_DEFINITELY_NOT_SET_XYZ') }}", &mask);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not set"));
    }

    #[test]
    fn string_without_template_passthrough() {
        let mask = MaskList::new();
        let result = resolve_string("no templates here", &mask).unwrap();
        assert_eq!(result, "no templates here");
    }

    /// Regression: literal text was copied byte by byte as `u8 as char`, so
    /// every non-ASCII character was mangled (São → SÃ£o), including inside
    /// queries sent to plugins.
    #[test]
    fn non_ascii_outside_templates_is_preserved() {
        let mask = MaskList::new();
        let input = "SELECT * FROM cidades WHERE nome = 'São Paulo' -- Zürich, 東京, 🚚";
        assert_eq!(resolve_string(input, &mask).unwrap(), input);
    }

    #[test]
    fn non_ascii_around_and_inside_templates_is_preserved() {
        std::env::set_var("LS_TEST_UTF8_VAR", "Conceição");
        let mask = MaskList::new();
        let result = resolve_string(
            "ação={{ env('LS_TEST_UTF8_VAR') }}·{{env(\"LS_TEST_UTF8_VAR\")}}é",
            &mask,
        )
        .unwrap();
        assert_eq!(result, "ação=Conceição·Conceiçãoé");
    }

    #[test]
    fn file_values_are_read_trimmed_and_masked() {
        let dir = std::env::temp_dir().join(format!("ls-tpl-utf8-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("v.txt");
        std::fs::write(&path, "Florianópolis\n").unwrap();
        let mask = MaskList::new();
        let input = format!("cidade: {{{{ file('{}') }}}} — ok", path.display());
        let result = resolve_string(&input, &mask);
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(result.unwrap(), "cidade: Florianópolis — ok");
        // D2: file() values are masked like env() and secret() values.
        assert_eq!(mask.apply("at Florianópolis"), "at ***");
    }

    #[test]
    fn single_braces_are_literal() {
        let mask = MaskList::new();
        assert_eq!(resolve_string("{ \"a\": \"ç\" }", &mask).unwrap(), "{ \"a\": \"ç\" }");
    }

    #[test]
    fn unclosed_brace_returns_error() {
        let mask = MaskList::new();
        assert!(resolve_string("{{ env('VAR')", &mask).is_err());
    }

    #[test]
    fn retired_aws_sm_form_points_to_secret() {
        let err = parse_expr("aws.sm('/path').field").unwrap_err().to_string();
        assert!(err.contains("unknown template expression"), "{err}");
        assert!(err.contains("secret("), "{err}");
    }

    #[test]
    fn secret_calls_parse_with_either_quote_and_commas_inside() {
        assert_eq!(
            parse_expr("secret('creds', \"db.json#a,b\")").unwrap(),
            Expr::Secret(SecretRef { instance: "creds".into(), reference: "db.json#a,b".into() })
        );
        assert_eq!(
            parse_expr("secret( 'sm' ,'prod/db' )").unwrap(),
            Expr::Secret(SecretRef { instance: "sm".into(), reference: "prod/db".into() })
        );
    }

    #[test]
    fn bad_calls_are_syntax_errors() {
        for bad in [
            "secret('only-one')",
            "secret('a', 'b', 'c')",
            "secret(creds, 'x')",
            "secret('', 'x')",
            "secret('a', env('X'))",
            "env()",
            "env('A', 'B')",
            "file('/p').field",
            "nope('x')",
            "env",
        ] {
            assert!(parse_expr(bad).is_err(), "{bad} should not parse");
        }
    }

    #[test]
    fn secret_without_values_is_an_error_and_with_values_is_masked() {
        let mask = MaskList::new();
        let input = "pw={{ secret('creds', 'db#pw') }}";
        assert!(resolve_string(input, &mask).is_err());
        let mut values = SecretValues::new();
        values.insert(SecretRef { instance: "creds".into(), reference: "db#pw".into() }, "t0ps3cret".into());
        let out = substitute_string(input, Secrets::Values(&values), &mask).unwrap();
        assert_eq!(out, "pw=t0ps3cret");
        assert_eq!(mask.apply(&out), "pw=***");
    }

    #[test]
    fn scan_finds_secrets_with_their_location_and_locates_syntax_errors() {
        let v: serde_yaml::Value = serde_yaml::from_str(
            "source:\n  config:\n    pw: \"{{ secret('a', 'x') }}\"\n    list: [\"{{ secret('b', 'y') }}\"]\n",
        )
        .unwrap();
        let mut found = Vec::new();
        scan_value(&v, "", &mut found).unwrap();
        let at: Vec<_> = found.iter().map(|f| (f.call.instance.as_str(), f.at.as_str())).collect();
        assert_eq!(at, [("a", "source.config.pw"), ("b", "source.config.list[0]")]);

        let bad: serde_yaml::Value = serde_yaml::from_str("destination:\n  config:\n    x: \"{{ nope() }}\"\n").unwrap();
        let err = scan_value(&bad, "", &mut Vec::new()).unwrap_err().to_string();
        assert!(err.contains("destination.config.x"), "{err}");
    }
}
