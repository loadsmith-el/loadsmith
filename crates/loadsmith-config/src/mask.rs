//! Redaction of resolved template values.
//!
//! Every value produced by `env()`, `file()` or `secret()` is recorded in a
//! [`MaskList`]; [`MaskList::apply`] replaces each occurrence with `***`. The
//! engine applies it to everything it prints that could carry a value: plugin
//! log lines (fd4), plugin error texts (`configure_ack`, `finished`,
//! `resolved`), the final `error:` line, the run summary's cause and
//! `--print-resolved-config`.

use std::sync::{Arc, RwLock};

/// The replacement for a masked value.
pub const MASK: &str = "***";

/// Lines of a multi-line value at least this long (in characters, after
/// trimming) are masked on their own too: a log line rarely quotes a whole
/// PEM key, but it may quote one of its lines.
pub const MIN_MASKED_LINE_CHARS: usize = 8;

/// The values that must not appear in logs or printed output.
///
/// A cheap, shared handle: clones see (and add to) the same list, so a mask
/// handed to an event drain early also covers values resolved later.
/// `Debug` never shows the values.
#[derive(Default, Clone)]
pub struct MaskList(Arc<RwLock<Vec<String>>>);

impl std::fmt::Debug for MaskList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MaskList({} values)", self.len())
    }
}

impl MaskList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `secret`. A multi-line value is also recorded line by line
    /// (each trimmed line of at least [`MIN_MASKED_LINE_CHARS`] characters).
    /// Empty values are ignored.
    pub fn add(&self, secret: impl Into<String>) {
        let s = secret.into();
        if s.is_empty() {
            return;
        }
        let mut parts = Vec::new();
        if s.contains('\n') {
            for line in s.lines().map(str::trim) {
                if line.chars().count() >= MIN_MASKED_LINE_CHARS {
                    parts.push(line.to_string());
                }
            }
        }
        // A plugin that logs its config as JSON shows the value escaped
        // (`\n`, `\"`, `\\`): mask that spelling too.
        if let Ok(quoted) = serde_json::to_string(&s) {
            let escaped = &quoted[1..quoted.len() - 1];
            if escaped != s {
                parts.push(escaped.to_string());
            }
        }
        parts.push(s);
        let mut list = self.0.write().unwrap_or_else(|e| e.into_inner());
        for p in parts {
            if !list.contains(&p) {
                list.push(p);
            }
        }
        // Longest first, so a value that contains another is replaced whole.
        list.sort_by_key(|v| std::cmp::Reverse(v.len()));
    }

    /// Replaces every recorded value in `input` with `***`.
    pub fn apply(&self, input: &str) -> String {
        let list = self.0.read().unwrap_or_else(|e| e.into_inner());
        let mut out = input.to_string();
        for secret in list.iter() {
            if out.contains(secret.as_str()) {
                out = out.replace(secret.as_str(), MASK);
            }
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many values (including split-out lines) are recorded.
    pub fn len(&self) -> usize {
        self.0.read().unwrap_or_else(|e| e.into_inner()).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_secrets() {
        let list = MaskList::new();
        list.add("s3cr3t");
        list.add("pa$$word");
        assert_eq!(list.apply("user:s3cr3t and pa$$word here"), "user:*** and *** here");
    }

    #[test]
    fn empty_mask_is_noop() {
        let list = MaskList::new();
        assert_eq!(list.apply("hello world"), "hello world");
    }

    #[test]
    fn a_longer_value_is_masked_whole() {
        let list = MaskList::new();
        list.add("abc");
        list.add("abcdef");
        assert_eq!(list.apply("x abcdef y abc"), "x *** y ***");
    }

    #[test]
    fn multi_line_values_are_masked_line_by_line() {
        let pem = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEF\nshort\n-----END PRIVATE KEY-----\n";
        let list = MaskList::new();
        list.add(pem);
        // The whole value, and each long line quoted on its own.
        assert_eq!(list.apply(pem), "***");
        assert_eq!(list.apply("bad key line: MIIEvQIBADANBgkqhkiG9w0BAQEF"), "bad key line: ***");
        // A short line alone is not masked (it would mask common words).
        assert_eq!(list.apply("short"), "short");
    }

    #[test]
    fn json_escaped_spellings_are_masked() {
        let list = MaskList::new();
        list.add("a\"b\\c\nsecond-line");
        let logged = serde_json::json!({ "password": "a\"b\\c\nsecond-line" }).to_string();
        assert_eq!(list.apply(&logged), r#"{"password":"***"}"#);
    }

    #[test]
    fn clones_share_the_list_and_debug_hides_values() {
        let list = MaskList::new();
        let early = list.clone();
        list.add("hunter22");
        assert_eq!(early.apply("pw=hunter22"), "pw=***");
        assert!(!format!("{list:?}").contains("hunter22"));
    }
}
