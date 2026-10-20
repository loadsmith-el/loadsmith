//! A plugin's config block: typed parsing with the operator's unknown-keys
//! policy.
//!
//! Every plugin parses its `config:` the same way, so a typo behaves the same
//! in every plugin:
//!
//! ```ignore
//! #[derive(serde::Deserialize)]
//! struct MyConfig { host: String, #[serde(default)] port: Option<u16> }
//!
//! async fn configure(&mut self, config: PluginConfig) -> anyhow::Result<()> {
//!     let cfg: MyConfig = config.parse()?;
//!     // …
//!     Ok(())
//! }
//! ```
//!
//! [`PluginConfig::parse`] finds every key the target type does not use, at
//! any depth — nested structs, arrays of structs, and `#[serde(flatten)]`ed
//! structs (where serde's own `deny_unknown_fields` does not work). Under the
//! default policy ([`UnknownKeys::Error`]) it fails with a
//! [`ConfigRejection`] naming every unknown key path (`tls.verify_mode`,
//! `tables[2].nmae`), and the SDK answers `configure_ack` with the code
//! [`UNKNOWN_CONFIG_KEYS`]. Under [`UnknownKeys::Warn`] (the operator ran
//! `loadsmith run --ignore-unknown-keys`) the keys are ignored and the SDK
//! logs one warning per key on the event channel.
//!
//! Don't add `#[serde(deny_unknown_fields)]` to a config type: it would make
//! the warn policy impossible. The helper does that job.
//!
//! **How keys are found.** One pass with a tracing deserializer records how
//! each key's value was consumed: by a typed field (used), as an ignored value
//! (unknown), or buffered as opaque content (a `flatten`ed or `untagged`
//! value, or a `serde_json::Value`). Each buffered key is then *probed*: the
//! type is deserialized once more with that key's value replaced by bytes no
//! config field accepts; if that still succeeds, nothing used the key. A
//! field type that accepts raw bytes (`serde_bytes`) or an `untagged` enum
//! with a catch-all variant can hide a key from the probe.
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use serde_json::{Error, Value};

pub use loadsmith_protocol::UnknownKeys;

/// `configure_ack` code: the config has keys the plugin does not know.
pub const UNKNOWN_CONFIG_KEYS: &str = "UNKNOWN_CONFIG_KEYS";
/// `configure_ack` code: the config does not have the expected shape (a
/// missing field, a value of the wrong type).
pub const INVALID_CONFIG: &str = "INVALID_CONFIG";

/// The most keys whose use is probed by re-deserialization. Beyond it (only
/// reachable with large opaque `Value` blobs), a key is taken as used.
const PROBE_BUDGET: usize = 2000;

// ── The plugin-facing config ─────────────────────────────────────────────────

/// What a plugin's `configure` receives: its config block and the
/// unknown-keys policy the operator chose. Derefs to the raw JSON value.
#[derive(Debug)]
pub struct PluginConfig {
    value: Value,
    unknown_keys: UnknownKeys,
    warnings: Arc<Mutex<Vec<String>>>,
}

impl PluginConfig {
    pub fn new(value: Value, unknown_keys: UnknownKeys) -> Self {
        Self { value, unknown_keys, warnings: Arc::default() }
    }

    /// Parses the block into `T`, applying the policy: an unknown key is an
    /// error ([`UnknownKeys::Error`]) or is ignored with a warning the SDK
    /// sends on the event channel ([`UnknownKeys::Warn`]).
    pub fn parse<T: DeserializeOwned>(&self) -> Result<T, ConfigRejection> {
        let parsed = parse(&self.value, self.unknown_keys)?;
        let mut warnings = self.warnings.lock().unwrap_or_else(|e| e.into_inner());
        warnings.extend(parsed.ignored.iter().map(|k| format!("ignoring unknown config key {k}")));
        Ok(parsed.config)
    }

    /// The policy for unknown keys.
    pub fn unknown_keys(&self) -> UnknownKeys {
        self.unknown_keys
    }

    /// The raw block.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The raw block, owned.
    pub fn into_value(self) -> Value {
        self.value
    }

    /// The warnings [`parse`](Self::parse) produced; the SDK drains them to
    /// fd4 once `configure` returns.
    pub(crate) fn warnings(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.warnings)
    }
}

impl Deref for PluginConfig {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.value
    }
}

/// Takes the warnings collected so far.
pub(crate) fn take_warnings(warnings: &Mutex<Vec<String>>) -> Vec<String> {
    std::mem::take(&mut *warnings.lock().unwrap_or_else(|e| e.into_inner()))
}

// ── Results ───────────────────────────────────────────────────────────────────

/// A key the target type does not use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownKey {
    /// Its path in the block: `tls.verify_mode`, `tables[2].nmae`.
    pub path: String,
    /// A known field of the same struct within a small edit distance, as a
    /// full path (`tls.verify`), when the struct's field names are known.
    pub suggestion: Option<String>,
}

impl fmt::Display for UnknownKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}`", self.path)?;
        if let Some(s) = &self.suggestion {
            write!(f, " (did you mean `{s}`?)")?;
        }
        Ok(())
    }
}

/// A parsed block, with the unknown keys that were ignored (only ever
/// non-empty under [`UnknownKeys::Warn`]).
#[derive(Debug)]
pub struct Parsed<T> {
    pub config: T,
    pub ignored: Vec<UnknownKey>,
}

/// Why a config block was rejected. Converts into `anyhow::Error`, so
/// `config.parse()?` works in `configure`; the SDK then answers
/// `configure_ack` with [`code`](Self::code).
#[derive(Debug, Clone)]
pub enum ConfigRejection {
    /// The block does not have the expected shape.
    Invalid {
        /// Where, when known (`tls.port`).
        path: Option<String>,
        message: String,
    },
    /// Keys the plugin does not know, under [`UnknownKeys::Error`].
    UnknownKeys(Vec<UnknownKey>),
}

impl ConfigRejection {
    /// The `configure_ack` error code.
    pub fn code(&self) -> &'static str {
        match self {
            ConfigRejection::Invalid { .. } => INVALID_CONFIG,
            ConfigRejection::UnknownKeys(_) => UNKNOWN_CONFIG_KEYS,
        }
    }
}

impl fmt::Display for ConfigRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigRejection::Invalid { path: Some(p), message } => write!(f, "invalid config at `{p}`: {message}"),
            ConfigRejection::Invalid { path: None, message } => write!(f, "invalid config: {message}"),
            ConfigRejection::UnknownKeys(keys) => {
                let list: Vec<String> = keys.iter().map(ToString::to_string).collect();
                write!(
                    f,
                    "unknown config key{} {} (fix or remove {}, or run with --ignore-unknown-keys to \
                     ignore unknown keys)",
                    if keys.len() == 1 { "" } else { "s" },
                    list.join(", "),
                    if keys.len() == 1 { "it" } else { "them" }
                )
            }
        }
    }
}

impl std::error::Error for ConfigRejection {}

// ── The parser ────────────────────────────────────────────────────────────────

/// Parses `value` into `T` and finds the keys `T` does not use. Under
/// [`UnknownKeys::Error`] any such key rejects the block; under
/// [`UnknownKeys::Warn`] they are returned in [`Parsed::ignored`].
pub fn parse<T: DeserializeOwned>(value: &Value, policy: UnknownKeys) -> Result<Parsed<T>, ConfigRejection> {
    let ctx = Ctx { sentinel: None, trace: Some(RefCell::default()), error_at: RefCell::default() };
    let config = match T::deserialize(Node::root(value, &ctx)) {
        Ok(config) => config,
        Err(e) => {
            let path = ctx.error_at.borrow().as_ref().filter(|p| !p.is_empty()).map(|p| render(p));
            return Err(ConfigRejection::Invalid { path, message: e.to_string() });
        }
    };
    let trace = ctx.trace.map(RefCell::into_inner).unwrap_or_default();

    let mut found: Vec<Path> = Vec::new();
    let mut budget = PROBE_BUDGET;
    let mut path = Vec::new();
    let mut probe = |p: &Path| {
        let ctx = Ctx { sentinel: Some(p.clone()), trace: None, error_at: RefCell::default() };
        T::deserialize(Node::root(value, &ctx)).is_ok()
    };
    walk(value, &mut path, &trace, &mut probe, &mut budget, &mut found);

    let keys: Vec<UnknownKey> = found.iter().map(|p| unknown_key(p, &trace)).collect();
    match policy {
        UnknownKeys::Error if !keys.is_empty() => Err(ConfigRejection::UnknownKeys(keys)),
        UnknownKeys::Error => Ok(Parsed { config, ignored: Vec::new() }),
        UnknownKeys::Warn => Ok(Parsed { config, ignored: keys }),
    }
}

/// Classifies every key under `value` from the trace, probing the ambiguous
/// ones; pushes the unknown ones (not their children) to `found`.
fn walk(
    value: &Value,
    path: &mut Path,
    trace: &Trace,
    probe: &mut dyn FnMut(&Path) -> bool,
    budget: &mut usize,
    found: &mut Vec<Path>,
) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                path.push(Seg::Key(key.clone()));
                match trace.uses.get(path.as_slice()) {
                    Some(Use::Typed) => walk(child, path, trace, probe, budget, found),
                    Some(Use::Ignored) => found.push(path.clone()),
                    Some(Use::Any) | None => {
                        if *budget > 0 {
                            *budget -= 1;
                            if probe(path) {
                                found.push(path.clone());
                            } else {
                                walk(child, path, trace, probe, budget, found);
                            }
                        }
                    }
                }
                path.pop();
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                path.push(Seg::Index(i));
                walk(child, path, trace, probe, budget, found);
                path.pop();
            }
        }
        _ => {}
    }
}

fn unknown_key(path: &Path, trace: &Trace) -> UnknownKey {
    let (Some(Seg::Key(key)), parent) = (path.last(), &path[..path.len() - 1]) else {
        return UnknownKey { path: render(path), suggestion: None };
    };
    let suggestion = trace.fields.get(parent).and_then(|fields| closest(key, fields)).map(|f| {
        let mut p = parent.to_vec();
        p.push(Seg::Key(f.to_string()));
        render(&p)
    });
    UnknownKey { path: render(path), suggestion }
}

/// The known field closest to `key`, if it is a plausible typo of it.
fn closest<'f>(key: &str, fields: &BTreeSet<&'f str>) -> Option<&'f str> {
    let limit = (key.chars().count() / 3).clamp(1, 3);
    fields
        .iter()
        .filter(|f| **f != key)
        .map(|f| (edit_distance(key, f), *f))
        .filter(|(d, _)| *d <= limit)
        .min()
        .map(|(_, f)| f)
}

/// Levenshtein distance, counting a swap of two adjacent characters as one
/// edit (`hots` → `host`).
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut best = (d[i - 1][j] + 1).min(d[i][j - 1] + 1).min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                best = best.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = best;
        }
    }
    d[a.len()][b.len()]
}

// ── The tracing deserializer ─────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Seg {
    Key(String),
    Index(usize),
}

type Path = Vec<Seg>;

fn render(path: &[Seg]) -> String {
    let mut out = String::new();
    for seg in path {
        match seg {
            Seg::Key(k) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(k);
            }
            Seg::Index(i) => out.push_str(&format!("[{i}]")),
        }
    }
    out
}

/// How a value was consumed. When a path is consumed more than once, the
/// strongest use wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Use {
    /// Skipped by its parent: an unknown key.
    Ignored,
    /// Buffered as opaque content (`flatten`, `untagged`, `Value`): used or
    /// not, only a probe can tell.
    Any,
    /// Read by a typed field.
    Typed,
}

#[derive(Debug, Default)]
struct Trace {
    uses: BTreeMap<Path, Use>,
    /// The field names of each struct read at a path (for suggestions).
    fields: BTreeMap<Path, BTreeSet<&'static str>>,
}

struct Ctx {
    /// The path whose value is replaced by [`SENTINEL`] (a probe).
    sentinel: Option<Path>,
    /// Recorded on the first pass only.
    trace: Option<RefCell<Trace>>,
    /// The innermost path an error passed through.
    error_at: RefCell<Option<Path>>,
}

/// Invalid UTF-8: no string, number, struct, sequence, enum or `Value`
/// accepts it.
const SENTINEL: &[u8] = b"\xff\xfe\x00loadsmith-probe";

struct Node<'de> {
    value: &'de Value,
    path: Path,
    ctx: &'de Ctx,
}

impl<'de> Node<'de> {
    fn root(value: &'de Value, ctx: &'de Ctx) -> Self {
        Node { value, path: Vec::new(), ctx }
    }

    fn child(&self, seg: Seg, value: &'de Value) -> Node<'de> {
        let mut path = self.path.clone();
        path.push(seg);
        Node { value, path, ctx: self.ctx }
    }

    fn is_sentinel(&self) -> bool {
        self.ctx.sentinel.as_ref() == Some(&self.path)
    }

    fn mark(&self, u: Use) {
        if let Some(trace) = &self.ctx.trace {
            let mut trace = trace.borrow_mut();
            let entry = trace.uses.entry(self.path.clone()).or_insert(u);
            if u > *entry {
                *entry = u;
            }
        }
    }

    fn note_fields(&self, fields: &'static [&'static str]) {
        if let (Some(trace), true) = (&self.ctx.trace, self.value.is_object()) {
            trace.borrow_mut().fields.entry(self.path.clone()).or_default().extend(fields.iter().copied());
        }
    }

    /// Records where an error happened (the innermost node it passed).
    fn track<T>(&self, r: Result<T, Error>) -> Result<T, Error> {
        if r.is_err() {
            let mut at = self.ctx.error_at.borrow_mut();
            if at.is_none() {
                *at = Some(self.path.clone());
            }
        }
        r
    }

    fn is_container(&self) -> bool {
        matches!(self.value, Value::Object(_) | Value::Array(_))
    }

    /// Visits an object as a map and an array as a sequence, as serde_json
    /// does for `&Value`, failing when the visitor leaves entries unread.
    fn container<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.value {
            Value::Object(map) => {
                let mut access = Entries { iter: map.iter(), node: &self, pending: None };
                let out = visitor.visit_map(&mut access)?;
                match access.iter.len() {
                    0 => Ok(out),
                    _ => Err(de::Error::invalid_length(map.len(), &"fewer elements in map")),
                }
            }
            Value::Array(items) => {
                let mut access = Items { iter: items.iter().enumerate(), node: &self };
                let out = visitor.visit_seq(&mut access)?;
                match access.iter.len() {
                    0 => Ok(out),
                    _ => Err(de::Error::invalid_length(items.len(), &"fewer elements in array")),
                }
            }
            _ => unreachable!("container() on a scalar"),
        }
    }
}

macro_rules! typed {
    ($($method:ident)*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            if self.is_sentinel() {
                return visitor.visit_bytes(SENTINEL);
            }
            self.mark(Use::Typed);
            if self.is_container() {
                self.container_tracked(visitor)
            } else {
                let r = self.value.$method(visitor);
                self.track(r)
            }
        }
    )*};
}

impl<'de> Deserializer<'de> for Node<'de> {
    type Error = Error;

    typed! {
        deserialize_bool deserialize_i8 deserialize_i16 deserialize_i32 deserialize_i64 deserialize_i128
        deserialize_u8 deserialize_u16 deserialize_u32 deserialize_u64 deserialize_u128
        deserialize_f32 deserialize_f64 deserialize_char deserialize_str deserialize_string
        deserialize_bytes deserialize_byte_buf deserialize_unit deserialize_seq deserialize_map
        deserialize_identifier
    }

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        self.mark(Use::Any);
        if self.is_container() {
            self.container_tracked(visitor)
        } else {
            let r = self.value.deserialize_any(visitor);
            self.track(r)
        }
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.mark(Use::Ignored);
        visitor.visit_unit()
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        if self.value.is_null() {
            self.mark(Use::Typed);
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        self.mark(Use::Typed);
        if self.is_container() { self.container_tracked(visitor) } else { self.value.deserialize_unit_struct(name, visitor) }
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        self.mark(Use::Typed);
        if self.is_container() { self.container_tracked(visitor) } else { self.value.deserialize_tuple(len, visitor) }
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        self.mark(Use::Typed);
        if self.is_container() {
            self.container_tracked(visitor)
        } else {
            self.value.deserialize_tuple_struct(name, len, visitor)
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        self.mark(Use::Typed);
        self.note_fields(fields);
        if self.is_container() {
            self.container_tracked(visitor)
        } else {
            let r = self.value.deserialize_struct(name, fields, visitor);
            self.track(r)
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        if self.is_sentinel() {
            return visitor.visit_bytes(SENTINEL);
        }
        self.mark(Use::Typed);
        match self.value {
            Value::Object(map) if map.len() == 1 => {
                let (variant, payload) = map.iter().next().expect("one entry");
                let payload = self.child(Seg::Key(variant.clone()), payload);
                let r = visitor.visit_enum(Variant { name: variant, payload });
                self.track(r)
            }
            _ => {
                let r = self.value.deserialize_enum(name, variants, visitor);
                self.track(r)
            }
        }
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

impl<'de> Node<'de> {
    fn container_tracked<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let path = self.path.clone();
        let ctx = self.ctx;
        let r = self.container(visitor);
        if r.is_err() {
            let mut at = ctx.error_at.borrow_mut();
            if at.is_none() {
                *at = Some(path);
            }
        }
        r
    }
}

struct Entries<'a, 'de> {
    iter: serde_json::map::Iter<'de>,
    node: &'a Node<'de>,
    pending: Option<(&'de String, &'de Value)>,
}

impl<'de> MapAccess<'de> for Entries<'_, 'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Error> {
        match self.iter.next() {
            Some((key, value)) => {
                self.pending = Some((key, value));
                seed.deserialize(Key(key)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        let (key, value) = self.pending.take().ok_or_else(|| de::Error::custom("value before key"))?;
        seed.deserialize(self.node.child(Seg::Key(key.clone()), value))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

struct Items<'a, 'de> {
    iter: std::iter::Enumerate<std::slice::Iter<'de, Value>>,
    node: &'a Node<'de>,
}

impl<'de> SeqAccess<'de> for Items<'_, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Error> {
        match self.iter.next() {
            Some((i, value)) => seed.deserialize(self.node.child(Seg::Index(i), value)).map(Some),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

/// An externally tagged enum value: `{"<variant>": <payload>}`.
struct Variant<'de> {
    name: &'de String,
    payload: Node<'de>,
}

impl<'de> EnumAccess<'de> for Variant<'de> {
    type Error = Error;
    type Variant = Node<'de>;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Node<'de>), Error> {
        Ok((seed.deserialize(Key(self.name))?, self.payload))
    }
}

impl<'de> VariantAccess<'de> for Node<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        de::Deserialize::deserialize(self)
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_seq(visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.deserialize_struct("", fields, visitor)
    }
}

/// A map key: a string, also readable as a number or a bool (as serde_json
/// reads object keys), so `HashMap<u32, _>` works.
struct Key<'de>(&'de str);

macro_rules! parsed_key {
    ($($method:ident => $visit:ident: $ty:ty),*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            match self.0.parse::<$ty>() {
                Ok(n) => visitor.$visit(n),
                Err(_) => visitor.visit_borrowed_str(self.0),
            }
        }
    )*};
}

impl<'de> Deserializer<'de> for Key<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_borrowed_str(self.0)
    }

    parsed_key! {
        deserialize_bool => visit_bool: bool,
        deserialize_i8 => visit_i8: i8, deserialize_i16 => visit_i16: i16,
        deserialize_i32 => visit_i32: i32, deserialize_i64 => visit_i64: i64,
        deserialize_i128 => visit_i128: i128,
        deserialize_u8 => visit_u8: u8, deserialize_u16 => visit_u16: u16,
        deserialize_u32 => visit_u32: u32, deserialize_u64 => visit_u64: u64,
        deserialize_u128 => visit_u128: u128,
        deserialize_f32 => visit_f32: f32, deserialize_f64 => visit_f64: f64
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        de::IntoDeserializer::<Error>::into_deserializer(self.0).deserialize_enum(name, variants, visitor)
    }

    serde::forward_to_deserialize_any! {
        char str string bytes byte_buf unit unit_struct seq tuple tuple_struct map struct
        identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Tls {
        verify: bool,
        #[serde(default)]
        ca_file: Option<String>,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Common {
        host: String,
        #[serde(default)]
        port: Option<u16>,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Table {
        name: String,
        #[serde(default)]
        key: Vec<String>,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "snake_case")]
    enum Auth {
        Password { user: String, password: String },
        Token(String),
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Cfg {
        #[serde(flatten)]
        common: Common,
        #[serde(default)]
        tls: Option<Tls>,
        #[serde(default)]
        tables: Vec<Table>,
        #[serde(default)]
        auth: Option<Auth>,
        #[serde(default)]
        ssl_mode: Option<String>,
        #[serde(default)]
        options: std::collections::BTreeMap<String, String>,
        #[serde(default)]
        extra: Option<Value>,
    }

    fn unknown(value: Value) -> Vec<String> {
        match parse::<Cfg>(&value, UnknownKeys::Error) {
            Ok(_) => Vec::new(),
            Err(ConfigRejection::UnknownKeys(keys)) => keys.into_iter().map(|k| k.path).collect(),
            Err(e) => panic!("unexpected rejection: {e}"),
        }
    }

    fn full() -> Value {
        json!({
            "host": "db", "port": 5432,
            "tls": {"verify": true, "ca_file": "/ca.pem"},
            "tables": [{"name": "a", "key": ["id"]}, {"name": "b"}],
            "auth": {"password": {"user": "u", "password": "p"}},
            "ssl_mode": "require",
            "options": {"application_name": "x"},
            "extra": {"anything": [1, {"goes": true}]}
        })
    }

    #[test]
    fn a_fully_known_config_parses_with_no_unknown_keys() {
        let parsed = parse::<Cfg>(&full(), UnknownKeys::Error).unwrap();
        assert!(parsed.ignored.is_empty());
        assert_eq!(parsed.config.common, Common { host: "db".into(), port: Some(5432) });
        assert_eq!(parsed.config.tables[0].key, ["id"]);
        assert_eq!(parsed.config.auth, Some(Auth::Password { user: "u".into(), password: "p".into() }));
        assert_eq!(unknown(json!({"host": "h", "auth": {"token": "t"}})), Vec::<String>::new());
    }

    #[test]
    fn top_level_unknown_key() {
        let mut v = full();
        v["sslmode"] = json!("require");
        assert_eq!(unknown(v), ["sslmode"]);
    }

    #[test]
    fn nested_unknown_key() {
        let mut v = full();
        v["tls"]["verify_mode"] = json!("full");
        assert_eq!(unknown(v), ["tls.verify_mode"]);
    }

    #[test]
    fn unknown_keys_next_to_a_flattened_struct() {
        // Unused by the outer struct and by the flattened one: serde drops
        // these silently, even with deny_unknown_fields on `Common`.
        let mut v = full();
        v["hots"] = json!("typo");
        v["x-generator"] = json!({"by": "tool"});
        assert_eq!(unknown(v), ["hots", "x-generator"]);
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Outer {
        #[serde(flatten)]
        inner: Inner,
    }
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Inner {
        tls: Tls,
    }

    #[test]
    fn unknown_key_nested_inside_a_flattened_struct() {
        let v = json!({"tls": {"verify": false, "verfy": true}, "other": 1});
        let Err(ConfigRejection::UnknownKeys(keys)) = parse::<Outer>(&v, UnknownKeys::Error) else { panic!() };
        let paths: Vec<_> = keys.iter().map(|k| k.path.as_str()).collect();
        assert_eq!(paths, ["other", "tls.verfy"]);
    }

    #[test]
    fn unknown_keys_in_arrays_of_structs() {
        let mut v = full();
        v["tables"][1]["nmae"] = json!("c");
        v["tables"][0]["keys"] = json!(["x"]);
        assert_eq!(unknown(v), ["tables[0].keys", "tables[1].nmae"]);
    }

    #[test]
    fn unknown_key_in_an_enum_variant() {
        let mut v = full();
        v["auth"]["password"]["role"] = json!("admin");
        assert_eq!(unknown(v), ["auth.password.role"]);
    }

    #[test]
    fn maps_and_opaque_values_accept_any_key() {
        let mut v = full();
        v["options"]["whatever"] = json!("x");
        v["extra"]["more"] = json!({"deep": 1});
        assert_eq!(unknown(v), Vec::<String>::new());
    }

    #[test]
    fn warn_returns_the_config_and_the_ignored_keys() {
        let mut v = full();
        v["hots"] = json!("typo");
        v["tls"]["verfy"] = json!(true);
        let parsed = parse::<Cfg>(&v, UnknownKeys::Warn).unwrap();
        assert_eq!(parsed.config.common.host, "db");
        let paths: Vec<_> = parsed.ignored.iter().map(|k| k.path.as_str()).collect();
        assert_eq!(paths, ["hots", "tls.verfy"]);
    }

    #[test]
    fn error_lists_every_key_with_suggestions() {
        let mut v = full();
        v["tls"]["verfy"] = json!(true);
        v["sslmode"] = json!("x");
        v["zzz"] = json!(1);
        let err = parse::<Cfg>(&v, UnknownKeys::Error).unwrap_err();
        assert_eq!(err.code(), UNKNOWN_CONFIG_KEYS);
        let ConfigRejection::UnknownKeys(keys) = &err else { panic!() };
        let tls = keys.iter().find(|k| k.path == "tls.verfy").unwrap();
        assert_eq!(tls.suggestion.as_deref(), Some("tls.verify"));
        let ssl = keys.iter().find(|k| k.path == "sslmode").unwrap();
        // `Cfg` has a flattened field, so its own names are not traced; a
        // plain struct gets suggestions.
        assert_eq!(ssl.suggestion, None);
        assert_eq!(keys.iter().find(|k| k.path == "zzz").unwrap().suggestion, None);
        let text = err.to_string();
        assert!(text.contains("`tls.verfy` (did you mean `tls.verify`?)"), "{text}");
        assert!(text.contains("--ignore-unknown-keys"), "{text}");
    }

    #[test]
    fn suggestions_on_a_plain_struct() {
        let v = json!({"name": "t", "kye": ["id"]});
        let Err(ConfigRejection::UnknownKeys(keys)) = parse::<Table>(&v, UnknownKeys::Error) else { panic!() };
        assert_eq!(keys, [UnknownKey { path: "kye".into(), suggestion: Some("key".into()) }]);
    }

    #[test]
    fn invalid_values_are_rejected_with_their_path() {
        let mut v = full();
        v["tls"]["verify"] = json!("yes");
        let err = parse::<Cfg>(&v, UnknownKeys::Warn).unwrap_err();
        assert_eq!(err.code(), INVALID_CONFIG);
        let ConfigRejection::Invalid { path, message } = &err else { panic!() };
        assert_eq!(path.as_deref(), Some("tls.verify"));
        assert!(message.contains("expected a boolean"), "{message}");
        let missing = parse::<Cfg>(&json!({"port": 1}), UnknownKeys::Error).unwrap_err();
        assert!(missing.to_string().contains("missing field `host`"), "{missing}");
    }

    #[test]
    fn plugin_config_collects_warnings_only_under_warn() {
        let v = json!({"name": "t", "kye": 1});
        let strict = PluginConfig::new(v.clone(), UnknownKeys::Error);
        assert!(strict.parse::<Table>().is_err());
        let lenient = PluginConfig::new(v, UnknownKeys::Warn);
        let table: Table = lenient.parse().unwrap();
        assert_eq!(table.name, "t");
        assert_eq!(lenient["name"], "t");
        assert_eq!(
            take_warnings(&lenient.warnings()),
            ["ignoring unknown config key `kye` (did you mean `key`?)"]
        );
    }

    #[test]
    fn a_rejection_survives_anyhow_context() {
        let err: anyhow::Error = parse::<Table>(&json!({"name": "t", "x": 1}), UnknownKeys::Error)
            .map(|_| ())
            .map_err(anyhow::Error::from)
            .map_err(|e| e.context("configure"))
            .unwrap_err();
        assert_eq!(err.downcast_ref::<ConfigRejection>().map(ConfigRejection::code), Some(UNKNOWN_CONFIG_KEYS));
    }

    #[test]
    fn edit_distance_counts_swaps_as_one() {
        assert_eq!(edit_distance("hots", "host"), 1);
        assert_eq!(edit_distance("sslmode", "ssl_mode"), 1);
        assert_eq!(edit_distance("abc", "xyz"), 3);
    }
}
