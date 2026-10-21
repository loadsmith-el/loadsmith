//! Resolving the run's telemetry settings: CLI flags > the `telemetry:`
//! block > the standard `OTEL_*` variables > defaults. Pure (the
//! environment is passed in), so the precedence is unit-tested.

use std::collections::BTreeMap;
use std::time::Duration;

use loadsmith_config::{ConfigError, OtlpProtocol, SamplingConfig, TelemetryConfig, TelemetrySignal};
use loadsmith_protocol::LogLevel;

/// Telemetry settings given on the command line.
#[derive(Debug, Clone, Default)]
pub struct TelemetryFlags {
    /// `--otel-endpoint`.
    pub endpoint: Option<String>,
    /// `--otel-protocol`.
    pub protocol: Option<OtlpProtocol>,
    /// `--otel-header k=v`, in order (a later key wins).
    pub headers: Vec<(String, String)>,
    /// `--no-telemetry`.
    pub disabled: bool,
}

/// The effective telemetry of a run (when it is on).
#[derive(Debug, Clone, PartialEq)]
pub struct TelemetrySettings {
    pub protocol: OtlpProtocol,
    /// The base endpoint (from the flag, the file, `OTEL_EXPORTER_OTLP_ENDPOINT`
    /// or the protocol's default).
    pub endpoint: String,
    /// The full export URL of each enabled signal (a per-signal
    /// `OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT` may differ from the base).
    pub endpoints: BTreeMap<TelemetrySignal, String>,
    /// Headers (gRPC metadata) sent with every export. Values are secrets.
    pub headers: BTreeMap<String, String>,
    pub timeout: Duration,
    /// Parent-based trace-id ratio, `0.0`–`1.0`.
    pub sampling_ratio: f64,
    pub metrics_interval: Duration,
    pub log_level: LogLevel,
    /// Resource attributes from `OTEL_RESOURCE_ATTRIBUTES`,
    /// `OTEL_SERVICE_NAME` and `telemetry.resource` (in that order of
    /// precedence, lowest first). The engine adds its own on top.
    pub resource: BTreeMap<String, String>,
    /// A PEM CA bundle trusted besides the web roots
    /// (`OTEL_EXPORTER_OTLP_CERTIFICATE`).
    pub certificate: Option<std::path::PathBuf>,
}

impl TelemetrySettings {
    pub fn signal(&self, signal: TelemetrySignal) -> bool {
        self.endpoints.contains_key(&signal)
    }

    /// The settings as a `telemetry:` block, for `--print-resolved-config`
    /// (header values are in the mask list: they print as `***`).
    pub fn as_config(&self) -> TelemetryConfig {
        TelemetryConfig {
            enabled: Some(true),
            endpoint: Some(self.endpoint.clone()),
            protocol: Some(self.protocol),
            headers: self.headers.clone(),
            timeout_seconds: Some(self.timeout.as_secs().max(1)),
            signals: Some(self.endpoints.keys().copied().collect()),
            sampling: Some(SamplingConfig { ratio: self.sampling_ratio }),
            metrics_interval_seconds: Some(self.metrics_interval.as_secs().max(1)),
            log_level: Some(self.log_level.clone()),
            resource: self.resource.iter().map(|(k, v)| (k.clone(), serde_yaml::Value::String(v.clone()))).collect(),
        }
    }
}

/// Why `protocol: grpc` fails in a build without the `grpc` feature.
pub(crate) const NO_GRPC: &str =
    "this loadsmith build has no OTLP/gRPC exporter (built without the `grpc` feature); use http-protobuf";

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_METRICS_INTERVAL: Duration = Duration::from_secs(15);

fn invalid(msg: impl Into<String>) -> ConfigError {
    ConfigError::Validation(msg.into())
}

/// Resolves the settings; `Ok(None)` when telemetry is off. `env` reads an
/// environment variable (empty values count as unset).
pub fn resolve(
    flags: &TelemetryFlags,
    yaml: Option<&TelemetryConfig>,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<TelemetrySettings>, ConfigError> {
    let env = |name: &str| env(name).map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    let empty = TelemetryConfig::default();
    let yaml = yaml.unwrap_or(&empty);

    let env_endpoint = env("OTEL_EXPORTER_OTLP_ENDPOINT");
    let env_signal_endpoint = |s: TelemetrySignal| env(&format!("OTEL_EXPORTER_OTLP_{}_ENDPOINT", upper(s)));
    let any_env_endpoint = env_endpoint.is_some() || ALL.iter().any(|s| env_signal_endpoint(*s).is_some());

    // ── On or off ────────────────────────────────────────────────────────
    let on = if flags.disabled || yaml.enabled == Some(false) {
        false
    } else if flags.endpoint.is_some() || yaml.enabled == Some(true) || yaml.endpoint.is_some() {
        true
    } else if env_bool("OTEL_SDK_DISABLED", env("OTEL_SDK_DISABLED"))? {
        false
    } else {
        any_env_endpoint
    };
    if !on {
        return Ok(None);
    }

    // ── Protocol and endpoints ───────────────────────────────────────────
    let protocol = match (flags.protocol, yaml.protocol, env("OTEL_EXPORTER_OTLP_PROTOCOL")) {
        (Some(p), _, _) | (None, Some(p), _) => p,
        (None, None, Some(raw)) => OtlpProtocol::parse(&raw).ok_or_else(|| {
            invalid(format!("OTEL_EXPORTER_OTLP_PROTOCOL={raw}: expected grpc or http/protobuf"))
        })?,
        (None, None, None) => OtlpProtocol::HttpProtobuf,
    };
    if protocol == OtlpProtocol::Grpc && !cfg!(feature = "grpc") {
        return Err(invalid(NO_GRPC));
    }

    let base = flags.endpoint.clone().or_else(|| yaml.endpoint.clone()).map(|e| e.trim().to_string());
    if let Some(b) = &base {
        check_url("the telemetry endpoint", b)?;
    }

    let signals: Vec<TelemetrySignal> = match &yaml.signals {
        Some(list) if list.is_empty() => return Err(invalid("telemetry.signals: list at least one signal")),
        Some(list) => list.clone(),
        None => ALL
            .iter()
            .copied()
            .filter(|s| env(&format!("OTEL_{}_EXPORTER", upper(*s))).is_none_or(|v| !v.eq_ignore_ascii_case("none")))
            .collect(),
    };
    let mut endpoints = BTreeMap::new();
    for signal in signals {
        let url = match (&base, env_signal_endpoint(signal), &env_endpoint) {
            (Some(b), _, _) => signal_url(protocol, b, signal),
            (None, Some(e), _) => {
                check_url(&format!("OTEL_EXPORTER_OTLP_{}_ENDPOINT", upper(signal)), &e)?;
                e
            }
            (None, None, Some(e)) => {
                check_url("OTEL_EXPORTER_OTLP_ENDPOINT", e)?;
                signal_url(protocol, e, signal)
            }
            (None, None, None) => signal_url(protocol, default_endpoint(protocol), signal),
        };
        endpoints.insert(signal, url);
    }
    if endpoints.is_empty() {
        return Ok(None);
    }

    // ── Headers ──────────────────────────────────────────────────────────
    let mut headers = BTreeMap::new();
    if let Some(raw) = env("OTEL_EXPORTER_OTLP_HEADERS") {
        for (k, v) in parse_pairs(&raw) {
            headers.insert(k.to_ascii_lowercase(), v);
        }
    }
    for (k, v) in yaml.headers.iter().map(|(k, v)| (k.clone(), v.clone())).chain(flags.headers.iter().cloned()) {
        let k = k.trim().to_ascii_lowercase();
        if k.is_empty() || !k.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) {
            return Err(invalid(format!("telemetry header name {k:?} is not a valid header name")));
        }
        headers.insert(k, v);
    }

    // ── Timing, sampling, levels ─────────────────────────────────────────
    let timeout = match (yaml.timeout_seconds, env("OTEL_EXPORTER_OTLP_TIMEOUT")) {
        (Some(0), _) => return Err(invalid("telemetry.timeout_seconds must be at least 1")),
        (Some(s), _) => Duration::from_secs(s),
        (None, Some(ms)) => Duration::from_millis(env_u64("OTEL_EXPORTER_OTLP_TIMEOUT", &ms)?.max(1)),
        (None, None) => DEFAULT_TIMEOUT,
    };
    let metrics_interval = match (yaml.metrics_interval_seconds, env("OTEL_METRIC_EXPORT_INTERVAL")) {
        (Some(0), _) => return Err(invalid("telemetry.metrics_interval_seconds must be at least 1")),
        (Some(s), _) => Duration::from_secs(s),
        (None, Some(ms)) => Duration::from_millis(env_u64("OTEL_METRIC_EXPORT_INTERVAL", &ms)?.max(1)),
        (None, None) => DEFAULT_METRICS_INTERVAL,
    };
    let sampling_ratio = match yaml.sampling {
        Some(SamplingConfig { ratio }) if !(0.0..=1.0).contains(&ratio) => {
            return Err(invalid(format!("telemetry.sampling.ratio {ratio}: expected 0.0 to 1.0")))
        }
        Some(SamplingConfig { ratio }) => ratio,
        None => env_sampler(env("OTEL_TRACES_SAMPLER"), env("OTEL_TRACES_SAMPLER_ARG"))?,
    };
    let log_level = yaml.log_level.clone().unwrap_or(LogLevel::Info);

    // ── Resource ─────────────────────────────────────────────────────────
    let mut resource = BTreeMap::new();
    if let Some(raw) = env("OTEL_RESOURCE_ATTRIBUTES") {
        resource.extend(parse_pairs(&raw));
    }
    if let Some(name) = env("OTEL_SERVICE_NAME") {
        resource.insert("service.name".to_string(), name);
    }
    for (k, v) in &yaml.resource {
        let v = match v {
            serde_yaml::Value::String(s) => s.clone(),
            serde_yaml::Value::Number(n) => n.to_string(),
            serde_yaml::Value::Bool(b) => b.to_string(),
            _ => return Err(invalid(format!("telemetry.resource.{k}: expected a string, number or boolean"))),
        };
        resource.insert(k.clone(), v);
    }

    let endpoint = base.or(env_endpoint).unwrap_or_else(|| default_endpoint(protocol).to_string());
    Ok(Some(TelemetrySettings {
        protocol,
        endpoint,

        endpoints,
        headers,
        timeout,
        sampling_ratio,
        metrics_interval,
        log_level,
        resource,
        certificate: env("OTEL_EXPORTER_OTLP_CERTIFICATE").map(std::path::PathBuf::from),
    }))

}

const ALL: [TelemetrySignal; 3] = [TelemetrySignal::Traces, TelemetrySignal::Metrics, TelemetrySignal::Logs];

fn upper(s: TelemetrySignal) -> &'static str {
    match s {
        TelemetrySignal::Traces => "TRACES",
        TelemetrySignal::Metrics => "METRICS",
        TelemetrySignal::Logs => "LOGS",
    }
}

fn default_endpoint(protocol: OtlpProtocol) -> &'static str {
    match protocol {
        OtlpProtocol::Grpc => "http://localhost:4317",
        OtlpProtocol::HttpProtobuf => "http://localhost:4318",
    }
}

/// The export URL of `signal` from a base endpoint: OTLP/HTTP appends
/// `/v1/<signal>`; gRPC uses the base as is.
fn signal_url(protocol: OtlpProtocol, base: &str, signal: TelemetrySignal) -> String {
    match protocol {
        OtlpProtocol::Grpc => base.to_string(),
        OtlpProtocol::HttpProtobuf => {
            let path = match signal {
                TelemetrySignal::Traces => "v1/traces",
                TelemetrySignal::Metrics => "v1/metrics",
                TelemetrySignal::Logs => "v1/logs",
            };
            format!("{}/{path}", base.trim_end_matches('/'))
        }
    }
}

fn check_url(what: &str, url: &str) -> Result<(), ConfigError> {
    let rest = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://"));
    match rest {
        Some(r) if !r.is_empty() && !r.starts_with('/') => Ok(()),
        _ => Err(invalid(format!("{what} {url:?}: expected an http:// or https:// URL"))),
    }
}

/// `k=v,k2=v2` (W3C baggage-style, as `OTEL_EXPORTER_OTLP_HEADERS` and
/// `OTEL_RESOURCE_ATTRIBUTES` spell them), percent-decoded.
fn parse_pairs(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let k = percent_decode(k.trim());
            (!k.is_empty()).then(|| (k, percent_decode(v.trim())))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 3 <= bytes.len() {
            if let Some(b) = s.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn env_bool(name: &str, value: Option<String>) -> Result<bool, ConfigError> {
    match value.map(|v| v.to_ascii_lowercase()).as_deref() {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(other) => Err(invalid(format!("{name}={other}: expected true or false"))),
    }
}

fn env_u64(name: &str, value: &str) -> Result<u64, ConfigError> {
    value.parse().map_err(|_| invalid(format!("{name}={value}: expected a number of milliseconds")))
}

/// `OTEL_TRACES_SAMPLER(_ARG)` as a parent-based ratio: the run span is
/// always the root, so the `parentbased_*` and plain forms mean the same.
fn env_sampler(sampler: Option<String>, arg: Option<String>) -> Result<f64, ConfigError> {
    let ratio = || -> Result<f64, ConfigError> {
        match &arg {
            None => Ok(1.0),
            Some(a) => match a.parse::<f64>() {
                Ok(r) if (0.0..=1.0).contains(&r) => Ok(r),
                _ => Err(invalid(format!("OTEL_TRACES_SAMPLER_ARG={a}: expected 0.0 to 1.0"))),
            },
        }
    };
    match sampler.map(|s| s.to_ascii_lowercase()).as_deref() {
        None | Some("always_on") | Some("parentbased_always_on") => Ok(1.0),
        Some("always_off") | Some("parentbased_always_off") => Ok(0.0),
        Some("traceidratio") | Some("parentbased_traceidratio") => ratio(),
        Some(other) => Err(invalid(format!(
            "OTEL_TRACES_SAMPLER={other}: supported are always_on, always_off, traceidratio and their parentbased_ forms"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k: &str| map.get(k).cloned()
    }

    fn yaml(text: &str) -> TelemetryConfig {
        serde_yaml::from_str(text).unwrap()
    }

    fn flags_endpoint(e: &str) -> TelemetryFlags {
        TelemetryFlags { endpoint: Some(e.into()), ..Default::default() }
    }

    #[test]
    fn off_by_default_and_on_with_an_endpoint_anywhere() {
        let none = TelemetryFlags::default();
        assert_eq!(resolve(&none, None, &env_of(&[])).unwrap(), None);
        let on_env = resolve(&none, None, &env_of(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318")])).unwrap();
        assert_eq!(on_env.unwrap().endpoints[&TelemetrySignal::Traces], "http://c:4318/v1/traces");
        let on_signal =
            resolve(&none, None, &env_of(&[("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://l:1/custom")])).unwrap();
        assert_eq!(on_signal.unwrap().endpoints[&TelemetrySignal::Logs], "http://l:1/custom");
        let on_yaml = resolve(&none, Some(&yaml("endpoint: http://y:4318/")), &env_of(&[])).unwrap().unwrap();
        assert_eq!(on_yaml.endpoints[&TelemetrySignal::Metrics], "http://y:4318/v1/metrics");
        // enabled: true without any endpoint: the OTel default for the protocol.
        let default = resolve(&none, Some(&yaml("enabled: true\nprotocol: grpc")), &env_of(&[])).unwrap().unwrap();
        assert_eq!(default.endpoints[&TelemetrySignal::Traces], "http://localhost:4317");
    }

    #[test]
    fn precedence_flag_over_yaml_over_env() {
        let env = env_of(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://env:4318"),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer%20env,x-env=1"),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "2500"),
            ("OTEL_METRIC_EXPORT_INTERVAL", "5000"),
            ("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attrs,team=a"),
            ("OTEL_SERVICE_NAME", "from-service-name"),
            ("OTEL_TRACES_SAMPLER", "parentbased_traceidratio"),
            ("OTEL_TRACES_SAMPLER_ARG", "0.25"),
        ]);
        // env only
        let s = resolve(&TelemetryFlags::default(), None, &env).unwrap().unwrap();
        assert_eq!(s.protocol, OtlpProtocol::Grpc);
        assert_eq!(s.endpoints[&TelemetrySignal::Traces], "http://env:4318");
        assert_eq!(s.headers["authorization"], "Bearer env");
        assert_eq!((s.timeout, s.metrics_interval), (Duration::from_millis(2500), Duration::from_secs(5)));
        assert_eq!(s.resource["service.name"], "from-service-name");
        assert_eq!(s.sampling_ratio, 0.25);

        // yaml over env
        let y = yaml(
            "endpoint: http://yaml:4318\nprotocol: http-protobuf\nheaders: { authorization: Bearer yaml }\n\
             timeout_seconds: 3\nmetrics_interval_seconds: 7\nsampling: { ratio: 0.5 }\n\
             resource: { service.name: from-yaml, deployment.environment.name: prod }\n",
        );
        let s = resolve(&TelemetryFlags::default(), Some(&y), &env).unwrap().unwrap();
        assert_eq!(s.protocol, OtlpProtocol::HttpProtobuf);
        assert_eq!(s.endpoints[&TelemetrySignal::Traces], "http://yaml:4318/v1/traces");
        assert_eq!((s.headers["authorization"].as_str(), s.headers["x-env"].as_str()), ("Bearer yaml", "1"));
        assert_eq!((s.timeout, s.metrics_interval), (Duration::from_secs(3), Duration::from_secs(7)));
        assert_eq!(s.sampling_ratio, 0.5);
        assert_eq!((s.resource["service.name"].as_str(), s.resource["team"].as_str()), ("from-yaml", "a"));

        // flags over yaml
        let flags = TelemetryFlags {
            endpoint: Some("http://flag:4317".into()),
            protocol: Some(OtlpProtocol::Grpc),
            headers: vec![("Authorization".into(), "Bearer flag".into())],
            disabled: false,
        };
        let s = resolve(&flags, Some(&y), &env).unwrap().unwrap();
        assert_eq!(s.protocol, OtlpProtocol::Grpc);
        assert_eq!(s.endpoints[&TelemetrySignal::Logs], "http://flag:4317");
        assert_eq!(s.headers["authorization"], "Bearer flag");
    }

    #[test]
    fn off_switches_win() {
        let env = env_of(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://env:4318")]);
        let disabled = TelemetryFlags { disabled: true, ..flags_endpoint("http://flag:1") };
        assert_eq!(resolve(&disabled, Some(&yaml("enabled: true")), &env).unwrap(), None);
        assert_eq!(resolve(&flags_endpoint("http://flag:1"), Some(&yaml("enabled: false")), &env).unwrap(), None);
        let sdk_off = env_of(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://env:4318"), ("OTEL_SDK_DISABLED", "true")]);
        assert_eq!(resolve(&TelemetryFlags::default(), None, &sdk_off).unwrap(), None);
        // The file and the flags are above the environment.
        assert!(resolve(&TelemetryFlags::default(), Some(&yaml("enabled: true")), &sdk_off).unwrap().is_some());
        assert!(resolve(&flags_endpoint("http://flag:1"), None, &sdk_off).unwrap().is_some());
    }

    #[test]
    fn signals_select_endpoints() {
        let s = resolve(&flags_endpoint("http://c:4318"), Some(&yaml("signals: [traces]")), &env_of(&[]))
            .unwrap()
            .unwrap();
        assert!(s.signal(TelemetrySignal::Traces) && !s.signal(TelemetrySignal::Logs));
        let env = env_of(&[("OTEL_LOGS_EXPORTER", "none"), ("OTEL_METRICS_EXPORTER", "otlp")]);
        let s = resolve(&flags_endpoint("http://c:4318"), None, &env).unwrap().unwrap();
        assert!(!s.signal(TelemetrySignal::Logs) && s.signal(TelemetrySignal::Metrics));
    }

    #[test]
    fn bad_values_are_config_errors() {
        let on = flags_endpoint("http://c:4318");
        for (y, env) in [
            ("signals: []", vec![]),
            ("sampling: { ratio: 2 }", vec![]),
            ("timeout_seconds: 0", vec![]),
            ("resource: { a: [1] }", vec![]),
            ("headers: { 'bad name': x }", vec![]),
            ("{}", vec![("OTEL_TRACES_SAMPLER", "jaeger_remote")]),
            ("{}", vec![("OTEL_EXPORTER_OTLP_TIMEOUT", "soon")]),
        ] {
            assert!(resolve(&on, Some(&yaml(y)), &env_of(&env)).is_err(), "{y} {env:?}");
        }
        let env = env_of(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json"), ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c")]);
        assert!(resolve(&TelemetryFlags::default(), None, &env).is_err());
        assert!(resolve(&flags_endpoint("c:4318"), None, &env_of(&[])).is_err());
    }

    #[test]
    fn effective_config_roundtrips_for_printing() {
        let s = resolve(&flags_endpoint("http://c:4318"), Some(&yaml("headers: { a: secret }")), &env_of(&[]))
            .unwrap()
            .unwrap();
        let printed = serde_yaml::to_string(&s.as_config()).unwrap();
        assert!(printed.contains("protocol: http-protobuf") && printed.contains("a: secret"), "{printed}");
    }
}
