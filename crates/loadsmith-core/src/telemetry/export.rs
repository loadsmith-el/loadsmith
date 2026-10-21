//! The OTLP pipelines: exporters (http/protobuf or gRPC, pure-Rust TLS),
//! wrapped so that a failing export is reported once and never fails the
//! run, and so that span texts are masked on the way out.

use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use loadsmith_config::{MaskList, OtlpProtocol, TelemetrySignal};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry::{Array, KeyValue, StringValue, Value};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogBatch, LogExporter, SdkLoggerProvider};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator, Sampler, SdkTracerProvider, SpanData, SpanExporter};
use opentelemetry_sdk::Resource;

use super::settings::TelemetrySettings;

/// The three providers (each only when its signal is on).
pub(super) struct Pipelines {
    pub tracer_provider: Option<SdkTracerProvider>,
    pub meter_provider: Option<SdkMeterProvider>,
    pub logger_provider: Option<SdkLoggerProvider>,
}

/// Builds the OTLP exporters for `settings` and the pipelines around them.
pub(super) fn build(settings: &TelemetrySettings, resource: Resource, mask: &MaskList) -> Result<Pipelines, String> {
    let traces = settings.endpoints.get(&TelemetrySignal::Traces);
    let metrics = settings.endpoints.get(&TelemetrySignal::Metrics);
    let logs = settings.endpoints.get(&TelemetrySignal::Logs);
    let err = |signal: &str, e: &dyn fmt::Display| format!("{signal} exporter: {e}");
    match settings.protocol {
        OtlpProtocol::HttpProtobuf => {
            let client = HeaderClient::new(settings)?;
            let span_exporter = traces
                .map(|url| {
                    opentelemetry_otlp::SpanExporter::builder()
                        .with_http()
                        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                        .with_endpoint(url.clone())
                        .with_timeout(settings.timeout)
                        .with_http_client(client.clone())
                        .build()
                        .map_err(|e| err("traces", &e))
                })
                .transpose()?;
            let metric_exporter = metrics
                .map(|url| {
                    opentelemetry_otlp::MetricExporter::builder()
                        .with_http()
                        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                        .with_endpoint(url.clone())
                        .with_timeout(settings.timeout)
                        .with_http_client(client.clone())
                        .build()
                        .map_err(|e| err("metrics", &e))
                })
                .transpose()?;
            let log_exporter = logs
                .map(|url| {
                    opentelemetry_otlp::LogExporter::builder()
                        .with_http()
                        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                        .with_endpoint(url.clone())
                        .with_timeout(settings.timeout)
                        .with_http_client(client.clone())
                        .build()
                        .map_err(|e| err("logs", &e))
                })
                .transpose()?;
            Ok(pipelines(settings, resource, mask, span_exporter, metric_exporter, log_exporter))
        }
        OtlpProtocol::Grpc => {
            let grpc = GrpcConfig::new(settings)?;
            let span_exporter = traces
                .map(|url| {
                    grpc.apply(opentelemetry_otlp::SpanExporter::builder().with_tonic(), url, settings.timeout)
                        .build()
                        .map_err(|e| err("traces", &e))
                })
                .transpose()?;
            let metric_exporter = metrics
                .map(|url| {
                    grpc.apply(opentelemetry_otlp::MetricExporter::builder().with_tonic(), url, settings.timeout)
                        .build()
                        .map_err(|e| err("metrics", &e))
                })
                .transpose()?;
            let log_exporter = logs
                .map(|url| {
                    grpc.apply(opentelemetry_otlp::LogExporter::builder().with_tonic(), url, settings.timeout)
                        .build()
                        .map_err(|e| err("logs", &e))
                })
                .transpose()?;
            Ok(pipelines(settings, resource, mask, span_exporter, metric_exporter, log_exporter))
        }
    }
}

/// The pipelines around any exporters (the OTLP ones, or in-memory ones in
/// tests).
pub(super) fn pipelines<S, M, L>(
    settings: &TelemetrySettings,
    resource: Resource,
    mask: &MaskList,
    spans: Option<S>,
    metrics: Option<M>,
    logs: Option<L>,
) -> Pipelines
where
    S: SpanExporter + 'static,
    M: PushMetricExporter,
    L: LogExporter + 'static,
{
    let reporter = Reporter::default();
    let tracer_provider = spans.map(|exporter| {
        SdkTracerProvider::builder()
            .with_batch_exporter(Guarded { inner: exporter, reporter: reporter.clone(), mask: mask.clone() })
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(settings.sampling_ratio))))
            .with_id_generator(ForcedIds::default())
            .with_resource(resource.clone())
            .build()
    });
    let meter_provider = metrics.map(|exporter| {
        let reader = PeriodicReader::builder(Guarded { inner: exporter, reporter: reporter.clone(), mask: mask.clone() })
            .with_interval(settings.metrics_interval)
            .build();
        SdkMeterProvider::builder().with_reader(reader).with_resource(resource.clone()).build()
    });
    let logger_provider = logs.map(|exporter| {
        SdkLoggerProvider::builder()
            .with_batch_exporter(Guarded { inner: exporter, reporter: reporter.clone(), mask: mask.clone() })
            .with_resource(resource.clone())
            .build()
    });
    Pipelines { tracer_provider, meter_provider, logger_provider }
}

// ── HTTP: reqwest (blocking) over rustls with the pure-Rust provider ──────────

/// The rustls client config of the exporters: webpki roots, plus the PEM
/// CA bundle of `OTEL_EXPORTER_OTLP_CERTIFICATE` when set.
fn tls_roots(settings: &TelemetrySettings) -> Result<rustls::RootCertStore, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(pem) = ca_pem(settings)? {
        use rustls::pki_types::pem::PemObject;
        for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
            let cert = cert.map_err(|e| format!("OTEL_EXPORTER_OTLP_CERTIFICATE: {e}"))?;
            roots.add(cert).map_err(|e| format!("OTEL_EXPORTER_OTLP_CERTIFICATE: {e}"))?;
        }
    }
    Ok(roots)
}

fn ca_pem(settings: &TelemetrySettings) -> Result<Option<Vec<u8>>, String> {
    match &settings.certificate {
        None => Ok(None),
        Some(path) => std::fs::read(path)
            .map(Some)
            .map_err(|e| format!("OTEL_EXPORTER_OTLP_CERTIFICATE {}: {e}", path.display())),
    }
}

/// The HTTP client of the three exporters. It sets the resolved headers on
/// every request last, so they win over what the OTLP exporter adds from
/// `OTEL_EXPORTER_OTLP_HEADERS` (the file and the flags are above the
/// environment).
#[derive(Clone)]
struct HeaderClient {
    inner: reqwest::blocking::Client,
    headers: Arc<Vec<(http::HeaderName, http::HeaderValue)>>,
}

impl fmt::Debug for HeaderClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Header values are secrets.
        f.debug_struct("HeaderClient").field("headers", &self.headers.len()).finish()
    }
}

impl HeaderClient {
    fn new(settings: &TelemetrySettings) -> Result<Self, String> {
        let mut headers = Vec::new();
        for (k, v) in &settings.headers {
            let name = http::HeaderName::from_bytes(k.as_bytes()).map_err(|e| format!("header {k}: {e}"))?;
            let mut value = http::HeaderValue::from_str(v).map_err(|_| format!("header {k}: invalid value"))?;
            value.set_sensitive(true);
            headers.push((name, value));
        }
        let roots = tls_roots(settings)?;
        let timeout = settings.timeout;
        // The blocking client runs its own runtime: build it off the async
        // runtime's threads.
        let inner = std::thread::spawn(move || {
            loadsmith_tls::install_provider();
            let provider = rustls::crypto::CryptoProvider::get_default()
                .ok_or_else(|| "no rustls crypto provider".to_string())?
                .clone();
            let mut tls = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| e.to_string())?
                .with_root_certificates(roots)
                .with_no_client_auth();
            tls.alpn_protocols = vec![b"http/1.1".to_vec()];
            reqwest::blocking::Client::builder()
                .tls_backend_preconfigured(tls)
                .timeout(timeout)
                .build()
                .map_err(|e| format!("HTTP client: {e}"))
        })
        .join()
        .map_err(|_| "building the HTTP client panicked".to_string())??;
        Ok(Self { inner, headers: Arc::new(headers) })
    }
}

#[async_trait::async_trait]
impl opentelemetry_http::HttpClient for HeaderClient {
    async fn send_bytes(
        &self,
        mut request: http::Request<opentelemetry_http::Bytes>,
    ) -> Result<http::Response<opentelemetry_http::Bytes>, opentelemetry_http::HttpError> {
        for (k, v) in self.headers.iter() {
            request.headers_mut().insert(k.clone(), v.clone());
        }
        self.inner.send_bytes(request).await
    }
}

// ── gRPC: tonic over rustls ───────────────────────────────────────────────────

struct GrpcConfig {
    tls: Option<tonic::transport::ClientTlsConfig>,
    metadata: Arc<Vec<(tonic::metadata::AsciiMetadataKey, tonic::metadata::AsciiMetadataValue)>>,
}

impl GrpcConfig {
    fn new(settings: &TelemetrySettings) -> Result<Self, String> {
        let mut metadata = Vec::new();
        for (k, v) in &settings.headers {
            let key = tonic::metadata::AsciiMetadataKey::from_bytes(k.as_bytes()).map_err(|e| format!("header {k}: {e}"))?;
            let value: tonic::metadata::AsciiMetadataValue =
                v.parse().map_err(|_| format!("header {k}: invalid value"))?;
            metadata.push((key, value));
        }
        let https = settings.endpoints.values().any(|u| u.starts_with("https://"));
        let tls = if https {
            loadsmith_tls::install_provider();
            let mut tls = tonic::transport::ClientTlsConfig::new().with_webpki_roots();
            if let Some(pem) = ca_pem(settings)? {
                tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(pem));
            }
            Some(tls)
        } else {
            None
        };
        Ok(Self { tls, metadata: Arc::new(metadata) })
    }

    fn apply<B: WithTonicConfig + WithExportConfig>(&self, builder: B, url: &str, timeout: Duration) -> B {
        let mut builder = builder.with_endpoint(url).with_timeout(timeout);
        if let Some(tls) = &self.tls {
            if url.starts_with("https://") {
                builder = builder.with_tls_config(tls.clone());
            }
        }
        // Runs after the exporter added OTEL_EXPORTER_OTLP_HEADERS: the
        // resolved headers win.
        let metadata = Arc::clone(&self.metadata);
        builder.with_interceptor(move |mut req: tonic::Request<()>| {
            for (k, v) in metadata.iter() {
                req.metadata_mut().insert(k.clone(), v.clone());
            }
            Ok(req)
        })
    }
}

// ── Guarded exporters ─────────────────────────────────────────────────────────

/// Reports the first failed export of the process, once.
#[derive(Clone, Default, Debug)]
struct Reporter(Arc<AtomicBool>);

impl Reporter {
    fn check(&self, signal: &str, result: OTelSdkResult, mask: &MaskList) -> OTelSdkResult {
        if let Err(e) = &result {
            if !self.0.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    target: "loadsmith::telemetry",
                    "telemetry export failed ({signal}): {}; the run goes on, further export errors are not reported",
                    mask.apply(&e.to_string())
                );
            }
        }
        // Never an error upstream: the SDK would only retry or log it.
        Ok(())
    }
}

#[derive(Debug)]
struct Guarded<E> {
    inner: E,
    reporter: Reporter,
    mask: MaskList,
}

impl<E: SpanExporter> SpanExporter for Guarded<E> {
    async fn export(&self, mut batch: Vec<SpanData>) -> OTelSdkResult {
        if !self.mask.is_empty() {
            for span in &mut batch {
                mask_span(span, &self.mask);
            }
        }
        let result = self.inner.export(batch).await;
        self.reporter.check("traces", result, &self.mask)
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

impl<E: PushMetricExporter> PushMetricExporter for Guarded<E> {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let result = self.inner.export(metrics).await;
        self.reporter.check("metrics", result, &self.mask)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

impl<E: LogExporter> LogExporter for Guarded<E> {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        // Log texts are masked when the records are made.
        let result = self.inner.export(batch).await;
        self.reporter.check("logs", result, &self.mask)
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// Masks every text of a span: name, string attributes, status message,
/// events (core spans come from `tracing` fields the masking of the log
/// lines never saw).
fn mask_span(span: &mut SpanData, mask: &MaskList) {
    if let std::borrow::Cow::Owned(name) = &span.name {
        let masked = mask.apply(name);
        if &masked != name {
            span.name = masked.into();
        }
    }
    mask_attributes(&mut span.attributes, mask);
    if let opentelemetry::trace::Status::Error { description } = &span.status {
        let masked = mask.apply(description);
        if masked != *description {
            span.status = opentelemetry::trace::Status::error(masked);
        }
    }
    for event in span.events.events.iter_mut() {
        let masked = mask.apply(&event.name);
        if masked != event.name {
            event.name = masked.into();
        }
        mask_attributes(&mut event.attributes, mask);
    }
}

pub(super) fn mask_attributes(attrs: &mut [KeyValue], mask: &MaskList) {
    for kv in attrs {
        match &kv.value {
            Value::String(s) => {
                let masked = mask.apply(s.as_str());
                if masked != s.as_str() {
                    kv.value = Value::String(masked.into());
                }
            }
            Value::Array(Array::String(items)) => {
                let masked: Vec<StringValue> = items.iter().map(|s| mask.apply(s.as_str()).into()).collect();
                kv.value = Value::Array(Array::String(masked));
            }
            _ => {}
        }
    }
}

// ── Span ids of re-emitted plugin spans ───────────────────────────────────────

thread_local! {
    static FORCED_SPAN_ID: Cell<Option<SpanId>> = const { Cell::new(None) };
}

/// Random ids, except while [`with_span_id`] runs: a plugin's span keeps the
/// id the plugin gave it (its children and log lines refer to it).
#[derive(Debug, Default)]
struct ForcedIds(RandomIdGenerator);

impl IdGenerator for ForcedIds {
    fn new_trace_id(&self) -> TraceId {
        self.0.new_trace_id()
    }

    fn new_span_id(&self) -> SpanId {
        FORCED_SPAN_ID.with(|f| f.take()).unwrap_or_else(|| self.0.new_span_id())
    }
}

/// Runs `f` (which builds exactly one span) so that span gets `id`.
pub(super) fn with_span_id<T>(id: SpanId, f: impl FnOnce() -> T) -> T {
    FORCED_SPAN_ID.with(|c| c.set(Some(id)));
    let out = f();
    FORCED_SPAN_ID.with(|c| c.set(None));
    out
}
