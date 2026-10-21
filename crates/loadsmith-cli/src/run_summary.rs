//! The machine-readable run summary of `loadsmith run` (`--output json`,
//! `--summary-file`). Its shape is a covered interface, versioned by its
//! `version` field: see the CLI reference and the stability policy. Fields
//! may be added in a minor release; none is removed or changes meaning
//! within a version. Every field is always present (`null` when unknown).

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use loadsmith_config::{MaskList, PipelineConfig};
use loadsmith_core::{CoreError, RunFacts, Summary, SummaryStatus};
use serde::Serialize;
use serde_json::Value;

use crate::exit::RunOutcome;

/// Version of the run-summary document.
pub const SUMMARY_FORMAT_VERSION: u32 = 1;

/// What the CLI knows about the run before (or without) the core: from the
/// command line and the parsed pipeline file.
#[derive(Debug, Default)]
pub struct RunContext {
    pub pipeline: Option<String>,
    /// `(kind, type)` per stage, in pipeline order.
    pub route: Vec<(&'static str, String)>,
    pub has_state: bool,
    pub has_sink: bool,
    pub dry_run: bool,
    /// The run's trace id, when its traces were exported.
    pub trace_id: Option<String>,
}

impl RunContext {
    pub fn learn(&mut self, config: &PipelineConfig) {
        self.pipeline = Some(config.pipeline.name.clone());
        self.route = vec![("source", config.source.plugin_type.clone())];
        if let Some(parser) = &config.parser {
            self.route.push(("parser", parser.plugin_type.clone()));
        }
        self.route.push(("destination", config.destination.plugin_type.clone()));
        if let Some(sink) = &config.sink {
            self.route.push(("sink", sink.plugin_type.clone()));
        }
        self.has_state = config.state.is_some();
        self.has_sink = config.sink.is_some();
    }
}

#[derive(Debug, Serialize)]
pub struct RunDocument {
    pub version: u32,
    pub loadsmith_version: String,
    pub pipeline: Option<String>,
    pub status: &'static str,
    pub exit_code: u8,
    pub cause: Option<String>,
    pub signal: Option<&'static str>,
    pub dry_run: bool,
    pub route: Vec<RouteEntry>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub batches: u64,
    pub duration_ms: u64,
    pub throughput_rows_per_sec: u64,
    pub started_at: String,
    pub finished_at: String,
    pub state: Option<StateInfo>,
    pub sink: Option<SinkInfo>,
    /// The OpenTelemetry trace of the run (only when traces were exported).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RouteEntry {
    pub kind: &'static str,
    #[serde(rename = "type")]
    pub plugin_type: String,
    pub name: Option<String>,
    pub version: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StateInfo {
    pub watermark_before: Option<Value>,
    pub watermark_after: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct SinkInfo {
    pub objects_delivered: Option<u64>,
}

/// Everything a document is built from.
pub struct RunRecord<'a> {
    pub ctx: &'a RunContext,
    pub facts: &'a RunFacts,
    /// The core's summary, when the run got far enough to produce one.
    pub summary: Option<&'a Summary>,
    /// The command's error, if it failed.
    pub error: Option<&'a anyhow::Error>,
    /// Redacts resolved template values from `cause`.
    pub mask: &'a MaskList,
    pub exit_code: u8,
    pub started: SystemTime,
    pub finished: SystemTime,
}

pub fn build(rec: &RunRecord<'_>) -> RunDocument {
    let summary = rec.summary;
    let signal = summary.and_then(|s| s.signal).or_else(|| {
        rec.error.and_then(|e| {
            e.chain().find_map(|c| match c.downcast_ref::<CoreError>() {
                Some(CoreError::Interrupted(sig)) => Some(*sig),
                _ => None,
            })
        })
    });
    let status = match summary.map(|s| &s.status) {
        Some(SummaryStatus::Success) if rec.exit_code == 0 => "success",
        Some(SummaryStatus::Cancelled) => "cancelled",
        Some(_) => "error",
        None if signal.is_some() => "cancelled",
        None if rec.exit_code == 0 => "success",
        None => "error",
    };
    let cause = summary.and_then(|s| s.root_cause.clone()).or_else(|| {
        // A run that produced a summary failed for the reason in it; the
        // `RunOutcome` error only says "see the summary".
        rec.error
            .filter(|e| e.downcast_ref::<RunOutcome>().is_none())
            .map(|e| format!("{e:#}"))
    })
    .map(|c| rec.mask.apply(&c));

    let route = rec
        .ctx
        .route
        .iter()
        .map(|(kind, plugin_type)| {
            let facts = match *kind {
                "source" => &rec.facts.source,
                "parser" => &rec.facts.parser,
                "destination" => &rec.facts.destination,
                _ => &rec.facts.sink,
            };
            RouteEntry {
                kind,
                plugin_type: plugin_type.clone(),
                name: facts.name.clone(),
                version: facts.version.clone(),
                path: facts.path.as_ref().map(|p| p.display().to_string()),
            }
        })
        .collect();

    let duration = rec.finished.duration_since(rec.started).unwrap_or_default();
    let rows_read = summary.map_or(0, |s| s.rows_read);
    let secs = duration.as_secs_f64();
    let throughput = if secs > 0.0 { (rows_read as f64 / secs) as u64 } else { 0 };

    RunDocument {
        version: SUMMARY_FORMAT_VERSION,
        loadsmith_version: env!("CARGO_PKG_VERSION").to_string(),
        pipeline: rec.ctx.pipeline.clone(),
        status,
        exit_code: rec.exit_code,
        cause,
        signal: signal.map(|s| s.name()),
        dry_run: rec.ctx.dry_run,
        route,
        rows_read,
        rows_written: summary.map_or(0, |s| s.rows_written),
        batches: summary.map_or(0, |s| s.batches_read),
        duration_ms: duration.as_millis() as u64,
        throughput_rows_per_sec: throughput,
        started_at: rfc3339(rec.started),
        finished_at: rfc3339(rec.finished),
        state: rec.ctx.has_state.then(|| StateInfo {
            watermark_before: rec.facts.watermark_before.clone(),
            watermark_after: rec
                .facts
                .watermark_persisted
                .clone()
                .or_else(|| rec.facts.watermark_before.clone()),
        }),
        sink: rec.ctx.has_sink.then(|| SinkInfo {
            objects_delivered: summary.and_then(|s| s.objects_delivered),
        }),
        trace_id: rec.ctx.trace_id.clone(),
    }

}

/// Writes `doc` (pretty-printed) to `path` atomically: a temporary sibling
/// renamed into place, so a reader never sees a partial document.
pub fn write_file(path: &Path, doc: &RunDocument) -> std::io::Result<()> {
    let mut json = serde_json::to_string_pretty(doc).map_err(std::io::Error::other)?;
    json.push('\n');
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, path)
}

/// UTC RFC 3339 with millisecond precision, e.g. `2026-10-20T14:03:08.398Z`.
pub fn rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = d.as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, day) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
        d.subsec_millis()
    )
}

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian
/// (H. Hinnant's `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_formats_utc_with_millis() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        let t = UNIX_EPOCH + Duration::from_millis(1_781_078_400_123);
        assert_eq!(rfc3339(t), "2026-06-10T08:00:00.123Z");
        // A leap day.
        let t = UNIX_EPOCH + Duration::from_secs(951_782_400);
        assert_eq!(rfc3339(t), "2000-02-29T00:00:00.000Z");
    }

    #[test]
    fn a_setup_failure_has_no_counts_and_carries_the_cause() {
        let ctx = RunContext {
            pipeline: Some("p".into()),
            route: vec![("source", "pg".into()), ("destination", "jsonl".into())],
            has_state: true,
            ..Default::default()
        };
        let err = anyhow::Error::new(CoreError::State("locked".into()));
        let doc = build(&RunRecord {
            ctx: &ctx,
            facts: &RunFacts::default(),
            summary: None,
            error: Some(&err),
            mask: &MaskList::new(),
            exit_code: 5,
            started: UNIX_EPOCH,
            finished: UNIX_EPOCH + Duration::from_millis(7),
        });
        let v = serde_json::to_value(&doc).unwrap();
        assert_eq!(v["status"], "error");
        assert_eq!(v["exit_code"], 5);
        assert_eq!(v["cause"], "state error: locked");
        assert_eq!(v["route"][0]["type"], "pg");
        assert_eq!(v["route"][0]["path"], Value::Null);
        assert_eq!(v["duration_ms"], 7);
        assert_eq!(v["state"]["watermark_before"], Value::Null);
        assert_eq!(v["sink"], Value::Null);
    }

    #[test]
    fn a_parser_is_a_route_stage_between_source_and_destination() {
        let yaml = "pipeline:\n  name: p\nsource:\n  type: file\nparser:\n  type: csv\n\
                    destination:\n  type: postgres\n";
        let (config, _) = loadsmith_config::parse_pipeline_yaml(yaml).unwrap();
        let mut ctx = RunContext::default();
        ctx.learn(&config);
        let mut facts = RunFacts::default();
        facts.parser.name = Some("loadsmith-parser-csv".into());
        let doc = build(&RunRecord {
            ctx: &ctx,
            facts: &facts,
            summary: None,
            error: None,
            mask: &MaskList::new(),
            exit_code: 0,
            started: UNIX_EPOCH,
            finished: UNIX_EPOCH,
        });
        let v = serde_json::to_value(&doc).unwrap();
        let kinds: Vec<&str> = v["route"].as_array().unwrap().iter().map(|e| e["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["source", "parser", "destination"]);
        assert_eq!(v["route"][1]["type"], "csv");
        assert_eq!(v["route"][1]["name"], "loadsmith-parser-csv");
    }

    #[test]
    fn the_cause_never_carries_a_resolved_value() {
        let mask = MaskList::new();
        mask.add("hunter22");
        let err = anyhow::Error::new(CoreError::PluginRejectedConfig("bad password hunter22".into()));
        let doc = build(&RunRecord {
            ctx: &RunContext::default(),
            facts: &RunFacts::default(),
            summary: None,
            error: Some(&err),
            mask: &mask,
            exit_code: 3,
            started: UNIX_EPOCH,
            finished: UNIX_EPOCH,
        });
        assert_eq!(doc.cause.as_deref(), Some("plugin rejected its config: bad password ***"));
    }
}
