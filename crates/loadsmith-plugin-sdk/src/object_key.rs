//! Object names for sinks: where a delivered file lands, from a `key`
//! template that can name the run.
//!
//! Every sink names objects the same way, so a pipeline moved from one sink
//! to another keeps its layout: `prefix/` + the `key` template, whose
//! placeholders are `{file}` (required: the staged file's name), `{run_id}`,
//! `{run_date}` (`YYYY-MM-DD`, UTC), `{run_time}` (`YYYYMMDDTHHMMSSZ`) and
//! `{pipeline}`, all from the core's `configure.run`. The default,
//! [`ObjectKey::DEFAULT`], gives every run its own prefix, so a later run
//! never replaces an earlier run's files; within a run the name of a file is
//! stable, so a re-delivery replaces the same object.

use anyhow::{bail, Context, Result};

use crate::RunInfo;

/// One segment of a parsed template.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Text(String),
    File,
    RunId,
    RunDate,
    RunTime,
    Pipeline,
}

/// A validated object key template: `prefix/` + `key` with its placeholders.
#[derive(Debug, Clone)]
pub struct ObjectKey {
    segments: Vec<Segment>,
}

impl ObjectKey {
    /// The default `key`: one prefix per run.
    pub const DEFAULT: &'static str = "{run_date}/{run_id}/{file}";

    /// Parses `key` under `prefix` (slashes around the prefix are trimmed).
    /// `run` is the core's `configure.run`: a template that names the run
    /// fails here, at configure, when the core sent none.
    pub fn parse(prefix: Option<&str>, key: &str, run: Option<&RunInfo>) -> Result<Self> {
        let mut segments = Vec::new();
        if let Some(p) = prefix.map(|p| p.trim_matches('/')).filter(|p| !p.is_empty()) {
            segments.push(Segment::Text(format!("{p}/")));
        }
        let mut rest = key.trim_start_matches('/');
        while let Some(open) = rest.find('{') {
            if open > 0 {
                segments.push(Segment::Text(rest[..open].to_string()));
            }
            let close = rest[open..].find('}').map(|c| open + c).with_context(|| format!("`key` {key:?}: unclosed `{{`"))?;
            segments.push(match &rest[open + 1..close] {
                "file" => Segment::File,
                "run_id" => Segment::RunId,
                "run_date" => Segment::RunDate,
                "run_time" => Segment::RunTime,
                "pipeline" => Segment::Pipeline,
                other => bail!(
                    "`key` {key:?}: unknown placeholder `{{{other}}}` (known: {{file}}, {{run_id}}, {{run_date}}, {{run_time}}, {{pipeline}})"
                ),
            });
            rest = &rest[close + 1..];
        }
        if !rest.is_empty() {
            segments.push(Segment::Text(rest.to_string()));
        }
        if !segments.contains(&Segment::File) {
            bail!("`key` {key:?} must contain `{{file}}`, or every file of a run lands on the same object");
        }
        let template = Self { segments };
        if template.needs_run() && run.is_none() {
            bail!(
                "`key` {key:?} names the run, but this Loadsmith core does not send the run (`configure.run`); \
                 upgrade the engine, or use a key without {{run_id}}/{{run_date}}/{{run_time}}/{{pipeline}}"
            );
        }
        Ok(template)
    }

    /// Whether the template names the run (and so needs `configure.run`).
    pub fn needs_run(&self) -> bool {
        self.segments.iter().any(|s| matches!(s, Segment::RunId | Segment::RunDate | Segment::RunTime | Segment::Pipeline))
    }

    /// The object name of the staged file `file`.
    pub fn render(&self, file: &str, run: Option<&RunInfo>) -> Result<String> {
        let run = || run.context("the key names the run, but the core sent no `configure.run`");
        let mut key = String::new();
        for s in &self.segments {
            match s {
                Segment::Text(t) => key.push_str(t),
                Segment::File => key.push_str(file),
                Segment::RunId => key.push_str(&run()?.id),
                Segment::Pipeline => key.push_str(&run()?.pipeline),
                Segment::RunDate => key.push_str(&utc(run()?.started_at_unix_ms).date()),
                Segment::RunTime => key.push_str(&utc(run()?.started_at_unix_ms).time()),
            }
        }
        Ok(key)
    }
}

/// A UTC instant, broken down (proleptic Gregorian calendar).
struct Utc {
    year: i64,
    month: u32,
    day: u32,
    secs_of_day: u64,
}

impl Utc {
    /// `YYYY-MM-DD`.
    fn date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    /// `YYYYMMDDTHHMMSSZ`.
    fn time(&self) -> String {
        let s = self.secs_of_day;
        format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", self.year, self.month, self.day, s / 3600, s / 60 % 60, s % 60)
    }
}

/// Days since 1970-01-01 to a civil date (H. Hinnant's `civil_from_days`).
fn utc(unix_ms: u64) -> Utc {
    let secs = unix_ms / 1000;
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    Utc { year, month, day, secs_of_day: secs % 86_400 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> RunInfo {
        // 2026-10-22T12:00:00Z
        RunInfo { id: "4242-1792670400000".into(), pipeline: "orders".into(), started_at_unix_ms: 1_792_670_400_000 }
    }

    #[test]
    fn keys_name_the_run() {
        let t = ObjectKey::parse(Some("/raw/orders/"), ObjectKey::DEFAULT, Some(&run())).unwrap();
        assert_eq!(t.render("events.parquet", Some(&run())).unwrap(), "raw/orders/2026-10-22/4242-1792670400000/events.parquet");
        let t = ObjectKey::parse(None, "{pipeline}/dt={run_date}/{run_time}-{file}", Some(&run())).unwrap();
        assert_eq!(t.render("f.parquet", Some(&run())).unwrap(), "orders/dt=2026-10-22/20261022T120000Z-f.parquet");
        let t = ObjectKey::parse(None, "{file}", None).unwrap();
        assert!(!t.needs_run());
        assert_eq!(t.render("f", None).unwrap(), "f");
    }

    #[test]
    fn templates_are_validated() {
        let err = |prefix, key, run: Option<&RunInfo>| ObjectKey::parse(prefix, key, run).unwrap_err().to_string();
        assert!(err(None, "{run_id}/x", Some(&run())).contains("{file}"));
        assert!(err(None, "{file}/{date}", Some(&run())).contains("unknown placeholder `{date}`"));
        assert!(err(None, "{file", Some(&run())).contains("unclosed"));
        assert!(err(None, ObjectKey::DEFAULT, None).contains("upgrade the engine"));
    }

    #[test]
    fn utc_dates_are_civil() {
        let at = |ms| {
            let u = utc(ms);
            format!("{} {}", u.date(), u.time())
        };
        assert_eq!(at(0), "1970-01-01 19700101T000000Z");
        assert_eq!(at(951_782_400_000), "2000-02-29 20000229T000000Z");
        assert_eq!(at(1_792_670_400_000 + 3_723_000), "2026-10-22 20261022T130203Z");
        assert_eq!(at(4_107_542_399_000), "2100-02-28 21000228T235959Z");
    }
}
