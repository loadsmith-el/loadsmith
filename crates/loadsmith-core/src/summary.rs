use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::fmt::{fmt_duration, fmt_number};

/// What the runner learned about one plugin of the route.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PluginFacts {
    /// The binary that was resolved for it.
    pub path: Option<PathBuf>,
    /// `plugin_name` from its `handshake_ack`.
    pub name: Option<String>,
    /// `plugin_version` from its `handshake_ack`.
    pub version: Option<String>,
}

/// Facts the runner records as it learns them, so a caller can report them
/// even when the run fails before it produces a [`Summary`] (a missing
/// plugin, a rejected config, a held lock).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunFacts {
    pub source: PluginFacts,
    /// The parser, when the pipeline has a `parser:` block.
    pub parser: PluginFacts,
    pub destination: PluginFacts,
    pub sink: PluginFacts,
    /// The incremental watermark loaded at the start (`None`: no `state:`
    /// block, or no state recorded yet).
    pub watermark_before: Option<serde_json::Value>,
    /// The watermark persisted by this run, if it persisted one.
    pub watermark_persisted: Option<serde_json::Value>,
}

/// A shared, cheaply cloned handle on the [`RunFacts`] of one run.
#[derive(Debug, Clone, Default)]
pub struct FactsHandle(Arc<Mutex<RunFacts>>);

impl FactsHandle {
    /// Applies `f` to the facts.
    pub fn update(&self, f: impl FnOnce(&mut RunFacts)) {
        f(&mut self.0.lock().unwrap_or_else(|p| p.into_inner()));
    }

    /// A copy of the facts recorded so far.
    pub fn snapshot(&self) -> RunFacts {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

/// Final execution summary printed (to stdout) after a pipeline run.
#[derive(Debug, Default)]
pub struct Summary {
    pub pipeline_name: String,
    pub status: SummaryStatus,
    pub loadsmith_version: String,
    pub source_name: String,
    /// Parsing stage, when a parser is attached. `None` ⇒ no parser.
    pub parser_name: Option<String>,
    pub destination_name: String,
    /// Delivery stage, when a sink is attached. `None` ⇒ no sink.
    pub sink_name: Option<String>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub batches_read: u64,
    pub batches_written: u64,
    /// Files delivered by the sink, when one is attached.
    pub objects_delivered: Option<u64>,
    pub duration: Duration,
    /// Why the run did not succeed (the first failure observed). `None` on
    /// success.
    pub root_cause: Option<String>,
    /// The operator signal that stopped the run, when status is `Cancelled`.
    pub signal: Option<crate::shutdown::StopSignal>,
    /// Which attempt of the run this is (1-based; above 1 only with
    /// `pipeline.recovery`).
    pub attempts: u32,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum SummaryStatus {
    #[default]
    Success,
    Error,
    Cancelled,
}

impl SummaryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SummaryStatus::Success => "success",
            SummaryStatus::Error => "error",
            SummaryStatus::Cancelled => "cancelled",
        }
    }
}

impl Summary {
    pub fn print(&self) {
        let secs = self.duration.as_secs_f64();
        let throughput = if secs > 0.0 { (self.rows_read as f64 / secs) as u64 } else { 0 };

        let rule = "─".repeat(43);
        println!();
        println!("{rule}");
        println!("Pipeline:     {}", self.pipeline_name);
        if !self.source_name.is_empty() {
            let stages: Vec<&str> = std::iter::once(self.source_name.as_str())
                .chain(self.parser_name.as_deref())
                .chain(std::iter::once(self.destination_name.as_str()))
                .chain(self.sink_name.as_deref())
                .collect();
            println!("Route:        {}", stages.join(" → "));
        }
        println!("Status:       {}", self.status.as_str());
        if self.attempts > 1 {
            println!("Attempt:      {}", self.attempts);
        }
        if let Some(cause) = &self.root_cause {
            println!("Cause:        {cause}");
        }
        println!("Rows read:    {}", fmt_number(self.rows_read));
        println!("Rows written: {}", fmt_number(self.rows_written));
        println!("Batches:      {}", fmt_number(self.batches_read));
        if let Some(objects) = self.objects_delivered {
            println!("Objects sent: {}", fmt_number(objects));
        }
        println!("Duration:     {}", fmt_duration(self.duration));
        if throughput > 0 {
            println!("Throughput:   {} rows/s", fmt_number(throughput));
        }
        println!("{rule}");
    }
}
