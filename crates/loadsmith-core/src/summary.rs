use std::time::Duration;

use crate::fmt::{fmt_duration, fmt_number};

/// Final execution summary printed (to stdout) after a pipeline run.
#[derive(Debug, Default)]
pub struct Summary {
    pub pipeline_name: String,
    pub status: SummaryStatus,
    pub loadsmith_version: String,
    pub source_name: String,
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
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum SummaryStatus {
    #[default]
    Success,
    Error,
    Cancelled,
}

impl SummaryStatus {
    fn as_str(&self) -> &'static str {
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
            match &self.sink_name {
                Some(sink) => println!(
                    "Route:        {} → {} → {}",
                    self.source_name, self.destination_name, sink
                ),
                None => {
                    println!("Route:        {} → {}", self.source_name, self.destination_name)
                }
            }
        }
        println!("Status:       {}", self.status.as_str());
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
