//! Human-facing progress output on stdout.
//!
//! loadsmith prints two kinds of output:
//!   - stdout: the human report — a header, live transfer progress, and the
//!     final summary box. This is what a person (or the lab) wants to read.
//!   - stderr: diagnostics via `tracing` (handshake, protocol negotiation,
//!     plugin logs), controlled by `--log-level`.
//!
//! Keeping them on separate streams means tools can consume the clean report
//! without the diagnostic noise.

use crate::fmt::fmt_number;

/// Writes the human report to stdout. `Copy` so it can move into a blocking
/// pump thread cheaply.
#[derive(Debug, Clone, Copy)]
pub struct Reporter {
    color: bool,
}

impl Reporter {
    pub fn new(color: bool) -> Self {
        Self { color }
    }

    /// Printed once, before data starts flowing. `parser` and `sink` are
    /// shown only when that stage is attached.
    pub fn header(&self, version: &str, source: &str, parser: Option<&str>, destination: &str, sink: Option<&str>) {
        let arrow = format!(" {} ", self.dim("→"));
        let stages: Vec<&str> =
            std::iter::once(source).chain(parser).chain(std::iter::once(destination)).chain(sink).collect();
        println!();
        println!("{} {}", self.bold("loadsmith"), self.dim(version));
        println!("{}", stages.join(&arrow));
        println!();
    }

    /// Reports the schema of the Arrow stream (the source's, or the parser's) (from its Arrow stream header) — one line
    /// per field with its exact type (`timestamp(us, UTC)`,
    /// `decimal128(12, 2)`, `list<utf8?>`, a nullable child marked `?`) and
    /// `not null` on a non-nullable column, plus a summary line. Emitted
    /// through `tracing` (not stdout), same as `progress`, so each line carries
    /// the same timestamp and formatting as the rest of the run log.
    pub fn schema(&self, fields: &[loadsmith_protocol::Field]) {
        let count = fields.len();
        let unit = if count == 1 { "column" } else { "columns" };
        tracing::info!("schema negotiated — {count} {unit}");
        for field in fields {
            let null = if field.nullable { "" } else { " not null" };
            tracing::info!("{}: {}{null}", field.name, field.field_type);
        }
    }

    /// One progress tick. Called at doubling batch intervals by the pump.
    /// Emitted through `tracing` (not stdout) so it carries the same timestamp
    /// and formatting as the rest of the run log and lines up with it.
    pub fn progress(&self, rows: u64, batches: u64) {
        let unit = if batches == 1 { "batch" } else { "batches" };
        tracing::info!("{} rows · {} {}", fmt_number(rows), fmt_number(batches), unit);
    }

    fn bold(&self, s: &str) -> String {
        if self.color {
            format!("\x1b[1m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }

    fn dim(&self, s: &str) -> String {
        if self.color {
            format!("\x1b[2m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
}
