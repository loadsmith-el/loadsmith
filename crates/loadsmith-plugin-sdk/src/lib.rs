pub mod source;
pub mod destination;
pub mod sink;
pub mod context;
mod control;

pub use source::{SourcePlugin, run_source};
pub use destination::{DestinationPlugin, run_destination};
pub use sink::{SinkPlugin, run_sink};
pub use context::EventSender;
