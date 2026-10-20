pub mod source;
pub mod destination;
pub mod sink;
pub mod provider;
pub mod parser;
pub mod object_source;
pub mod context;
pub mod config;
mod control;

pub use source::{SourcePlugin, run_source};
pub use destination::{DestinationPlugin, run_destination};
pub use sink::{SinkPlugin, run_sink};
pub use provider::{ProviderPlugin, ResolveFailure, run_provider};
pub use parser::{ParserPlugin, run_parser};
pub use object_source::{ObjectSourcePlugin, run_object_source};
pub use context::EventSender;
pub use config::{ConfigRejection, PluginConfig, UnknownKeys};
