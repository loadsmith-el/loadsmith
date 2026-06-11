pub mod source;
pub mod destination;
pub mod sink;
pub mod config_provider;
pub mod context;

pub use source::{SourcePlugin, run_source};
pub use destination::{DestinationPlugin, run_destination};
pub use sink::{SinkPlugin, run_sink};
pub use config_provider::{ConfigProviderPlugin, run_config_provider};
pub use context::EventSender;
