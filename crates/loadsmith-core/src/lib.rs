pub mod discovery;
pub mod error;
pub mod events;
pub mod fmt;
pub mod lifecycle;
pub mod pump;
pub mod reporter;
pub mod runner;
pub mod sink_supervisor;
pub mod spawner;
pub mod state;
pub mod state_supervisor;
pub mod summary;

pub use runner::{run_pipeline, RunOpts};
pub use summary::Summary;
pub use error::CoreError;
