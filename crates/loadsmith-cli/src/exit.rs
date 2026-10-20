//! The `loadsmith` exit codes — a covered interface, frozen at 1.0 (see the
//! CLI reference and the stability policy). Every failure is classified here
//! from the typed error that caused it.

use std::fmt;

use loadsmith_config::ConfigError;
use loadsmith_core::{CoreError, StopSignal};

/// The process exit code of `loadsmith`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// The command succeeded (`run`: summary status `success`).
    Success,
    /// The run failed: a plugin reported an error, crashed, broke the
    /// protocol or timed out; the data plane failed or stalled; delivery or
    /// state persistence failed. Also any failure not listed below.
    RunFailed,
    /// Bad command-line usage (clap's own code for argument errors).
    Usage,
    /// The pipeline file is unreadable or invalid (YAML, templates,
    /// validation), a plugin rejected its config, or a capability gate failed.
    Config,
    /// A plugin binary was not found, could not be started, or is
    /// incompatible (no common protocol version, wrong kind).
    Plugin,
    /// Incremental state: the lock is held by another run, the state file is
    /// unreadable or from a newer engine, or the schema drifted (`refuse`).
    State,
    /// Stopped by SIGINT (128 + 2) after cancelling the plugins.
    Interrupted,
    /// Stopped by SIGTERM (128 + 15) after cancelling the plugins.
    Terminated,
}

impl Exit {
    pub fn code(self) -> u8 {
        match self {
            Exit::Success => 0,
            Exit::RunFailed => 1,
            Exit::Usage => 2,
            Exit::Config => 3,
            Exit::Plugin => 4,
            Exit::State => 5,
            Exit::Interrupted => 130,
            Exit::Terminated => 143,
        }
    }

    pub fn for_signal(sig: StopSignal) -> Exit {
        match sig {
            StopSignal::Interrupt => Exit::Interrupted,
            StopSignal::Terminate => Exit::Terminated,
        }
    }
}

/// A command-line usage mistake that clap cannot catch (conflicting
/// selectors, a missing argument combination).
#[derive(Debug)]
pub struct UsageError(pub String);

impl fmt::Display for UsageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UsageError {}

/// A pipeline that ran and did not succeed. Its summary (with the root
/// cause) is already on stdout.
#[derive(Debug)]
pub enum RunOutcome {
    Failed { pipeline: String },
    Cancelled { pipeline: String, signal: StopSignal },
}

impl fmt::Display for RunOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunOutcome::Failed { pipeline } => {
                write!(f, "pipeline '{pipeline}' failed (see the summary and the errors above)")
            }
            RunOutcome::Cancelled { pipeline, signal } => {
                write!(f, "pipeline '{pipeline}' was cancelled by {}", signal.name())
            }
        }
    }
}

impl std::error::Error for RunOutcome {}

/// The exit code for a failed command: the first typed error in the chain
/// decides; anything unrecognised is a run failure.
pub fn classify(err: &anyhow::Error) -> Exit {
    for cause in err.chain() {
        if let Some(e) = cause.downcast_ref::<CoreError>() {
            return match e {
                CoreError::PluginNotFound(_) | CoreError::PluginIncompatible(_) => Exit::Plugin,
                CoreError::PluginRejectedConfig(_) | CoreError::Config(_) => Exit::Config,
                CoreError::State(_) => Exit::State,
                CoreError::Interrupted(sig) => Exit::for_signal(*sig),
                CoreError::PluginFailed(_)
                | CoreError::PluginProcess(_)
                | CoreError::Protocol(_)
                | CoreError::Timeout(_)
                | CoreError::Io(_)
                | CoreError::Transport(_) => Exit::RunFailed,
            };
        }
        if cause.downcast_ref::<ConfigError>().is_some() {
            return Exit::Config;
        }
        if cause.downcast_ref::<UsageError>().is_some() {
            return Exit::Usage;
        }
        if let Some(outcome) = cause.downcast_ref::<RunOutcome>() {
            return match outcome {
                RunOutcome::Failed { .. } => Exit::RunFailed,
                RunOutcome::Cancelled { signal, .. } => Exit::for_signal(*signal),
            };
        }
    }
    Exit::RunFailed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(err: impl Into<anyhow::Error>) -> u8 {
        classify(&err.into()).code()
    }

    #[test]
    fn the_table_is_stable() {
        let all = [
            (Exit::Success, 0),
            (Exit::RunFailed, 1),
            (Exit::Usage, 2),
            (Exit::Config, 3),
            (Exit::Plugin, 4),
            (Exit::State, 5),
            (Exit::Interrupted, 130),
            (Exit::Terminated, 143),
        ];
        for (exit, expected) in all {
            assert_eq!(exit.code(), expected, "{exit:?}");
        }
    }

    #[test]
    fn core_errors_map_to_their_class() {
        assert_eq!(code(CoreError::PluginNotFound("x".into())), 4);
        assert_eq!(code(CoreError::PluginIncompatible("x".into())), 4);
        assert_eq!(code(CoreError::PluginRejectedConfig("x".into())), 3);
        assert_eq!(code(CoreError::Config(ConfigError::Validation("x".into()))), 3);
        assert_eq!(code(CoreError::State("locked".into())), 5);
        assert_eq!(code(CoreError::Interrupted(StopSignal::Interrupt)), 130);
        assert_eq!(code(CoreError::Interrupted(StopSignal::Terminate)), 143);
        assert_eq!(code(CoreError::PluginFailed("x".into())), 1);
        assert_eq!(code(CoreError::Timeout("x".into())), 1);
        assert_eq!(code(CoreError::Protocol("x".into())), 1);
    }

    #[test]
    fn other_typed_errors_and_context() {
        assert_eq!(code(ConfigError::Template("x".into())), 3);
        assert_eq!(code(UsageError("x".into())), 2);
        assert_eq!(code(RunOutcome::Failed { pipeline: "p".into() }), 1);
        assert_eq!(
            code(RunOutcome::Cancelled { pipeline: "p".into(), signal: StopSignal::Terminate }),
            143
        );
        // Context added on top does not hide the typed cause.
        let err = anyhow::Error::new(CoreError::State("x".into())).context("while running");
        assert_eq!(classify(&err).code(), 5);
        assert_eq!(code(anyhow::anyhow!("something else")), 1);
    }
}
