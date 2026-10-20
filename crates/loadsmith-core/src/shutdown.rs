//! Run-wide stop requests: a signal from the operator (SIGINT/SIGTERM) or an
//! abort decided by the runner.
//!
//! The core runs as PID 1 in the container images, where the kernel applies
//! no default action to a signal without a handler: an unhandled SIGTERM is
//! simply ignored. [`listen_for_signals`] installs explicit handlers, and the
//! runner turns a signal into the graceful abort path (cancel every plugin,
//! wait the grace period, kill what is left, release the state lock).

use tokio::sync::watch;

/// The operator signal that stopped a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopSignal {
    /// SIGINT (Ctrl+C).
    Interrupt,
    /// SIGTERM (`docker stop`, Kubernetes pod termination).
    Terminate,
}

impl StopSignal {
    pub fn name(&self) -> &'static str {
        match self {
            StopSignal::Interrupt => "SIGINT",
            StopSignal::Terminate => "SIGTERM",
        }
    }

    /// The conventional `128 + signal number` exit code.
    pub fn exit_code(&self) -> u8 {
        match self {
            StopSignal::Interrupt => 130,
            StopSignal::Terminate => 143,
        }
    }
}

/// Why a [`Shutdown`] fired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Halt {
    /// An operator signal.
    Signal(StopSignal),
    /// The run is being aborted for this reason (used to stop the sink stage
    /// when the run fails elsewhere).
    Abort(String),
}

/// A cloneable, cancel-safe "stop now" input. It fires at most once; later
/// triggers are ignored.
#[derive(Debug, Clone)]
pub struct Shutdown {
    rx: watch::Receiver<Option<Halt>>,
}

/// The sending side of a [`Shutdown`].
#[derive(Debug)]
pub struct ShutdownTrigger {
    tx: watch::Sender<Option<Halt>>,
}

impl ShutdownTrigger {
    /// Fires the shutdown, unless it already fired.
    pub fn trigger(&self, halt: Halt) {
        self.tx.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(halt);
                true
            } else {
                false
            }
        });
    }
}

impl Shutdown {
    pub fn channel() -> (ShutdownTrigger, Shutdown) {
        let (tx, rx) = watch::channel(None);
        (ShutdownTrigger { tx }, Shutdown { rx })
    }

    /// A shutdown that never fires.
    pub fn never() -> Shutdown {
        Shutdown::channel().1
    }

    /// The halt, if it already fired.
    pub fn fired(&self) -> Option<Halt> {
        self.rx.borrow().clone()
    }

    /// Waits until the shutdown fires. Cancel-safe. Never resolves for a
    /// shutdown whose trigger was dropped without firing.
    pub async fn wait(&mut self) -> Halt {
        let fired = self.rx.wait_for(|h| h.is_some()).await.ok().and_then(|h| h.clone());
        match fired {
            Some(halt) => halt,
            None => std::future::pending().await,
        }
    }
}

/// Installs SIGINT and SIGTERM handlers and returns the [`Shutdown`] they
/// fire. Must be called inside a Tokio runtime. A second signal while the
/// run is already stopping is logged and otherwise ignored: the abort path is
/// already bounded by the cancel grace period.
pub fn listen_for_signals() -> std::io::Result<Shutdown> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let (trigger, shutdown) = Shutdown::channel();
    tokio::spawn(async move {
        loop {
            let sig = tokio::select! {
                Some(()) = interrupt.recv() => StopSignal::Interrupt,
                Some(()) = terminate.recv() => StopSignal::Terminate,
                else => break,
            };
            if trigger.tx.borrow().is_some() {
                tracing::warn!(signal = sig.name(), "already stopping; waiting for plugins to exit");
            } else {
                tracing::warn!(signal = sig.name(), "received signal; cancelling the run");
            }
            trigger.trigger(Halt::Signal(sig));
        }
    });
    Ok(shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fires_once_and_keeps_the_first_halt() {
        let (trigger, mut shutdown) = Shutdown::channel();
        assert!(shutdown.fired().is_none());
        trigger.trigger(Halt::Signal(StopSignal::Terminate));
        trigger.trigger(Halt::Signal(StopSignal::Interrupt));
        assert_eq!(shutdown.wait().await, Halt::Signal(StopSignal::Terminate));
        assert_eq!(shutdown.fired(), Some(Halt::Signal(StopSignal::Terminate)));
    }

    #[tokio::test]
    async fn never_does_not_fire() {
        let mut shutdown = Shutdown::never();
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(20), shutdown.wait()).await;
        assert!(waited.is_err());
    }

    #[test]
    fn exit_codes_follow_the_128_plus_signal_convention() {
        assert_eq!(StopSignal::Interrupt.exit_code(), 128 + 2);
        assert_eq!(StopSignal::Terminate.exit_code(), 128 + 15);
    }
}
