//! Resolves a pipeline's `secret('<instance>', '<ref>')` calls by running
//! the provider instances they reference.
//!
//! Runs before the state lock and before any data plugin is spawned. Each
//! referenced instance is spawned once, all of them in parallel: handshake →
//! capabilities (must include `resolve`) → `configure` → `start`/`ready`,
//! then one `resolve` per distinct reference (one at a time, so the
//! `resend` rule replays exactly the request in flight), then the core
//! closes the provider's stdin and waits for its `finished`. fd4 is drained
//! (and masked) for the whole conversation.
//!
//! Each instance has one budget, from spawn to `finished`
//! (`providers.<name>.timeout_seconds`, default 60s). The first instance to
//! fail aborts the others (`cancel`; `error` to the one at fault, as in a
//! run). Every value is added to the [`MaskList`] the moment it arrives, so
//! a provider that logs it is masked too.
//!
//! Exit classes: a missing binary is `PluginNotFound` and an unusable one
//! `PluginIncompatible` (both exit 4); a rejected config is
//! `PluginRejectedConfig`; every other provider failure — a `resolved`
//! error, a crash, a protocol violation, a timeout — is
//! `ConfigError::Provider` (exit 3): the pipeline's configuration could not
//! be resolved, and nothing ran.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use loadsmith_config::{ConfigError, MaskList, ProviderRef, SecretRef, SecretValues, Timeouts};
use loadsmith_protocol::{capabilities, error_codes, FinishedStatus, Message, PluginKind, Resolve};
use loadsmith_transport::ControlReader;
use tokio::io::BufReader;

use crate::{
    discovery::find_plugin_binary,
    error::CoreError,
    events::{drain_events, EventForward},
    lifecycle::{run_handshake_and_configure, start_and_await_ready},
    peer::{describe_exit, finished_failure, Event, Failure, Peer},
    shutdown::{Halt, Shutdown, ShutdownTrigger},
    spawner::spawn_plugin,
};

/// One provider instance to run, with the references to resolve.
#[derive(Debug, Clone)]
pub struct ProviderJob {
    /// The instance name (`providers.<instance>`).
    pub instance: String,
    /// Its block, with `env()`/`file()` already resolved.
    pub provider: ProviderRef,
    /// Distinct references, in document order.
    pub references: Vec<String>,
}

/// Groups `secrets` by instance into jobs, taking each instance's block from
/// `providers`. Instances no call references get no job.
pub fn jobs_for(
    secrets: &[SecretRef],
    providers: &std::collections::BTreeMap<String, ProviderRef>,
) -> Vec<ProviderJob> {
    let mut jobs: Vec<ProviderJob> = Vec::new();
    for call in secrets {
        match jobs.iter_mut().find(|j| j.instance == call.instance) {
            Some(job) => {
                if !job.references.contains(&call.reference) {
                    job.references.push(call.reference.clone());
                }
            }
            None => {
                let Some(provider) = providers.get(&call.instance) else { continue };
                jobs.push(ProviderJob {
                    instance: call.instance.clone(),
                    provider: provider.clone(),
                    references: vec![call.reference.clone()],
                });
            }
        }
    }
    jobs
}

/// Runs every job and returns the resolved values. `timeouts` are the
/// pipeline's (handshake, configure, start, cancel grace); each instance's
/// own budget bounds its whole conversation.
pub async fn resolve_providers(
    jobs: Vec<ProviderJob>,
    plugin_dir: &Path,
    timeouts: &Timeouts,
    mask: &MaskList,
    shutdown: &Shutdown,
) -> Result<SecretValues, CoreError> {
    if let Some(halt) = shutdown.fired() {
        return Err(Failure::halted(halt).error);
    }
    // Find every binary before spawning anything.
    let mut bins: Vec<PathBuf> = Vec::with_capacity(jobs.len());
    for job in &jobs {
        let bin = find_plugin_binary(PluginKind::Provider, &job.provider.plugin_type, plugin_dir)
            .map_err(|e| match e {
                CoreError::PluginNotFound(m) => {
                    CoreError::PluginNotFound(format!("provider instance '{}': {m}", job.instance))
                }
                other => other,
            })?;
        bins.push(bin);
    }
    let count: usize = jobs.iter().map(|j| j.references.len()).sum();
    tracing::info!(
        references = count,
        instances = %jobs.iter().map(|j| j.instance.as_str()).collect::<Vec<_>>().join(", "),
        "resolving secret() values"
    );

    // One stop input for all sessions: an operator signal, or the first
    // session's failure.
    let (trigger, local) = Shutdown::channel();
    let trigger = Arc::new(trigger);
    let forward_signal = {
        let trigger = Arc::clone(&trigger);
        let mut outer = shutdown.clone();
        tokio::spawn(async move {
            let halt = outer.wait().await;
            trigger.trigger(halt);
        })
    };

    let mut set = tokio::task::JoinSet::new();
    for (job, bin) in jobs.into_iter().zip(bins) {
        set.spawn(session(job, bin, *timeouts, mask.clone(), local.clone(), Arc::clone(&trigger)));
    }

    let mut values = SecretValues::new();
    let mut root: Option<CoreError> = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok((instance, resolved))) => {
                for (reference, value) in resolved {
                    values.insert(SecretRef { instance: instance.clone(), reference }, value);
                }
            }
            Ok(Err(SessionError { error, induced })) => {
                if !induced && root.is_none() {
                    root = Some(error);
                }
            }
            Err(e) => {
                root.get_or_insert(CoreError::PluginProcess(format!("provider task: {e}")));
                trigger.trigger(Halt::Abort("a provider task panicked".into()));
            }
        }
    }
    forward_signal.abort();

    if let Some(Halt::Signal(sig)) = shutdown.fired() {
        return Err(CoreError::Interrupted(sig));
    }
    match root {
        Some(e) => Err(e),
        None if values.len() == count => Ok(values),
        None => Err(CoreError::Config(ConfigError::Provider(
            "provider resolution was aborted before every value was resolved".into(),
        ))),
    }
}

struct SessionError {
    error: CoreError,
    /// The session stopped because another one failed (or a signal): not
    /// the root cause.
    induced: bool,
}

/// One instance's whole conversation. Returns its `(reference, value)`s.
async fn session(
    job: ProviderJob,
    bin: PathBuf,
    timeouts: Timeouts,
    mask: MaskList,
    mut shutdown: Shutdown,
    abort: Arc<ShutdownTrigger>,
) -> Result<(String, Vec<(String, String)>), SessionError> {
    // Peer labels are `'static` (they tag every log line and failure); an
    // instance label lives for the rest of the process, a few bytes per run.
    let label: &'static str = Box::leak(format!("provider '{}'", job.instance).into_boxed_str());
    let grace = Duration::from_secs(timeouts.cancel_grace_seconds);
    let fail = |error: CoreError| {
        abort.trigger(Halt::Abort(format!("{label} failed")));
        SessionError { error, induced: false }
    };

    let config = serde_json::to_value(&job.provider.config)
        .map_err(|e| fail(CoreError::Config(ConfigError::Provider(format!("{label}: config: {e}")))))?;
    let mut sp = spawn_plugin(&bin, None).map_err(|e| {
        fail(CoreError::PluginIncompatible(format!("{label}: spawn {}: {e}", bin.display())))
    })?;
    let event = tokio::fs::File::from_std(sp.event_fd_take());
    let drain_mask = mask.clone();
    let drain = tokio::spawn(async move {
        drain_events(
            ControlReader::new(BufReader::new(event)),
            label,
            EventForward { mask: drain_mask, ..Default::default() },
        )
        .await;
    });
    let mut peer = Peer::new(label, sp).with_mask(mask.clone());

    let limit = job.provider.timeout();
    let conversation = converse(&mut peer, config, &job.references, &timeouts, &mut shutdown, &mask);
    let result = match limit {
        None => conversation.await,
        Some(limit) => match tokio::time::timeout(limit, conversation).await {
            Ok(result) => result,
            Err(_) => Err(Failure::by(
                label,
                error_codes::TIMEOUT,
                CoreError::Timeout(format!(
                    "{label} did not finish within {}s (providers.{}.timeout_seconds)",
                    limit.as_secs(),
                    job.instance
                )),
            )),
        },
    };

    let outcome = match result {
        Ok(resolved) => {
            peer.reap(grace).await;
            tracing::debug!(plugin = label, references = resolved.len(), "provider finished");
            Ok((job.instance.clone(), resolved))
        }
        Err(failure) => {
            let induced = failure.culprit.is_none() && shutdown.fired().is_some();
            if !induced {
                abort.trigger(Halt::Abort(format!("{label} failed")));
            }
            let reason = mask.apply(&failure.reason());
            peer.notify_abort(&reason, failure.code).await;
            peer.wind_down(grace).await;
            Err(SessionError { error: classify(label, failure.error), induced })
        }
    };
    let _ = drain.await;
    outcome
}

/// Handshake to `finished`.
async fn converse(
    peer: &mut Peer,
    config: serde_json::Value,
    references: &[String],
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
    mask: &MaskList,
) -> Result<Vec<(String, String)>, Failure> {
    let label = peer.label();
    let info = run_handshake_and_configure(peer, config, PluginKind::Provider, timeouts, shutdown).await?;
    if !info.supports.iter().any(|c| c == capabilities::RESOLVE) {
        return Err(Failure::of(
            label,
            CoreError::PluginIncompatible(format!(
                "{label} ({}) does not advertise the '{}' capability",
                info.name,
                capabilities::RESOLVE
            )),
        ));
    }
    start_and_await_ready(peer, timeouts, shutdown).await?;

    let mut resolved = Vec::with_capacity(references.len());
    for (i, reference) in references.iter().enumerate() {
        let id = i as u64 + 1;
        tracing::debug!(plugin = label, id, reference = %reference, "→ resolve");
        peer.send(Message::Resolve(Resolve { id, reference: reference.clone() })).await?;
        // The instance budget bounds this wait.
        let reply = match peer.recv_expect("resolved", None, shutdown).await? {
            Message::Resolved(r) if r.id == id => r,
            Message::Resolved(r) => {
                if let Some(v) = &r.value {
                    mask.add(v.clone());
                }
                return Err(Failure::by(
                    label,
                    error_codes::PROTOCOL_VIOLATION,
                    CoreError::Protocol(format!("{label}: resolved for id {}, expected id {id}", r.id)),
                ));
            }
            other => {
                return Err(Failure::by(
                    label,
                    error_codes::PROTOCOL_VIOLATION,
                    CoreError::Protocol(mask.apply(&format!("{label}: expected resolved, got {other:?}"))),
                ))
            }
        };
        match (reply.value, reply.error) {
            (Some(value), None) => {
                mask.add(value.clone());
                resolved.push((reference.clone(), value));
            }
            (None, Some(e)) => {
                return Err(Failure::of(
                    label,
                    CoreError::Config(ConfigError::Provider(mask.apply(&format!(
                        "{label} could not resolve '{reference}': {}: {}",
                        e.code, e.message
                    )))),
                ));
            }
            (value, _) => {
                if let Some(v) = value {
                    mask.add(v);
                }
                return Err(Failure::by(
                    label,
                    error_codes::PROTOCOL_VIOLATION,
                    CoreError::Protocol(format!(
                        "{label}: resolved for '{reference}' must carry exactly one of value / error"
                    )),
                ));
            }
        }
    }

    // Everything resolved: end the conversation and wait for `finished`.
    peer.close_input();
    loop {
        let inbound = tokio::select! {
            inbound = peer.next_inbound() => inbound,
            halt = shutdown.wait() => return Err(Failure::halted(halt)),
        };
        match peer.process(inbound).await? {
            Some(Event::Msg(Message::Finished(f))) => {
                return match f.status {
                    FinishedStatus::Success => Ok(resolved),
                    _ => Err(Failure::of(label, finished_failure(label, &f, mask))),
                };
            }
            Some(Event::Eof) => {
                let status = peer.exit_status_soon().await;
                return Err(Failure::of(
                    label,
                    CoreError::PluginFailed(format!("{label} {} without sending finished", describe_exit(status))),
                ));
            }
            Some(Event::Msg(Message::Log(_) | Message::Progress(_) | Message::Pong))
            | Some(Event::ResendRequested)
            | None => {}
            Some(Event::Msg(other)) => {
                let shown = mask.apply(&format!("{other:?}"));
                tracing::warn!(plugin = label, "ignoring unexpected control message: {shown}");
            }
        }
    }
}

/// The exit class of a provider failure: the plugin classes stay (a missing
/// or unusable binary, a rejected config); everything else means the
/// configuration could not be resolved.
fn classify(label: &str, error: CoreError) -> CoreError {
    match error {
        e @ (CoreError::PluginNotFound(_)
        | CoreError::PluginIncompatible(_)
        | CoreError::Interrupted(_)
        | CoreError::Config(_)) => e,
        CoreError::PluginRejectedConfig(m) => CoreError::PluginRejectedConfig(format!("{label}: {m}")),
        other => {
            let text = other.to_string();
            CoreError::Config(ConfigError::Provider(if text.contains(label) {
                text
            } else {
                format!("{label}: {text}")
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(i: &str, reference: &str) -> SecretRef {
        SecretRef { instance: i.into(), reference: reference.into() }
    }

    #[test]
    fn jobs_group_by_instance_and_dedupe() {
        let mut providers = std::collections::BTreeMap::new();
        for name in ["a", "b", "unused"] {
            providers.insert(
                name.to_string(),
                ProviderRef { plugin_type: "file".into(), config: Default::default(), timeout_seconds: None },
            );
        }
        let jobs = jobs_for(&[r("b", "x"), r("a", "y"), r("b", "z"), r("b", "x")], &providers);
        let shape: Vec<_> = jobs.iter().map(|j| (j.instance.as_str(), j.references.clone())).collect();
        assert_eq!(shape, [("b", vec!["x".to_string(), "z".to_string()]), ("a", vec!["y".to_string()])]);
    }

    #[test]
    fn failures_are_config_class_except_plugin_problems() {
        let l = "provider 'p'";
        assert!(matches!(classify(l, CoreError::PluginNotFound("x".into())), CoreError::PluginNotFound(_)));
        assert!(matches!(classify(l, CoreError::PluginIncompatible("x".into())), CoreError::PluginIncompatible(_)));
        for e in [
            CoreError::Timeout("provider 'p' did not finish".into()),
            CoreError::PluginFailed("provider 'p' exited".into()),
            CoreError::Protocol("bad".into()),
        ] {
            match classify(l, e) {
                CoreError::Config(ConfigError::Provider(m)) => assert!(m.contains("provider 'p'"), "{m}"),
                other => panic!("{other:?}"),
            }
        }
        match classify(l, CoreError::PluginRejectedConfig("bad key".into())) {
            CoreError::PluginRejectedConfig(m) => assert_eq!(m, "provider 'p': bad key"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_binary_names_the_instance_and_spawns_nothing() {
        let dir = std::env::temp_dir().join(format!("ls-prov-none-{}", std::process::id()));
        let job = ProviderJob {
            instance: "creds".into(),
            provider: ProviderRef {
                plugin_type: "definitely-not-installed-xyz".into(),
                config: Default::default(),
                timeout_seconds: None,
            },
            references: vec!["a".into()],
        };
        let err = resolve_providers(vec![job], &dir, &Timeouts::default(), &MaskList::new(), &Shutdown::never())
            .await
            .unwrap_err();
        match err {
            CoreError::PluginNotFound(m) => {
                assert!(m.contains("'creds'") && m.contains("loadsmith-provider-definitely-not-installed-xyz"), "{m}")
            }
            other => panic!("{other:?}"),
        }
    }
}
