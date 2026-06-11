use anyhow::Result;
use async_trait::async_trait;
use loadsmith_protocol::{
    ConfigureAck, HandshakeAck, Message, PluginKind, SetProtocolVersion,
};
use loadsmith_transport::{ControlReader, ControlWriter};
use tokio::io::BufReader;

const PROTOCOL_VERSION: u32 = 1;

/// Implemented by configuration provider plugins — they load configuration
/// content from an external location (a `file://`, `s3://`, … URI). Distinct
/// from secret resolution, which the core does inline via `{{ }}` templates.
#[async_trait]
pub trait ConfigProviderPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Validates the provider config (e.g., URI scheme).
    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

    /// Fetches the content at the configured URI. Returns raw bytes.
    async fn fetch(&mut self) -> Result<Vec<u8>>;
}

/// Entry point for config-provider plugin binaries. Never returns normally.
pub async fn run_config_provider<P: ConfigProviderPlugin + 'static>(mut plugin: P) -> ! {
    if let Err(e) = run_config_provider_inner(&mut plugin).await {
        eprintln!("[plugin-sdk] fatal error: {e:#}");
        std::process::exit(1);
    }
    std::process::exit(0);
}

async fn run_config_provider_inner<P: ConfigProviderPlugin>(plugin: &mut P) -> Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let mut reader = ControlReader::new(BufReader::new(stdin));
    let mut writer = ControlWriter::new(stdout);

    // Handshake
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before handshake"))?;
    anyhow::ensure!(matches!(msg, Message::Handshake));

    writer
        .send(&Message::HandshakeAck(HandshakeAck {
            protocol_supported_versions: vec![PROTOCOL_VERSION],
            plugin_name: plugin.plugin_name().to_string(),
            plugin_version: plugin.plugin_version().to_string(),
            kind: PluginKind::ConfigProvider,
        }))
        .await?;

    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF after handshake_ack"))?;
    match msg {
        Message::SetProtocolVersion(SetProtocolVersion { protocol_version }) => {
            anyhow::ensure!(protocol_version == PROTOCOL_VERSION);
        }
        other => anyhow::bail!("expected set_protocol_version, got {other:?}"),
    }

    // Capabilities
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF"))?;
    anyhow::ensure!(matches!(msg, Message::CapabilitiesRequest));
    writer
        .send(&Message::CapabilitiesResponse(loadsmith_protocol::CapabilitiesResponse {
            supports: vec!["fetch".into()],
        }))
        .await?;

    // Configure
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before configure"))?;
    let config = match msg {
        Message::Configure(c) => c.config,
        other => anyhow::bail!("expected configure, got {other:?}"),
    };

    match plugin.configure(config).await {
        Ok(()) => writer.send(&Message::ConfigureAck(ConfigureAck::ok())).await?,
        Err(e) => {
            writer
                .send(&Message::ConfigureAck(ConfigureAck::error("CONFIGURE_FAILED", e.to_string())))
                .await?;
            anyhow::bail!("configure rejected: {e}");
        }
    }

    // Start + fetch
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before start"))?;
    anyhow::ensure!(matches!(msg, Message::Start(_)));

    let content = plugin.fetch().await?;

    // Return content as a JSON string in a Finished message's `message` field
    let content_str = String::from_utf8_lossy(&content).to_string();
    use loadsmith_protocol::Finished;
    writer
        .send(&Message::Finished(Finished {
            status: loadsmith_protocol::FinishedStatus::Success,
            rows_read: None,
            batches_read: None,
            rows_written: None,
            batches_written: None,
            code: None,
            message: Some(content_str),
        }))
        .await?;

    Ok(())
}
