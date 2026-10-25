use anyhow::Result;
use browser_provider::{
    cdp::DEFAULT_CDP_BASE, ipc as browser_ipc, BrowserProvider, BrowserProviderConfig,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let provider_id = std::env::var("SPACEOS_PROVIDER_ID").unwrap_or_else(|_| "browser".into());
    let cdp_base = std::env::var("SPACEOS_BROWSER_CDP_BASE")
        .or_else(|_| std::env::var("SPACEOS_CDP_BASE"))
        .unwrap_or_else(|_| DEFAULT_CDP_BASE.to_string());
    let poll_secs: u64 = std::env::var("SPACEOS_POLL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let core_pipe = space_core::ipc::configured_provider_pipe("SPACEOS_CORE_PIPE", space_core::ipc::CORE_PIPE)?;
    space_core::ipc::provider_principal_credential()?;
    if std::env::var_os("SPACEOS_BROWSER_OBSERVATION_SOURCE").is_some(){space_core::ipc::provider_instance_credential()?;}
    space_core::dispatch::key_from_env("browser").map_err(anyhow::Error::msg)?;
    if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        let port = cdp_base
            .strip_prefix("http://127.0.0.1:")
            .and_then(|s| s.split('/').next())
            .and_then(|s| s.parse::<u16>().ok());
        anyhow::ensure!(
            matches!(port, Some(p) if p > 1024 && p != 8765 && p != 9222),
            "unsafe isolated CDP endpoint"
        );
    }
    let provider = BrowserProvider::with_config(BrowserProviderConfig {
        cdp_base,
        provider_id,
        poll_interval_secs: poll_secs,
    });
    tracing::info!(
        "browser-provider starting (cdp={}, core_pipe={})",
        provider.cdp_base(),
        core_pipe
    );
    // Serve provider actions (navigate/reload/get_tabs/...) while the
    // standalone runner observes CDP and emits record_event IPC.
    let server_provider = provider.clone();
    tokio::spawn(async move {
        if let Err(e) = browser_ipc::run_server(server_provider).await {
            tracing::error!("browser provider server error: {e}");
        }
    });
    provider.run_standalone(&core_pipe).await
}
