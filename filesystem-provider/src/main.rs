use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let provider_id = std::env::var("SPACEOS_PROVIDER_ID").unwrap_or_else(|_| "filesystem".into());
    let root = std::env::var("SPACEOS_ROOT")
        .ok()
        .map(std::path::PathBuf::from);
    filesystem_provider::ipc::run_server(provider_id, root).await
}
