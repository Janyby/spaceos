use anyhow::Result;
use space_core::{ipc, Storage};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let db_path =
        std::env::var("SPACEOS_DB").unwrap_or_else(|_| "C:\\spaceos\\runtime\\core.db".into());
    if let Some(lab_root) = std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT") {
        let allowed = std::path::PathBuf::from(lab_root).join("test-tmp");
        let candidate = std::path::Path::new(&db_path);
        anyhow::ensure!(
            candidate.is_absolute()
                && candidate.starts_with(&allowed)
                && !candidate.components().any(|part| matches!(part, std::path::Component::ParentDir)),
            "isolated Core database path is outside the laboratory"
        );
    }
    // Fail before opening/migrating SQLite if the transport credential has
    // not been provisioned. This is never a system-principal credential.
    let transport = ipc::try_core_token()?;
    space_core::security_config::validate_present_credential_domains(&transport)?;
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let storage = Storage::open(&db_path)?;
    let state = Arc::new(ipc::CoreState::new_with_storage(storage));
    tracing::info!("space-core listening on {}", ipc::CORE_PIPE);
    ipc::run_server(state, ipc::CORE_PIPE).await
}
