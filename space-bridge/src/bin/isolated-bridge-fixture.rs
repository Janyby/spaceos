//! Laboratory-only Bridge process for cross-process registration verification.
#![cfg(windows)]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use space_bridge::{handlers, BridgeState};
use std::sync::Arc;

fn i4x_login_file(root: &str) -> anyhow::Result<String> {
    let path = std::path::PathBuf::from(std::env::var("SPACEOS_FIXTURE_I4X_LOGIN_FILE")?);
    let parent = std::fs::canonicalize(path.parent().ok_or_else(|| anyhow::anyhow!("login file missing parent"))?)?;
    let allowed = std::fs::canonicalize(format!(r"{root}\test-tmp"))?;
    anyhow::ensure!(parent.starts_with(&allowed) && parent != allowed, "unsafe I.4-X login path");
    let secret = std::fs::read_to_string(path)?;
    anyhow::ensure!(secret.len() >= 32 && secret.len() <= 256, "invalid synthetic login fixture");
    Ok(secret)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).with_writer(std::io::stderr).init();
    let root = std::env::var("SPACEOS_TEST_ISOLATION_ROOT")?;
    anyhow::ensure!(
        root == r"C:\spaceos",
        "fixture outside lab"
    );
    let pipe = std::env::var("SPACEOS_BRIDGE_CORE_PIPE")?;
    anyhow::ensure!(
        pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-process-"),
        "unsafe fixture pipe"
    );
    let port: u16 = std::env::var("SPACEOS_FIXTURE_HTTP_PORT")?.parse()?;
    anyhow::ensure!(
        port != 8765 && port != 9222 && port > 1024,
        "unsafe fixture port"
    );
    let world = std::env::var("SPACEOS_FIXTURE_WORLD")?;
    anyhow::ensure!(world == "synthetic-process-world", "unsafe fixture world");
    anyhow::ensure!(
        std::env::var_os("SPACEOS_CORE_TOKEN").is_some(),
        "missing synthetic transport token"
    );
    space_bridge::ipc_client::configure_isolated_read_service(
        URL_SAFE_NO_PAD.encode([110u8; 32]),
        world.clone(),
    )?;
    space_bridge::ipc_client::configure_isolated_mcp_read_service(URL_SAFE_NO_PAD.encode([112u8;32]), world.clone())?;
    let mut state = BridgeState::new_with_semantic_world_and_registrars(
        Some(&world),
        Some(URL_SAFE_NO_PAD.encode([91u8; 32])),
        None,
    );
    state.set_isolated_agent_origin(port)?;
    if std::env::var("SPACEOS_FIXTURE_I4X").ok().as_deref() == Some("1") {
        state.set_isolated_lab_login(i4x_login_file(&root)?, std::time::Duration::from_secs(300))?;
    }
    let state = Arc::new(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    axum::serve(listener, handlers::create_router(state)).await?;
    Ok(())
}
