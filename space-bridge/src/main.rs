//! SpaceOS Bridge — HTTP/WebSocket gateway for SpaceOS IPC.

use anyhow::Result;
use space_bridge::{BridgeConfig, BridgeServer};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // Load configuration
    let config = BridgeConfig::default();

    // Create and run server
    let server = BridgeServer::new(config);
    server.run().await
}
