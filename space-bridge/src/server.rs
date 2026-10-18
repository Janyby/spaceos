//! Bridge Server — HTTP/WebSocket server for SpaceOS.

use anyhow::Result;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tracing::info;

use crate::handlers::create_router;
use crate::state::BridgeState;

/// Bridge server configuration.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// HTTP server bind address.
    pub bind_addr: SocketAddr,
    /// Enable WebSocket support.
    pub enable_ws: bool,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8765".parse().unwrap(),
            enable_ws: true,
        }
    }
}

/// HTTP/WebSocket bridge server.
pub struct BridgeServer {
    config: BridgeConfig,
    state: Arc<BridgeState>,
    router: Router,
}

impl BridgeServer {
    /// Create a new bridge server.
    pub fn new(config: BridgeConfig) -> Self {
        let state = Arc::new(BridgeState::new());
        let router = create_router(state.clone());
        Self {
            config,
            state,
            router,
        }
    }

    /// Get the shared state.
    pub fn state(&self) -> Arc<BridgeState> {
        self.state.clone()
    }

    /// Run the bridge server.
    pub async fn run(self) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(self.config.bind_addr).await?;
        info!("SpaceOS Bridge listening on {}", self.config.bind_addr);
        info!("  HTTP:  http://{}", self.config.bind_addr);
        if self.config.enable_ws {
            info!("  WS:    ws://{}/ws", self.config.bind_addr);
        }
        info!("  Dashboard: http://{}/", self.config.bind_addr);

        // Start background component health checks
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                state.refresh_all().await;
            }
        });

        // Run server with graceful shutdown
        axum::serve(listener, self.router)
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        info!("SpaceOS Bridge stopped");
        Ok(())
    }
}

/// Wait for shutdown signal.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down..."),
        _ = terminate => info!("Received SIGTERM, shutting down..."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bridge_config_default() {
        let config = BridgeConfig::default();
        assert_eq!(config.bind_addr.to_string(), "127.0.0.1:8765");
        assert!(config.enable_ws);
    }
}
