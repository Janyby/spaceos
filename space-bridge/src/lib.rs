//! SpaceOS HTTP/WebSocket Bridge — exposes SpaceOS IPC via REST and WebSocket.

pub mod app;
pub mod handlers;
pub mod ipc_client;
pub mod mcp;
pub mod mcp_client;
pub mod mission;
pub mod server;
pub mod state;

pub use server::{BridgeConfig, BridgeServer};
pub use state::BridgeState;
