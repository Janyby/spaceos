//! SpaceOS HTTP/WebSocket Bridge — exposes SpaceOS IPC via REST and WebSocket.

pub mod agent_gateway;
pub mod app;
pub mod handlers;
pub mod ipc_client;
pub mod event_stream;
pub mod mcp;
pub mod mcp_client;
pub mod mission;
pub mod mission_commands;
pub mod server;
mod service_credentials;
pub mod state;

pub use server::{BridgeConfig, BridgeServer};
pub use state::BridgeState;
