//! Bridge State — shared application state.

use axum::extract::ws::Message;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::info;

/// Connected WebSocket client (presence marker; fan-out goes through
/// the broadcast channel, the per-client sender lives in the handler
/// task, so no sender is stored here).
struct WsClient;

/// Bridge agent identity for Core-routed user actions.
#[derive(Debug, Clone)]
pub struct BridgeAgent {
    pub name: String,
    pub token: String,
}

/// Capabilities the bridge agents need for dashboard/MCP actions.
/// `event.record` is required because async terminal events (e.g. scan
/// completion) are recorded under the requesting actor with its token.
const BRIDGE_ACTION_CAPS: &[&str] = &[
    "action.navigate",
    "action.reload",
    "action.get_tabs",
    "action.get_page_content",
    "action.screenshot",
    "action.new_tab",
    "action.activate_tab",
    "action.close_tab",
    "action.scan",
    "action.read",
    "action.create",
    "action.write",
    "event.record",
    "observation.report",
];

/// Dashboard HTTP calls always act as `space-bridge`: the actor is fixed
/// here, never taken from client input, so a browser client cannot spoof
/// another identity. (The MCP server acts as `space-bridge-mcp` through
/// the same Core path, with its own registration.)
const DASHBOARD_AGENT: &str = "space-bridge";

/// Shared bridge state.
#[derive(Clone)]
pub struct BridgeState {
    /// Broadcast channel for WebSocket messages.
    ws_tx: broadcast::Sender<Value>,
    /// Active WebSocket clients.
    ws_clients: Arc<Mutex<HashMap<String, WsClient>>>,
    /// Connected component status.
    component_status: Arc<RwLock<HashMap<String, ComponentStatus>>>,
    /// Registered Core agent token (lazy, refreshed on denial).
    agent: Arc<RwLock<Option<BridgeAgent>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ComponentStatus {
    pub name: String,
    pub connected: bool,
    pub last_seen: Option<String>,
    pub capabilities: Vec<String>,
    pub error: Option<String>,
}

impl BridgeState {
    pub fn new() -> Self {
        let (ws_tx, _) = broadcast::channel(1024);
        Self {
            ws_tx,
            ws_clients: Arc::new(Mutex::new(HashMap::new())),
            component_status: Arc::new(RwLock::new(HashMap::new())),
            agent: Arc::new(RwLock::new(None)),
        }
    }

    /// Register (once) the bridge agent with Core and cache its token.
    /// All dashboard user actions run as this agent through Core's
    /// `request_action`, so policy, Action* events and SpaceTrace apply.
    pub async fn ensure_agent(&self) -> anyhow::Result<BridgeAgent> {
        if let Some(a) = self.agent.read().await.clone() {
            return Ok(a);
        }
        let mut w = self.agent.write().await;
        if let Some(a) = w.clone() {
            return Ok(a);
        }
        let caps: Vec<serde_json::Value> = BRIDGE_ACTION_CAPS
            .iter()
            .map(|c| serde_json::Value::String(c.to_string()))
            .collect();
        let resp = crate::ipc_client::core_request(
            "register_agent",
            json!({
                "principal": "system",
                "name": DASHBOARD_AGENT,
                "capabilities": caps,
                "worlds": [],
            }),
        )
        .await?;
        let token = resp
            .get("result")
            .and_then(|r| r.get("token"))
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("register_agent denied: {resp}"))?
            .to_string();
        let agent = BridgeAgent {
            name: DASHBOARD_AGENT.to_string(),
            token,
        };
        *w = Some(agent.clone());
        info!("bridge agent registered with Core");
        Ok(agent)
    }

    /// Forget the cached agent token (e.g. Core restarted and rotated it).
    async fn forget_agent(&self) {
        *self.agent.write().await = None;
    }

    fn is_denial(resp: &Value) -> bool {
        resp.get("result")
            .and_then(|r| r.get("status"))
            .and_then(|s| s.as_str())
            == Some("Denied")
    }

    /// Run a user action through Core (`request_action`): policy check,
    /// ActionRequested event, provider execution, terminal Action* event.
    ///
    /// A `Denied` answer is retried once with a freshly registered token:
    /// a rotated token (Core restart) then succeeds, while a genuine
    /// policy denial fails twice and is returned as-is.
    pub async fn core_action(
        &self,
        provider_id: &str,
        action: &str,
        target: Value,
    ) -> anyhow::Result<Value> {
        let agent = self.ensure_agent().await?;
        let first = crate::ipc_client::request_action(
            action,
            target.clone(),
            provider_id,
            &agent.name,
            Some(agent.token),
        )
        .await?;
        if !Self::is_denial(&first) {
            return Ok(first);
        }
        // Possibly a stale token: re-register once and retry a single time.
        self.forget_agent().await;
        let agent = self.ensure_agent().await?;
        crate::ipc_client::request_action(
            action,
            target,
            provider_id,
            &agent.name,
            Some(agent.token),
        )
        .await
    }

    /// Get the WebSocket broadcast receiver.
    pub fn ws_receiver(&self) -> broadcast::Receiver<Value> {
        self.ws_tx.subscribe()
    }

    /// Broadcast a message to all WebSocket clients.
    pub fn broadcast(&self, msg: Value) {
        let _ = self.ws_tx.send(msg);
    }

    /// Register a WebSocket client.
    pub async fn register_ws_client(
        &self,
        id: String,
        _sender: tokio::sync::mpsc::UnboundedSender<Message>,
    ) {
        let mut clients = self.ws_clients.lock().await;
        clients.insert(id.clone(), WsClient);
        info!("WebSocket client connected: {}", id);
    }

    /// Unregister a WebSocket client.
    pub async fn unregister_ws_client(&self, id: &str) {
        let mut clients = self.ws_clients.lock().await;
        clients.remove(id);
        info!("WebSocket client disconnected: {}", id);
    }

    /// Update component status.
    pub async fn update_component(&self, name: String, status: ComponentStatus) {
        let mut components = self.component_status.write().await;
        components.insert(name.clone(), status.clone());
        self.broadcast(json!({
            "type": "component_status",
            "component": name,
            "status": status,
        }));
    }

    /// Get all component statuses.
    pub async fn get_components(&self) -> HashMap<String, ComponentStatus> {
        self.component_status.read().await.clone()
    }

    /// Check core connectivity.
    pub async fn check_core(&self) -> ComponentStatus {
        use crate::ipc_client::list_providers;
        match list_providers().await {
            Ok(_) => ComponentStatus {
                name: "core".to_string(),
                connected: true,
                last_seen: Some(chrono::Utc::now().to_rfc3339()),
                capabilities: vec!["ipc".to_string()],
                error: None,
            },
            Err(e) => ComponentStatus {
                name: "core".to_string(),
                connected: false,
                last_seen: Some(chrono::Utc::now().to_rfc3339()),
                capabilities: vec![],
                error: Some(e.to_string()),
            },
        }
    }

    /// Check browser provider connectivity.
    pub async fn check_browser(&self) -> ComponentStatus {
        use crate::ipc_client::browser_action;
        match browser_action("get_tabs", json!({})).await {
            Ok(resp) => {
                let connected = resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                ComponentStatus {
                    name: "browser-provider".to_string(),
                    connected,
                    last_seen: Some(chrono::Utc::now().to_rfc3339()),
                    capabilities: vec!["observe".to_string(), "navigate".to_string()],
                    error: if connected {
                        None
                    } else {
                        Some(
                            resp.get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string(),
                        )
                    },
                }
            }
            Err(e) => ComponentStatus {
                name: "browser-provider".to_string(),
                connected: false,
                last_seen: Some(chrono::Utc::now().to_rfc3339()),
                capabilities: vec![],
                error: Some(e.to_string()),
            },
        }
    }

    /// Check filesystem provider connectivity.
    /// Uses the side-effect-free `stat` probe (a `scan` probe would spawn
    /// a real background scan job on every health refresh).
    pub async fn check_filesystem(&self) -> ComponentStatus {
        use crate::ipc_client::filesystem_stat;
        match filesystem_stat(".").await {
            Ok(resp) => {
                let connected = resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                ComponentStatus {
                    name: "filesystem-provider".to_string(),
                    connected,
                    last_seen: Some(chrono::Utc::now().to_rfc3339()),
                    capabilities: vec![
                        "scan".to_string(),
                        "watch".to_string(),
                        "format".to_string(),
                        "actions".to_string(),
                    ],
                    error: if connected {
                        None
                    } else {
                        Some(
                            resp.get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string(),
                        )
                    },
                }
            }
            Err(e) => ComponentStatus {
                name: "filesystem-provider".to_string(),
                connected: false,
                last_seen: Some(chrono::Utc::now().to_rfc3339()),
                capabilities: vec![],
                error: Some(e.to_string()),
            },
        }
    }

    /// Refresh all component statuses.
    pub async fn refresh_all(&self) {
        let core = self.check_core().await;
        let browser = self.check_browser().await;
        let fs = self.check_filesystem().await;

        self.update_component("core".to_string(), core).await;
        self.update_component("browser-provider".to_string(), browser)
            .await;
        self.update_component("filesystem-provider".to_string(), fs)
            .await;
    }
}

impl Default for BridgeState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A broadcast message emitted by `update_component` must actually
    /// arrive at a subscribed WS receiver (real event delivery, not just
    /// an HTTP 101 upgrade).
    #[tokio::test]
    async fn component_update_reaches_subscribers() {
        let state = BridgeState::new();
        let mut rx = state.ws_receiver();
        state
            .update_component(
                "core".to_string(),
                ComponentStatus {
                    name: "core".to_string(),
                    connected: true,
                    last_seen: None,
                    capabilities: vec!["ipc".to_string()],
                    error: None,
                },
            )
            .await;
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("broadcast timeout")
            .expect("sender alive");
        assert_eq!(msg["type"], "component_status");
        assert_eq!(msg["component"], "core");
        assert_eq!(msg["status"]["connected"], true);
        // And the state itself reflects it for polling clients.
        assert!(state.get_components().await["core"].connected);
    }

    #[tokio::test]
    async fn reconnect_gets_current_state() {
        // A late subscriber sees current state via get_components (the
        // documented recovery path after a WS reconnect).
        let state = BridgeState::new();
        state
            .update_component(
                "x".to_string(),
                ComponentStatus {
                    name: "x".to_string(),
                    connected: false,
                    last_seen: None,
                    capabilities: vec![],
                    error: Some("down".to_string()),
                },
            )
            .await;
        let comps = state.get_components().await;
        assert_eq!(comps["x"].connected, false);
        assert_eq!(comps["x"].error.as_deref(), Some("down"));
    }
}
