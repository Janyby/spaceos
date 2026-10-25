//! Bridge State — shared application state.

use axum::extract::ws::Message;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::info;

/// Connected WebSocket client (presence marker; fan-out goes through
/// the broadcast channel, the per-client sender lives in the handler
/// task, so no sender is stored here).
struct WsClient;

#[derive(Clone)]
struct LabSession {
    world_id: String,
    csrf: String,
    expires_at: Instant,
}

/// Bridge agent identity for Core-routed user actions.
#[derive(Debug, Clone)]
pub struct BridgeAgent {
    pub name: String,
    pub token: String,
}

/// Capabilities the bridge agents need for dashboard/MCP actions.
/// `event.record` is required because async terminal events (e.g. scan
/// completion) are recorded under the requesting actor with its token.
const BRIDGE_ACTION_CAPS: &[&str] = space_core::service_registration::DASHBOARD_CAPS;

/// Dashboard HTTP calls always act as `space-bridge`: the actor is fixed
/// here, never taken from client input, so a browser client cannot spoof
/// another identity. (The MCP server acts as `space-bridge-mcp` through
/// the same Core path, with its own registration.)
const DASHBOARD_AGENT: &str = "space-bridge";
const SEMANTIC_AGENT: &str = space_core::semantic::SEMANTIC_ACTOR;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserFailureKind { Invalid, Denied, Missing, Conflict, Unavailable, Upstream, Internal }
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct BrowserFailure {
    pub kind: BrowserFailureKind,
    pub message: String,
}
fn browser_failure(kind: BrowserFailureKind, message: impl Into<String>) -> anyhow::Error {
    BrowserFailure { kind, message: message.into() }.into()
}
/// Classify only the existing scoped Core error contract; unknown errors stay 500.
fn scoped_core_failure(message: &str) -> anyhow::Error {
    use BrowserFailureKind::*;
    let kind = match message {
        "denied: Browser World authentication or capability" => Denied,
        "Browser object unavailable" | "Browser snapshot missing" => Missing,
        "Browser canonical live handle unavailable" | "Browser snapshot changed or forged" | "Browser canonical authority mismatch" | "Browser binding changed" | "ambiguous Browser live handle" => Conflict,
        "missing or ambiguous Browser source" | "Browser instance not registered" | "Browser authority pin missing" | "Browser scoped protocol unavailable" | "Browser live resolution failed" => Unavailable,
        "Browser screenshot failed" | "Browser screenshot response mismatch" | "Browser response authority mismatch" | "empty Browser screenshot" => Upstream,
        _ if message.starts_with("denied:") => Denied,
        _ => Internal,
    };
    browser_failure(kind, message)
}
fn scoped_transport_failure(error: anyhow::Error) -> anyhow::Error {
    if error.to_string().contains("open pipe") || error.chain().any(|e|e.downcast_ref::<std::io::Error>()
        .is_some_and(|e|matches!(e.kind(),std::io::ErrorKind::NotFound|std::io::ErrorKind::ConnectionRefused|std::io::ErrorKind::BrokenPipe))) {
        browser_failure(BrowserFailureKind::Unavailable,"scoped Browser Core transport unavailable; action not retried")
    } else { error }
}

/// Shared bridge state.
#[derive(Clone)]
pub struct BridgeState {
    pub mission_models: Arc<RwLock<Vec<Value>>>,
    /// Explicit synthetic lab executor configuration; absent means disabled.
    pub mission_runtime: Option<crate::agent_gateway::MissionRuntimeConfig>,
    /// One isolated MVP execution lease. No implicit session/attempt creation.
    pub mission_attempt: Arc<Mutex<Option<crate::agent_gateway::BoundAttempt>>>,
    /// Broadcast channel for WebSocket messages.
    ws_tx: broadcast::Sender<Value>,
    /// Active WebSocket clients.
    ws_clients: Arc<Mutex<HashMap<String, WsClient>>>,
    /// Connected component status.
    component_status: Arc<RwLock<HashMap<String, ComponentStatus>>>,
    /// Registered Core agent token (lazy, refreshed on denial).
    agent: Arc<RwLock<Option<BridgeAgent>>>,
    /// Dedicated least-privilege identity for human semantic commands.
    semantic_agent: Arc<RwLock<Option<BridgeAgent>>>,
    /// Server-only, profile-specific registration credentials.
    registrar_semantic: Arc<Option<String>>,
    registrar_dashboard: Arc<Option<String>>,
    browser_world_registrar: Arc<Option<String>>,
    browser_world_agent: Arc<RwLock<Option<BridgeAgent>>>,
    browser_world_mode: bool,
    /// Canonical scope captured at startup. Invalid configuration disables
    /// only semantic endpoints; the rest of Bridge remains available.
    semantic_world_id: Result<String, String>,
    /// Exact browser Origin admitted for agent and semantic writes.
    /// Production retains the fixed loopback origin; isolated fixtures may
    /// select their own loopback port before the router is constructed.
    agent_origin: String,
    /// Transient OpenCode request state for polling/reconnect recovery.
    /// This is transport state only; Core remains the semantic state source.
    agent_runs: Arc<RwLock<HashMap<String, Value>>>,
    /// Opt-in isolated prototype only. No production bootstrap is inferred.
    lab_login: Option<(String, Duration)>,
    lab_sessions: Arc<RwLock<HashMap<String, LabSession>>>,
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
    /// An MVP Gateway restart is an interruption, never an implicit resume.
    /// Only configured-World authoritative attempts are reconciled.
    pub async fn interrupt_orphan_attempts(&self) -> anyhow::Result<()> {
        let response=crate::ipc_client::world_read_request("query_objects",json!({"type":"MissionAttempt"})).await?;
        let objects=response["result"].as_array().ok_or_else(||anyhow::anyhow!("invalid attempt query"))?;
        for object in objects {
            let p=&object["properties"];
            if !matches!(p["status"].as_str(),Some("registered"|"running")) {continue}
            let r=self.semantic_command("mission_execution",json!({"world_id":p["world_id"],"mission_id":p["mission_id"],
                "attempt_id":p["attempt_id"],"agent_id":p["agent_id"],"opencode_session_id":p["opencode_session_id"],
                "admitted":p["admitted"],"model":p["model"],"status":"interrupted","result":"Gateway restarted; execution is not resumed",
                "correlation_id":uuid::Uuid::new_v4().to_string()})).await?;
            anyhow::ensure!(r["ok"]==true,"orphan interruption rejected by Core");
        }
        Ok(())
    }

    pub fn new() -> Self {
        let (ws_tx, _) = broadcast::channel(1024);
        let mode = if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_none() {
            std::env::var("SPACEOS_BROWSER_MODE").ok()
        } else { None };
        let scoped_mode = mode.as_deref().is_some_and(|v|v != "legacy");
        let browser_registrar = if mode.as_deref() == Some("scoped") {
            crate::service_credentials::from_env(space_core::service_registration::BROWSER_WORLD_PROFILE)
        } else { None };
        Self {
            mission_models: Arc::new(RwLock::new(Vec::new())),
            mission_runtime: None,
            mission_attempt: Arc::new(Mutex::new(None)),
            ws_tx,
            ws_clients: Arc::new(Mutex::new(HashMap::new())),
            component_status: Arc::new(RwLock::new(HashMap::new())),
            agent: Arc::new(RwLock::new(None)),
            browser_world_agent: Arc::new(RwLock::new(None)),
            browser_world_registrar: Arc::new(browser_registrar),
            browser_world_mode: scoped_mode,
            semantic_agent: Arc::new(RwLock::new(None)),
            registrar_semantic: Arc::new(crate::service_credentials::from_env(
                space_core::service_registration::SEMANTIC_PROFILE,
            )),
            registrar_dashboard: Arc::new(crate::service_credentials::from_env(
                space_core::service_registration::DASHBOARD_PROFILE,
            )),
            semantic_world_id: space_core::semantic::semantic_world_id_from_env(),
            agent_origin: "http://127.0.0.1:8765".to_string(),
            agent_runs: Arc::new(RwLock::new(HashMap::new())),
            lab_login: None,
            lab_sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn new_with_semantic_world(world_id: Option<&str>) -> Self {
        let mut state = Self::new();
        state.semantic_world_id =
            world_id.map_or_else(
                || {
                    Err("semantic mutation API is unavailable: SPACEOS_SEMANTIC_WORLD_ID is not set"
                    .to_string())
                },
                space_core::semantic::validate_semantic_world_id,
            );
        state
    }

    /// Explicit synthetic credentials for isolated integration fixtures.
    pub fn new_with_semantic_world_and_registrars(
        world_id: Option<&str>,
        semantic: Option<String>,
        dashboard: Option<String>,
    ) -> Self {
        let mut state = Self::new_with_semantic_world(world_id);
        state.registrar_semantic = Arc::new(semantic);
        state.registrar_dashboard = Arc::new(dashboard);
        state
    }

    pub fn semantic_world_id(&self) -> anyhow::Result<&str> {
        self.semantic_world_id
            .as_deref()
            .map_err(|error| anyhow::anyhow!(error.clone()))
    }

    pub fn agent_origin(&self) -> &str {
        &self.agent_origin
    }

    pub fn set_isolated_lab_login(&mut self, secret: String, ttl: Duration) -> anyhow::Result<()> {
        anyhow::ensure!(
            std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(),
            "Browser login prototype requires laboratory guard"
        );
        anyhow::ensure!(!secret.is_empty() && ttl.as_millis() > 0, "invalid laboratory login fixture");
        self.lab_login = Some((secret, ttl));
        Ok(())
    }

    pub fn lab_login_enabled(&self) -> bool {
        self.lab_login.is_some()
    }

    pub async fn create_lab_session(&self, proof: &str, world_id: &str) -> Option<(String, String)> {
        let (expected, ttl) = self.lab_login.as_ref()?;
        if expected.len() != proof.len()
            || expected.as_bytes().iter().zip(proof.as_bytes()).fold(0u8, |diff, (a, b)| diff | (a ^ b)) != 0
            || self.semantic_world_id().ok()? != world_id
        {
            return None;
        }
        let id = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
        let csrf = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
        self.lab_sessions.write().await.insert(id.clone(), LabSession {
            world_id: world_id.to_string(),
            csrf: csrf.clone(),
            expires_at: Instant::now() + *ttl,
        });
        Some((id, csrf))
    }

    pub async fn validate_lab_session(&self, id: &str, csrf: Option<&str>) -> bool {
        let sessions = self.lab_sessions.read().await;
        let Some(session) = sessions.get(id) else { return false; };
        session.expires_at > Instant::now()
            && self.semantic_world_id().ok() == Some(session.world_id.as_str())
            && csrf.is_none_or(|value| value == session.csrf)
    }

    pub async fn revoke_lab_session(&self, id: &str) {
        self.lab_sessions.write().await.remove(id);
    }

    /// Only the guarded laboratory fixture may override the production
    /// default, and only for one exact IPv4 loopback origin.
    pub fn set_isolated_agent_origin(&mut self, port: u16) -> anyhow::Result<()> {
        anyhow::ensure!(
            std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(),
            "isolated Origin requires laboratory guard"
        );
        anyhow::ensure!(port > 1024 && port != 8765 && port != 9222, "unsafe isolated Origin port");
        self.agent_origin = format!("http://127.0.0.1:{port}");
        Ok(())
    }

    async fn ensure_semantic_agent(&self) -> anyhow::Result<BridgeAgent> {
        let world_id = self.semantic_world_id()?.to_string();
        if let Some(agent) = self.semantic_agent.read().await.clone() {
            return Ok(agent);
        }
        let mut slot = self.semantic_agent.write().await;
        if let Some(agent) = slot.clone() {
            return Ok(agent);
        }
        let registrar_token = self
            .registrar_semantic
            .as_ref()
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("semantic registrar credential missing"))?;
        let capabilities = space_core::service_registration::SEMANTIC_CAPS;
        let response = crate::ipc_client::core_request(
            "register_service_actor",
            json!({
                "profile": space_core::service_registration::SEMANTIC_PROFILE,
                "name": SEMANTIC_AGENT,
                "capabilities": capabilities,
                "worlds": [world_id],
                "registrar_token": registrar_token,
            }),
        )
        .await?;
        let token = response
            .get("result")
            .and_then(|result| result.get("token"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "semantic agent registration denied: {}",
                    response["error"].as_str().unwrap_or("unknown Core error")
                )
            })?
            .to_string();
        let agent = BridgeAgent {
            name: SEMANTIC_AGENT.to_string(),
            token,
        };
        *slot = Some(agent.clone());
        Ok(agent)
    }

    pub async fn semantic_command(&self, method: &str, command: Value) -> anyhow::Result<Value> {
        self.semantic_world_id()?;
        let agent = self.ensure_semantic_agent().await?;
        let request = || {
            json!({
                "actor": agent.name,
                "actor_token": agent.token,
                "command": command,
            })
        };
        let first = crate::ipc_client::core_request(method, request()).await?;
        if first.get("ok").and_then(Value::as_bool) == Some(true)
            || !first
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("missing auth")
        {
            return Ok(first);
        }
        *self.semantic_agent.write().await = None;
        let refreshed = self.ensure_semantic_agent().await?;
        crate::ipc_client::core_request(
            method,
            json!({
                "actor": refreshed.name,
                "actor_token": refreshed.token,
                "command": command,
            }),
        )
        .await
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
        let registrar_token = self
            .registrar_dashboard
            .as_ref()
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("dashboard registrar credential missing"))?;
        let resp = crate::ipc_client::core_request(
            "register_service_actor",
            json!({
                "profile": space_core::service_registration::DASHBOARD_PROFILE,
                "name": DASHBOARD_AGENT,
                "capabilities": caps,
                "worlds": [],
                "registrar_token": registrar_token,
            }),
        )
        .await?;
        let token = resp
            .get("result")
            .and_then(|r| r.get("token"))
            .and_then(|t| t.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "dashboard service registration denied: {}",
                    resp["error"].as_str().unwrap_or("unknown Core error")
                )
            })?
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
        if self.browser_world_mode && provider_id == "browser" {
            return self.browser_world_action(action, target).await;
        }
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
    pub fn scoped_browser(&self) -> bool { self.browser_world_mode }

    pub fn browser_capabilities(&self) -> &'static [&'static str] {
        if self.scoped_browser() { &["action.get_tabs", "action.screenshot"] }
        else { &["action.get_tabs", "action.screenshot", "action.navigate", "action.reload", "action.get_page_content", "action.new_tab", "action.activate_tab", "action.close_tab"] }
    }

    pub fn configure_scoped_browser(&mut self, registrar: String) {
        self.browser_world_mode = true;
        self.browser_world_registrar = Arc::new(Some(registrar));
    }

    async fn browser_world_action(&self, action: &str, target: Value) -> anyhow::Result<Value> {
        use BrowserFailureKind::*;
        if !matches!(action, "get_tabs" | "screenshot") { return Err(browser_failure(Denied,"scoped Browser action unavailable")); }
        let world = self.semantic_world_id().map_err(|e|browser_failure(Unavailable,e.to_string()))?;
        let input = if action == "get_tabs" {
            if target != json!({}) { return Err(browser_failure(Invalid,"scoped get_tabs takes no target")); }
            json!({"scope_version":1,"world_id":world})
        } else {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Shot { object_id: String, #[serde(default)] options: space_core::action::BrowserShotOptions }
            let shot: Shot = serde_json::from_value(target).map_err(|e|browser_failure(Invalid,e.to_string()))?;
            shot.options.validate().map_err(|e|browser_failure(Invalid,e.to_string()))?;
            if shot.object_id.trim().is_empty() { return Err(browser_failure(Invalid,"canonical object_id required")); }
            json!({"scope_version":1,"world_id":world,"object_id":shot.object_id,"options":shot.options})
        };
        let agent = self.ensure_browser_world_agent().await?;
        let r = crate::ipc_client::request_action(action, input, "browser", &agent.name, Some(agent.token.clone())).await
            .map_err(scoped_transport_failure)?;
        if r["ok"] != true {
            let message = r["error"].as_str().unwrap_or("Core rejected scoped Browser request");
            // Only this exact pre-dispatch authorization failure can identify a
            // stale token. Re-register for FUTURE requests; never resend this action.
            if message == "denied: Browser World authentication or capability" {
                let mut cache = self.browser_world_agent.write().await;
                if cache.as_ref().is_some_and(|current|current.token == agent.token) { *cache = None; }
                drop(cache);
                self.ensure_browser_world_agent().await?;
                return Err(browser_failure(Denied,"scoped Browser authorization denied; identity refreshed for subsequent requests; action not retried"));
            }
            return Err(scoped_core_failure(message));
        }
        match r["result"]["status"].as_str() {
            Some("Completed") => Ok(r),
            Some("Denied") => Err(browser_failure(Denied,"scoped Browser action denied; action not retried")),
            Some("Failed") => Err(scoped_core_failure(r["result"]["detail"]["error"].as_str().unwrap_or("Core scoped Browser operation failed"))),
            _ => Err(browser_failure(Upstream,"invalid scoped Browser outcome")),
        }
    }

    async fn ensure_browser_world_agent(&self) -> anyhow::Result<BridgeAgent> {
        use space_core::service_registration::{BROWSER_WORLD_ACTOR, BROWSER_WORLD_PROFILE, BROWSER_WORLD_CAPS};
        let world = self.semantic_world_id().map_err(|e|browser_failure(BrowserFailureKind::Unavailable,e.to_string()))?;
        let mut cache = self.browser_world_agent.write().await;
        if cache.is_none() {
            let registrar = self.browser_world_registrar.as_ref().as_ref().ok_or_else(||browser_failure(BrowserFailureKind::Unavailable,"scoped Browser registrar unavailable"))?;
            let r = crate::ipc_client::core_request("register_service_actor", json!({"profile":BROWSER_WORLD_PROFILE,
                "name":BROWSER_WORLD_ACTOR,"worlds":[world],"capabilities":BROWSER_WORLD_CAPS,"registrar_token":registrar})).await.map_err(scoped_transport_failure)?;
            let token = r["result"]["token"].as_str().ok_or_else(||browser_failure(BrowserFailureKind::Unavailable,"scoped Browser registration unavailable"))?;
            *cache = Some(BridgeAgent { name:BROWSER_WORLD_ACTOR.into(),token:token.into() });
        }
        Ok(cache.as_ref().unwrap().clone())
    }

    pub fn ws_receiver(&self) -> broadcast::Receiver<Value> {
        self.ws_tx.subscribe()
    }

    /// Broadcast a message to all WebSocket clients.
    pub fn broadcast(&self, msg: Value) {
        let _ = self.ws_tx.send(msg);
    }

    /// Store and broadcast one transient Agent Gateway update.
    pub async fn update_agent_run(&self, request_id: &str, update: Value) {
        self.agent_runs
            .write()
            .await
            .insert(request_id.to_string(), update.clone());
        self.broadcast(json!({
            "type": "agent_update",
            "request_id": request_id,
            "update": update,
        }));
    }

    pub async fn get_agent_run(&self, request_id: &str) -> Option<Value> {
        self.agent_runs.read().await.get(request_id).cloned()
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
