//! HTTP Handlers — REST and WebSocket endpoints.
//!
//! Every state-changing operation runs through Core `request_action` as a
//! registered bridge agent (see `BridgeState::core_action`), so policy is
//! checked before execution and the ActionRequested -> terminal Action*
//! event chain is stored with SpaceTrace correlation. Error responses are
//! always JSON (never an empty body).

use axum::{
    extract::{Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{Json, Response},
    routing::{get, post},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use crate::state::BridgeState;
use space_core::query::EventQuery;

/// API result with a JSON error body (empty bodies break `response.json()`).
type ApiResult = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn api_err(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "ok": false,
            "error": msg.into(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
}

/// Map a Core/IPC failure to HTTP: timeouts are 504, everything else 500.
fn core_err(e: anyhow::Error) -> (StatusCode, Json<Value>) {
    if e.to_string().contains("timed out") {
        api_err(StatusCode::GATEWAY_TIMEOUT, e.to_string())
    } else {
        api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

/// Dashboard HTTP calls always act as the `space-bridge` agent: the actor
/// is fixed server-side inside `BridgeState::core_action`, never taken
/// from client input, so a browser client cannot spoof another identity.
/// (The MCP server acts as `space-bridge-mcp` through the same Core path.)

/// Create the HTTP router with all routes.
pub fn create_router(state: Arc<BridgeState>) -> Router {
    let cors = CorsLayer::permissive();

    Router::new()
        // Static files: `/` is the World (spatial UI, the default entry
        // point); `/index.html` stays the full Shell. The launcher opens
        // `/` and therefore lands in World without any redirect.
        .route_service("/", ServeFile::new("space-bridge/static/world.html"))
        .fallback_service(ServeDir::new("space-bridge/static"))
        // API routes
        .route("/api/health", get(health_check))
        .route("/api/components", get(get_components))
        .route("/api/components/refresh", post(refresh_components))
        .route("/api/providers", get(get_providers))
        .route("/api/providers/:id", get(get_provider))
        .route("/api/providers/:id/actions", post(execute_provider_action))
        .route("/api/browser/tabs", get(get_browser_tabs))
        .route("/api/browser/navigate", post(navigate_browser))
        .route("/api/browser/reload", post(reload_browser))
        .route("/api/browser/content", post(browser_content))
        .route("/api/browser/new-tab", post(browser_new_tab))
        .route("/api/browser/activate", post(browser_activate))
        .route("/api/browser/close", post(browser_close))
        .route("/api/filesystem/roots", get(filesystem_roots))
        .route("/api/filesystem/list", post(filesystem_list))
        .route("/api/filesystem/search", post(filesystem_search))
        .route("/api/filesystem/read", post(filesystem_read))
        .route("/api/filesystem/create", post(filesystem_create))
        .route("/api/filesystem/write", post(filesystem_write))
        .route("/api/filesystem/scan", post(scan_filesystem))
        .route("/api/objects/query", post(query_objects_endpoint))
        .route("/api/relations/query", post(query_relations_endpoint))
        .route("/api/events", get(query_events_endpoint))
        .route("/api/context", get(context_endpoint))
        .route("/api/bridge/refresh", post(refresh_bridge))
        .route("/api/actions/recent", get(actions_recent))
        .route("/api/actions/:id", get(action_status))
        .route(
            "/api/missions",
            post(create_mission_ep).get(list_missions_ep),
        )
        .route("/api/missions/:id", get(get_mission_ep))
        .route("/api/missions/:id/plan", post(plan_mission_ep))
        .route("/api/missions/:id/transition", post(transition_mission_ep))
        .route("/api/missions/:id/verify", post(verify_mission_ep))
        .route(
            "/api/missions/:id/steps/:step_id/transition",
            post(transition_step_ep),
        )
        .route(
            "/api/missions/:id/steps/:step_id/proposal",
            post(propose_step_change_ep),
        )
        .route("/api/external-mcp/call", post(external_mcp_call_ep))
        // WebSocket
        .route("/ws", get(websocket_handler))
        .layer(cors)
        .with_state(state)
}

/// Health check endpoint.
async fn health_check() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "space-bridge",
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

/// Get all component statuses.
async fn get_components(State(state): State<Arc<BridgeState>>) -> Json<Value> {
    let components = state.get_components().await;
    Json(json!({
        "components": components,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

/// Refresh all component statuses.
async fn refresh_components(State(state): State<Arc<BridgeState>>) -> Json<Value> {
    state.refresh_all().await;
    let components = state.get_components().await;
    Json(json!({
        "components": components,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

/// Get all providers.
async fn get_providers() -> ApiResult {
    match crate::ipc_client::list_providers().await {
        Ok(providers) => Ok(Json(json!({
            "providers": providers,
            "count": providers.len(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Get provider by ID.
async fn get_provider(axum::extract::Path(id): axum::extract::Path<String>) -> ApiResult {
    match crate::ipc_client::provider_info(&id).await {
        Ok(Some(provider)) => Ok(Json(json!({
            "provider": provider,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Ok(None) => Err(api_err(
            StatusCode::NOT_FOUND,
            format!("unknown provider: {id}"),
        )),
        Err(e) => Err(core_err(e)),
    }
}

/// Execute an action on a provider.
///
/// Runs through Core `request_action` as the registered bridge agent, so
/// policy is checked before execution and the full ActionRequested ->
/// terminal Action* event chain (with SpaceTrace correlation) is stored.
async fn execute_provider_action(
    axum::extract::Path(provider_id): axum::extract::Path<String>,
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    if provider_id != "browser" && provider_id != "filesystem" {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            format!("unknown provider: {provider_id}"),
        ));
    }
    let action = payload
        .get("action")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing action"))?;
    let target = payload.get("target").cloned().unwrap_or(json!({}));

    match state.core_action(&provider_id, action, target).await {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Get browser tabs (via Core + policy).
async fn get_browser_tabs(State(state): State<Arc<BridgeState>>) -> ApiResult {
    match state.core_action("browser", "get_tabs", json!({})).await {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Navigate browser to URL (via Core + policy).
async fn navigate_browser(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let url = payload
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing url"))?;
    if url.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "empty url"));
    }
    match state
        .core_action("browser", "navigate", json!({ "url": url }))
        .await
    {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Open a URL in a NEW browser tab (via Core + policy). Never retargets
/// an existing user tab: creation is the only way the Shell opens pages
/// it did not already own.
async fn browser_new_tab(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let url = payload
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("about:blank");
    if url.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "empty url"));
    }
    match state
        .core_action("browser", "new_tab", json!({ "url": url }))
        .await
    {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Activate (focus) one tab by CDP target id (via Core + policy).
async fn browser_activate(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let id = payload
        .get("target_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing target_id"))?;
    if id.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "empty target_id"));
    }
    match state
        .core_action("browser", "activate_tab", json!({ "target_id": id }))
        .await
    {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Close one tab by CDP target id (via Core + policy). The UI confirms
/// with the user first; the provider never auto-closes anything.
async fn browser_close(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let id = payload
        .get("target_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing target_id"))?;
    if id.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "empty target_id"));
    }
    match state
        .core_action("browser", "close_tab", json!({ "target_id": id }))
        .await
    {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Reload current browser tab (via Core + policy).
async fn reload_browser(State(state): State<Arc<BridgeState>>) -> ApiResult {
    match state.core_action("browser", "reload", json!({})).await {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Current page DOM text excerpt (via Core + policy). No embedded page
/// rendering: arbitrary sites forbid framing (X-Frame-Options/CSP), so
/// the Shell shows real tabs plus a text preview instead of a fake
/// in-workspace browser. For live pixels use WebView2 (documented
/// boundary, not implemented).
async fn browser_content(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let max = payload
        .get("max_chars")
        .and_then(|v| v.as_u64())
        .unwrap_or(4000)
        .min(20000) as usize;
    match state
        .core_action("browser", "get_page_content", json!({}))
        .await
    {
        Ok(resp) => {
            let doc = resp
                .get("result")
                .and_then(|x| x.get("detail"))
                .and_then(|d| d.get("document"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let text = crate::app::strip_tags(doc);
            let excerpt: String = text.chars().take(max).collect();
            Ok(Json(json!({
                "result": resp,
                "length": text.len(),
                "truncated": text.len() > max,
                "excerpt": excerpt,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })))
        }
        Err(e) => Err(core_err(e)),
    }
}

/// Allowed filesystem roots of the provider daemon.
async fn filesystem_roots() -> ApiResult {
    match crate::ipc_client::filesystem_roots().await {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// List stored objects under one directory (via object store, no scan).
async fn filesystem_list(Json(payload): Json<Value>) -> ApiResult {
    let dir = payload
        .get("dir")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing dir"))?;
    if dir.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "empty dir"));
    }
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(200)
        .min(1000) as usize;
    match crate::ipc_client::query_objects(json!({"provider_id": "filesystem"})).await {
        Ok(objects) => {
            let (items, truncated) = crate::app::filter_by_dir(objects, dir, limit);
            Ok(Json(json!({
                "dir": dir,
                "items": items,
                "count": items.len(),
                "truncated": truncated,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })))
        }
        Err(e) => Err(core_err(e)),
    }
}

/// Search stored objects by name substring (same query the Dashboard uses).
async fn filesystem_search(Json(payload): Json<Value>) -> ApiResult {
    let q = payload
        .get("q")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing q"))?;
    if q.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "empty q"));
    }
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(100)
        .min(500) as usize;
    match crate::ipc_client::query_objects(json!({"provider_id": "filesystem", "path_contains": q}))
        .await
    {
        Ok(objects) => {
            let (items, truncated) = crate::app::summarize(objects, limit);
            Ok(Json(json!({
                "q": q,
                "items": items,
                "count": items.len(),
                "truncated": truncated,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })))
        }
        Err(e) => Err(core_err(e)),
    }
}

/// Read a text file (via Core + policy + roots gate).
async fn filesystem_read(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let path = payload
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing path"))?;
    match state
        .core_action("filesystem", "read", json!({ "path": path }))
        .await
    {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Create a text file (via Core + policy + roots gate). Fails loudly when
/// the file already exists — edit it through `write` instead.
async fn filesystem_create(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let path = payload
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing path"))?;
    let content = payload
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match state
        .core_action(
            "filesystem",
            "create",
            json!({ "path": path, "content": content }),
        )
        .await
    {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Overwrite an existing text file (via Core + policy + roots gate).
async fn filesystem_write(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let path = payload
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing path"))?;
    let content = payload
        .get("content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing content"))?;
    let mut target = json!({ "path": path, "content": content });
    // Optional optimistic-concurrency stamp from a previous read
    // (timestamp + size: Windows mtime can stall across rapid writes).
    if let Some(stamp) = payload.get("expected_modified").and_then(|v| v.as_str()) {
        target["expected_modified"] = json!(stamp);
    }
    if let Some(size) = payload.get("expected_size").and_then(|v| v.as_u64()) {
        target["expected_size"] = json!(size);
    }
    match state.core_action("filesystem", "write", target).await {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Scan filesystem (via Core + policy).
///
/// Long scans answer `Started` with a job id immediately; the terminal
/// `ActionCompleted`/`ActionFailed` event lands under the same action id
/// as correlation_id — poll `/api/actions/:id` for it.
async fn scan_filesystem(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let path = payload.get("path").and_then(|v| v.as_str()).unwrap_or(".");
    match state
        .core_action("filesystem", "scan", json!({ "path": path }))
        .await
    {
        Ok(resp) => Ok(Json(json!({
            "result": resp,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Lifecycle view of one action: all Core events correlated to it
/// (ActionRequested -> ActionCompleted/Failed/Denied, oldest first),
/// plus a computed Queued/Running/Completed/Failed/Denied/Unknown status.
async fn action_status(axum::extract::Path(id): axum::extract::Path<String>) -> ApiResult {
    match crate::ipc_client::action_events(&id).await {
        Ok(events) => {
            let summary = crate::app::action_summary(&id, &events);
            Ok(Json(json!({
                "action_id": id,
                "count": events.len(),
                "status": summary["status"],
                "events": events,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })))
        }
        Err(e) => Err(core_err(e)),
    }
}

/// Recent action history, newest first (shared by Dashboard and MCP).
async fn actions_recent(Query(query): Query<HashMap<String, String>>) -> ApiResult {
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(50)
        .min(200);
    match crate::ipc_client::core_request("query_events", json!({"limit": 500})).await {
        Ok(resp) => {
            let events: Vec<Value> = resp
                .get("result")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let rows = crate::app::action_history(events, limit);
            Ok(Json(json!({
                "actions": rows,
                "count": rows.len(),
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })))
        }
        Err(e) => Err(core_err(e)),
    }
}

/// Query objects.
async fn query_objects_endpoint(Json(payload): Json<Value>) -> ApiResult {
    let query = payload.get("query").cloned().unwrap_or(json!({}));
    match crate::ipc_client::query_objects(query).await {
        Ok(objects) => Ok(Json(json!({
            "objects": objects,
            "count": objects.len(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Refresh all bridge components.
async fn refresh_bridge(State(state): State<Arc<BridgeState>>) -> Json<Value> {
    state.refresh_all().await;
    let components = state.get_components().await;
    Json(json!({
        "components": components,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

/// WebSocket handler.
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<BridgeState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

async fn handle_websocket(socket: axum::extract::ws::WebSocket, state: Arc<BridgeState>) {
    let client_id = uuid::Uuid::new_v4().to_string();
    let (mut sender, mut receiver) = socket.split();

    // Create a channel for sending messages to this client
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<axum::extract::ws::Message>();

    // Clone tx for use in tasks
    let tx_for_send = tx.clone();
    let tx_for_recv = tx.clone();

    // Register client
    state.register_ws_client(client_id.clone(), tx).await;

    // Subscribe to broadcast channel
    let mut broadcast_rx = state.ws_receiver();

    // Send initial component status
    let components = state.get_components().await;
    let init_msg = serde_json::to_string(&json!({
        "type": "init",
        "components": components,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
    .unwrap();
    let _ = sender
        .send(axum::extract::ws::Message::Text(init_msg))
        .await;

    // Task to send broadcast messages to client via the rx channel
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = broadcast_rx.recv().await {
            let text = serde_json::to_string(&msg).unwrap();
            if tx_for_send
                .send(axum::extract::ws::Message::Text(text))
                .is_err()
            {
                break;
            }
        }
    });

    // Task to forward messages from rx channel to WebSocket sender
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Handle incoming messages from client
    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(axum::extract::ws::Message::Text(text)) => {
                    if let Ok(cmd) = serde_json::from_str::<Value>(&text) {
                        if let Some(cmd_type) = cmd.get("type").and_then(|v| v.as_str()) {
                            match cmd_type {
                                "ping" => {
                                    let pong = json!({ "type": "pong", "timestamp": chrono::Utc::now().to_rfc3339() });
                                    let _ = tx_for_recv
                                        .send(axum::extract::ws::Message::Text(pong.to_string()));
                                }
                                "refresh" => {
                                    // Trigger component refresh
                                    // The bridge will broadcast the update
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Ok(axum::extract::ws::Message::Close(_)) => break,
                _ => {}
            }
        }
    });

    // Wait for any task to complete
    tokio::select! {
        _ = forward_task => {},
        _ = recv_task => {},
    }

    // Cleanup
    state.unregister_ws_client(&client_id).await;
    drop(send_task);
}

/// Query relations with filtering.
async fn query_relations_endpoint(Json(payload): Json<Value>) -> ApiResult {
    let query = payload.get("query").cloned().unwrap_or(json!({}));
    match crate::ipc_client::query_relations(query).await {
        Ok(relations) => Ok(Json(json!({
            "relations": relations,
            "count": relations.len(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

// ---------------------------------------------------------------------------
// Mission Workflow endpoints. All state changes go through Core
// `report_observation` (see `crate::mission`); the actor is the fixed
// `space-bridge` agent, never client input.
// ---------------------------------------------------------------------------

async fn mission_agent(
    state: &Arc<BridgeState>,
) -> Result<crate::state::BridgeAgent, (StatusCode, Json<Value>)> {
    state.ensure_agent().await.map_err(core_err)
}

fn mission_view_json(view: crate::mission::MissionView, applied: bool) -> Value {
    let mut v = view.to_json();
    v["applied"] = json!(applied);
    v["timestamp"] = json!(chrono::Utc::now().to_rfc3339());
    v
}

/// Create a Mission (state `created`).
async fn create_mission_ep(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let agent = mission_agent(&state).await?;
    let goal = payload.get("goal").and_then(|v| v.as_str()).unwrap_or("");
    let workspace = payload
        .get("workspace")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let assignee = payload
        .get("assignee")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match crate::mission::create_mission(&agent.name, &agent.token, goal, workspace, assignee).await
    {
        Ok((view, applied)) => Ok(Json(mission_view_json(view, applied))),
        Err(e) => Err(api_err(StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// List mission summaries.
async fn list_missions_ep() -> ApiResult {
    match crate::mission::list_missions().await {
        Ok(missions) => Ok(Json(json!({
            "missions": missions,
            "count": missions.len(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Full mission view (mission + steps + events + progress).
async fn get_mission_ep(axum::extract::Path(id): axum::extract::Path<String>) -> ApiResult {
    match crate::mission::get_view(&id).await {
        Ok(view) => Ok(Json(mission_view_json(view, true))),
        Err(e) => Err(api_err(StatusCode::NOT_FOUND, e.to_string())),
    }
}

/// Attach a plan (`created → planned`).
async fn plan_mission_ep(
    State(state): State<Arc<BridgeState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let agent = mission_agent(&state).await?;
    let arr = payload
        .get("steps")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut steps = vec![];
    for s in arr {
        steps.push((
            s.get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            s.get("detail")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            s.get("requires_approval")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        ));
    }
    match crate::mission::plan_mission(&agent.name, &agent.token, &id, steps).await {
        Ok((view, applied)) => Ok(Json(mission_view_json(view, applied))),
        Err(e) => Err(api_err(StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// Mission-level transition (start / request_approval / approve /
/// verify-state entry / fail / cancel / resume).
async fn transition_mission_ep(
    State(state): State<Arc<BridgeState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let agent = mission_agent(&state).await?;
    let to = payload.get("to").and_then(|v| v.as_str()).unwrap_or("");
    let mut extra = payload.get("extra").cloned().unwrap_or(json!({}));
    if !extra.is_object() {
        extra = json!({});
    }
    match crate::mission::transition_mission(&agent.name, &agent.token, &id, to, extra).await {
        Ok((view, applied)) => Ok(Json(mission_view_json(view, applied))),
        Err(e) => Err(api_err(StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// Record verification evidence (`verifying` only; unlocks `complete`).
async fn verify_mission_ep(
    State(state): State<Arc<BridgeState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let agent = mission_agent(&state).await?;
    let passed = payload
        .get("passed")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let method = payload
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or("tests");
    let detail = payload.get("detail").cloned().unwrap_or(json!({}));
    match crate::mission::record_verification(
        &agent.name,
        &agent.token,
        &id,
        passed,
        method,
        detail,
    )
    .await
    {
        Ok((view, applied)) => Ok(Json(mission_view_json(view, applied))),
        Err(e) => Err(api_err(StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// Step transition: start, request_approval, approve, complete, fail,
/// skip — with evidence and action linkage.
async fn transition_step_ep(
    State(state): State<Arc<BridgeState>>,
    axum::extract::Path((id, step_id)): axum::extract::Path<(String, String)>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let agent = mission_agent(&state).await?;
    let to = payload.get("to").and_then(|v| v.as_str()).unwrap_or("");
    let mut extra = payload.get("extra").cloned().unwrap_or(json!({}));
    if !extra.is_object() {
        extra = json!({});
    }
    // Flat convenience fields land in the step patch too.
    for k in ["evidence", "result", "action_ids"] {
        if let Some(v) = payload.get(k) {
            extra[k] = v.clone();
        }
    }
    match crate::mission::transition_step(&agent.name, &agent.token, &id, &step_id, to, extra).await
    {
        Ok((view, applied)) => Ok(Json(mission_view_json(view, applied))),
        Err(e) => Err(api_err(StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// Record a file-change proposal (snapshots current content, no write).
async fn propose_step_change_ep(
    State(state): State<Arc<BridgeState>>,
    axum::extract::Path((id, step_id)): axum::extract::Path<(String, String)>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let agent = mission_agent(&state).await?;
    let path = payload.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let new_content = payload
        .get("new_content")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if path.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "missing path"));
    }
    // Proposal needs the live file content: read through the normal
    // Core path first (policy + roots gate apply), then record.
    let cur = match state
        .core_action("filesystem", "read", json!({ "path": path }))
        .await
    {
        Ok(resp) => resp,
        Err(e) => return Err(core_err(e)),
    };
    if cur
        .get("result")
        .and_then(|r| r.get("status"))
        .and_then(|s| s.as_str())
        != Some("Completed")
    {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            format!("read before proposal failed: {cur}"),
        ));
    }
    let agent_name = agent.name.clone();
    let agent_token = agent.token.clone();
    let path_s = path.to_string();
    let new_s = new_content.to_string();
    match crate::mission::propose_step_change(
        &agent_name,
        &agent_token,
        &cur,
        &id,
        &step_id,
        &path_s,
        &new_s,
    )
    .await
    {
        Ok((view, applied)) => Ok(Json(mission_view_json(view, applied))),
        Err(e) => Err(api_err(StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// Call a registered external MCP server (guarded adapter).
/// The server program comes from bridge env config, never from the
/// caller; the tool must be allow-listed; arguments are validated
/// against the live-discovered schema; the outcome (or honest error)
/// is persisted through Core with provenance before answering.
async fn external_mcp_call_ep(
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    let agent = mission_agent(&state).await?;
    let server = payload.get("server").and_then(|v| v.as_str()).unwrap_or("");
    let tool = payload.get("tool").and_then(|v| v.as_str()).unwrap_or("");
    let mut args = payload.get("arguments").cloned().unwrap_or(json!({}));
    // Accept both shapes: a JSON object directly, or a JSON-encoded
    // object string (the MCP tool form). Anything else is rejected.
    if let Some(s) = args.as_str() {
        args = serde_json::from_str(s)
            .map_err(|e| api_err(StatusCode::BAD_REQUEST, format!("arguments must be a JSON object: {e}")))?;
        if !args.is_object() {
            return Err(api_err(StatusCode::BAD_REQUEST, "arguments must be a JSON object"));
        }
    } else if !args.is_object() {
        return Err(api_err(StatusCode::BAD_REQUEST, "arguments must be a JSON object"));
    }
    let mission_id = payload
        .get("mission_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if server.trim().is_empty() || tool.trim().is_empty() {
        return Err(api_err(StatusCode::BAD_REQUEST, "missing server/tool"));
    }
    let (servers, warnings) = crate::mcp_client::servers_from_env();
    let policy = crate::mcp_client::ExternalPolicy::from_env();
    let timeout = std::time::Duration::from_secs(
        payload
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(15)
            .min(120),
    );
    let out = match crate::mcp_client::call_external(
        &servers,
        &policy,
        &agent.name,
        server,
        tool,
        args,
        timeout,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return Err(api_err(StatusCode::BAD_REQUEST, e)),
    };
    let provenance = out.get("provenance").cloned().unwrap_or(json!({}));
    let obs_id = format!(
        "ext-{}-{}",
        out.get("correlation_id")
            .and_then(|v| v.as_str())
            .unwrap_or("noid"),
        uuid::Uuid::new_v4()
    );
    match crate::mcp_client::report_external_provenance(
        &agent.name,
        &agent.token,
        mission_id,
        &provenance,
        &obs_id,
    )
    .await
    {
        Ok(outcome) => Ok(Json(json!({
            "result": out,
            "warnings": warnings,
            "provenance_applied": outcome.get("applied").and_then(|v| v.as_bool()).unwrap_or(false),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Query events with filtering and cursor support.
async fn query_events_endpoint(Query(query): Query<HashMap<String, String>>) -> ApiResult {
    let mut eq = EventQuery::default();
    if let Some(v) = query.get("event_type") {
        eq.event_type = Some(v.clone());
    }
    if let Some(v) = query.get("provider_id") {
        eq.provider_id = Some(v.clone());
    }
    if let Some(v) = query.get("correlation_id") {
        eq.correlation_id = Some(v.clone());
    }
    if let Some(v) = query.get("after_seq") {
        eq.after_seq = v.parse().ok();
    }
    if let Some(v) = query.get("limit") {
        eq.limit = v.parse().ok();
    }
    if let Some(v) = query.get("world_id") {
        eq.world_id = Some(v.clone());
    }
    match crate::ipc_client::query_events(eq).await {
        Ok(events) => Ok(Json(json!({
            "events": events,
            "count": events.len(),
            "next_seq": events.last().map(|e| e.seq),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        Err(e) => Err(core_err(e)),
    }
}

/// Context endpoint: space summary for UI/MCP.
async fn context_endpoint(Query(query): Query<HashMap<String, String>>) -> ApiResult {
    let space_id = query.get("space_id").cloned().unwrap_or_default();
    let limit: usize = query
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
        .min(200);
    let mut ctx = json!({"space_id": space_id});
    // Objects
    let obj_query = space_core::query::ObjectQuery {
        world_id: if space_id.is_empty() {
            None
        } else {
            Some(space_id.clone())
        },
        ..Default::default()
    };
    if let Ok(objects) = crate::ipc_client::query_objects(
        serde_json::to_value(obj_query).map_err(|e| core_err(anyhow::anyhow!(e)))?,
    )
    .await
    {
        ctx["objects"] = json!(objects);
        ctx["object_count"] = json!(objects.len());
    }
    // Relations
    let rel_query = space_core::query::RelationQuery {
        world_id: if space_id.is_empty() {
            None
        } else {
            Some(space_id.clone())
        },
        ..Default::default()
    };
    if let Ok(relations) = crate::ipc_client::query_relations(
        serde_json::to_value(rel_query).map_err(|e| core_err(anyhow::anyhow!(e)))?,
    )
    .await
    {
        ctx["relations"] = json!(relations);
    }
    // Recent events
    let evt_query = EventQuery {
        world_id: if space_id.is_empty() {
            None
        } else {
            Some(space_id.clone())
        },
        limit: Some(limit),
        ..Default::default()
    };
    if let Ok(events) = crate::ipc_client::query_events(evt_query).await {
        ctx["recent_events"] = json!(events);
    }
    Ok(Json(json!({
        "context": ctx,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    })))
}
