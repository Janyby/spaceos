//! HTTP Handlers — REST and WebSocket endpoints.
//!
//! Every state-changing operation runs through Core `request_action` as a
//! registered bridge agent (see `BridgeState::core_action`), so policy is
//! checked before execution and the ActionRequested -> terminal Action*
//! event chain is stored with SpaceTrace correlation. Error responses are
//! always JSON (never an empty body).

use axum::{
    extract::{Query, Request, State, WebSocketUpgrade},
    http::{header::{CONTENT_TYPE, COOKIE, ORIGIN, SET_COOKIE}, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, patch, post},
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
    if let Some(browser) = e.downcast_ref::<crate::state::BrowserFailure>() {
        use crate::state::BrowserFailureKind::*;
        let status = match browser.kind {
            Invalid => StatusCode::BAD_REQUEST,
            Denied => StatusCode::FORBIDDEN,
            Missing => StatusCode::NOT_FOUND,
            Conflict => StatusCode::CONFLICT,
            Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Upstream => StatusCode::BAD_GATEWAY,
            Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        return api_err(status, browser.message.clone());
    }
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
    // Browser access is same-origin. Do not advertise every local API to
    // arbitrary web origins; this is defense-in-depth, not caller auth.
    let allowed_origin = HeaderValue::from_str(state.agent_origin())
        .expect("validated Bridge origin must be an HTTP header value");
    let cors = CorsLayer::new()
        .allow_origin(allowed_origin)
        .allow_methods([Method::GET, Method::POST, Method::PATCH])
        .allow_headers([CONTENT_TYPE]);

    Router::new()
        // Static files: `/` is the World (spatial UI, the default entry
        // point); `/index.html` stays the full Shell. The launcher opens
        // `/` and therefore lands in World without any redirect.
        .route_service("/", ServeFile::new("space-bridge/static/world.html"))
        .fallback_service(ServeDir::new("space-bridge/static"))
        // API routes
        .route("/api/health", get(health_check))
        .route("/api/lab-auth/login", post(lab_login))
        .route("/api/lab-auth/logout", post(lab_logout))
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
        .route("/api/semantic/objects", post(create_semantic_object_ep))
        .route("/api/semantic/objects/:id", patch(patch_semantic_object_ep))
        .route("/api/semantic/relations", post(create_semantic_relation_ep))
        .route("/api/events", get(query_events_endpoint))
        .route("/api/context", get(context_endpoint))
        .route("/api/bridge/refresh", post(refresh_bridge))
        .route("/api/actions/recent", get(actions_recent))
        .route("/api/actions/:id", get(action_status))
        .route(
            "/api/missions",
            post(crate::mission_commands::create).get(list_missions_ep),
        )
        .route("/api/missions/:id", get(get_mission_ep))
        .route("/api/missions/command", post(crate::mission_commands::command))
        .route("/api/missions/:id/plan", post(crate::mission_commands::plan))
        .route("/api/missions/:id/transition", post(crate::mission_commands::transition))
        .route("/api/missions/:id/execute", post(crate::mission_commands::execute))
        .route("/api/missions/:id/interrupt", post(crate::mission_commands::interrupt))
        .route("/api/missions/:id/execution", get(crate::mission_commands::execution_status))
        .route("/api/missions/:id/verify", post(crate::mission_commands::verify))
        .route(
            "/api/missions/:id/steps/:step_id/transition",
            post(crate::mission_commands::step),
        )
        .route(
            "/api/missions/:id/steps/:step_id/proposal",
            post(crate::mission_commands::proposal_unavailable),
        )
        .route("/api/external-mcp/call", post(external_mcp_call_ep))
        // Agent Gateway: server-side proxy to an existing loopback OpenCode
        // Server. OpenCode credentials never cross into the browser.
        .route("/api/agent/sessions", get(agent_sessions_ep))
        .route("/api/agent/models", get(crate::mission_commands::execution_models))
        .route("/api/agent/send", post(agent_send_ep))
        .route("/api/agent/status/:request_id", get(agent_status_ep))
        .route("/internal/mission-mcp", post(bound_mission_mcp))
        // WebSocket
        .route("/ws", get(websocket_handler))
        .layer(middleware::from_fn_with_state(state.clone(), lab_session_gate))
        .layer(cors)
        .with_state(state)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundToolCall { name: String, arguments: Value }

/// Native stdio MCP attachment to an already-created trusted attempt. Browser
/// cookies never authorize this route; a separate startup lease is required.
async fn bound_mission_mcp(State(state): State<Arc<BridgeState>>, headers: HeaderMap,
    Json(call): Json<BoundToolCall>) -> ApiResult {
    if std::env::var("SPACEOS_TEST_ISOLATION_ROOT").ok().as_deref()
        != Some(r"C:\spaceos") {
        return Err(api_err(StatusCode::NOT_FOUND,"unavailable"));
    }
    if headers.contains_key(ORIGIN) {return Err(api_err(StatusCode::FORBIDDEN,"browser attachment denied"))}
    let credential=headers.get("x-spaceos-attempt").and_then(|v|v.to_str().ok()).unwrap_or("");
    let mut slot=state.mission_attempt.lock().await;
    let Some(attempt)=slot.as_mut() else {return Err(api_err(StatusCode::UNAUTHORIZED,"no active attempt"))};
    match attempt.dispatch(&state,credential,&call.name,call.arguments).await {
        Ok(value)=>Ok(Json(value)),
        Err(error)=>Err(api_err(StatusCode::FORBIDDEN,error.to_string())),
    }
}

// This guard is dormant in the normal Bridge. A laboratory fixture must
// explicitly install a synthetic login secret under SPACEOS_TEST_ISOLATION_ROOT.
// It deliberately does not turn a localhost visit into authority.
async fn lab_session_gate(
    State(state): State<Arc<BridgeState>>,
    request: Request,
    next: Next,
) -> Response {
    if !state.lab_login_enabled() {
        return next.run(request).await;
    }
    let path = request.uri().path();
    if path == "/api/health" || path == "/api/lab-auth/login" ||
        (!path.starts_with("/api/") && path != "/ws") {
        return next.run(request).await;
    }
    if let Some(origin) = request.headers().get(ORIGIN) {
        if origin.to_str().ok() != Some(state.agent_origin()) {
            return api_err(StatusCode::FORBIDDEN, "foreign Origin").into_response();
        }
    }
    let Some(session) = lab_cookie(request.headers()) else {
        return api_err(StatusCode::UNAUTHORIZED, "laboratory session required").into_response();
    };
    let mutating = !matches!(*request.method(), Method::GET | Method::HEAD);
    let csrf = if mutating {
        request.headers().get("x-spaceos-csrf").and_then(|v| v.to_str().ok())
    } else { None };
    if (mutating && csrf.is_none()) || !state.validate_lab_session(&session, csrf).await {
        return api_err(StatusCode::FORBIDDEN, "invalid or expired laboratory session").into_response();
    }
    next.run(request).await
}

fn lab_cookie(headers: &HeaderMap) -> Option<String> {
    headers.get(COOKIE)?.to_str().ok()?.split(';').find_map(|part| {
        part.trim().strip_prefix("spaceos_lab_session=").filter(|id| id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit())).map(str::to_owned)
    })
}

async fn lab_login(
    State(state): State<Arc<BridgeState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if !state.lab_login_enabled() {
        return api_err(StatusCode::NOT_FOUND, "unavailable").into_response();
    }
    if let Err(error) = ensure_agent_origin(&headers, &state) {
        return error.into_response();
    }
    let proof = body.get("proof").and_then(Value::as_str).unwrap_or("");
    let world_id = body.get("world_id").and_then(Value::as_str).unwrap_or("");
    let Some((session, csrf)) = state.create_lab_session(proof, world_id).await else {
        return api_err(StatusCode::UNAUTHORIZED, "invalid laboratory login").into_response();
    };
    let mut response = Json(json!({"ok": true, "world_id": world_id, "csrf": csrf})).into_response();
    // HTTP loopback cannot use a Secure cookie; SameSite and HttpOnly are
    // explicit. This is synthetic laboratory behavior, not a production scheme.
    let cookie = format!("spaceos_lab_session={session}; Path=/; HttpOnly; SameSite=Strict");
    response.headers_mut().insert(SET_COOKIE, HeaderValue::from_str(&cookie).expect("hex session cookie"));
    response
}

async fn lab_logout(State(state): State<Arc<BridgeState>>, headers: HeaderMap) -> Response {
    if let Some(session) = lab_cookie(&headers) {
        state.revoke_lab_session(&session).await;
    }
    let mut response = Json(json!({"ok": true})).into_response();
    response.headers_mut().insert(SET_COOKIE, HeaderValue::from_static("spaceos_lab_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"));
    response
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
            "browser_mode": if state.scoped_browser() { "scoped" } else { "legacy" },
            "browser_capabilities": state.browser_capabilities(),
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
            "browser_mode": if state.scoped_browser() { "scoped" } else { "legacy" },
            "browser_capabilities": state.browser_capabilities(),
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
    match crate::ipc_client::scoped_read_request("query_events", json!({"limit": 500})).await {
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

async fn create_semantic_object_ep(
    headers: HeaderMap,
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    ensure_agent_origin(&headers, &state)?;
    let command: space_core::CreateSemanticObject =
        serde_json::from_value(payload).map_err(|e| {
            api_err(
                StatusCode::BAD_REQUEST,
                format!("invalid semantic object: {e}"),
            )
        })?;
    ensure_semantic_world(&state, &command.world_id)?;
    let response = state
        .semantic_command("create_semantic_object", json!(command))
        .await
        .map_err(semantic_bridge_error)?;
    semantic_core_response(response)
}

async fn patch_semantic_object_ep(
    headers: HeaderMap,
    axum::extract::Path(object_id): axum::extract::Path<String>,
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    ensure_agent_origin(&headers, &state)?;
    let command: space_core::PatchSemanticObject =
        serde_json::from_value(payload).map_err(|e| {
            api_err(
                StatusCode::BAD_REQUEST,
                format!("invalid semantic patch: {e}"),
            )
        })?;
    if command.object_id != object_id {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            "object_id must match the URL path",
        ));
    }
    ensure_semantic_world(&state, &command.world_id)?;
    let response = state
        .semantic_command("patch_semantic_object", json!(command))
        .await
        .map_err(semantic_bridge_error)?;
    semantic_core_response(response)
}

async fn create_semantic_relation_ep(
    headers: HeaderMap,
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    ensure_agent_origin(&headers, &state)?;
    let command: space_core::CreateSemanticRelation =
        serde_json::from_value(payload).map_err(|e| {
            api_err(
                StatusCode::BAD_REQUEST,
                format!("invalid semantic relation: {e}"),
            )
        })?;
    ensure_semantic_world(&state, &command.world_id)?;
    let response = state
        .semantic_command("create_semantic_relation", json!(command))
        .await
        .map_err(semantic_bridge_error)?;
    semantic_core_response(response)
}

fn ensure_semantic_world(
    state: &BridgeState,
    requested_world: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    let configured = state.semantic_world_id().map_err(semantic_bridge_error)?;
    if requested_world != configured {
        return Err(api_err(
            StatusCode::FORBIDDEN,
            "requested world_id is not the configured semantic world",
        ));
    }
    Ok(())
}

fn semantic_bridge_error(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    let message = error.to_string();
    if message.contains("SPACEOS_SEMANTIC_WORLD_ID")
        || message.contains("semantic registrar credential missing")
        || message.contains("semantic agent registration denied")
        || message.contains("Core IPC request timed out")
        || message.contains("open pipe")
        || message.contains("no response")
    {
        api_err(StatusCode::SERVICE_UNAVAILABLE, message)
    } else {
        core_err(error)
    }
}

fn semantic_core_response(response: Value) -> ApiResult {
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(Json(response))
    } else {
        let message = response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("semantic command failed")
            .to_string();
        let status = if message.contains("SPACEOS_SEMANTIC_WORLD_ID") {
            StatusCode::SERVICE_UNAVAILABLE
        } else if message.starts_with("denied:") {
            StatusCode::FORBIDDEN
        } else if message.contains("conflict") {
            StatusCode::CONFLICT
        } else {
            StatusCode::BAD_REQUEST
        };
        Err(api_err(status, message))
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
    headers: HeaderMap,
    State(state): State<Arc<BridgeState>>,
) -> Response {
    if let Err(error) = ensure_agent_origin(&headers, &state) {
        return error.into_response();
    }
    let session_id = if state.lab_login_enabled() { lab_cookie(&headers) } else { None };
    ws.on_upgrade(move |socket| handle_websocket(socket, state, session_id))
}

async fn handle_websocket(socket: axum::extract::ws::WebSocket, state: Arc<BridgeState>, session_id: Option<String>) {
    let client_id = uuid::Uuid::new_v4().to_string();
    let (mut sender, mut receiver) = socket.split();

    // Create a channel for sending messages to this client
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<axum::extract::ws::Message>();

    // Clone tx for use in tasks
    let tx_for_send = tx.clone();
    let tx_for_recv = tx.clone();
    let tx_for_events = tx.clone();

    // Register client
    state.register_ws_client(client_id.clone(), tx).await;

    // Subscribe to broadcast channel
    let mut broadcast_rx = state.ws_receiver();

    // Establish a durable cursor before announcing readiness. Events created
    // after this query are delivered from Core's committed event log.
    let initial = crate::ipc_client::query_events(EventQuery {
        limit: Some(1),
        ..Default::default()
    })
    .await;
    let mut pump = match crate::event_stream::EventPump::initialize(initial) {
        Ok(pump) => pump,
        Err(error) => {
            tracing::warn!(operation="cursor_initialization_failure", category=crate::event_stream::query_failure(&error), "WebSocket stream initialization failed");
            let _ = sender.send(axum::extract::ws::Message::Close(None)).await;
            state.unregister_ws_client(&client_id).await;
            return;
        }
    };
    let event_task = tokio::spawn(async move {
        loop {
            let result = crate::ipc_client::query_events(EventQuery {
                after_seq: Some(pump.cursor),
                ..Default::default()
            })
            .await;
            if let Err(category) = pump.poll(result, |event| {
                        let message = json!({"type": "semantic_event", "event": event});
                        tx_for_events
                            .send(axum::extract::ws::Message::Text(message.to_string()))
                            .is_ok()
            }) {
                if pump.should_log(category) { tracing::warn!(operation="event_poll", category, "WebSocket event stream unavailable"); }
                if category == "delivery_failure" { return; }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });

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
    let session_state = state.clone();
    let mut forward_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Some(id) = session_id.as_deref() {
                        if !session_state.validate_lab_session(id, None).await {
                            let _ = sender.send(axum::extract::ws::Message::Close(None)).await;
                            break;
                        }
                    }
                }
                message = rx.recv() => {
                    let Some(msg) = message else { break; };
                    if let Some(id) = session_id.as_deref() {
                        if !session_state.validate_lab_session(id, None).await { break; }
                    }
                    if sender.send(msg).await.is_err() {
                        tracing::warn!(operation="delivery_failure", "WebSocket send failed");
                        break;
                    }
                }
            }
        }
    });

    // Handle incoming messages from client
    let mut recv_task = tokio::spawn(async move {
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
        _ = &mut forward_task => {},
        _ = &mut recv_task => {},
    }

    // Cleanup
    state.unregister_ws_client(&client_id).await;
    send_task.abort();
    forward_task.abort();
    recv_task.abort();
    event_task.abort();
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
// Legacy Mission endpoint helpers retained for baseline comparison. Mutating
// routes above now use mission_commands -> Core mission_command. These old
// observation-based handlers are no longer registered in the router.
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
        args = serde_json::from_str(s).map_err(|e| {
            api_err(
                StatusCode::BAD_REQUEST,
                format!("arguments must be a JSON object: {e}"),
            )
        })?;
        if !args.is_object() {
            return Err(api_err(
                StatusCode::BAD_REQUEST,
                "arguments must be a JSON object",
            ));
        }
    } else if !args.is_object() {
        return Err(api_err(
            StatusCode::BAD_REQUEST,
            "arguments must be a JSON object",
        ));
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

/// List sanitized sessions from the configured, already-running OpenCode
/// Server. This endpoint never starts a process and never falls back to an LLM.
async fn agent_sessions_ep(headers: HeaderMap, State(state): State<Arc<BridgeState>>) -> ApiResult {
    ensure_agent_origin(&headers, &state)?;
    let gateway = crate::agent_gateway::AgentGateway::from_env()
        .map_err(|e| api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    let snapshot = gateway
        .sessions()
        .await
        .map_err(|e| api_err(StatusCode::BAD_GATEWAY, e.to_string()))?;
    let sessions: Vec<Value> = snapshot
        .sessions
        .iter()
        .filter_map(sanitize_agent_session)
        .map(|mut session| {
            if let Some(id) = session.get("id").and_then(Value::as_str) {
                session["status"] = json!(if crate::agent_gateway::session_is_active(
                    &snapshot.active,
                    id
                ) {
                    "running"
                } else {
                    "idle"
                });
            }
            session
        })
        .collect();
    Ok(Json(json!({
        "sessions": sessions,
        "count": sessions.len(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    })))
}

/// Admit a prompt to a selected OpenCode session, then monitor its real
/// execution state and projected assistant response in the background.
async fn agent_send_ep(
    headers: HeaderMap,
    State(state): State<Arc<BridgeState>>,
    Json(payload): Json<Value>,
) -> ApiResult {
    ensure_agent_origin(&headers, &state)?;
    let session_id = payload
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing session_id"))?
        .trim()
        .to_string();
    let message = payload
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| api_err(StatusCode::BAD_REQUEST, "missing message"))?
        .trim()
        .to_string();
    let gateway = crate::agent_gateway::AgentGateway::from_env()
        .map_err(|e| api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    let before = gateway
        .assistant_messages(&session_id)
        .await
        .map_err(|e| api_err(StatusCode::BAD_GATEWAY, e.to_string()))?;
    let previous_ids = crate::agent_gateway::message_ids(&before);
    let admitted = gateway
        .prompt(&session_id, &message)
        .await
        .map_err(|e| api_err(StatusCode::BAD_GATEWAY, e.to_string()))?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let running = json!({
        "status": "running",
        "session_id": session_id,
        "started_at": chrono::Utc::now().to_rfc3339(),
    });
    state.update_agent_run(&request_id, running).await;

    let task_state = state.clone();
    let task_request_id = request_id.clone();
    let task_session_id = session_id.clone();
    tokio::spawn(async move {
        monitor_agent_response(
            task_state,
            gateway,
            task_request_id,
            task_session_id,
            previous_ids,
        )
        .await;
    });

    Ok(Json(json!({
        "ok": true,
        "request_id": request_id,
        "session_id": session_id,
        "status": "running",
        "admitted": admitted,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    })))
}

async fn agent_status_ep(
    headers: HeaderMap,
    axum::extract::Path(request_id): axum::extract::Path<String>,
    State(state): State<Arc<BridgeState>>,
) -> ApiResult {
    ensure_agent_origin(&headers, &state)?;
    match state.get_agent_run(&request_id).await {
        Some(update) => Ok(Json(json!({
            "request_id": request_id,
            "update": update,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))),
        None => Err(api_err(StatusCode::NOT_FOUND, "unknown agent request")),
    }
}

async fn monitor_agent_response(
    state: Arc<BridgeState>,
    gateway: crate::agent_gateway::AgentGateway,
    request_id: String,
    session_id: String,
    previous_ids: std::collections::HashSet<String>,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        if tokio::time::Instant::now() >= deadline {
            state
                .update_agent_run(
                    &request_id,
                    json!({
                        "status": "error",
                        "session_id": session_id,
                        "error": "OpenCode response timed out after 300 seconds",
                        "finished_at": chrono::Utc::now().to_rfc3339(),
                    }),
                )
                .await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(750)).await;
        let active = match gateway.active().await {
            Ok(v) => v,
            Err(e) => {
                finish_agent_error(&state, &request_id, &session_id, e.to_string()).await;
                return;
            }
        };
        let messages = match gateway.assistant_messages(&session_id).await {
            Ok(v) => v,
            Err(e) => {
                finish_agent_error(&state, &request_id, &session_id, e.to_string()).await;
                return;
            }
        };
        if let Some((message_id, response)) =
            crate::agent_gateway::newest_response(&messages, &previous_ids)
        {
            if !crate::agent_gateway::session_is_active(&active, &session_id) {
                state
                    .update_agent_run(
                        &request_id,
                        json!({
                            "status": "completed",
                            "session_id": session_id,
                            "message_id": message_id,
                            "response": response,
                            "finished_at": chrono::Utc::now().to_rfc3339(),
                        }),
                    )
                    .await;
                return;
            }
        }
    }
}

async fn finish_agent_error(
    state: &BridgeState,
    request_id: &str,
    session_id: &str,
    error: String,
) {
    state
        .update_agent_run(
            request_id,
            json!({
                "status": "error",
                "session_id": session_id,
                "error": error,
                "finished_at": chrono::Utc::now().to_rfc3339(),
            }),
        )
        .await;
}

fn sanitize_agent_session(session: &Value) -> Option<Value> {
    let id = session.get("id")?.as_str()?;
    let mut out = json!({ "id": id });
    for key in [
        "title",
        "parentID",
        "projectID",
        "agent",
        "model",
        "location",
        "time",
    ] {
        if let Some(value) = session.get(key) {
            out[key] = value.clone();
        }
    }
    Some(out)
}

/// Although Bridge is loopback-only, reject cross-origin browser requests to
/// the agent control surface. Requests without Origin remain available to
/// local diagnostics and native clients.
fn ensure_agent_origin(headers: &HeaderMap, state: &BridgeState) -> Result<(), (StatusCode, Json<Value>)> {
    let Some(origin) = headers.get(ORIGIN) else {
        return Ok(());
    };
    let allowed = origin
        .to_str()
        .map(|value| value == state.agent_origin())
        .unwrap_or(false);
    if allowed {
        Ok(())
    } else {
        Err(api_err(
            StatusCode::FORBIDDEN,
            "cross-origin access to Agent Gateway is not allowed",
        ))
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
async fn context_endpoint(
    Query(query): Query<HashMap<String, String>>,
    State(state): State<Arc<BridgeState>>,
) -> ApiResult {
    let space_id = query.get("space_id").cloned().unwrap_or_default();
    let limit: usize = query
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
        .min(200);
    let mut ctx = json!({"space_id": space_id,"browser_mode":if state.scoped_browser() {"scoped"} else {"legacy"},"browser_capabilities":state.browser_capabilities()});
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
    // Semantic world configuration (for UI)
    if let Ok(semantic_world_id) = state.semantic_world_id() {
        ctx["semantic_world_id"] = json!(semantic_world_id);
    }
    Ok(Json(json!({
        "context": ctx,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    })))
}

#[cfg(test)]
mod cors_security_tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    #[test]
    fn scoped_browser_error_categories_do_not_reclassify_legacy_errors() {
        use crate::state::{BrowserFailure, BrowserFailureKind::*};
        for (kind, expected) in [(Invalid,400),(Denied,403),(Missing,404),(Conflict,409),(Unavailable,503),(Upstream,502),(Internal,500)] {
            let (status, _) = core_err(BrowserFailure { kind,message:"synthetic scoped error".into() }.into());
            assert_eq!(status.as_u16(), expected);
        }
        assert_eq!(core_err(anyhow::anyhow!("legacy unavailable error")).0,StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(core_err(anyhow::anyhow!("Core IPC request timed out")).0,StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn cors_advertises_only_the_exact_world_origin() {
        let router = create_router(Arc::new(BridgeState::new()));
        for origin in ["http://127.0.0.1:8765", "http://127.0.0.1:19999", "null"] {
            let request = Request::builder()
                .uri("/api/health")
                .header(ORIGIN, origin)
                .body(Body::empty())
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get("access-control-allow-origin")
                    .and_then(|value| value.to_str().ok()),
                Some("http://127.0.0.1:8765"),
                "CORS policy for {origin}"
            );
            // A foreign browser origin cannot read the response because the
            // advertised origin never matches that browser's Origin. This is
            // not a server-side identity check against a native HTTP client.
            if origin != "http://127.0.0.1:8765" {
                assert_ne!(
                    response.headers()["access-control-allow-origin"],
                    origin
                );
            }
        }
    }

    #[test]
    fn forged_or_missing_http_origin_is_not_caller_authentication() {
        let state = BridgeState::new();
        let forged = HeaderMap::from_iter([(ORIGIN, HeaderValue::from_static("http://127.0.0.1:8765"))]);
        assert!(ensure_agent_origin(&forged, &state).is_ok());
        assert!(ensure_agent_origin(&HeaderMap::new(), &state).is_ok());
        let foreign = HeaderMap::from_iter([(ORIGIN, HeaderValue::from_static("http://127.0.0.1:19999"))]);
        assert!(ensure_agent_origin(&foreign, &state).is_err());
        // This demonstrates the known limit; it does not make forged Origin
        // an authenticated browser session.
    }

    #[tokio::test]
    async fn foreign_websocket_origin_is_rejected_before_upgrade() {
        let router = create_router(Arc::new(BridgeState::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert(ORIGIN, HeaderValue::from_static("http://127.0.0.1:19999"));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tokio_tungstenite::connect_async(request),
        ).await.unwrap();
        match result {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::FORBIDDEN);
            }
            other => panic!("foreign WebSocket Origin was not rejected: {other:?}"),
        }
        server.abort();
    }

    #[tokio::test]
    async fn synthetic_lab_session_requires_proof_cookie_csrf_and_revokes() {
        if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_none() {
            // Test-only prototype is intentionally impossible to enable in
            // an ordinary process. The I.4 command supplies this guard.
            return;
        }
        let world = "550e8400-e29b-41d4-a716-446655440000";
        let mut state = BridgeState::new_with_semantic_world(Some(world));
        state.set_isolated_lab_login("synthetic-proof-I4".into(), std::time::Duration::from_millis(300)).unwrap();
        let router = create_router(Arc::new(state));
        let request = |uri: &str, method: Method, origin: Option<&str>, cookie: Option<&str>, csrf: Option<&str>, body: &str| {
            let mut builder = Request::builder().uri(uri).method(method).header(CONTENT_TYPE, "application/json");
            if let Some(value) = origin { builder = builder.header(ORIGIN, value); }
            if let Some(value) = cookie { builder = builder.header(COOKIE, value); }
            if let Some(value) = csrf { builder = builder.header("x-spaceos-csrf", value); }
            builder.body(Body::from(body.to_owned())).unwrap()
        };
        let origin = "http://127.0.0.1:8765";
        let read = || request("/api/components", Method::GET, Some(origin), None, None, "");
        assert_eq!(router.clone().oneshot(read()).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        let body = format!(r#"{{"proof":"synthetic-proof-I4","world_id":"{world}"}}"#);
        let forged = request("/api/lab-auth/login", Method::POST, Some("http://127.0.0.1:19999"), None, None, &body);
        assert_eq!(router.clone().oneshot(forged).await.unwrap().status(), StatusCode::FORBIDDEN);
        let bad = request("/api/lab-auth/login", Method::POST, Some(origin), None, None,
            &format!(r#"{{"proof":"wrong","world_id":"{world}"}}"#));
        assert_eq!(router.clone().oneshot(bad).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        let foreign_world = request("/api/lab-auth/login", Method::POST, Some(origin), None, None,
            r#"{"proof":"synthetic-proof-I4","world_id":"550e8400-e29b-41d4-a716-446655440001"}"#);
        assert_eq!(router.clone().oneshot(foreign_world).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        let login = router.clone().oneshot(request("/api/lab-auth/login", Method::POST, Some(origin), None, None, &body)).await.unwrap();
        assert_eq!(login.status(), StatusCode::OK);
        let cookie = login.headers().get(SET_COOKIE).unwrap().to_str().unwrap().split(';').next().unwrap().to_owned();
        let bytes = axum::body::to_bytes(login.into_body(), 4096).await.unwrap();
        let payload: Value = serde_json::from_slice(&bytes).unwrap();
        let csrf = payload["csrf"].as_str().unwrap();
        assert!(!payload.to_string().contains("synthetic-proof-I4"));
        let valid_read = request("/api/components", Method::GET, Some(origin), Some(&cookie), None, "");
        assert_eq!(router.clone().oneshot(valid_read).await.unwrap().status(), StatusCode::OK);
        let foreign_read = request("/api/components", Method::GET, Some("http://127.0.0.1:19999"), Some(&cookie), None, "");
        assert_eq!(router.clone().oneshot(foreign_read).await.unwrap().status(), StatusCode::FORBIDDEN);
        let no_csrf = request("/api/semantic/objects", Method::POST, Some(origin), Some(&cookie), None, "{}");
        assert_eq!(router.clone().oneshot(no_csrf).await.unwrap().status(), StatusCode::FORBIDDEN);
        let logout = request("/api/lab-auth/logout", Method::POST, Some(origin), Some(&cookie), Some(csrf), "{}");
        assert_eq!(router.clone().oneshot(logout).await.unwrap().status(), StatusCode::OK);
        let revoked = request("/api/components", Method::GET, Some(origin), Some(&cookie), None, "");
        assert_eq!(router.clone().oneshot(revoked).await.unwrap().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn synthetic_lab_session_expires_and_blocks_anonymous_websocket() {
        if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_none() { return; }
        let world = "550e8400-e29b-41d4-a716-446655440000";
        let mut state = BridgeState::new_with_semantic_world(Some(world));
        state.set_isolated_lab_login("short-proof".into(), std::time::Duration::from_millis(15)).unwrap();
        let (id, csrf) = state.create_lab_session("short-proof", world).await.unwrap();
        assert!(state.validate_lab_session(&id, Some(&csrf)).await);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(!state.validate_lab_session(&id, None).await);
        let router = create_router(Arc::new(state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert(ORIGIN, HeaderValue::from_static("http://127.0.0.1:8765"));
        let result = tokio_tungstenite::connect_async(request).await;
        match result {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            }
            other => panic!("anonymous laboratory WebSocket was not rejected: {other:?}"),
        }
        server.abort();
    }
}
