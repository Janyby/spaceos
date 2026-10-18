//! IPC Client — communicates with SpaceOS Core via named pipes.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use space_core::ipc::{core_token, send_request, CORE_PIPE};
use std::time::Duration;
use tokio::time::timeout;

/// Timeout for Core IPC requests.
const IPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Send a request to SpaceOS Core and return the parsed response.
pub async fn core_request(method: &str, params: Value) -> Result<Value> {
    let params = params;
    let result = timeout(IPC_TIMEOUT, send_request(CORE_PIPE, method, params))
        .await
        .context("Core IPC request timed out")??;
    Ok(result)
}

/// Send a request to a specific provider pipe.
pub async fn provider_request(pipe_path: &str, method: &str, params: Value) -> Result<Value> {
    let mut params = params;
    params["auth"] = json!(core_token());
    let result = timeout(IPC_TIMEOUT, send_request(pipe_path, method, params))
        .await
        .context("Provider IPC request timed out")??;
    Ok(result)
}

/// List all registered providers.
pub async fn list_providers() -> Result<Vec<Value>> {
    let resp = core_request("list_providers", json!({})).await?;
    let providers = resp.get("result").cloned().unwrap_or(json!([]));
    Ok(providers.as_array().cloned().unwrap_or_default())
}

/// Get provider info by ID.
pub async fn get_provider(provider_id: &str) -> Result<Option<Value>> {
    let resp = core_request("list_providers", json!({})).await?;
    let providers = resp.get("result").cloned().unwrap_or(json!([]));
    let providers = providers.as_array().cloned().unwrap_or_default();
    Ok(providers
        .into_iter()
        .find(|p| p.get("id").and_then(|v| v.as_str()) == Some(provider_id)))
}

/// Send an action request to a provider via Core.
pub async fn request_action(
    action: &str,
    target: Value,
    provider_id: &str,
    actor: &str,
    actor_token: Option<String>,
) -> Result<Value> {
    let req = json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "action": action,
        "target": target,
        "provider_id": provider_id,
        "actor": actor,
        "actor_token": actor_token.unwrap_or_default(),
    });
    let resp = core_request("request_action", req).await?;
    Ok(resp)
}

/// Query objects from Core storage.
pub async fn query_objects(query: Value) -> Result<Vec<Value>> {
    let resp = core_request("query_objects", query).await?;
    let objects = resp.get("result").cloned().unwrap_or(json!([]));
    Ok(objects.as_array().cloned().unwrap_or_default())
}

/// Get provider info via Core (requires storage access).
pub async fn provider_info(provider_id: &str) -> Result<Option<Value>> {
    // This would need CoreHandle::provider_info which isn't fully implemented
    // Fall back to list_providers
    list_providers().await.map(|ps| {
        ps.into_iter()
            .find(|p| p.get("id").and_then(|v| v.as_str()) == Some(provider_id))
    })
}

/// Execute a browser action via browser provider pipe.
pub async fn browser_action(action: &str, target: Value) -> Result<Value> {
    use space_core::ipc::BROWSER_PROVIDER_PIPE;
    provider_request(
        BROWSER_PROVIDER_PIPE,
        "action",
        json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "action": action,
            "target": target,
            "provider_id": "browser",
            "actor": "bridge",
            "actor_token": "",
        }),
    )
    .await
}

/// Scan a path via the filesystem provider's `scan` method (not `action`:
/// `scan` is a top-level provider method, same contract as `spaceos scan`).
pub async fn filesystem_scan(path: &str) -> Result<Value> {
    use space_core::ipc::PROVIDER_PIPE;
    provider_request(PROVIDER_PIPE, "scan", json!({ "path": path })).await
}

/// Allowed roots reported by the filesystem provider daemon.
pub async fn filesystem_roots() -> Result<Value> {
    use space_core::ipc::PROVIDER_PIPE;
    provider_request(PROVIDER_PIPE, "roots", json!({})).await
}

/// Cheap, side-effect-free filesystem probe via the provider `stat` method.
pub async fn filesystem_stat(path: &str) -> Result<Value> {
    use space_core::ipc::PROVIDER_PIPE;
    provider_request(PROVIDER_PIPE, "stat", json!({ "path": path })).await
}

/// Fetch Core events correlated to one action id (lifecycle view).
pub async fn action_events(action_id: &str) -> Result<Vec<Value>> {
    let resp = core_request("query_events", json!({ "limit": 500 })).await?;
    let events = resp.get("result").cloned().unwrap_or(json!([]));
    let out: Vec<Value> = events
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|e| e.get("correlation_id").and_then(|v| v.as_str()) == Some(action_id))
        .collect();
    Ok(out)
}

/// Execute a filesystem action via filesystem provider pipe.
pub async fn filesystem_action(action: &str, target: Value) -> Result<Value> {
    use space_core::ipc::PROVIDER_PIPE;
    provider_request(
        PROVIDER_PIPE,
        "action",
        json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "action": action,
            "target": target,
            "provider_id": "filesystem",
            "actor": "bridge",
            "actor_token": "",
        }),
    )
    .await
}

/// Query relations from Core storage.
pub async fn query_relations(query: Value) -> Result<Vec<Value>> {
    let resp = core_request("query_relations", query).await?;
    let relations = resp.get("result").cloned().unwrap_or(json!([]));
    Ok(relations.as_array().cloned().unwrap_or_default())
}

/// Query events from Core storage with cursor support.
pub async fn query_events(query: space_core::query::EventQuery) -> Result<Vec<space_core::Event>> {
    let resp = core_request("query_events", serde_json::to_value(query)?).await?;
    let events = resp.get("result").cloned().unwrap_or(json!([]));
    let events = events.as_array().cloned().unwrap_or_default();
    let mut out = Vec::new();
    for v in events {
        if let Ok(e) = serde_json::from_value(v) {
            out.push(e);
        }
    }
    Ok(out)
}
