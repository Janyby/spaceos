//! IPC Client — communicates with SpaceOS Core via named pipes.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use space_core::ipc::{core_token, send_request, CORE_PIPE};
use std::time::Duration;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Timeout for Core IPC requests.
const IPC_TIMEOUT: Duration = Duration::from_secs(10);
static READ_TOKEN: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static ISOLATED_READ_CONFIG: OnceLock<(String, String)> = OnceLock::new();
static MCP_READ_TOKEN: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static ISOLATED_MCP_READ_CONFIG: OnceLock<(String, String)> = OnceLock::new();

pub fn configure_isolated_mcp_read_service(registrar: String, world: String) -> Result<()> {
    anyhow::ensure!(std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(), "isolated MCP fixture required");
    ISOLATED_MCP_READ_CONFIG.set((registrar, world)).map_err(|_| anyhow::anyhow!("MCP read fixture already configured"))
}

/// Distinct MCP read identity. Never borrows the Bridge reader registrar or its
/// legacy allowlist. The server profile pins both capability and exact World.
pub async fn mcp_read_request(method: &str, mut params: Value) -> Result<Value> {
    use space_core::service_registration::{MCP_READ_ACTOR, MCP_READ_PROFILE, WORLD_READ_CAPS};
    let (registrar, world) = if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        ISOLATED_MCP_READ_CONFIG.get().cloned().context("isolated MCP read service not configured")?
    } else {
        (crate::service_credentials::from_env(MCP_READ_PROFILE).context("MCP read registrar missing")?,
         space_core::semantic::semantic_world_id_from_env().map_err(anyhow::Error::msg)?)
    };
    anyhow::ensure!(params.get("world_id").is_none_or(|v| v.is_null() || v.as_str() == Some(&world)), "MCP read World denied");
    params["world_id"] = json!(world);
    let mut token = MCP_READ_TOKEN.get_or_init(|| Mutex::new(None)).lock().await;
    for attempt in 0..2 {
        if token.is_none() {
            let registered = core_request("register_service_actor", json!({"profile":MCP_READ_PROFILE,
                "name":MCP_READ_ACTOR,"capabilities":WORLD_READ_CAPS,"worlds":[world],"registrar_token":registrar})).await?;
            *token = Some(registered["result"]["token"].as_str().context("MCP read registration denied")?.to_owned());
        }
        params["actor"] = json!(MCP_READ_ACTOR);
        params["actor_token"] = json!(token.as_deref());
        let response = core_request(method, params.clone()).await?;
        if response["ok"] == true { return Ok(response); }
        if attempt == 0 && response["error"].as_str().is_some_and(|e| e.starts_with("denied:")) { *token = None; continue; }
        anyhow::bail!("MCP scoped read denied or failed");
    }
    unreachable!()
}

pub async fn world_read_request(method: &str, mut params: Value) -> Result<Value> {
    let (_, world) = read_registration_config()?;
    anyhow::ensure!(params.get("world_id").is_none_or(|v| v.is_null() || v.as_str() == Some(&world)), "World read scope mismatch");
    params["world_id"] = json!(world);
    let response = scoped_read_request(method, params).await?;
    anyhow::ensure!(response["ok"] == true, "World read denied or failed");
    Ok(response)
}

/// Only laboratory fixtures may inject synthetic read registration data.
pub fn configure_isolated_read_service(registrar: String, world_id: String) -> Result<()> {
    anyhow::ensure!(
        std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(),
        "read fixture configuration requires isolation"
    );
    ISOLATED_READ_CONFIG
        .set((registrar, world_id))
        .map_err(|_| anyhow::anyhow!("read fixture is already configured"))
}

fn read_registration_config() -> Result<(String, String)> {
    if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        return ISOLATED_READ_CONFIG
            .get()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("isolated read-service fixture is not configured"));
    }
    let registrar = crate::service_credentials::from_env(
        space_core::service_registration::WORLD_READ_PROFILE,
    )
    .ok_or_else(|| anyhow::anyhow!("World read registrar credential missing"))?;
    let world = space_core::semantic::semantic_world_id_from_env()
        .map_err(anyhow::Error::msg)?;
    Ok((registrar, world))
}

/// Authenticate a separate read-only service profile, never the dashboard
/// action actor or a general transport/system credential. Core still checks
/// capability, World and the server-side legacy object allowlist.
pub async fn scoped_read_request(method: &str, mut params: Value) -> Result<Value> {
    let slot = READ_TOKEN.get_or_init(|| Mutex::new(None));
    let mut token = slot.lock().await;
    for attempt in 0..2 {
        if token.is_none() {
            let (registrar, world) = read_registration_config()?;
            let registration = core_request(
                "register_service_actor",
                json!({
                    "profile": space_core::service_registration::WORLD_READ_PROFILE,
                    "name": space_core::service_registration::WORLD_READ_ACTOR,
                    "capabilities": space_core::service_registration::WORLD_READ_CAPS,
                    "worlds": [world],
                    "registrar_token": registrar,
                }),
            )
            .await?;
            let issued = registration["result"]["token"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("World read service registration denied"))?;
            *token = Some(issued.to_string());
        }
        params["actor"] = json!(space_core::service_registration::WORLD_READ_ACTOR);
        params["actor_token"] = json!(token.as_ref().unwrap());
        let response = core_request(method, params.clone()).await?;
        if response["ok"] == true || attempt == 1 {
            return Ok(response);
        }
        let denied = response["error"].as_str().unwrap_or("").starts_with("denied:");
        if !denied {
            return Ok(response);
        }
        *token = None;
    }
    unreachable!()
}

/// Send a request to SpaceOS Core and return the parsed response.
pub async fn core_request(method: &str, params: Value) -> Result<Value> {
    let params = params;
    // Explicit process-local override supports isolated integration stacks.
    // An invalid override never falls back to the production pipe.
    let pipe = if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        let explicit = std::env::var("SPACEOS_BRIDGE_CORE_PIPE").context("isolated Core endpoint missing")?;
        anyhow::ensure!(explicit != CORE_PIPE, "production Core endpoint forbidden in isolation");
        explicit
    } else {
        std::env::var("SPACEOS_BRIDGE_CORE_PIPE").unwrap_or_else(|_| CORE_PIPE.into())
    };
    anyhow::ensure!(
        pipe.starts_with(r"\\.\pipe\") && pipe.len() > 9,
        "invalid SPACEOS_BRIDGE_CORE_PIPE"
    );
    let result = timeout(IPC_TIMEOUT, send_request(&pipe, method, params))
        .await
        .context("Core IPC request timed out")??;
    Ok(result)
}

/// Send a request to a specific provider pipe.
pub async fn provider_request(pipe_path: &str, method: &str, params: Value) -> Result<Value> {
    let filesystem = std::env::var("SPACEOS_FILESYSTEM_PROVIDER_PIPE").ok();
    let browser = std::env::var("SPACEOS_BROWSER_PROVIDER_PIPE").ok();
    let endpoint = provider_request_endpoint(
        pipe_path,
        std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(),
        filesystem.as_deref(),
        browser.as_deref(),
    )?;
    let mut params = params;
    params["auth"] = json!(core_token());
    let result = timeout(IPC_TIMEOUT, send_request(&endpoint, method, params))
        .await
        .context("Provider IPC request timed out")??;
    Ok(result)
}

// Legacy production behavior is unchanged. Laboratory metadata probes must
// never fall back to live Provider pipes merely because no daemon was started.
fn provider_request_endpoint(pipe: &str, isolated: bool, filesystem: Option<&str>, browser: Option<&str>) -> Result<String> {
    if !isolated { return Ok(pipe.to_owned()); }
    let endpoint = match pipe {
        space_core::ipc::PROVIDER_PIPE => filesystem,
        space_core::ipc::BROWSER_PROVIDER_PIPE => browser,
        _ => anyhow::bail!("unknown isolated Provider endpoint"),
    }.context("isolated Provider endpoint not configured")?;
    anyhow::ensure!(endpoint.starts_with(r"\\.\pipe\spaceos-4-3a4-"), "unsafe isolated Provider endpoint");
    Ok(endpoint.to_owned())
}

#[cfg(test)]
mod provider_endpoint_isolation_tests {
    use super::provider_request_endpoint as resolve;
    use space_core::ipc::{PROVIDER_PIPE, BROWSER_PROVIDER_PIPE};
    #[test] fn missing_lab_endpoint_denied() { assert!(resolve(PROVIDER_PIPE,true,None,None).is_err()); }
    #[test] fn production_endpoint_denied_in_lab() { assert!(resolve(PROVIDER_PIPE,true,Some(PROVIDER_PIPE),None).is_err()); }
    #[test] fn explicit_filesystem_endpoint_used() { let p=r"\\.\pipe\spaceos-4-3a4-first-mission-fs"; assert_eq!(resolve(PROVIDER_PIPE,true,Some(p),None).unwrap(),p); }
    #[test] fn explicit_browser_endpoint_used() { let p=r"\\.\pipe\spaceos-4-3a4-first-mission-browser"; assert_eq!(resolve(BROWSER_PROVIDER_PIPE,true,None,Some(p)).unwrap(),p); }
    #[test] fn unknown_lab_endpoint_denied() { assert!(resolve(r"\\.\pipe\arbitrary",true,None,None).is_err()); }
    #[test] fn production_behavior_unchanged() { assert_eq!(resolve(PROVIDER_PIPE,false,Some("ignored"),None).unwrap(),PROVIDER_PIPE); }
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
    let resp = scoped_read_request("query_objects", query).await?;
    anyhow::ensure!(resp["ok"] == true, "Core object read denied");
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

/// Side-effect-free Provider readiness probe; no resource access or action.
pub async fn filesystem_stat(path: &str) -> Result<Value> {
    use space_core::ipc::PROVIDER_PIPE;
    let _ = path;
    provider_request(PROVIDER_PIPE, "ping", json!({})).await
}

/// Fetch Core events correlated to one action id (lifecycle view).
pub async fn action_events(action_id: &str) -> Result<Vec<Value>> {
    let resp = scoped_read_request("query_events", json!({ "limit": 500 })).await?;
    anyhow::ensure!(resp["ok"] == true, "Core event read denied");
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
    let resp = scoped_read_request("query_relations", query).await?;
    anyhow::ensure!(resp["ok"] == true, "Core relation read denied");
    let relations = resp.get("result").cloned().unwrap_or(json!([]));
    Ok(relations.as_array().cloned().unwrap_or_default())
}

/// Query events from Core storage with cursor support.
pub async fn query_events(query: space_core::query::EventQuery) -> Result<Vec<space_core::Event>> {
    let resp = scoped_read_request("query_events", serde_json::to_value(query)?).await?;
    if resp["ok"] != true {
        if resp["error"].as_str().is_some_and(|e| e.starts_with("denied:")) {
            anyhow::bail!("Core event read denied");
        }
        anyhow::bail!("Core event query failed");
    }
    serde_json::from_value(resp.get("result").cloned().context("invalid event query response")?)
        .context("invalid event query response")
}
