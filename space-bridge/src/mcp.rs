//! SpaceOS MCP server core: JSON-RPC 2.0 over stdio for OpenCode.
//!
//! Transport choice: this server talks to Core IPC (named pipes) directly,
//! exactly like the HTTP bridge does — every state-changing tool goes
//! through Core `request_action` as the registered `space-bridge-mcp`
//! agent, so policy, Action* events and SpaceTrace correlation are
//! identical for Dashboard and MCP callers. No provider pipe is touched
//! for execution (the read-only `roots` probe is the single exception,
//! mirroring the bridge health checks).
//!
//! Actor identity: MCP-originated actions act as `space-bridge-mcp`,
//! distinct from the dashboard's `space-bridge` actor, so both sides see
//! each other's operations in the shared history.

use serde_json::{json, Value};
use space_core::ipc::{self, CORE_PIPE};
use std::time::Duration;
use tokio::time::timeout;

pub const PROTOCOL_VERSION: &str = "2024-11-05";
pub const SERVER_NAME: &str = "spaceos";
pub const SERVER_VERSION: &str = "0.1.0";
pub const MCP_ACTOR: &str = "space-bridge-mcp";

/// Caps the MCP agent needs (mirrors the bridge dashboard agent).
const MCP_CAPS: &[&str] = &[
    "action.navigate",
    "action.reload",
    "action.get_tabs",
    "action.get_page_content",
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

const CALL_TIMEOUT: Duration = Duration::from_secs(15);

/// Cached agent registration (re-registered on denial, like bridge).
async fn ensure_agent(cached: &tokio::sync::Mutex<Option<String>>) -> anyhow::Result<String> {
    if let Some(t) = cached.lock().await.clone() {
        return Ok(t);
    }
    let caps: Vec<Value> = MCP_CAPS.iter().map(|c| json!(c)).collect();
    let resp = core_request(
        "register_agent",
        json!({
            "principal": "system",
            "name": MCP_ACTOR,
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
    *cached.lock().await = Some(token.clone());
    Ok(token)
}

async fn core_request(method: &str, params: Value) -> anyhow::Result<Value> {
    timeout(CALL_TIMEOUT, ipc::send_request(CORE_PIPE, method, params))
        .await
        .map_err(|_| anyhow::anyhow!("core request timed out"))?
        .map_err(anyhow::Error::from)
}

async fn core_action(
    token_cache: &tokio::sync::Mutex<Option<String>>,
    provider_id: &str,
    action: &str,
    target: Value,
) -> anyhow::Result<Value> {
    let token = ensure_agent(token_cache).await?;
    let first = core_request(
        "request_action",
        json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "action": action,
            "target": target,
            "provider_id": provider_id,
            "actor": MCP_ACTOR,
            "actor_token": token,
            "requested_at": chrono::Utc::now().to_rfc3339(),
        }),
    )
    .await?;
    let denied = first
        .get("result")
        .and_then(|r| r.get("status"))
        .and_then(|s| s.as_str())
        == Some("Denied");
    if !denied {
        return Ok(first);
    }
    // Possibly a rotated token (Core restart): re-register once, retry once.
    *token_cache.lock().await = None;
    let token = ensure_agent(token_cache).await?;
    core_request(
        "request_action",
        json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "action": action,
            "target": target,
            "provider_id": provider_id,
            "actor": MCP_ACTOR,
            "actor_token": token,
            "requested_at": chrono::Utc::now().to_rfc3339(),
        }),
    )
    .await
}

/// Unwrap a Core `request_action` envelope into (ok, action_result).
/// Transport/timeout failures are Err; Denied/Failed are Ok with status.
fn action_outcome(resp: &Value) -> Value {
    resp.get("result").cloned().unwrap_or(json!(null))
}

// ---------------------------------------------------------------------------
// Tool definitions (name, description, input schema).
// ---------------------------------------------------------------------------

struct ToolDef {
    name: &'static str,
    description: &'static str,
    schema: Value,
}

fn str_prop(desc: &str) -> Value {
    json!({"type": "string", "description": desc})
}

fn tool_defs() -> Vec<ToolDef> {
    vec![
        ToolDef {
            name: "spaceos_status",
            description: "SpaceOS component status: Core providers plus browser and filesystem connectivity. No arguments.",
            schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
        },
        ToolDef {
            name: "spaceos_actions",
            description: "Recent SpaceOS actions (newest first) with status, actor and provider. Dashboard and MCP actions share this history.",
            schema: json!({"type": "object", "properties": {
                "limit": {"type": "integer", "description": "Max actions (default 20, max 100)", "default": 20},
            }, "additionalProperties": false}),
        },
        ToolDef {
            name: "spaceos_action_status",
            description: "Lifecycle of one action_id: Queued/Running/Completed/Failed/Denied plus the correlated Core events.",
            schema: json!({"type": "object", "properties": {
                "action_id": str_prop("Action ID returned by a previous tool call"),
            }, "required": ["action_id"], "additionalProperties": false}),
        },
        ToolDef {
            name: "spaceos_trace",
            description: "Diagnostics for one action/correlation ID: lifecycle events and job linkage. Core span buffers stay per-process by design; the durable event chain is returned.",
            schema: json!({"type": "object", "properties": {
                "action_id": str_prop("Action or correlation ID to trace"),
            }, "required": ["action_id"], "additionalProperties": false}),
        },
        ToolDef {
            name: "browser_tabs",
            description: "List real browser tabs (id, url, title) from Chrome via CDP.",
            schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
        },
        ToolDef {
            name: "browser_navigate",
            description: "Navigate the current browser tab to a URL through Core policy and the browser provider.",
            schema: json!({"type": "object", "properties": {
                "url": str_prop("Absolute http(s) URL to open"),
            }, "required": ["url"], "additionalProperties": false}),
        },
        ToolDef {
            name: "browser_reload",
            description: "Reload the current browser tab through Core policy and the browser provider.",
            schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
        },
        ToolDef {
            name: "browser_get_state",
            description: "Current browser state: open tabs plus recent browser actions, if the architecture reports them.",
            schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
        },
        ToolDef {
            name: "browser_get_content",
            description: "Read the current page DOM text via CDP through Core policy (no embedded rendering; arbitrary sites forbid framing).",
            schema: json!({"type": "object", "properties": {
                "max_chars": {"type": "integer", "description": "Max characters returned (default 4000, max 20000)", "default": 4000},
            }, "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_roots",
            description: "Allowed filesystem roots enforced by the provider (paths outside are refused).",
            schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_scan",
            description: "Start scanning an allowed root. Returns Started plus a job id immediately; poll filesystem_job_status for the terminal result.",
            schema: json!({"type": "object", "properties": {
                "path": str_prop("Allowed root or subdirectory to scan"),
            }, "required": ["path"], "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_list",
            description: "List indexed files and folders under a directory (from the object store, no scan).",
            schema: json!({"type": "object", "properties": {
                "dir": str_prop("Windows directory path, e.g. C:\\spaceos\\data"),
                "limit": {"type": "integer", "description": "Max entries (default 200)", "default": 200},
            }, "required": ["dir"], "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_search",
            description: "Search indexed files by name substring (same query the Dashboard uses).",
            schema: json!({"type": "object", "properties": {
                "q": str_prop("Name substring to search for"),
                "limit": {"type": "integer", "description": "Max entries (default 100)", "default": 100},
            }, "required": ["q"], "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_read_file",
            description: "Read an allowed UTF-8 text file (max 256 KB) through Core policy.",
            schema: json!({"type": "object", "properties": {
                "path": str_prop("Windows file path, e.g. C:\\spaceos\\data\\hello.txt"),
            }, "required": ["path"], "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_create_file",
            description: "Create a new text file. Fails loudly if the file already exists (edit it instead).",
            schema: json!({"type": "object", "properties": {
                "path": str_prop("Windows file path to create"),
                "content": str_prop("Initial text content (optional)"),
            }, "required": ["path"], "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_write_file",
            description: "Overwrite an existing text file through Core policy. Fails if the file does not exist. Pass expected_modified/expected_size from a previous read to detect concurrent changes.",
            schema: json!({"type": "object", "properties": {
                "path": str_prop("Windows file path to overwrite"),
                "content": str_prop("New text content"),
                "expected_modified": str_prop("Optional modification stamp from filesystem_read_file; refuses on mismatch"),
                "expected_size": {"type": "integer", "description": "Optional byte size from filesystem_read_file; refuses on mismatch (Windows mtime can stall across rapid writes)"},
            }, "required": ["path", "content"], "additionalProperties": false}),
        },
        ToolDef {
            name: "filesystem_job_status",
            description: "Status of a background scan job: Queued/Running/Completed/Failed plus stats. Poll until terminal.",
            schema: json!({"type": "object", "properties": {
                "action_id": str_prop("Action ID returned by filesystem_scan"),
            }, "required": ["action_id"], "additionalProperties": false}),
        },
        ToolDef {
            name: "mission_get",
            description: "Full Mission context: goal, workspace, state, ordered steps with states/approvals/evidence, linked mission events, progress. Call before planning or resuming work on a mission.",
            schema: json!({"type": "object", "properties": {
                "mission_id": str_prop("Mission id, e.g. mission-<uuid>"),
            }, "required": ["mission_id"], "additionalProperties": false}),
        },
        ToolDef {
            name: "mission_update",
            description: "Validated Mission/Step transition. Mission-level: pass only mission_id + to (running/awaiting_approval/verifying/completed/failed/cancelled). Step-level: also pass step_id + to (running/awaiting_approval/completed/failed/skipped), optional evidence/action_ids/result. Repeating the current state is a safe no-op (applied:false). Steps with requires_approval complete only after approval (awaiting_approval -> running).",
            schema: json!({"type": "object", "properties": {
                "mission_id": str_prop("Mission id"),
                "step_id": str_prop("Step id for step-level transitions (omit for mission-level)"),
                "to": str_prop("Target state"),
                "evidence": str_prop("Human-readable evidence for completion (optional)"),
                "result": str_prop("Machine-readable result summary (optional)"),
                "action_id": str_prop("Core action id to link to this step; repeat the tool per action (optional)"),
            }, "required": ["mission_id", "to"], "additionalProperties": false}),
        },
        ToolDef {
            name: "external_mcp_call",
            description: "Call a locally-registered external MCP server (default-deny allow-list, live schema validation, provenance recorded in Core). The server program comes from bridge configuration, never from this call. Optional mission_id links the provenance record to a Mission.",
            schema: json!({"type": "object", "properties": {
                "server": str_prop("Registered external server id (bridge env config)"),
                "tool": str_prop("Allow-listed tool name on that server"),
                "arguments": str_prop("JSON object string with the tool arguments (default {})"),
                "mission_id": str_prop("Mission id for provenance linkage (optional)"),
                "timeout_secs": {"type": "integer", "description": "Call timeout in seconds (default 15, max 120)", "default": 15},
            }, "required": ["server", "tool"], "additionalProperties": false}),
        },
    ]
}

fn tools_list() -> Value {
    let tools: Vec<Value> = tool_defs()
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": t.schema,
            })
        })
        .collect();
    json!({"tools": tools})
}

// ---------------------------------------------------------------------------
// Tool dispatch. Every state-changing tool goes through Core request_action.
// Errors are returned as MCP isError content, never as transport failures
// (unless Core itself is unreachable).
// ---------------------------------------------------------------------------

fn text_result(text: String, is_error: bool) -> Value {
    let mut r = json!({"content": [{"type": "text", "text": text}]});
    if is_error {
        r["isError"] = json!(true);
    }
    r
}

fn arg_str(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn arg_u64(args: &Value, key: &str, default: u64, max: u64) -> usize {
    args.get(key)
        .and_then(|v| v.as_u64())
        .unwrap_or(default)
        .min(max) as usize
}

fn strip_tags(html: &str) -> String {
    crate::app::strip_tags(html)
}

async fn events_for(action_id: &str) -> anyhow::Result<Vec<Value>> {
    let resp = core_request("query_events", json!({"limit": 500})).await?;
    let all: Vec<Value> = resp
        .get("result")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(all
        .into_iter()
        .filter(|e| e.get("correlation_id").and_then(|v| v.as_str()) == Some(action_id))
        .collect())
}

pub async fn call_tool(
    token_cache: &tokio::sync::Mutex<Option<String>>,
    name: &str,
    args: &Value,
) -> Value {
    let args = if args.is_null() { &json!({}) } else { args };
    match dispatch(token_cache, name, args).await {
        Ok(text) => text_result(text, false),
        Err(e) => text_result(format!("SpaceOS tool {name} failed: {e:#}"), true),
    }
}

async fn dispatch(
    token_cache: &tokio::sync::Mutex<Option<String>>,
    name: &str,
    args: &Value,
) -> anyhow::Result<String> {
    let missing = |k: &str| anyhow::anyhow!("missing required argument: {k}");
    let ok = |v: Value| Ok(serde_json::to_string_pretty(&v).unwrap());
    match name {
        "spaceos_status" => {
            let providers = core_request("list_providers", json!({})).await?;
            let rows: Vec<Value> = providers
                .get("result")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            ok(json!({"core": "ok", "providers": rows}))
        }
        "spaceos_actions" => {
            let limit = arg_u64(args, "limit", 20, 100);
            let resp = core_request("query_events", json!({"limit": 500})).await?;
            let events: Vec<Value> = resp
                .get("result")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            ok(json!({"actions": crate::app::action_history(events, limit)}))
        }
        "spaceos_action_status" | "filesystem_job_status" => {
            let id = arg_str(args, "action_id").ok_or_else(|| missing("action_id"))?;
            let evs = events_for(&id).await?;
            ok(json!(crate::app::action_summary(&id, &evs)))
        }
        "spaceos_trace" => {
            let id = arg_str(args, "action_id").ok_or_else(|| missing("action_id"))?;
            let evs = events_for(&id).await?;
            let summary = crate::app::action_summary(&id, &evs);
            ok(json!({
                "action_id": id,
                "lifecycle": summary,
                "events": evs,
                "note": "Durable Core event chain for this correlation. In-process SpaceTrace span buffers stay per-process by design and are not shipped between processes.",
            }))
        }
        "browser_tabs" => {
            let r = core_action(token_cache, "browser", "get_tabs", json!({})).await?;
            ok(action_outcome(&r))
        }
        "browser_navigate" => {
            let url = arg_str(args, "url").ok_or_else(|| missing("url"))?;
            if url.trim().is_empty() {
                anyhow::bail!("empty url");
            }
            let r = core_action(token_cache, "browser", "navigate", json!({"url": url})).await?;
            ok(action_outcome(&r))
        }
        "browser_reload" => {
            let r = core_action(token_cache, "browser", "reload", json!({})).await?;
            ok(action_outcome(&r))
        }
        "browser_get_state" => {
            let r = core_action(token_cache, "browser", "get_tabs", json!({})).await?;
            let tabs = r
                .get("result")
                .and_then(|x| x.get("detail"))
                .and_then(|d| d.get("tabs"))
                .cloned()
                .unwrap_or(json!([]));
            let recent = core_request(
                "query_events",
                json!({"provider_id": "browser", "limit": 5}),
            )
            .await?;
            ok(json!({
                "tabs": tabs,
                "recent_events": recent.get("result").cloned().unwrap_or(json!([])),
            }))
        }
        "browser_get_content" => {
            let max = arg_u64(args, "max_chars", 4000, 20000);
            let r = core_action(token_cache, "browser", "get_page_content", json!({})).await?;
            let doc = r
                .get("result")
                .and_then(|x| x.get("detail"))
                .and_then(|d| d.get("document"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // Text excerpt, not raw HTML dump: honest preview of page text.
            let text = strip_tags(doc);
            let truncated = text.len() > max;
            let excerpt: String = text.chars().take(max).collect();
            ok(json!({
                "length": text.len(),
                "truncated": truncated,
                "excerpt": excerpt,
                "action_id": r.get("result").and_then(|x| x.get("action_id")).cloned().unwrap_or(json!(null)),
                "status": r.get("result").and_then(|x| x.get("status")).cloned().unwrap_or(json!(null)),
            }))
        }
        "filesystem_roots" => {
            let r = provider_roots().await?;
            ok(r)
        }
        "filesystem_scan" => {
            let path = arg_str(args, "path").ok_or_else(|| missing("path"))?;
            let r = core_action(token_cache, "filesystem", "scan", json!({"path": path})).await?;
            ok(action_outcome(&r))
        }
        "filesystem_list" => {
            let dir = arg_str(args, "dir").ok_or_else(|| missing("dir"))?;
            let limit = arg_u64(args, "limit", 200, 1000);
            let resp = core_request("query_objects", json!({"provider_id": "filesystem"})).await?;
            let objects: Vec<Value> = resp
                .get("result")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let (items, truncated) = crate::app::filter_by_dir(objects, &dir, limit);
            ok(json!({"dir": dir, "items": items, "count": items.len(), "truncated": truncated}))
        }
        "filesystem_search" => {
            let q = arg_str(args, "q").ok_or_else(|| missing("q"))?;
            if q.trim().is_empty() {
                anyhow::bail!("empty q");
            }
            let limit = arg_u64(args, "limit", 100, 500);
            let resp = core_request(
                "query_objects",
                json!({"provider_id": "filesystem", "path_contains": q}),
            )
            .await?;
            let objects: Vec<Value> = resp
                .get("result")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let (items, truncated) = crate::app::summarize(objects, limit);
            ok(json!({"q": q, "items": items, "count": items.len(), "truncated": truncated}))
        }
        "filesystem_read_file" => {
            let path = arg_str(args, "path").ok_or_else(|| missing("path"))?;
            let r = core_action(token_cache, "filesystem", "read", json!({"path": path})).await?;
            ok(action_outcome(&r))
        }
        "filesystem_create_file" => {
            let path = arg_str(args, "path").ok_or_else(|| missing("path"))?;
            let content = arg_str(args, "content").unwrap_or_default();
            let r = core_action(
                token_cache,
                "filesystem",
                "create",
                json!({"path": path, "content": content}),
            )
            .await?;
            ok(action_outcome(&r))
        }
        "filesystem_write_file" => {
            let path = arg_str(args, "path").ok_or_else(|| missing("path"))?;
            let content = arg_str(args, "content").ok_or_else(|| missing("content"))?;
            let mut target = json!({"path": path, "content": content});
            if let Some(stamp) = arg_str(args, "expected_modified") {
                target["expected_modified"] = json!(stamp);
            }
            if let Some(size) = args.get("expected_size").and_then(|v| v.as_u64()) {
                target["expected_size"] = json!(size);
            }
            let r = core_action(token_cache, "filesystem", "write", target).await?;
            ok(action_outcome(&r))
        }
        "mission_get" => {
            let id = arg_str(args, "mission_id").ok_or_else(|| missing("mission_id"))?;
            let view = crate::mission::get_view(&id).await?;
            // Attach linked action lifecycles so the agent sees what each
            // step already executed (same Core event chain as the UI).
            let mut steps = view.steps.clone();
            for s in &mut steps {
                let ids: Vec<String> = s
                    .get("properties")
                    .and_then(|p| p.get("action_ids"))
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|v| v.as_str().map(|x| x.to_string()))
                    .collect();
                let mut linked = vec![];
                for aid in ids {
                    if let Ok(evs) = events_for(&aid).await {
                        linked.push(crate::app::action_summary(&aid, &evs));
                    }
                }
                s["linked_actions"] = json!(linked);
            }
            let mut v = view.to_json();
            v["steps"] = json!(steps);
            ok(v)
        }
        "mission_update" => {
            let token = ensure_agent(token_cache).await?;
            let id = arg_str(args, "mission_id").ok_or_else(|| missing("mission_id"))?;
            let to = arg_str(args, "to").ok_or_else(|| missing("to"))?;
            let mut extra = json!({});
            for k in ["evidence", "result"] {
                if let Some(val) = arg_str(args, k) {
                    extra[k] = json!(val);
                }
            }
            let (view, applied) = match arg_str(args, "step_id") {
                Some(step_id) => {
                    if let Some(aid) = arg_str(args, "action_id") {
                        extra["action_ids"] = json!([aid]);
                    }
                    crate::mission::transition_step(MCP_ACTOR, &token, &id, &step_id, &to, extra)
                        .await?
                }
                None => {
                    crate::mission::transition_mission(MCP_ACTOR, &token, &id, &to, extra).await?
                }
            };
            let mut v = view.to_json();
            v["applied"] = json!(applied);
            ok(v)
        }
        "external_mcp_call" => {
            let token = ensure_agent(token_cache).await?;
            let server = arg_str(args, "server").ok_or_else(|| missing("server"))?;
            let tool = arg_str(args, "tool").ok_or_else(|| missing("tool"))?;
            let args_v: Value = match arg_str(args, "arguments") {
                Some(s) if !s.trim().is_empty() => serde_json::from_str(&s)
                    .map_err(|e| anyhow::anyhow!("arguments must be a JSON object string: {e}"))?,
                _ => json!({}),
            };
            let mission_id = arg_str(args, "mission_id").unwrap_or_default();
            let timeout = std::time::Duration::from_secs(
                args.get("timeout_secs")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(15)
                    .min(120),
            );
            let (servers, _warnings) = crate::mcp_client::servers_from_env();
            let policy = crate::mcp_client::ExternalPolicy::from_env();
            let out = crate::mcp_client::call_external(
                &servers, &policy, MCP_ACTOR, &server, &tool, args_v, timeout,
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
            let provenance = out.get("provenance").cloned().unwrap_or(json!({}));
            let obs_id = format!(
                "ext-{}-{}",
                out.get("correlation_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("noid"),
                uuid::Uuid::new_v4()
            );
            let outcome = crate::mcp_client::report_external_provenance(
                MCP_ACTOR,
                &token,
                &mission_id,
                &provenance,
                &obs_id,
            )
            .await?;
            let mut v = out;
            v["provenance_applied"] = json!(outcome
                .get("applied")
                .and_then(|v| v.as_bool())
                .unwrap_or(false));
            ok(v)
        }
        other => anyhow::bail!("unknown tool: {other}"),
    }
}

/// Read-only roots probe straight at the provider pipe (no state change,
/// mirrors the bridge health checks; all execution still goes via Core).
async fn provider_roots() -> anyhow::Result<Value> {
    use space_core::ipc::PROVIDER_PIPE;
    let mut params = json!({});
    params["auth"] = json!(space_core::ipc::core_token());
    timeout(
        CALL_TIMEOUT,
        ipc::send_request(PROVIDER_PIPE, "roots", params),
    )
    .await
    .map_err(|_| anyhow::anyhow!("provider roots request timed out"))?
    .map_err(anyhow::Error::from)
}

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 framing over stdio.
// ---------------------------------------------------------------------------

fn rpc_error(id: Value, code: i64, message: String) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn rpc_ok(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

/// Handle one parsed JSON-RPC value. Returns Some(response) for requests,
/// None for notifications (never answered).
pub async fn handle_message(
    token_cache: &tokio::sync::Mutex<Option<String>>,
    msg: &Value,
) -> Option<Value> {
    if msg.is_array() {
        let mut out = Vec::new();
        for item in msg.as_array().unwrap() {
            if let Some(r) = handle_single(token_cache, item).await {
                out.push(r);
            }
        }
        return Some(json!(out));
    }
    handle_single(token_cache, msg).await
}

async fn handle_single(
    token_cache: &tokio::sync::Mutex<Option<String>>,
    msg: &Value,
) -> Option<Value> {
    let obj = msg.as_object()?;
    if obj.get("jsonrpc").and_then(|v| v.as_str()) != Some("2.0") {
        return Some(rpc_error(
            obj.get("id").cloned().unwrap_or(Value::Null),
            -32600,
            "invalid JSON-RPC version".into(),
        ));
    }
    let method = obj.get("method").and_then(|v| v.as_str())?;
    // Notification (no id): acknowledge silently, never answer.
    let id = match obj.get("id") {
        Some(v) if !v.is_null() => v.clone(),
        _ => return None,
    };
    match method {
        "initialize" => {
            let params = obj.get("params").cloned().unwrap_or(json!({}));
            let want = params
                .get("protocolVersion")
                .and_then(|v| v.as_str())
                .unwrap_or(PROTOCOL_VERSION);
            // Negotiate: we speak 2024-11-05; accept anything the client
            // offers and answer with ours (client decides compatibility).
            let _ = want;
            Some(rpc_ok(
                id,
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                }),
            ))
        }
        "ping" => Some(rpc_ok(id, json!({}))),
        "tools/list" => Some(rpc_ok(id, tools_list())),
        "tools/call" => {
            let params = obj.get("params").cloned().unwrap_or(json!({}));
            let name = match params.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => {
                    return Some(rpc_error(id, -32602, "missing tool name".into()));
                }
            };
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            let known = tool_defs().iter().any(|t| t.name == name);
            if !known {
                return Some(rpc_error(id, -32602, format!("unknown tool: {name}")));
            }
            let result = call_tool(token_cache, &name, &args).await;
            Some(rpc_ok(id, result))
        }
        _ => Some(rpc_error(id, -32601, format!("method not found: {method}"))),
    }
}

/// Handle one raw input line. Malformed JSON yields a -32700 response;
/// the server never crashes on bad input.
pub async fn handle_line(
    token_cache: &tokio::sync::Mutex<Option<String>>,
    line: &str,
) -> Option<Value> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(v) => handle_message(token_cache, &v).await,
        Err(e) => Some(rpc_error(Value::Null, -32700, format!("parse error: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache() -> tokio::sync::Mutex<Option<String>> {
        tokio::sync::Mutex::new(None)
    }

    #[tokio::test]
    async fn initialize_negotiates() {
        let c = cache();
        let r = handle_line(
            &c,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(r["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(r["result"]["serverInfo"]["name"], "spaceos");
    }

    #[tokio::test]
    async fn tools_list_has_all_spec_tools_with_schemas() {
        do_list_assert(
            20,
            &[
                "browser_get_content",
                "mission_get",
                "mission_update",
                "external_mcp_call",
            ],
        )
        .await;
    }

    async fn do_list_assert(count: usize, extra: &[&str]) {
        let c = cache();
        let r = handle_line(
            &c,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
        )
        .await
        .unwrap();
        let tools = r["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), count, "all spec tools present");
        for t in tools {
            assert!(t["name"].is_string());
            assert!(t["description"].is_string());
            assert_eq!(t["inputSchema"]["type"], "object");
        }
        for name in extra {
            assert!(
                tools.iter().any(|t| t["name"] == *name),
                "tool {name} listed"
            );
        }
    }

    #[test]
    fn strip_tags_preview() {
        let t = strip_tags(
            "<html><head><style>.x{color:red}</style></head><body><h1>Hi &amp; bye</h1><script>alert(1)</script><p>a&nbsp;b</p></body></html>",
        );
        assert_eq!(t, "Hi & bye a b");
        assert_eq!(strip_tags("plain"), "plain");
        assert_eq!(strip_tags(""), "");
    }

    #[tokio::test]
    async fn unknown_tool_is_protocol_error() {
        let c = cache();
        let r = handle_line(
            &c,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"nope","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(r["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn malformed_input_never_crashes() {
        let c = cache();
        let r = handle_line(&c, "{not json").await.unwrap();
        assert_eq!(r["error"]["code"], -32700);
        // Server still answers afterwards.
        let r2 = handle_line(&c, r#"{"jsonrpc":"2.0","id":4,"method":"ping"}"#)
            .await
            .unwrap();
        assert!(r2.get("result").is_some());
    }

    #[tokio::test]
    async fn notifications_are_silent() {
        let c = cache();
        let r = handle_line(
            &c,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .await;
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn missing_args_are_tool_errors_not_transport_errors() {
        let c = cache();
        let r = handle_line(
            &c,
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"browser_navigate","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert!(r["result"]["isError"].as_bool().unwrap_or(false));
    }
}
