//! Outgoing MCP client — SpaceOS as an MCP *client* of external servers.
//!
//! This is the other direction from `mcp.rs` (where SpaceOS serves its
//! own tools). The transport is real MCP over child-process stdio
//! (JSON-RPC 2.0, newline-delimited): spawn, `initialize`, `tools/list`,
//! `tools/call`, `ping`, time-bounded reads, deterministic teardown
//! (kill on drop / close). No new transport is invented and no protocol
//! is faked: every exchange on the wire is a JSON-RPC message the peer
//! must answer.
//!
//! Trust boundary (see `ExternalPolicy`): an external tool is NEVER
//! available-by-default. Calls need an explicit allow-list entry
//! (`server_id:tool`), validated arguments, and a provenance record
//! (who initiated, who executes, correlation, result or error) written
//! through the normal Core path by the caller. Timeouts never imply
//! success: an unknown post-timeout outcome is reported as unknown and
//! is never auto-retried (external effects may be non-idempotent).

use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Default per-call timeout (matches the server side).
pub const CLIENT_CALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Protocol we speak.
pub const CLIENT_PROTOCOL_VERSION: &str = "2024-11-05";

/// Redact secret-looking arguments before they reach logs/traces.
/// Keys compared case-insensitively; values replaced, shape kept.
pub fn redact(value: &Value) -> Value {
    const SECRET_HINTS: &[&str] = &[
        "secret",
        "password",
        "passwd",
        "token",
        "api_key",
        "apikey",
        "auth",
        "credential",
        "private_key",
    ];
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                let kl = k.to_lowercase();
                if SECRET_HINTS.iter().any(|h| kl.contains(h)) {
                    out.insert(k.clone(), json!("[redacted]"));
                } else {
                    out.insert(k.clone(), redact(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(redact).collect()),
        other => other.clone(),
    }
}

/// Allow-list gate for external tool execution. Default-deny: an empty
/// table allows nothing. Entries are exact `server_id:tool` pairs.
/// Parse from env: `SPACEOS_EXTERNAL_MCP_ALLOW="srv:tool,srv2:tool2"`.
#[derive(Debug, Clone, Default)]
pub struct ExternalPolicy {
    allow: Vec<(String, String)>,
}

impl ExternalPolicy {
    pub fn empty() -> Self {
        Self { allow: vec![] }
    }

    pub fn from_env() -> Self {
        Self::parse(&std::env::var("SPACEOS_EXTERNAL_MCP_ALLOW").unwrap_or_default())
    }

    pub fn parse(spec: &str) -> Self {
        let mut allow = vec![];
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((s, t)) = part.split_once(':') {
                let (s, t) = (s.trim(), t.trim());
                if !s.is_empty() && !t.is_empty() {
                    allow.push((s.to_string(), t.to_string()));
                }
            }
        }
        Self { allow }
    }

    /// Explicit permission check BEFORE any external execution.
    pub fn check(&self, server_id: &str, tool: &str) -> Result<(), String> {
        if self.allow.iter().any(|(s, t)| s == server_id && t == tool) {
            Ok(())
        } else {
            Err(format!(
                "denied: external tool {server_id}:{tool} not allow-listed"
            ))
        }
    }

    /// Provenance record the caller persists through Core (MissionNote /
    /// report_observation shape): initiator, executor, correlation,
    /// result-or-error. Never claims success without a peer answer.
    pub fn provenance(
        &self,
        initiator: &str,
        server_id: &str,
        tool: &str,
        correlation_id: &str,
        outcome: &Value,
    ) -> Value {
        json!({
            "initiator": initiator,
            "executor": format!("external-mcp:{server_id}"),
            "tool": tool,
            "correlation_id": correlation_id,
            "outcome": outcome,
            "policy": "allow-list",
        })
    }
}

/// One validated external tool description.
#[derive(Debug, Clone)]
pub struct ExternalTool {
    pub name: String,
    pub description: String,
    pub schema: Value,
}

fn validate_tool(v: &Value) -> Result<ExternalTool, String> {
    let name = v
        .get("name")
        .and_then(|n| n.as_str())
        .filter(|n| !n.trim().is_empty())
        .ok_or_else(|| "external tool without a name".to_string())?;
    let schema = v.get("inputSchema").cloned().unwrap_or(json!({}));
    if schema.get("type").and_then(|t| t.as_str()) != Some("object") {
        return Err(format!("tool {name}: inputSchema.type must be object"));
    }
    Ok(ExternalTool {
        name: name.to_string(),
        description: v
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or("")
            .to_string(),
        schema,
    })
}

/// Validate call arguments against the tool schema: object shape +
/// every `required` key present. Types beyond presence are the peer's
/// business (it validates and answers with errors, never panics us).
pub fn validate_args(tool: &ExternalTool, args: &Value) -> Result<(), String> {
    let obj = args
        .as_object()
        .ok_or_else(|| format!("tool {}: arguments must be an object", tool.name))?;
    let required: Vec<String> = tool
        .schema
        .get("required")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    for key in required {
        if !obj.contains_key(&key) {
            return Err(format!(
                "tool {}: missing required argument: {key}",
                tool.name
            ));
        }
    }
    Ok(())
}

/// External server registration. The program path comes ONLY from
/// local bridge configuration (env), never from caller input: callers
/// name a registered `id`, they never choose what executes. Arguments
/// are fixed at registration too (space-separated, no quoting); only
/// tool arguments travel per call, as JSON data over stdio — no shell
/// is ever involved.
#[derive(Debug, Clone)]
pub struct ServerSpec {
    pub id: String,
    pub program: String,
    pub args: Vec<String>,
    /// Extra environment for the child (e.g. API keys). Never logged;
    /// `redact()` applies to call arguments, env values are never
    /// echoed back in results or provenance.
    pub env: HashMap<String, String>,
}

/// Parse `SPACEOS_EXTERNAL_MCP_SERVERS`:
/// `id|program|arg1 arg2;id2|program2|` (program without `|`).
/// Malformed entries are skipped loudly via the returned warnings.
pub fn servers_from_env() -> (Vec<ServerSpec>, Vec<String>) {
    parse_servers(&std::env::var("SPACEOS_EXTERNAL_MCP_SERVERS").unwrap_or_default())
}

pub fn parse_servers(spec: &str) -> (Vec<ServerSpec>, Vec<String>) {
    let mut out = vec![];
    let mut warnings = vec![];
    for part in spec.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut it = part.split('|');
        let (id, program, args) = (
            it.next().unwrap_or("").trim(),
            it.next().unwrap_or("").trim(),
            it.next().unwrap_or("").trim(),
        );
        if id.is_empty() || program.is_empty() {
            warnings.push(format!("skipped malformed server entry: {part:?}"));
            continue;
        }
        out.push(ServerSpec {
            id: id.to_string(),
            program: program.to_string(),
            args: args.split_whitespace().map(|s| s.to_string()).collect(),
            env: HashMap::new(),
        });
    }
    (out, warnings)
}

/// Guarded external call (the production-path adapter core):
/// server must be registered locally, tool must be allow-listed,
/// arguments validated against the LIVE-discovered schema, result (or
/// honest error) returned with a provenance record for the Core path.
/// One fresh session per call: slower than pooling, but every call
/// re-discovers the toolset (toolset change can never go stale) and no
/// session state survives between calls.
pub async fn call_external(
    servers: &[ServerSpec],
    policy: &ExternalPolicy,
    initiator: &str,
    server_id: &str,
    tool_name: &str,
    args: Value,
    timeout: Duration,
) -> Result<Value, String> {
    let spec = servers.iter().find(|s| s.id == server_id).ok_or_else(|| {
        format!("denied: unknown external server {server_id:?} (not registered locally)")
    })?;
    policy.check(server_id, tool_name)?;
    // Everything past the policy gate returns Ok with an honest outcome
    // (completed OR failed) plus provenance: even discovery and timeout
    // failures are recorded facts, never silent, never success.
    let correlation_id = format!("ext-{}", uuid::Uuid::new_v4());
    let outcome = match run_external(&spec, policy, initiator, tool_name, &args, timeout).await {
        Ok(v) => json!({"status": "completed", "result": v}),
        Err(e) => json!({"status": "failed", "error": e}),
    };
    let result_value = outcome.get("result").cloned().unwrap_or(json!(null));
    Ok(json!({
        "server": server_id,
        "tool": tool_name,
        "correlation_id": correlation_id.clone(),
        "result": result_value,
        "provenance": policy.provenance(initiator, server_id, tool_name, &correlation_id, &outcome),
    }))
}

/// One guarded session: connect, live discovery, arg validation, call.
/// Errors here mean "no verifiable result" (transport, timeout, peer
/// error, schema mismatch) — the caller records them as failed outcome.
async fn run_external(
    spec: &ServerSpec,
    policy: &ExternalPolicy,
    initiator: &str,
    tool_name: &str,
    args: &Value,
    timeout: Duration,
) -> Result<Value, String> {
    let mut client =
        McpClient::connect(&spec.id, &spec.program, &spec.args, &spec.env, timeout).await?;
    let tools = client.list_tools(timeout).await?;
    let tool = tools.iter().find(|t| t.name == tool_name).ok_or_else(|| {
        format!(
            "denied: {}:{} not advertised by the peer",
            spec.id, tool_name
        )
    })?;
    validate_args(tool, args)?;
    let v = client
        .call_tool(policy, initiator, tool, args.clone(), timeout)
        .await
        .map_err(|e| format!("{e:#}"))?;
    let _ = client.close().await;
    // A peer-reported isError is a FAILED outcome, not a success with
    // funny content: never let it pass as completed. (`call_tool`
    // already unwrapped the RPC envelope.)
    if v.get("isError") == Some(&json!(true)) {
        let text = v
            .get("content")
            .and_then(|c| c.get(0))
            .and_then(|t| t.get("text"))
            .cloned()
            .unwrap_or(json!("peer reported failure"));
        return Err(format!("peer tool failed: {text}"));
    }
    Ok(v)
}

/// Persist an external-call provenance record through Core
/// (`report_observation`, same atomic path as missions). Returns the
/// outcome (`applied` false only on recognized redelivery).
pub async fn report_external_provenance(
    actor: &str,
    token: &str,
    mission_id: &str,
    provenance: &Value,
    observation_id: &str,
) -> anyhow::Result<Value> {
    let params = serde_json::json!({
        "observation_id": observation_id,
        "world_id": "",
        "facts": [],
        "objects": [],
        "relations": [],
        "delete_ids": [],
        "event": {
            "id": uuid::Uuid::new_v4().to_string(),
            "event_type": "ExternalToolCalled",
            "actor": actor,
            "provider_id": "space-bridge",
            "object_ids": [],
            "payload": { "mission_id": mission_id, "provenance": provenance },
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "correlation_id": provenance.get("correlation_id").cloned().unwrap_or(json!("")),
            "world_id": "",
            "seq": 0,
        },
        "actor": actor,
        "actor_token": token,
    });
    let resp = crate::ipc_client::core_request("report_observation", params).await?;
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        anyhow::bail!("report_observation rejected: {resp}");
    }
    Ok(resp.get("result").cloned().unwrap_or(json!({})))
}

/// Connected session to one external MCP server process.
pub struct McpClient {
    server_id: String,
    child: Child,
    stdin: ChildStdin,
    lines: BufReader<ChildStdout>,
    next_id: i64,
    closed: bool,
}

impl McpClient {
    /// Spawn the server command and run `initialize`. The child is
    /// killed on drop/close; no orphan processes by construction.
    pub async fn connect(
        server_id: &str,
        program: &str,
        args: &[String],
        envs: &HashMap<String, String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        let mut cmd = Command::new(program);
        cmd.args(args)
            .envs(envs)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| format!("spawn {program}: {e}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "no child stdin".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "no child stdout".to_string())?;
        let mut client = Self {
            server_id: server_id.to_string(),
            child,
            stdin,
            lines: BufReader::new(stdout),
            next_id: 1,
            closed: false,
        };
        let resp = client
            .rpc(
                "initialize",
                json!({"protocolVersion": CLIENT_PROTOCOL_VERSION,
                       "capabilities": {},
                       "clientInfo": {"name": "spaceos", "version": "0.1.0"}}),
                timeout,
            )
            .await?;
        if resp.get("result").is_none() {
            return Err(format!("initialize failed: {resp}"));
        }
        Ok(client)
    }

    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    async fn rpc(
        &mut self,
        method: &str,
        params: Value,
        timeout_d: Duration,
    ) -> Result<Value, String> {
        if self.closed {
            return Err("session closed".to_string());
        }
        let id = self.next_id;
        self.next_id += 1;
        let mut line = serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params,
        }))
        .map_err(|e| format!("encode: {e}"))?;
        line.push('\n');
        tokio::time::timeout(timeout_d, self.stdin.write_all(line.as_bytes()))
            .await
            .map_err(|_| "write timeout".to_string())?
            .map_err(|e| format!("write: {e}"))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| format!("flush: {e}"))?;
        // Skip protocol-legal blank lines (harness chatter, keepalives);
        // only a zero-byte read is EOF. Bounded by the same timeout.
        let mut out = String::new();
        loop {
            out.clear();
            let n = tokio::time::timeout(timeout_d, self.lines.read_line(&mut out))
                .await
                .map_err(|_| format!("timeout waiting for {method} reply"))?
                .map_err(|e| format!("read: {e}"))?;
            if n == 0 {
                return Err("server closed the session (EOF)".to_string());
            }
            if !out.trim().is_empty() {
                break;
            }
        }
        let resp: Value =
            serde_json::from_str(out.trim()).map_err(|e| format!("malformed reply: {e}"))?;
        if resp.get("id") != Some(&json!(id)) {
            return Err(format!("id mismatch: {resp}"));
        }
        if let Some(err) = resp.get("error") {
            return Err(format!("peer error: {err}"));
        }
        Ok(resp)
    }

    pub async fn ping(&mut self, timeout_d: Duration) -> Result<(), String> {
        self.rpc("ping", json!({}), timeout_d).await.map(|_| ())
    }

    /// Discovery: `tools/list` + per-tool schema validation. A changed
    /// toolset is simply what the peer advertises now; callers compare
    /// against expectations themselves.
    pub async fn list_tools(&mut self, timeout_d: Duration) -> Result<Vec<ExternalTool>, String> {
        let resp = self.rpc("tools/list", json!({}), timeout_d).await?;
        let tools = resp
            .get("result")
            .and_then(|r| r.get("tools"))
            .and_then(|t| t.as_array())
            .ok_or_else(|| format!("bad tools/list shape: {resp}"))?;
        tools.iter().map(validate_tool).collect()
    }

    /// Call one tool: policy check + argument validation happen BEFORE
    /// anything is sent. Unknown post-timeout outcomes are returned as
    /// errors and never auto-retried here.
    pub async fn call_tool(
        &mut self,
        policy: &ExternalPolicy,
        initiator: &str,
        tool: &ExternalTool,
        args: Value,
        timeout_d: Duration,
    ) -> Result<Value, String> {
        policy.check(&self.server_id, &tool.name)?;
        validate_args(tool, &args)?;
        let _ = initiator;
        let resp = self
            .rpc(
                "tools/call",
                json!({"name": tool.name, "arguments": args}),
                timeout_d,
            )
            .await?;
        resp.get("result")
            .cloned()
            .ok_or_else(|| format!("bad tools/call shape: {resp}"))
    }

    /// Graceful close: ask, then kill. Never leaves the child behind.
    pub async fn close(mut self) -> Result<(), String> {
        self.closed = true;
        let _ = self.child.kill().await;
        Ok(())
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.child.start_kill();
        }
    }
}
