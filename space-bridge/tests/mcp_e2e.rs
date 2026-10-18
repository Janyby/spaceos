//! MCP end-to-end: real `spaceos-mcp` binary against a live in-process
//! Core with stub providers. Proves MCP calls travel the shared Core
//! policy/provider/event path and appear under the `space-bridge-mcp`
//! actor, visible to the Dashboard history.
#![cfg(windows)]

use serde_json::{json, Value};
use space_core::{ipc, ActionRequest, Storage};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[path = "support/global_lock.rs"]
mod global_lock;

use global_lock::{acquire_global_ipc, ServerGuard, TaskRegistry};

type Calls = Arc<Mutex<Vec<Value>>>;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-mcp-e2e").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn mcp_bin() -> PathBuf {
    let mut dir = std::env::current_exe().unwrap();
    dir.pop();
    if dir.ends_with("deps") {
        dir.pop();
    }
    dir.join("spaceos-mcp.exe")
}

/// Minimal browser stub: navigate/get_tabs complete, everything else fails.
async fn run_browser_stub(calls: Calls) {
    use tokio::net::windows::named_pipe::ServerOptions;
    loop {
        let server = match ServerOptions::new().create(ipc::BROWSER_PROVIDER_PIPE) {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        if server.connect().await.is_err() {
            continue;
        }
        let (reader, mut writer) = tokio::io::split(server);
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let req: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => break,
            };
            let params = req.get("params").cloned().unwrap_or(json!({}));
            let resp = if req["method"] == "ping" {
                json!({"ok": true, "result": {"pong": true}})
            } else {
                let areq: ActionRequest = match serde_json::from_value(params) {
                    Ok(r) => r,
                    Err(e) => {
                        let mut s =
                            serde_json::to_string(&json!({"ok": false, "error": e.to_string()}))
                                .unwrap();
                        s.push('\n');
                        let _ = writer.write_all(s.as_bytes()).await;
                        let _ = writer.flush().await;
                        break;
                    }
                };
                calls.lock().unwrap().push(json!({
                    "action": areq.action, "actor": areq.actor, "id": areq.id,
                }));
                match areq.action.as_str() {
                    "navigate" | "get_tabs" | "reload" => json!({"ok": true, "result": {
                        "action_id": areq.id, "status": "Completed",
                        "detail": {"tabs": [{"target_id": "t1", "url": "https://example.com/", "title": "Example"}]},
                    }}),
                    _ => json!({"ok": true, "result": {
                        "action_id": areq.id, "status": "Failed",
                        "detail": {"error": "stub cannot do it"},
                    }}),
                }
            };
            let mut s = serde_json::to_string(&resp).unwrap();
            s.push('\n');
            if writer.write_all(s.as_bytes()).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    }
}

struct McpProc {
    child: tokio::process::Child,
    reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    stdin: tokio::process::ChildStdin,
    next_id: i64,
}

impl McpProc {
    async fn spawn() -> Self {
        let bin = mcp_bin();
        assert!(bin.exists(), "spaceos-mcp.exe missing: {bin:?}");
        let mut child = tokio::process::Command::new(&bin)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn spaceos-mcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        Self {
            child,
            reader: BufReader::new(stdout).lines(),
            stdin,
            next_id: 1,
        }
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let mut s = serde_json::to_string(&req).unwrap();
        s.push('\n');
        self.stdin.write_all(s.as_bytes()).await.unwrap();
        self.stdin.flush().await.unwrap();
        let line = tokio::time::timeout(Duration::from_secs(30), self.reader.next_line())
            .await
            .expect("mcp response timeout")
            .expect("mcp stdout closed")
            .expect("no line");
        serde_json::from_str(&line).expect("response is JSON")
    }

    async fn call(&mut self, tool: &str, args: Value) -> Value {
        let r = self
            .rpc("tools/call", json!({"name": tool, "arguments": args}))
            .await;
        r["result"].clone()
    }

    async fn kill_wait(&mut self) {
        let _ = self.child.kill().await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await;
    }
}

fn tool_text(result: &Value) -> String {
    result["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

fn is_error(result: &Value) -> bool {
    result["isError"].as_bool().unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_actions_share_core_flow_and_history() {
    let _lock = acquire_global_ipc();
    let dir = tmp("e2e");
    let storage = Storage::open(&dir.join("core.db")).unwrap();
    let state = ipc::CoreState::new(storage);
    let reg: Arc<TaskRegistry> = Arc::new(TaskRegistry::default());
    let _guard = ServerGuard::new(reg.clone());
    reg.clone().spawn(async move {
        let _ = ipc::run_server(state, ipc::CORE_PIPE).await;
    });
    let calls: Calls = Arc::new(Mutex::new(vec![]));
    let cb = calls.clone();
    reg.clone().spawn(async move {
        run_browser_stub(cb).await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Providers registered so Core forwards (policy needs no caps on stubs;
    // Core only checks the CALLER side).
    for (id, caps) in [
        ("browser", vec!["observe", "navigate"]),
        ("filesystem", vec!["scan", "actions"]),
    ] {
        let r = ipc::send_request(
            ipc::CORE_PIPE,
            "register_provider",
            json!({"id": id, "name": id, "capabilities": caps}),
        )
        .await
        .unwrap();
        assert_eq!(r["ok"], true, "register {id}: {r}");
    }

    let mut mcp = McpProc::spawn().await;
    // Handshake first (protocol-level).
    let init = mcp
        .rpc(
            "initialize",
            json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {}}),
        )
        .await;
    assert_eq!(init["result"]["serverInfo"]["name"], "spaceos");

    // 1. Navigate through MCP: real provider execution via Core + policy.
    let nav = mcp
        .call("browser_navigate", json!({"url": "https://example.com"}))
        .await;
    assert!(!is_error(&nav), "navigate failed: {}", tool_text(&nav));
    let nav_json: Value = serde_json::from_str(&tool_text(&nav)).unwrap();
    assert_eq!(nav_json["status"], "Completed", "navigate: {nav_json}");
    let action_id = nav_json["action_id"].as_str().unwrap().to_string();

    // 2. The MCP actor is visible in Core events (shared history).
    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"limit": 500}))
        .await
        .unwrap();
    let all: Vec<Value> = evs["result"].as_array().cloned().unwrap_or_default();
    let mine: Vec<&Value> = all
        .iter()
        .filter(|e| e.get("correlation_id").and_then(|v| v.as_str()) == Some(action_id.as_str()))
        .collect();
    assert!(
        mine.iter().any(|e| e["event_type"] == "ActionRequested"),
        "requested event present"
    );
    assert!(
        mine.iter().any(|e| e["event_type"] == "ActionCompleted"),
        "completed event present"
    );
    assert!(
        mine.iter().all(|e| e["actor"] == "space-bridge-mcp"),
        "MCP actor identity preserved"
    );

    // 3. Same action visible through the MCP history tool (dashboard parity).
    let hist = mcp.call("spaceos_actions", json!({"limit": 50})).await;
    assert!(!is_error(&hist), "actions failed: {}", tool_text(&hist));
    let hist_json: Value = serde_json::from_str(&tool_text(&hist)).unwrap();
    let rows = hist_json["actions"].as_array();
    let found = rows
        .map(|a| {
            a.iter()
                .any(|row| row["action_id"] == action_id && row["actor"] == "space-bridge-mcp")
        })
        .unwrap_or(false);
    assert!(found, "MCP action visible in shared history");

    // 4. Status + trace tools resolve the same correlation.
    let st = mcp
        .call("spaceos_action_status", json!({"action_id": action_id}))
        .await;
    assert!(!is_error(&st), "status failed: {}", tool_text(&st));
    let st_json: Value = serde_json::from_str(&tool_text(&st)).unwrap();
    assert_eq!(st_json["status"], "Completed");
    let tr = mcp
        .call("spaceos_trace", json!({"action_id": action_id}))
        .await;
    assert!(!is_error(&tr), "trace failed: {}", tool_text(&tr));

    // 5. Provider saw the Core-forwarded call (no bypass).
    assert_eq!(calls.lock().unwrap().len(), 1, "exactly one provider call");
    assert_eq!(calls.lock().unwrap()[0]["actor"], "space-bridge-mcp");

    mcp.kill_wait().await;
}
