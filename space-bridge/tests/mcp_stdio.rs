//! MCP stdio protocol tests: real `spaceos-mcp` binary over pipes.
//!
//! No Core needed here: initialize/list/error paths are transport-level.
//! End-to-end tool execution lives in `mcp_e2e.rs`.
#![cfg(windows)]

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Cross-process exclusion: `mcp_errors_are_honest_without_core` asserts that
/// the GLOBAL `\\.\pipe\spaceos-core` is absent, and `spaceos-mcp` reaches the
/// Core through exactly that pipe (`mcp.rs`). Another suite's in-process Core
/// on the global pipe would make the assertion observe a live Core.
#[path = "support/global_lock.rs"]
mod global_lock;

fn mcp_bin() -> std::path::PathBuf {
    let mut dir = std::env::current_exe().unwrap();
    dir.pop();
    if dir.ends_with("deps") {
        dir.pop();
    }
    dir.join("spaceos-mcp.exe")
}

struct McpProc {
    child: tokio::process::Child,
    reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    stdin: tokio::process::ChildStdin,
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
        let reader = BufReader::new(stdout).lines();
        Self {
            child,
            reader,
            stdin,
        }
    }

    async fn rpc(&mut self, id: i64, method: &str, params: serde_json::Value) -> serde_json::Value {
        let req =
            serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let mut s = serde_json::to_string(&req).unwrap();
        s.push('\n');
        self.stdin.write_all(s.as_bytes()).await.unwrap();
        self.stdin.flush().await.unwrap();
        let line = tokio::time::timeout(Duration::from_secs(10), self.reader.next_line())
            .await
            .expect("mcp response timeout")
            .expect("mcp stdout closed")
            .expect("no line");
        serde_json::from_str(&line).expect("response is JSON")
    }

    async fn kill_wait(&mut self) {
        let _ = self.child.kill().await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_initialize_and_list() {
    let mut m = McpProc::spawn().await;
    let init = m
        .rpc(
            1,
            "initialize",
            serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            }),
        )
        .await;
    assert_eq!(init["result"]["protocolVersion"], "2024-11-05");
    assert_eq!(init["result"]["serverInfo"]["name"], "spaceos");

    let list = m.rpc(2, "tools/list", serde_json::json!({})).await;
    let tools = list["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 20, "all spec tools advertised");
    for name in [
        "spaceos_status",
        "spaceos_actions",
        "spaceos_action_status",
        "spaceos_trace",
        "browser_tabs",
        "browser_navigate",
        "browser_reload",
        "browser_get_state",
        "browser_get_content",
        "filesystem_roots",
        "filesystem_scan",
        "filesystem_list",
        "filesystem_search",
        "filesystem_read_file",
        "filesystem_create_file",
        "filesystem_write_file",
        "filesystem_job_status",
        "mission_get",
        "mission_update",
        "external_mcp_call",
    ] {
        assert!(
            tools.iter().any(|t| t["name"] == name),
            "tool {name} listed"
        );
    }
    // Every tool has a usable schema.
    for t in tools {
        assert_eq!(t["inputSchema"]["type"], "object", "schema: {}", t["name"]);
    }
    m.kill_wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_errors_are_honest_without_core() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let mut m = McpProc::spawn().await;
    // No Core running: status tool must report failure, not fake success.
    let r = m
        .rpc(
            1,
            "tools/call",
            serde_json::json!({"name": "spaceos_status", "arguments": {}}),
        )
        .await;
    assert_eq!(
        r["result"]["isError"], true,
        "unreachable Core must surface as tool error"
    );
    // Unknown tool -> JSON-RPC method-level error.
    let r = m
        .rpc(
            2,
            "tools/call",
            serde_json::json!({"name": "nope", "arguments": {}}),
        )
        .await;
    assert!(r.get("error").is_some(), "unknown tool: {r}");
    // Malformed line -> -32700, server survives.
    m.stdin.write_all(b"{not json\n").await.unwrap();
    m.stdin.flush().await.unwrap();
    let line = tokio::time::timeout(Duration::from_secs(5), m.reader.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["error"]["code"], -32700);
    let ping = m.rpc(3, "ping", serde_json::json!({})).await;
    assert!(ping.get("result").is_some(), "alive after bad input");
    m.kill_wait().await;
}
