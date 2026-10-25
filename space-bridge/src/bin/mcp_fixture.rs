//! MCP fixture server for `tests/mcp_client.rs` — TEST ONLY, never shipped
//! as a product surface (it rides along `cargo build -p space-bridge`
//! next to `spaceos-mcp`; release scripts use `spaceos-mcp.exe` only).
//!
//! Real MCP-over-stdio peer with scripted behaviors, driven by env:
//! - `MCP_FIXTURE_MODE`: unset/`ok` | `malformed` | `slow` | `exit-early`
//! - `MCP_FIXTURE_TOOLS=alt`: advertise a different toolset (change test)
//!
//! Stdout carries ONLY protocol lines, so framing is deterministic
//! (unlike re-spawning the test binary itself, whose harness chatter
//! shares stdout).

use serde_json::{json, Value};
use std::io::BufRead;
use std::time::Duration;

fn tools_for_mode() -> Value {
    if std::env::var("MCP_FIXTURE_TOOLS").as_deref() == Ok("alt") {
        return json!([{"name": "alt_tool", "description": "v2",
                       "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}}]);
    }
    json!([
        {"name": "echo", "description": "return text",
         "inputSchema": {"type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"], "additionalProperties": false}},
        {"name": "boom", "description": "always fails",
         "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}},
    ])
}

fn rpc_ok(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_err(id: &Value, code: i64, msg: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": msg}})
}

fn main() {
    let mode = std::env::var("MCP_FIXTURE_MODE").unwrap_or_default();
    if mode == "exit-early" {
        std::process::exit(0);
    }
    let stdin = std::io::stdin();
    let mut first = true;
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                println!("{}", rpc_err(&Value::Null, -32700, "parse error"));
                continue;
            }
        };
        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
        // Slow mode stalls real work (list/call) but answers session
        // setup immediately, so connect succeeds and the timeout is
        // observed exactly where the test expects it.
        if mode == "slow" && (method == "tools/list" || method == "tools/call") {
            std::thread::sleep(Duration::from_secs(30));
        }
        let resp = match method {
            "initialize" => rpc_ok(
                &id,
                json!({"protocolVersion": "2024-11-05", "capabilities": {"tools": {}},
                       "serverInfo": {"name": "fixture", "version": "0.0.1"}}),
            ),
            "ping" => rpc_ok(&id, json!({})),
            "tools/list" => rpc_ok(&id, json!({"tools": tools_for_mode()})),
            "tools/call" => {
                if mode == "malformed" && first {
                    first = false;
                    println!("NOT JSON {{{{");
                    continue;
                }
                first = false;
                let alt_only = std::env::var("MCP_FIXTURE_TOOLS").as_deref() == Ok("alt");
                let name = msg
                    .get("params")
                    .and_then(|p| p.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                // In alt mode the old tools are really gone (true toolset
                // change), not just unlisted.
                let echo_gone = alt_only && name == "echo";
                let boom_gone = alt_only && name == "boom";
                match name {
                    _ if echo_gone || boom_gone => rpc_err(&id, -32602, "unknown tool"),
                    "echo" => {
                        let text = msg
                            .get("params")
                            .and_then(|p| p.get("arguments"))
                            .and_then(|a| a.get("text"))
                            .cloned()
                            .unwrap_or(json!(null));
                        rpc_ok(&id, json!({"content": [{"type": "text", "text": text}]}))
                    }
                    "boom" => rpc_ok(
                        &id,
                        json!({"content": [{"type": "text", "text": "kaboom"}], "isError": true}),
                    ),
                    "alt_tool" => rpc_ok(
                        &id,
                        json!({"content": [{"type": "text", "text": "alt-ok"}]}),
                    ),
                    _ => rpc_err(&id, -32602, "unknown tool"),
                }
            }
            _ => rpc_err(&id, -32601, "method not found"),
        };
        println!("{resp}");
    }
}
