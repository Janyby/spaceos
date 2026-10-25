//! Outgoing MCP client: real stdio transport against the `mcp_fixture`
//! helper binary (a real separate MCP-speaking process; its stdout
//! carries only protocol lines). No MCP is faked: every exchange is
//! JSON-RPC over real child pipes. Policy, redaction and teardown are
//! asserted alongside transport.
#![cfg(windows)]

use serde_json::json;
use space_bridge::mcp_client::{redact, validate_args, ExternalPolicy, ExternalTool, McpClient};
use std::collections::HashMap;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Client tests (peer = the `mcp_fixture` helper binary via CARGO_BIN_EXE).
// ---------------------------------------------------------------------------

fn spawn_fixture(
    mode: &str,
    tools: Option<&str>,
) -> (String, Vec<String>, HashMap<String, String>) {
    let exe = env!("CARGO_BIN_EXE_mcp_fixture").to_string();
    let mut envs = HashMap::new();
    envs.insert("MCP_FIXTURE_MODE".to_string(), mode.to_string());
    if let Some(t) = tools {
        envs.insert("MCP_FIXTURE_TOOLS".to_string(), t.to_string());
    }
    (exe, vec![], envs)
}

async fn connect_fixture(mode: &str, tools: Option<&str>) -> McpClient {
    let (exe, args, envs) = spawn_fixture(mode, tools);
    McpClient::connect("fixture", &exe, &args, &envs, Duration::from_secs(10))
        .await
        .expect("fixture connect")
}

fn policy_all() -> ExternalPolicy {
    ExternalPolicy::parse("fixture:echo,fixture:boom,fixture:alt_tool")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_connect_ping_discover() {
    let mut c = connect_fixture("ok", None).await;
    assert_eq!(c.server_id(), "fixture");
    c.ping(Duration::from_secs(5)).await.expect("ping");
    let tools = c.list_tools(Duration::from_secs(5)).await.expect("list");
    let names: Vec<String> = tools.iter().map(|t| t.name.clone()).collect();
    assert_eq!(names, vec!["echo".to_string(), "boom".to_string()]);
    c.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_call_validated_and_recorded() {
    let mut c = connect_fixture("ok", None).await;
    let tools = c.list_tools(Duration::from_secs(5)).await.unwrap();
    let echo = tools.iter().find(|t| t.name == "echo").unwrap();
    // Missing required arg is rejected BEFORE anything is sent.
    assert!(validate_args(echo, &json!({})).is_err());
    let out = c
        .call_tool(
            &policy_all(),
            "opencode-test",
            echo,
            json!({"text": "hi"}),
            Duration::from_secs(5),
        )
        .await
        .expect("call echo");
    assert_eq!(out["content"][0]["text"], "hi");
    // Peer-side failure arrives as data, not transport success.
    let boom = tools.iter().find(|t| t.name == "boom").unwrap();
    let out = c
        .call_tool(
            &policy_all(),
            "opencode-test",
            boom,
            json!({}),
            Duration::from_secs(5),
        )
        .await
        .expect("call boom");
    assert_eq!(out["isError"], true);
    // Provenance record shape for the Core path.
    let prov = policy_all().provenance("opencode-test", "fixture", "echo", "corr-1", &out);
    assert_eq!(prov["initiator"], "opencode-test");
    assert_eq!(prov["executor"], "external-mcp:fixture");
    assert_eq!(prov["correlation_id"], "corr-1");
    c.close().await.unwrap();
}

#[test]
fn mcp_client_policy_default_deny_and_redaction() {
    let empty = ExternalPolicy::empty();
    assert!(empty.check("fixture", "echo").is_err());
    assert!(ExternalPolicy::parse("").check("a", "b").is_err());
    assert!(ExternalPolicy::parse("fixture:echo")
        .check("fixture", "echo")
        .is_ok());
    assert!(ExternalPolicy::parse("fixture:echo")
        .check("fixture", "boom")
        .is_err());
    // Secrets never reach logs/traces; shape is kept.
    let red = redact(&json!({"path": "a.txt", "api_token": "s3cr3t",
                             "nested": {"password": "x", "ok": 1}}));
    assert_eq!(red["api_token"], "[redacted]");
    assert_eq!(red["nested"]["password"], "[redacted]");
    assert_eq!(red["nested"]["ok"], 1);
    assert_eq!(red["path"], "a.txt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_policy_checked_before_send() {
    let mut c = connect_fixture("ok", None).await;
    let tools = c.list_tools(Duration::from_secs(5)).await.unwrap();
    let echo = tools.iter().find(|t| t.name == "echo").unwrap();
    let err = c
        .call_tool(
            &ExternalPolicy::empty(),
            "opencode-test",
            echo,
            json!({"text": "hi"}),
            Duration::from_secs(5),
        )
        .await
        .expect_err("default-deny");
    assert!(err.contains("not allow-listed"), "{err}");
    c.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_malformed_reply_is_honest_error() {
    let mut c = connect_fixture("malformed", None).await;
    let tools = c.list_tools(Duration::from_secs(5)).await.unwrap();
    let echo = tools.iter().find(|t| t.name == "echo").unwrap();
    let err = c
        .call_tool(
            &policy_all(),
            "t",
            echo,
            json!({"text": "x"}),
            Duration::from_secs(5),
        )
        .await
        .expect_err("malformed");
    assert!(err.contains("malformed"), "{err}");
    // Session still usable afterwards (framing recovered on next line).
    let out = c
        .call_tool(
            &policy_all(),
            "t",
            echo,
            json!({"text": "y"}),
            Duration::from_secs(5),
        )
        .await
        .expect("recovered");
    assert_eq!(out["content"][0]["text"], "y");
    c.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_timeout_never_reports_success() {
    let mut c = connect_fixture("slow", None).await;
    // list itself times out: unknown outcome, returned as error.
    let err = c
        .list_tools(Duration::from_secs(1))
        .await
        .expect_err("slow list must time out");
    assert!(err.contains("timeout"), "{err}");
    c.close().await.unwrap();
    // Reconnect after the failure: no hung session, no orphan.
    let mut c2 = connect_fixture("ok", None).await;
    c2.ping(Duration::from_secs(5))
        .await
        .expect("ping after reconnect");
    c2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_peer_exit_is_eof_not_success() {
    let (exe, args, envs) = spawn_fixture("exit-early", None);
    // Either initialize already sees EOF (connect fails) or the first
    // call does — both are honest errors, never success.
    match McpClient::connect("fixture", &exe, &args, &envs, Duration::from_secs(5)).await {
        Err(e) => assert!(!e.is_empty()),
        Ok(mut c) => {
            let res = c.list_tools(Duration::from_secs(5)).await;
            assert!(res.is_err(), "dead peer must error");
            let _ = c.close().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_toolset_change_is_discovered_live() {
    let mut c = connect_fixture("ok", Some("alt")).await;
    let tools = c.list_tools(Duration::from_secs(5)).await.unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "alt_tool");
    // The new tool works end to end through policy.
    let out = c
        .call_tool(
            &ExternalPolicy::parse("fixture:alt_tool"),
            "t",
            &tools[0],
            json!({}),
            Duration::from_secs(5),
        )
        .await
        .expect("call alt_tool");
    assert_eq!(out["content"][0]["text"], "alt-ok");
    // A stale assumption (echo, gone on this peer) fails loudly at the
    // peer instead of succeeding on stale data.
    let stale = ExternalTool {
        name: "echo".to_string(),
        description: String::new(),
        schema: json!({"type": "object"}),
    };
    let err = c
        .call_tool(
            &ExternalPolicy::parse("fixture:echo"),
            "t",
            &stale,
            json!({}),
            Duration::from_secs(5),
        )
        .await
        .expect_err("echo is gone on this peer");
    assert!(err.contains("unknown tool"), "{err}");
    c.close().await.unwrap();
}
