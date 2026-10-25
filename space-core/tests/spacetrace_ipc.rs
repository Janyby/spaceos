#![cfg(windows)]
//! SpaceTrace IPC: trace_query/trace_stats behind Core auth + world scope (I22/I23).

use serde_json::json;
use space_core::{ipc, Object, Storage};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Cross-process AND intra-process exclusion (see
/// support/global_lock.rs): SpaceTrace global hooks are a last-Core-wins
/// singleton (hooks.rs `initialize` overwrites), mutations route via the
/// globals while trace_query reads the per-Core store — so two live Cores
/// in one process misdeliver traces. Production holds one Core per
/// process (invariant); tests hosting >1 Core must serialize. The file
/// lock serializes same-PID holders too (own PID counts as alive).
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-st-ipc");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

async fn start_core(pipe: &str) -> String {
    let db = tmp(&pipe.replace('\\', "_").replace(':', "_"));
    let _ = std::fs::remove_file(&db);
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    pipe.to_string()
}

async fn call(pipe: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities": caps, "worlds": worlds}),
    )
    .await;
    assert_eq!(r["ok"], true, "register {name}: {r}");
    r["result"]["token"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trace_query_auth_and_world_scope() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe = start_core(r"\\.\pipe\spaceos-st-ipc").await;
    let tok_a = register(&pipe, "reader-a", &["object.create"], &["world-A"]).await;

    // Emit trace in world-A via a real mutation (Core trace_mutation hook).
    let mut o = Object::new(
        "File",
        "reader-a",
        json!({"path": "a.txt", "world_id": "world-A"}),
    );
    o.id = "st-ipc-obj-a".to_string();
    let mut p = json!(o);
    p["actor_token"] = json!(tok_a);
    p["correlation_id"] = json!("st-ipc-corr-a");
    let r = call(&pipe, "create_object", p).await;
    assert_eq!(r["ok"], true, "create: {r}");

    // Emit trace in world-B via core principal (no world membership needed).
    let mut o = Object::new(
        "File",
        "filesystem",
        json!({"path": "b.txt", "world_id": "world-B"}),
    );
    o.id = "st-ipc-obj-b".to_string();
    let mut p = json!(o);
    p["actor"] = json!("system");
    p["correlation_id"] = json!("st-ipc-corr-b");
    // system is a core principal; send_request adds core auth automatically.
    let r = call(&pipe, "create_object", p).await;
    assert_eq!(r["ok"], true, "core create: {r}");

    // 1. Unknown subscriber denied (not core principal, not registered).
    // Note: "cli" IS a core principal by design (local CLI trusted;
    // same-OS-user theft explicitly out of model), and send_request always
    // attaches core auth — so anonymity is tested via unknown names.
    let r = call(
        &pipe,
        "trace_query",
        json!({"subscriber": "ghost-xyz-unknown", "world_id": "world-A"}),
    )
    .await;
    assert_eq!(r["ok"], false, "unknown subscriber must be denied: {r}");

    // 1b. No secrets in trace payloads (§24, incl. nested): MutationStart
    // params must not contain auth/actor_token/token keys at any depth.
    let mut sneaky = Object::new(
        "File",
        "reader-a",
        json!({"path": "s.txt", "world_id": "world-A",
               "nested": {"token": "TOP-SECRET", "deep": {"password": "PWNED"}}}),
    );
    sneaky.id = "st-ipc-sneaky".to_string();
    let mut p = json!(sneaky);
    p["actor_token"] = json!(tok_a);
    p["correlation_id"] = json!("st-ipc-sneaky");
    assert_eq!(call(&pipe, "create_object", p).await["ok"], true);
    fn has_secret_key(v: &serde_json::Value) -> Option<String> {
        const SECRETS: [&str; 8] = [
            "auth",
            "actor_token",
            "token",
            "api_key",
            "password",
            "secret",
            "private_key",
            "session_secret",
        ];
        match v {
            serde_json::Value::Object(m) => {
                for (k, child) in m {
                    if SECRETS.contains(&k.as_str()) {
                        return Some(k.clone());
                    }
                    if let Some(hit) = has_secret_key(child) {
                        return Some(hit);
                    }
                }
                None
            }
            serde_json::Value::Array(items) => items.iter().find_map(has_secret_key),
            _ => None,
        }
    }
    let r = call(
        &pipe,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "st-ipc-sneaky"}),
    )
    .await;
    assert_eq!(r["ok"], true);
    for e in r["result"].as_array().unwrap() {
        if e["kind"] == "MutationStart" {
            assert!(
                has_secret_key(&e["payload"]).is_none(),
                "§24 violation (nested): {:?}",
                has_secret_key(&e["payload"])
            );
        }
    }

    // 2. Agent without world_id denied (same rule as subscribe).
    let r = call(
        &pipe,
        "trace_query",
        json!({"subscriber": "reader-a", "actor_token": tok_a}),
    )
    .await;
    assert_eq!(r["ok"], false, "unscoped agent must be denied: {r}");

    // 3. Agent in wrong world denied.
    let r = call(
        &pipe,
        "trace_query",
        json!({"subscriber": "reader-a", "actor_token": tok_a, "world_id": "world-B"}),
    )
    .await;
    assert_eq!(r["ok"], false, "cross-world agent must be denied: {r}");

    // 4. Agent in own world: ok, only world-A traces.
    let r = call(
        &pipe,
        "trace_query",
        json!({"subscriber": "reader-a", "actor_token": tok_a, "world_id": "world-A"}),
    )
    .await;
    assert_eq!(r["ok"], true, "scoped agent query: {r}");
    let events = r["result"].as_array().unwrap();
    assert!(!events.is_empty(), "world-A must have trace events");
    assert!(
        events
            .iter()
            .all(|e| e.get("world_id").and_then(|v| v.as_str()) == Some("world-A")),
        "I23: 0 cross-world leak in agent results"
    );

    // 5. Core principal unscoped: ok.
    let r = call(
        &pipe,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "st-ipc-corr-a"}),
    )
    .await;
    assert_eq!(r["ok"], true, "core query: {r}");
    assert!(!r["result"].as_array().unwrap().is_empty());

    // 6. trace_stats authenticated ok; unknown subscriber denied.
    let r = call(
        &pipe,
        "trace_stats",
        json!({"subscriber": "reader-a", "actor_token": tok_a}),
    )
    .await;
    assert_eq!(r["ok"], true, "stats: {r}");
    let r = call(
        &pipe,
        "trace_stats",
        json!({"subscriber": "ghost-xyz-unknown"}),
    )
    .await;
    assert_eq!(r["ok"], false, "unknown stats denied: {r}");
}

struct SubConn {
    lines: tokio::io::Lines<
        BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    >,
}

async fn subscribe(pipe: &str, params: serde_json::Value) -> (SubConn, serde_json::Value) {
    let stream = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(pipe)
        .unwrap();
    let (r, mut w) = tokio::io::split(stream);
    let req = json!({"method": "subscribe", "params": params});
    let mut s = serde_json::to_string(&req).unwrap();
    s.push('\n');
    w.write_all(s.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut lines = BufReader::new(r).lines();
    let ack = lines.next_line().await.unwrap().unwrap_or_default();
    let ack: serde_json::Value = serde_json::from_str(&ack).unwrap();
    (SubConn { lines }, ack)
}

async fn recv_event(conn: &mut SubConn, ms: u64) -> Option<serde_json::Value> {
    match tokio::time::timeout(std::time::Duration::from_millis(ms), conn.lines.next_line()).await {
        Ok(Ok(Some(l))) => serde_json::from_str(&l).ok(),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_chain_traced_end_to_end() {
    // T3 (commit) -> EventPublished -> SubscriptionDelivery share one
    // correlation; delivery trace exists only for delivered events.
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe = start_core(r"\\.\pipe\spaceos-st-ipc-pubsub").await;
    let tok_w = register(&pipe, "writer-w", &["event.record"], &["WQ"]).await;
    let tok_r = register(&pipe, "reader-r", &[], &["WQ"]).await;

    let (mut sub, ack) = subscribe(
        &pipe,
        json!({"subscriber": "reader-r", "actor_token": tok_r, "world_id": "WQ"}),
    )
    .await;
    assert_eq!(ack["ok"], true, "subscribe: {ack}");

    let mut e = space_core::Event::new("QTick", "writer-w", json!({"n": 1}));
    e.world_id = "WQ".to_string();
    e.correlation_id = Some("st-ipc-pubsub-001".to_string());
    e.object_ids = vec![];
    let mut p = json!(e);
    p["actor_token"] = json!(tok_w);
    let r = call(&pipe, "record_event", p).await;
    assert_eq!(r["ok"], true, "record: {r}");

    let got = recv_event(&mut sub, 5000)
        .await
        .expect("delivery must arrive");
    assert_eq!(got["event"]["correlation_id"], "st-ipc-pubsub-001");

    // Trace chain for this correlation: mutation + published + delivery.
    let r = call(
        &pipe,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "st-ipc-pubsub-001"}),
    )
    .await;
    assert_eq!(r["ok"], true, "trace query: {r}");
    let kinds: Vec<String> = r["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["kind"].as_str().unwrap_or("").to_string())
        .collect();
    for expected in [
        "MutationStart",
        "MutationCommit",
        "EventPublished",
        "SubscriptionDelivery",
    ] {
        assert!(
            kinds.contains(&expected.to_string()),
            "chain missing {expected} in {kinds:?}"
        );
    }
    assert!(
        !kinds.contains(&"MutationRollback".to_string()),
        "no rollback on success path: {kinds:?}"
    );
}
