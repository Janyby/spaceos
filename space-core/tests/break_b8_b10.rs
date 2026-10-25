//! B8 (agent lifecycle) + B10 (ordering under load).
//! Fixed conditions, see docs/BREAKAGE_CAMPAIGN.md.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::ipc;

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb810");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn start_core(pipe: &str, db: &std::path::Path) {
    let storage = space_core::Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call(
        pipe,
        "register_agent",
        json!({"principal": "system", "name": name, "capabilities": caps, "worlds": worlds}),
    )
    .await;
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

fn with_token(mut p: Value, tok: &str) -> Value {
    p["actor_token"] = json!(tok);
    p
}

struct SubConn {
    lines: tokio::io::Lines<
        tokio::io::BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    >,
}

async fn subscribe(pipe: &str, params: Value) -> (SubConn, Value) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
    (SubConn { lines }, serde_json::from_str(&ack).unwrap())
}

async fn recv_event(conn: &mut SubConn, ms: u64) -> Option<Value> {
    match tokio::time::timeout(std::time::Duration::from_millis(ms), conn.lines.next_line()).await {
        Ok(Ok(Some(l))) => serde_json::from_str(&l).ok(),
        _ => None,
    }
}

fn pipe_for(tag: &str) -> String {
    Box::leak(format!(r"\\.\pipe\spaceos-bb810-{tag}-{}", std::process::id()).into_boxed_str())
        .to_string()
}

// ---------- B8: crash, restart, resubscribe, no fork, no silent miss ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_crash_restart_resubscribe() {
    let pipe = pipe_for("agent");
    let db = tmp_db("agent.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    // Stable definition row (V1.7 identity rule).
    let agent_obj = json!({"id": "agent:worker-1", "type": "Agent",
        "properties": {"name": "worker-1"}, "provider_id": "worker-1",
        "external_id": "agent:worker-1",
        "created_at": chrono::Utc::now().to_rfc3339(),
        "updated_at": chrono::Utc::now().to_rfc3339(),
        "actor": "worker-1"});
    let tok = register(
        &pipe,
        "worker-1",
        &["object.create", "event.record"],
        &["WA"],
    )
    .await;
    assert_eq!(
        call(&pipe, "create_object", with_token(agent_obj.clone(), &tok)).await["ok"],
        true
    );
    let (mut sub, ack) = subscribe(
        &pipe,
        with_token(json!({"subscriber": "worker-1", "world_id": "WA"}), &tok),
    )
    .await;
    assert_eq!(ack["ok"], true);
    // Work: one world event, seen live.
    let mut e1 = space_core::Event::new("Tick", "worker-1", json!({"i": 1}));
    e1.world_id = "WA".into();
    call(&pipe, "record_event", with_token(json!(e1), &tok)).await;
    assert!(
        recv_event(&mut sub, 2000).await.is_some(),
        "live flow works"
    );
    // Crash: drop subscription (connection dies with it).
    drop(sub);
    // Events keep flowing while "down" (other writers).
    for i in 2..=4 {
        let mut e = space_core::Event::new("Tick", "worker-1", json!({"i": i}));
        e.world_id = "WA".into();
        call(&pipe, "record_event", with_token(json!(e), &tok)).await;
    }
    // Restart: re-register (rotation) + resubscribe.
    let tok2 = register(
        &pipe,
        "worker-1",
        &["object.create", "event.record"],
        &["WA"],
    )
    .await;
    assert_ne!(tok, tok2);
    // Re-creating the definition row upserts, never forks.
    assert_eq!(
        call(&pipe, "create_object", with_token(agent_obj, &tok2)).await["ok"],
        true
    );
    let agents = call(&pipe, "query_objects", json!({"type": "Agent"})).await;
    assert_eq!(
        agents["result"].as_array().unwrap().len(),
        1,
        "one Agent row per name"
    );
    // Capabilities identical.
    let caps = call(&pipe, "policy_caps", json!({"actor": "worker-1"})).await;
    assert!(caps["result"]["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c == "event.record"));
    // Resubscribed flow resumes live...
    let (mut sub2, ack2) = subscribe(
        &pipe,
        with_token(json!({"subscriber": "worker-1", "world_id": "WA"}), &tok2),
    )
    .await;
    assert_eq!(ack2["ok"], true);
    let mut e5 = space_core::Event::new("Tick", "worker-1", json!({"i": 5}));
    e5.world_id = "WA".into();
    call(&pipe, "record_event", with_token(json!(e5), &tok2)).await;
    let got = recv_event(&mut sub2, 2000).await.expect("flow resumes");
    assert_eq!(got["event"]["payload"]["i"], json!(5));
    // ...and the missed window is explicitly recoverable via query.
    let missed = call(
        &pipe,
        "query_events",
        json!({"event_type": "Tick", "world_id": "WA"}),
    )
    .await;
    assert_eq!(missed["result"].as_array().unwrap().len(), 5);
}

// ---------- B10: interleaved two-world ordering ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_world_ordering() {
    let pipe = pipe_for("ord2");
    let db = tmp_db("ord2.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let tok_a = register(&pipe, "ord-a", &["event.record"], &["WA"]).await;
    let tok_b = register(&pipe, "ord-b", &["event.record"], &["WB"]).await;
    let (mut sa, _) = subscribe(
        &pipe,
        with_token(json!({"subscriber": "ord-a", "world_id": "WA"}), &tok_a),
    )
    .await;
    let (mut sb, _) = subscribe(
        &pipe,
        with_token(json!({"subscriber": "ord-b", "world_id": "WB"}), &tok_b),
    )
    .await;
    // Interleave rapidly: A1 B1 A2 B2 ... A20 B20.
    for i in 0..20 {
        for (actor, tok, w) in [("ord-a", &tok_a, "WA"), ("ord-b", &tok_b, "WB")] {
            let mut e = space_core::Event::new("Tick", actor, json!({"i": i}));
            e.world_id = w.into();
            call(&pipe, "record_event", with_token(json!(e), tok)).await;
        }
    }
    // Each world-scoped subscriber sees exactly its own 20, in send order.
    for (conn, w) in [(&mut sa, "WA"), (&mut sb, "WB")] {
        let mut seen = vec![];
        for _ in 0..20 {
            let ev = recv_event(conn, 3000).await.expect("ordered event");
            assert_eq!(ev["event"]["world_id"], json!(w));
            seen.push(ev["event"]["payload"]["i"].as_u64().unwrap());
        }
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        assert_eq!(seen, sorted, "in-order per world");
        assert_eq!(seen, (0..20).collect::<Vec<_>>(), "no gaps, no dups");
        assert!(
            recv_event(conn, 500).await.is_none(),
            "nothing foreign arrives"
        );
    }
}
