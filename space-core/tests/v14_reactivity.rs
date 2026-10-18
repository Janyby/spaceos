//! V1.4 agent reactivity + event integrity over real subscriptions.
//! Windows-only: subscriptions ride named-pipe connections.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::{ipc, LoopGuard, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(pipe: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v14r");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{}.db", pipe.replace('\\', "_").replace(':', "_")))
}

async fn start_core(pipe: &str) {
    let db = tmp_db(pipe);
    let _ = std::fs::remove_file(&db);
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":worlds}),
    )
    .await;
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

fn with_token(mut p: Value, tok: &str) -> Value {
    p["actor_token"] = json!(tok);
    p
}

fn agent_event(event_type: &str, actor: &str, world: &str, corr: Option<&str>) -> Value {
    let mut e = space_core::Event::new(event_type, actor, json!({}));
    e.world_id = world.to_string();
    e.correlation_id = corr.map(|s| s.to_string());
    json!(e)
}

struct SubConn {
    lines: tokio::io::Lines<
        BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    >,
}

async fn subscribe(pipe: &str, params: Value) -> (SubConn, Value) {
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
    let ack: Value = serde_json::from_str(&ack).unwrap();
    (SubConn { lines }, ack)
}

async fn recv_event(conn: &mut SubConn, ms: u64) -> Option<Value> {
    match tokio::time::timeout(std::time::Duration::from_millis(ms), conn.lines.next_line()).await {
        Ok(Ok(Some(l))) => serde_json::from_str(&l).ok(),
        _ => None,
    }
}

// ---- LoopGuard unit semantics ----
#[test]
fn loop_guard_ignores_self() {
    let mut g = LoopGuard::new("agent-a");
    assert!(!g.should_react("agent-a", Some("c1")));
    assert!(!g.should_react("agent-a", None));
    assert!(g.should_react("agent-b", Some("c1")));
}

#[test]
fn loop_guard_once_per_correlation() {
    let mut g = LoopGuard::new("agent-a");
    assert!(g.should_react("agent-b", Some("chain-1")));
    assert!(!g.should_react("agent-b", Some("chain-1")));
    assert!(g.should_react("agent-b", Some("chain-2")));
}

#[test]
fn loop_guard_bounded_memory() {
    let mut g = LoopGuard::new("agent-a");
    for i in 0..5000 {
        g.should_react("agent-b", Some(&format!("c-{i}")));
    }
    assert!(g.seen_count() <= 1024);
}

// ---- 16 (spec 13). self-event causes no reaction ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_event_no_reaction() {
    let pipe = r"\\.\pipe\spaceos-v14-r16";
    start_core(pipe).await;
    let tok = register(pipe, "self-a", &["event.record"], &["W"]).await;
    let (mut c, ack) = subscribe(
        pipe,
        with_token(json!({"subscriber":"self-a","world_id":"W"}), &tok),
    )
    .await;
    assert_eq!(ack["ok"], true);
    // Agent records its own action result.
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("ActionCompleted", "self-a", "W", Some("act-1")),
            &tok,
        ),
    )
    .await;
    let got = recv_event(&mut c, 2000)
        .await
        .expect("own event is visible");
    assert_eq!(got["event"]["actor"], "self-a");
    // Guard: no reaction to own event. Harness records nothing further.
    let mut guard = LoopGuard::new("self-a");
    let react = guard.should_react(
        got["event"]["actor"].as_str().unwrap(),
        got["event"]["correlation_id"].as_str(),
    );
    assert!(!react, "must not react to own event");
    let evs = call(pipe, "query_events", json!({})).await;
    assert_eq!(evs["result"].as_array().unwrap().len(), 1, "no storm");
}

// ---- 17 (spec 14). external event triggers exactly one reaction ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_event_single_reaction() {
    let pipe = r"\\.\pipe\spaceos-v14-r17";
    start_core(pipe).await;
    let tok_a = register(pipe, "rx-a", &["event.record"], &["W"]).await;
    let tok_b = register(pipe, "tx-b", &["event.record"], &["W"]).await;
    let (mut ca, _) = subscribe(
        pipe,
        with_token(json!({"subscriber":"rx-a","world_id":"W"}), &tok_a),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("ObjectCreated", "tx-b", "W", Some("ext-1")),
            &tok_b,
        ),
    )
    .await;
    let got = recv_event(&mut ca, 2000).await.expect("external event");
    let mut guard = LoopGuard::new("rx-a");
    assert!(guard.should_react(
        got["event"]["actor"].as_str().unwrap(),
        got["event"]["correlation_id"].as_str()
    ));
    // The single bounded reaction: one follow-up event, new correlation.
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("ActionCompleted", "rx-a", "W", Some("ext-1-rx")),
            &tok_a,
        ),
    )
    .await;
    // Redelivery of the same external correlation must not react again.
    assert!(!guard.should_react("tx-b", Some("ext-1")));
    let evs = call(pipe, "query_events", json!({})).await;
    assert_eq!(evs["result"].as_array().unwrap().len(), 2);
}

// ---- 18 (spec 15). correlation chain remains bounded ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn correlation_chain_bounded() {
    let pipe = r"\\.\pipe\spaceos-v14-r18";
    start_core(pipe).await;
    let tok_a = register(pipe, "ch-a", &["event.record"], &["W"]).await;
    let tok_b = register(pipe, "ch-b", &["event.record"], &["W"]).await;
    let (mut ca, _) = subscribe(
        pipe,
        with_token(json!({"subscriber":"ch-a","world_id":"W"}), &tok_a),
    )
    .await;
    let mut guard = LoopGuard::new("ch-a");
    // E1 from B -> A reacts once with E2 (new correlation, causation of E1).
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("ObjectCreated", "ch-b", "W", Some("k1")),
            &tok_b,
        ),
    )
    .await;
    let e1 = recv_event(&mut ca, 2000).await.expect("E1");
    assert!(guard.should_react("ch-b", Some("k1")));
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("ActionCompleted", "ch-a", "W", Some("k2")),
            &tok_a,
        ),
    )
    .await;
    // E2 is A's own event: guard stops here, no E3.
    let e2 = recv_event(&mut ca, 2000).await.expect("E2");
    assert_eq!(e2["event"]["actor"], "ch-a");
    assert!(!guard.should_react("ch-a", Some("k2")));
    assert_eq!(e1["event"]["correlation_id"], "k1");
    let evs = call(pipe, "query_events", json!({})).await;
    assert_eq!(
        evs["result"].as_array().unwrap().len(),
        2,
        "chain terminates"
    );
}

// ---- integrity: actor attribution end-to-end ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_attribution_end_to_end() {
    let pipe = r"\\.\pipe\spaceos-v14-rattr";
    start_core(pipe).await;
    let tok_b = register(pipe, "attr-b", &["event.record"], &["W"]).await;
    let tok_a = register(pipe, "attr-a", &["event.record"], &["W"]).await;
    let (mut ca, _) = subscribe(
        pipe,
        with_token(json!({"subscriber":"attr-a","world_id":"W"}), &tok_a),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(agent_event("ObjectCreated", "attr-b", "W", None), &tok_b),
    )
    .await;
    let got = recv_event(&mut ca, 2000).await.expect("event");
    assert_eq!(got["event"]["actor"], "attr-b");
    // A cannot emit as B even with a valid token of its own.
    let mut spoof = agent_event("ObjectCreated", "attr-b", "W", None);
    spoof = with_token(spoof, &tok_a);
    let r = call(pipe, "record_event", spoof).await;
    assert_eq!(r["ok"], false, "actor spoof must be denied");
}

// ---- integrity: world_id attribution ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn world_id_attribution() {
    let pipe = r"\\.\pipe\spaceos-v14-rworld";
    start_core(pipe).await;
    let tok = register(pipe, "wattr-a", &["event.record"], &["WZ"]).await;
    let (mut c, _) = subscribe(
        pipe,
        with_token(json!({"subscriber":"wattr-a","world_id":"WZ"}), &tok),
    )
    .await;
    let mut ev = space_core::Event::new("FileChanged", "wattr-a", json!({"path":"x"}));
    ev.world_id = "WZ".into();
    ev.object_ids = vec!["OBJ-1".into()];
    call(pipe, "record_event", with_token(json!(ev), &tok)).await;
    let got = recv_event(&mut c, 2000).await.expect("event");
    assert_eq!(got["event"]["world_id"], "WZ");
    // and the stored row carries the same world
    let stored = call(pipe, "query_events", json!({"world_id":"WZ"})).await;
    assert_eq!(stored["result"].as_array().unwrap().len(), 1);
}

// ---- integrity: correlation_id propagation ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn correlation_propagation() {
    let pipe = r"\\.\pipe\spaceos-v14-rcorr";
    start_core(pipe).await;
    let tok = register(pipe, "corr-a", &["event.record"], &["W"]).await;
    let (mut c, _) = subscribe(
        pipe,
        with_token(json!({"subscriber":"corr-a","world_id":"W"}), &tok),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("ActionCompleted", "corr-a", "W", Some("corr-xyz")),
            &tok,
        ),
    )
    .await;
    let got = recv_event(&mut c, 2000).await.expect("event");
    assert_eq!(got["event"]["correlation_id"], "corr-xyz");
}
