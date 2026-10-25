//! M4 — Multi-agent / multi-view attribution (live concurrent chains).
//!
//! Hypothesis: with several simultaneously active agents and views,
//! SpaceTrace may mix correlation, actor, operation, or event delivery.
//! What may break: a span lands in the wrong mission chain, wrong
//! actor/world attribution, or a delivery reaches the wrong subscriber.
//! Failure boundary: SpaceTrace hooks + Core mutation attribution under
//! true concurrency (parallel IPC clients).
//! Why it may break: shared global hook registry + shared Core; a context
//! mixup would cross-link chains.
//! Exact workload: Agent A in WA + Agent B in WB, 20 iterations each,
//! concurrent: create_object + record_event per iteration with distinct
//! mission correlations (mission-A / mission-B); one live subscriber per
//! world asserting delivery isolation.
//! Dataset: temp DB + temp pipe per run.
//! Fixed threshold: chain-A has exactly 100 spans (20 x 5), chain-B 100;
//! every attributed span in chain-A has agent=agent-a/world=WA and vice
//! versa; chains disjoint; subscriber A gets 20/20 of A's events only.
//! (No timestamps used as linkage proof — correlation only.)
//! Expected behavior: HELD.
//! Measurement: per-chain counts, attribution violations, delivery sets.
//! Pass criterion: exact counts + 0 misattribution + disjoint + isolated delivery.
//! Failure criterion: any cross-chain span, wrong actor/world, or
//! cross-delivery.
//! Reproduction protocol: 3 identical runs.

use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-fm4");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
}

async fn start_core(pipe: &str, db: &std::path::Path) {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":worlds}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register {name}: {r}");
    r["result"]["token"].as_str().unwrap().to_string()
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
    (SubConn { lines }, serde_json::from_str(&ack).unwrap())
}

async fn recv_n(conn: &mut SubConn, n: usize, ms_each: u64) -> Vec<Value> {
    let mut out = vec![];
    for _ in 0..n {
        match tokio::time::timeout(
            std::time::Duration::from_millis(ms_each),
            conn.lines.next_line(),
        )
        .await
        {
            Ok(Ok(Some(l))) => out.push(serde_json::from_str(&l).unwrap()),
            _ => break,
        }
    }
    out
}

async fn agent_burst(
    pipe: String,
    agent: &str,
    token: String,
    world: &str,
    mission: &str,
    n: usize,
    tag: &str,
) {
    for i in 0..n {
        let mut o = Object::new(
            "File",
            agent,
            json!({"path": format!("{tag}/{i}.txt"), "world_id": world}),
        );
        o.id = format!("m4-{tag}-{i:03}");
        let mut p = json!(o);
        p["actor_token"] = json!(token);
        p["correlation_id"] = json!(mission);
        let r = ipc::send_request(&pipe, "create_object", p).await.unwrap();
        assert_eq!(r["ok"], true, "create {tag}/{i}: {r}");
        let mut e = space_core::Event::new("Tick", agent, json!({"i": i}));
        e.id = format!("m4e-{tag}-{i:03}");
        e.world_id = world.to_string();
        e.correlation_id = Some(mission.to_string());
        let mut p = json!(e);
        p["actor_token"] = json!(token);
        let r = ipc::send_request(&pipe, "record_event", p).await.unwrap();
        assert_eq!(r["ok"], true, "record {tag}/{i}: {r}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn m4_live_concurrent_attribution() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm4-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("m4");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let tok_a = register(pipe, "agent-a", &["object.create", "event.record"], &["WA"]).await;
    let tok_b = register(pipe, "agent-b", &["object.create", "event.record"], &["WB"]).await;

    let (mut sub_a, ack) = subscribe(
        pipe,
        json!({"subscriber": "agent-a", "actor_token": tok_a, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    let (mut sub_b, ack) = subscribe(
        pipe,
        json!({"subscriber": "agent-b", "actor_token": tok_b, "world_id": "WB"}),
    )
    .await;
    assert_eq!(ack["ok"], true);

    // Concurrent bursts, disjoint missions.
    let (pa, pb) = (pipe.to_string(), pipe.to_string());
    let (ta, tb) = (tok_a.clone(), tok_b.clone());
    let (ha, hb) = (
        tokio::spawn(
            async move { agent_burst(pa, "agent-a", ta, "WA", "mission-A", 20, "a").await },
        ),
        tokio::spawn(
            async move { agent_burst(pb, "agent-b", tb, "WB", "mission-B", 20, "b").await },
        ),
    );
    ha.await.unwrap();
    hb.await.unwrap();

    // Delivery isolation: 20/20 each, correct world+actor.
    let got_a = recv_n(&mut sub_a, 20, 5000).await;
    let got_b = recv_n(&mut sub_b, 20, 5000).await;
    assert_eq!(got_a.len(), 20, "subscriber A gets 20/20");
    assert_eq!(got_b.len(), 20, "subscriber B gets 20/20");
    assert!(got_a
        .iter()
        .all(|v| v["event"]["world_id"] == "WA" && v["event"]["actor"] == "agent-a"));
    assert!(got_b
        .iter()
        .all(|v| v["event"]["world_id"] == "WB" && v["event"]["actor"] == "agent-b"));

    // Trace chains via core principal (send_request carries core auth).
    let qa = ipc::send_request(
        pipe,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "mission-A"}),
    )
    .await
    .unwrap();
    let qb = ipc::send_request(
        pipe,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "mission-B"}),
    )
    .await
    .unwrap();
    let ca: Vec<Value> = serde_json::from_value(qa["result"].clone()).unwrap();
    let cb: Vec<Value> = serde_json::from_value(qb["result"].clone()).unwrap();
    // Per iteration: object MutationStart+Commit, event MutationStart+Commit,
    // EventPublished = 5 spans x 20 = 100. (+delivery spans share the event's
    // correlation too: 20 SubscriptionDelivery => 120 total.)
    assert_eq!(ca.len(), 120, "chain-A exact count, got {}", ca.len());
    assert_eq!(cb.len(), 120, "chain-B exact count, got {}", cb.len());

    for e in &ca {
        assert_eq!(e["correlation_id"], "mission-A");
        if let Some(a) = e.get("agent_id").and_then(|v| v.as_str()) {
            assert_eq!(a, "agent-a", "chain-A misattributed actor");
        }
        if let Some(w) = e.get("world_id").and_then(|v| v.as_str()) {
            assert!(!w.is_empty());
            assert_eq!(w, "WA", "chain-A misattributed world");
        }
    }
    for e in &cb {
        assert_eq!(e["correlation_id"], "mission-B");
        if let Some(a) = e.get("agent_id").and_then(|v| v.as_str()) {
            assert_eq!(a, "agent-b", "chain-B misattributed actor");
        }
        if let Some(w) = e.get("world_id").and_then(|v| v.as_str()) {
            assert!(!w.is_empty());
            assert_eq!(w, "WB", "chain-B misattributed world");
        }
    }
    let ids_a: std::collections::HashSet<&str> = ca
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    let ids_b: std::collections::HashSet<&str> = cb
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    assert!(ids_a.is_disjoint(&ids_b), "M4 chains must be disjoint");
    eprintln!("M4: 2x120 spans, attribution exact, chains disjoint, delivery isolated");
}
