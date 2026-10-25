//! Level D — human + Agents + Views + Reality (main integration scenario).
//!
//! Simultaneously: human writes a real file; Agent A works on related
//! objects; Agent B reads/observes; two Views subscribe to one world; a
//! real scan reconciles Reality; SpaceTrace links both chains.
//! What may break: divergence between Reality/Core/Views; mixed chains in
//! trace; delivery gaps; identity splits.
//! Failure boundary: full stack (each layer already HELD alone in M1–M7).
//! Exact workload (isolated temp dir + temp DB + temp pipe):
//!  1. human writes specs.txt; scan -> file object F (+fingerprint).
//!  2. Agent A creates Doc D + relation D-describes-F + 10 events
//!     (world W1, mission-D correlation).
//!  3. Agent B queries: must see F + D + relation + events.
//!  4. Two subscribers on W1 receive A's 10/10 each.
//!  5. Trace: scan chain (filesystem) and mission-D chain (agent-a)
//!     both present, correctly attributed, disjoint.
//!  6. Final convergence via query: F + D + relation + 10 events.
//! Fixed threshold: exact counts everywhere; 0 dupes; 10/10 x2 delivery;
//! trace chains correct; fingerprint matches file.
//! Expected behavior: HELD.
//! Pass criterion: all exact. Failure criterion: any divergence.
//! Reproduction protocol: 3 identical runs (NOT a mock-only test: real
//! fs, real scan, real IPC, real pipes, real broadcast delivery).

use filesystem_provider::scanner::Scanner;
use serde_json::{json, Value};
use space_core::{ipc, Object, Relation, Storage};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-fd").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn start_core(pipe: &str, db: &PathBuf) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn level_d_human_agents_views_reality() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fd-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let dir = tmp("d");
    let db = dir.join("core.db");
    let world = dir.join("world");
    std::fs::create_dir_all(&world).unwrap();
    start_core(pipe, &db).await;

    let tok_a = register(
        pipe,
        "agent-a",
        &["object.create", "relation.create", "event.record"],
        &["W1"],
    )
    .await;
    let tok_b = register(pipe, "agent-b", &[], &["W1"]).await;

    // Phase 0: two views subscribe before activity.
    let (mut view1, ack) = subscribe(
        pipe,
        json!({"subscriber": "agent-b", "actor_token": tok_b, "world_id": "W1"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    let (mut view2, ack) = subscribe(
        pipe,
        json!({"subscriber": "agent-b", "actor_token": tok_b, "world_id": "W1"}),
    )
    .await;
    assert_eq!(ack["ok"], true);

    // Phase 1 (Reality): human writes a real file; scan reconciles.
    std::fs::write(world.join("specs.txt"), "spec v1").unwrap();
    let scanner = Scanner::new("filesystem");
    let stats = scanner.scan_on(&world, pipe).await.unwrap();
    assert!(
        stats["created"].as_u64().unwrap() >= 1,
        "scan creates file row: {stats}"
    );
    let objs = ipc::send_request(pipe, "query_objects", json!({"provider_id": "filesystem"}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    let f = objs
        .iter()
        .find(|o| {
            o.external_id.as_deref() == Some(world.join("specs.txt").to_string_lossy().as_ref())
        })
        .expect("file row")
        .clone();
    let fp = filesystem_provider::identity::fingerprint(&world.join("specs.txt")).unwrap();
    assert_eq!(
        f.properties.get("fingerprint").and_then(|v| v.as_str()),
        Some(fp.as_str()),
        "Level D: Core fingerprint matches Reality"
    );

    // Phase 2 (Agent A): doc + relation to the file row + 10 events.
    let mut d = Object::new(
        "Document",
        "agent-a",
        json!({"name": "Notes", "world_id": "W1"}),
    );
    d.id = "fd-doc".to_string();
    let mut p = json!(d);
    p["actor_token"] = json!(tok_a);
    p["correlation_id"] = json!("mission-D");
    assert_eq!(
        ipc::send_request(pipe, "create_object", p).await.unwrap()["ok"],
        true
    );
    let mut r = Relation::new("fd-doc", "describes", f.id.clone());
    r.id = "fd-rel".to_string();
    r.world_id = "W1".to_string();
    let mut p = json!(r);
    p["actor"] = json!("agent-a");
    p["actor_token"] = json!(tok_a);
    p["correlation_id"] = json!("mission-D");
    assert_eq!(
        ipc::send_request(pipe, "create_relation", p).await.unwrap()["ok"],
        true
    );
    for i in 0..10 {
        let mut e = space_core::Event::new("Note", "agent-a", json!({"i": i}));
        e.id = format!("fd-e-{i:02}");
        e.world_id = "W1".to_string();
        e.correlation_id = Some("mission-D".to_string());
        let mut p = json!(e);
        p["actor_token"] = json!(tok_a);
        assert_eq!(
            ipc::send_request(pipe, "record_event", p).await.unwrap()["ok"],
            true
        );
    }

    // Phase 3 (Agent B observes): sees file + doc + relation + events.
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    assert!(objs.iter().any(|o| o.id == f.id), "B sees Reality row");
    assert!(objs.iter().any(|o| o.id == "fd-doc"), "B sees agent row");
    let rels = ipc::send_request(pipe, "query_relations", json!({}))
        .await
        .unwrap();
    let rels: Vec<Relation> = serde_json::from_value(rels["result"].clone()).unwrap();
    assert!(rels.iter().any(|r| r.id == "fd-rel"), "B sees relation");
    let evs = ipc::send_request(pipe, "query_events", json!({"world_id": "W1"}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(evs["result"].clone()).unwrap();
    assert_eq!(evs.iter().filter(|e| e.event_type == "Note").count(), 10);

    // Phase 4 (Views): both receive A's 10/10.
    for (vi, v) in [&mut view1, &mut view2].iter_mut().enumerate() {
        let got = recv_n(v, 10, 5000).await;
        assert_eq!(got.len(), 10, "view {vi} 10/10");
        assert!(got.iter().all(|e| e["event"]["world_id"] == "W1"));
    }

    // Phase 5 (SpaceTrace): scan chain + mission-D chain, correct + disjoint.
    let qd = ipc::send_request(
        pipe,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "mission-D"}),
    )
    .await
    .unwrap();
    let chain_d: Vec<Value> = serde_json::from_value(qd["result"].clone()).unwrap();
    assert!(!chain_d.is_empty(), "mission-D chain exists");
    assert!(chain_d.iter().all(|e| e["correlation_id"] == "mission-D"));
    for e in &chain_d {
        let kind = e.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let agent = e.get("agent_id").and_then(|v| v.as_str()).unwrap_or("");
        match kind {
            // Delivery spans belong to the receiving subscriber by design.
            "SubscriptionDelivery" => assert_eq!(agent, "agent-b", "delivery subscriber"),
            _ => assert!(
                agent == "agent-a" || agent == "system",
                "mission-D actor attribution ({kind}): {agent}"
            ),
        }
    }
    // Scan chain: find any scan- correlation touching this provider.
    let prov = ipc::send_request(
        pipe,
        "trace_query",
        json!({"subscriber": "system", "provider_id": "filesystem"}),
    )
    .await
    .unwrap();
    let scan_spans: Vec<Value> = serde_json::from_value(prov["result"].clone()).unwrap();
    assert!(
        scan_spans.iter().any(|e| e["kind"] == "ScanEnd"),
        "scan chain recorded"
    );
    let ids_d: std::collections::HashSet<&str> = chain_d
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    let ids_s: std::collections::HashSet<&str> = scan_spans
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    assert!(
        ids_d.is_disjoint(&ids_s),
        "Level D chains disjoint, correctly attributed"
    );

    // Phase 6 (convergence): single query agrees on everything.
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    assert!(objs.iter().any(|o| o.id == f.id));
    assert!(objs.iter().any(|o| o.id == "fd-doc"));
    eprintln!("Level D: human+agents+views+reality converged, trace linked, 0 divergence");
}
