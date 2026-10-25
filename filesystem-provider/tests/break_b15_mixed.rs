//! B15 — Recovery after mixed failures (max two factors each).
//! (a) fs changes + core abort mid-reconciliation + restart + confirm.
//! (b) covered by B8 resubscribe test; here: subscriber drop DURING
//! provider backlog, then resync purely via query.
//! Fixed conditions, see docs/BREAKAGE_CAMPAIGN.md.
#![cfg(windows)]
use filesystem_provider::scanner::Scanner;
use serde_json::{json, Value};
use space_core::ipc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb15");
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn call_retry(pipe: &str, method: &str, params: Value) -> Value {
    for _ in 0..120 {
        match ipc::send_request(pipe, method, params.clone()).await {
            Ok(v) => return v,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    call(pipe, method, params).await
}

async fn start_core(pipe: &str, db: &std::path::Path) -> tokio::task::JoinHandle<()> {
    let storage = space_core::Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    let h = tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h
}

fn world(name: &str) -> std::path::PathBuf {
    let w = tmp(name);
    let _ = std::fs::remove_dir_all(&w);
    std::fs::create_dir_all(&w).unwrap();
    w
}

fn pipe_for(tag: &str) -> String {
    Box::leak(format!(r"\\.\pipe\spaceos-bb15-{tag}-{}", std::process::id()).into_boxed_str())
        .to_string()
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
    let mut params = params;
    params["auth"] = json!(ipc::core_token());
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

// ---------- (a) fs backlog + core abort mid-reconciliation + restart ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_provider_core_restart_converges() {
    let pipe = pipe_for("mix");
    let db = tmp("mix.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(&pipe, &db).await;
    let w = world("mixw");
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    std::fs::write(w.join("keep.txt"), b"k").unwrap();
    std::fs::write(w.join("goner.txt"), b"g").unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    // Backlog accumulates, one confirming scan runs, then CORE dies
    // mid-reconciliation (before the remaining confirmations).
    std::fs::remove_file(w.join("goner.txt")).unwrap();
    std::fs::write(w.join("new.txt"), b"n").unwrap();
    sc.scan_on(&w, &pipe).await.unwrap(); // miss 1 for goner, create new
    h.abort();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // Restart with a FRESH provider ledger (worst case: counts reset).
    let h2 = start_core(&pipe, &db).await;
    let sc2 = std::sync::Arc::new(Scanner::new("filesystem"));
    sc2.scan_on(&w, &pipe).await.unwrap();
    sc2.scan_on(&w, &pipe).await.unwrap();
    sc2.scan_on(&w, &pipe).await.unwrap();
    // Converged world == disk reality: root + keep + new.
    let objs = call_retry(&pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let mut paths: Vec<_> = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["external_id"].as_str().map(|s| s.to_string()))
        .collect();
    paths.sort();
    let mut expected = vec![
        w.to_string_lossy().to_string(),
        w.join("keep.txt").to_string_lossy().to_string(),
        w.join("new.txt").to_string_lossy().to_string(),
    ];
    expected.sort();
    assert_eq!(paths, expected, "mixed failure converges exactly");
    // No duplicate identities.
    let ids: Vec<_> = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].clone())
        .collect();
    let uniq: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), uniq.len());
    h2.abort();
}

// ---------- (b) subscriber drop during provider backlog, resync via query ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriber_drop_during_backlog_resyncs() {
    let pipe = pipe_for("sub");
    let db = tmp("sub.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(&pipe, &db).await;
    let w = world("subw");
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    let (mut sub, ack) = subscribe(&pipe, json!({})).await;
    assert_eq!(ack["ok"], true);
    // Baseline event seen live.
    std::fs::write(w.join("a.txt"), b"a").unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    assert!(recv_event(&mut sub, 3000).await.is_some());
    // Subscriber "crashes" (drop) right as a backlog lands.
    drop(sub);
    for i in 0..10 {
        std::fs::write(w.join(format!("b{i:02}.txt")), b"x").unwrap();
    }
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    // Resubscribe: only NEW events flow live (no replay invented)...
    let (mut sub2, ack2) = subscribe(&pipe, json!({})).await;
    assert_eq!(ack2["ok"], true);
    std::fs::write(w.join("c.txt"), b"c").unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    assert!(
        recv_event(&mut sub2, 3000).await.is_some(),
        "live flow resumes"
    );
    // ...and the missed window is fully recoverable via query.
    let evs = call_retry(&pipe, "query_events", json!({"event_type": "FileCreated"})).await;
    assert!(
        evs["result"].as_array().unwrap().len() >= 12,
        "backlog queryable, nothing silently lost"
    );
    h.abort();
}
