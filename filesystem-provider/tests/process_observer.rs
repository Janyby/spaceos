//! Process observer as second observation-only provider.
//!
//! Wires `observation::watch_processes_on` (same daemon family as
//! `scan_on`/`watch_on`: explicit Core pipe, stoppable) against a live
//! Core on a test pipe and proves, with a real short-lived OS process:
//! registration of the second provider id, end-to-end delivery of
//! ProcessStarted/ProcessExited to the public `query_events` interface,
//! absence of any action on observed processes, and an intact filesystem
//! scenario beside it. No global pipes, no Core changes.
#![cfg(windows)]
use filesystem_provider::ipc as prov_ipc;
use filesystem_provider::observation;
use filesystem_provider::scanner::Scanner;
use serde_json::{json, Value};
use space_core::{ipc, Storage};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-procobs");
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
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    call(pipe, method, params).await
}

async fn start_core(pipe: &str, db: &std::path::Path) -> tokio::task::JoinHandle<()> {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    let h = tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h
}

async fn proc_events(pipe: &str, event_type: &str) -> Vec<Value> {
    let r = call_retry(
        pipe,
        "query_events",
        json!({"event_type": event_type, "provider_id": "process"}),
    )
    .await;
    assert_eq!(r["ok"], true, "query {event_type}: {r}");
    r["result"].as_array().cloned().unwrap_or_default()
}

fn has_named(events: &[Value], needle: &str) -> bool {
    events.iter().any(|e| {
        e["payload"]["name"]
            .as_str()
            .map(|n| n.to_lowercase().contains(needle))
            .unwrap_or(false)
    })
}

/// Poll until `f` returns true or the deadline passes (returns what it got).
async fn poll_deadline(desc: &str, ms: u64, f: impl AsyncFn() -> bool) {
    let t0 = std::time::Instant::now();
    loop {
        if f().await {
            return;
        }
        if t0.elapsed().as_millis() >= ms as u128 {
            panic!("deadline: {desc}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

// ---------- 1: second provider registers beside filesystem ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_provider_registers_beside_filesystem() {
    let pipe = r"\\.\pipe\spaceos-procobs-reg";
    let db = tmp("reg.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;

    // Same shapes the daemon registers in run_server.
    let fs = call_retry(
        pipe,
        "register_provider",
        json!({"id": "filesystem", "name": "filesystem-provider",
            "capabilities": ["scan", "watch", "format", "actions"]}),
    )
    .await;
    assert_eq!(fs["ok"], true, "filesystem registers: {fs}");
    let info = prov_ipc::process_provider_info();
    assert_eq!(info.id, "process");
    assert_eq!(info.capabilities, vec!["observe".to_string()]);
    let pr = call_retry(pipe, "register_provider", json!(info)).await;
    assert_eq!(pr["ok"], true, "process observer registers: {pr}");

    let list = call_retry(pipe, "list_providers", json!({})).await;
    let ids: Vec<String> = list["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|p| p["id"].as_str().map(|s| s.to_string()))
        .collect();
    assert!(ids.contains(&"filesystem".to_string()), "ids: {ids:?}");
    assert!(ids.contains(&"process".to_string()), "ids: {ids:?}");
    h.abort();
}

// ---------- 2: real process observed end-to-end + clean stop ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_observation_reaches_query_events() {
    let pipe = r"\\.\pipe\spaceos-procobs-e2e";
    let db = tmp("e2e.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let p = pipe.to_string();
    let obs = tokio::spawn(async move {
        let _ = observation::watch_processes_on("process".into(), Some("ping".into()), 200, &p, rx)
            .await;
    });
    // Let the observer take its baseline snapshot first.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut child = tokio::process::Command::new("ping")
        .args(["-n", "3", "127.0.0.1"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn ping");
    poll_deadline("ProcessStarted(ping) via query_events", 20000, async || {
        has_named(&proc_events(pipe, "ProcessStarted").await, "ping")
    })
    .await;

    let status = child.wait().await.expect("ping exits by itself");
    assert!(status.success(), "observer must not disturb the process");
    poll_deadline("ProcessExited(ping) via query_events", 20000, async || {
        has_named(&proc_events(pipe, "ProcessExited").await, "ping")
    })
    .await;

    // Graceful shutdown through the existing stop contract.
    let _ = tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(10), obs)
        .await
        .expect("observer stops on stop signal")
        .unwrap();
    h.abort();
}

// ---------- 3: observer emits no actions and mints no objects ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_observer_issues_no_actions() {
    let pipe = r"\\.\pipe\spaceos-procobs-noact";
    let db = tmp("noact.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let p = pipe.to_string();
    let obs = tokio::spawn(async move {
        let _ = observation::watch_processes_on("process".into(), Some("ping".into()), 200, &p, rx)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut child = tokio::process::Command::new("ping")
        .args(["-n", "2", "127.0.0.1"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn ping");
    let status = child.wait().await.expect("ping exits by itself");
    assert!(status.success(), "exit code untouched by observer");
    poll_deadline("any process event lands", 20000, async || {
        !proc_events(pipe, "ProcessStarted").await.is_empty()
    })
    .await;
    let _ = tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(10), obs)
        .await
        .expect("observer stops")
        .unwrap();

    // Every row attributed to the process source is a lifecycle event.
    let r = call_retry(pipe, "query_events", json!({"provider_id": "process"})).await;
    for e in r["result"].as_array().unwrap() {
        let t = e["event_type"].as_str().unwrap_or("");
        assert!(
            t == "ProcessStarted" || t == "ProcessExited",
            "process source emits only lifecycle events, got: {t}"
        );
    }
    // No objects or relations minted for processes.
    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "process"})).await;
    assert_eq!(objs["result"].as_array().unwrap().len(), 0);
    let rels = call_retry(pipe, "query_relations", json!({})).await;
    assert_eq!(rels["result"].as_array().unwrap().len(), 0);
    h.abort();
}

// ---------- 4: filesystem scenario intact beside the observer ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_scenario_intact_beside_observer() {
    let pipe = r"\\.\pipe\spaceos-procobs-coex";
    let db = tmp("coex.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let p = pipe.to_string();
    let obs = tokio::spawn(async move {
        let _ = observation::watch_processes_on("process".into(), Some("ping".into()), 200, &p, rx)
            .await;
    });

    // Filesystem path, unchanged mechanics, while the observer runs.
    let root = tmp("coexw");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("keep.txt"), "keep").unwrap();
    let sc = Scanner::new("filesystem");
    let stats = sc.scan_on(&root, pipe).await.unwrap();
    assert_eq!(stats["created"], json!(2), "root dir + file: {stats}");

    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let paths: Vec<String> = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["external_id"].as_str().map(|s| s.to_string()))
        .collect();
    assert!(
        paths.iter().any(|x| x.ends_with("keep.txt")),
        "paths: {paths:?}"
    );
    // Sources stay separated: no process rows among filesystem objects.
    let proc_objs = call_retry(pipe, "query_objects", json!({"provider_id": "process"})).await;
    assert_eq!(proc_objs["result"].as_array().unwrap().len(), 0);

    let _ = tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(10), obs)
        .await
        .expect("observer stops")
        .unwrap();
    h.abort();
}
