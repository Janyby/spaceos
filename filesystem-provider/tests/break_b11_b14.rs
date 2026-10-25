//! B11 (locked present file) + B14 (dual watcher reconnect).
//! Fixed conditions, see docs/BREAKAGE_CAMPAIGN.md.
#![cfg(windows)]
use filesystem_provider::{scanner::Scanner, watcher};
use serde_json::{json, Value};
use space_core::ipc;
use std::os::windows::fs::OpenOptionsExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb11");
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

fn world(name: &str) -> std::path::PathBuf {
    let w = tmp(name);
    let _ = std::fs::remove_dir_all(&w);
    std::fs::create_dir_all(&w).unwrap();
    w
}

fn pipe_for(tag: &str) -> String {
    Box::leak(format!(r"\\.\pipe\spaceos-bb11-{tag}-{}", std::process::id()).into_boxed_str())
        .to_string()
}

async fn fs_rows(pipe: &str) -> Vec<Value> {
    call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await["result"]
        .as_array()
        .unwrap()
        .clone()
}

async fn file_deleted(pipe: &str) -> usize {
    call_retry(pipe, "query_events", json!({"event_type": "FileDeleted"})).await["result"]
        .as_array()
        .unwrap()
        .len()
}

// ---------- B11: present-but-locked file is never confirmed-deleted ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn locked_file_never_confirmed_deleted() {
    let pipe = pipe_for("locked");
    let db = tmp("locked.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("lockedw");
    std::fs::write(w.join("a.txt"), b"stable").unwrap();
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    sc.scan_on(&w, &pipe).await.unwrap();
    let id0 = fs_rows(&pipe)
        .await
        .iter()
        .find(|o| o["external_id"].as_str().unwrap_or("").ends_with("a.txt"))
        .unwrap()["id"]
        .clone();
    // Exclusive lock: content unreadable, but the path still walk-lists.
    let _hold = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(w.join("a.txt"))
        .unwrap();
    for _ in 0..5 {
        sc.scan_on(&w, &pipe).await.unwrap();
    }
    let rows = fs_rows(&pipe).await;
    assert!(
        rows.iter().any(|o| o["id"] == id0),
        "locked row survives all scans"
    );
    assert_eq!(
        file_deleted(&pipe).await,
        0,
        "zero FileDeleted for a present file"
    );
    drop(_hold);
    // Readable again: meta refreshes, same identity, fingerprint intact.
    sc.scan_on(&w, &pipe).await.unwrap();
    let rows2 = fs_rows(&pipe).await;
    let obj = rows2.iter().find(|o| o["id"] == id0).unwrap();
    assert!(
        obj["properties"].get("fingerprint").is_some(),
        "fingerprint evidence preserved"
    );
    assert_eq!(file_deleted(&pipe).await, 0);
}

// ---------- B14: dual watchers, kill, reconnect ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dual_watcher_kill_reconnect() {
    let pipe = pipe_for("dual");
    let db = tmp("dual.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("dualw");
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    let spawn_watcher = |stop: tokio::sync::oneshot::Receiver<()>| {
        let (w2, s2, p2) = (w.clone(), sc.clone(), pipe.clone());
        tokio::spawn(async move {
            let _ = watcher::watch_on(&w2, s2, &p2, stop).await;
        })
    };
    let (_tx1, rx1) = tokio::sync::oneshot::channel::<()>();
    let (_tx2, rx2) = tokio::sync::oneshot::channel::<()>();
    // keep senders alive in this scope; dropping one stops its watcher
    let mut tx1 = Some(_tx1);
    let mut tx2 = Some(_tx2);
    spawn_watcher(rx1);
    spawn_watcher(rx2);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    for i in 0..30 {
        std::fs::write(w.join(format!("f{i:02}.txt")), "x").unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    // Kill watcher 1 mid-burst aftermath, keep writing, restart it.
    drop(tx1.take());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    for i in 30..50 {
        std::fs::write(w.join(format!("f{i:02}.txt")), "x").unwrap();
    }
    let (_tx3, rx3) = tokio::sync::oneshot::channel::<()>();
    spawn_watcher(rx3);
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    drop(tx2.take());
    // Exactly one row per path: no dup rows from dual observation.
    let rows = fs_rows(&pipe).await;
    let files: Vec<_> = rows
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|p| p.ends_with(".txt"))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(files.len(), 50, "exactly one row per path, none lost");
    let mut seen = std::collections::HashSet::new();
    for o in &files {
        assert!(
            seen.insert(o["external_id"].as_str().unwrap().to_string()),
            "no dup rows"
        );
    }
    // Events are at-least-once per observer: each path observed ≥1 time.
    // (Exact per-watcher attribution is impossible: identical payloads.
    // Convergence + bounded duplication is the checkable contract.)
    let evs = call_retry(&pipe, "query_events", json!({"event_type": "FileCreated"})).await;
    let n = evs["result"].as_array().unwrap().len();
    assert!(
        n >= 50 && n <= 150,
        "each file observed 1-3x across watchers/restarts, got {n}"
    );
    // Final rescan converges with no removals of live files.
    let st = sc.scan_on(&w, &pipe).await.unwrap();
    assert_eq!(st["removed"], json!(0));
    assert_eq!(
        fs_rows(&pipe)
            .await
            .iter()
            .filter(|o| {
                o["external_id"]
                    .as_str()
                    .map(|p| p.ends_with(".txt"))
                    .unwrap_or(false)
            })
            .count(),
        50
    );
}
