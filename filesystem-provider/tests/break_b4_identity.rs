//! B4 — Concurrent rename identity. Fixed conditions, see
//! docs/BREAKAGE_CAMPAIGN.md. One live row, same id, relation intact,
//! zero duplicate external_ids, checker-clean at the end. The zombie
//! window is proven by construction, then converged.
#![cfg(windows)]
use filesystem_provider::{scanner::Scanner, watcher};
use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb4");
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
    let storage = Storage::open(db).unwrap();
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
    Box::leak(format!(r"\\.\pipe\spaceos-bb4-{tag}-{}", std::process::id()).into_boxed_str())
        .to_string()
}

async fn fs_map(pipe: &str) -> Vec<(String, String)> {
    call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| {
            Some((
                o["external_id"].as_str()?.to_string(),
                o["id"].as_str()?.to_string(),
            ))
        })
        .collect()
}

async fn all_ids(pipe: &str) -> std::collections::HashSet<String> {
    call_retry(pipe, "query_objects", json!({})).await["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["id"].as_str().map(|s| s.to_string()))
        .collect()
}

fn checker_clean(
    all_ids: &std::collections::HashSet<String>,
    rows: &[(String, String)],
    rels: &Value,
) {
    for r in rels["result"].as_array().unwrap() {
        assert!(
            all_ids.contains(r["source_object_id"].as_str().unwrap()),
            "dangling {}",
            r["id"]
        );
        assert!(
            all_ids.contains(r["target_object_id"].as_str().unwrap()),
            "dangling {}",
            r["id"]
        );
    }
    let mut seen = std::collections::HashSet::new();
    for (p, _) in rows {
        assert!(seen.insert(p.clone()), "duplicate external {p}");
    }
}

// ---------- rename wobble + modify churn + scans + live watcher ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_rename_modify_rescan() {
    let pipe = pipe_for("wobble");
    let db = tmp("wobble.db");
    let _ = std::fs::remove_file(&db);
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("wobblew");
    std::fs::write(w.join("a.txt"), b"stable-content").unwrap();
    // ONE shared instance: watcher + all scans serialize on its guard.
    // (A second Scanner here would bypass the exclusion this test proves.)
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    sc.scan_on(&w, &pipe).await.unwrap();
    let id_x = fs_map(&pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    // Anchor relation on a REAL project object.
    let proj = space_core::Object::new("Project", "filesystem", json!({"name": "P"}));
    assert_eq!(
        call_retry(&pipe, "create_object", json!(proj)).await["ok"],
        true
    );
    let rel = json!({"id": uuid::Uuid::new_v4().to_string(), "source_object_id": id_x,
        "relation_type": "belongs_to", "target_object_id": proj.id,
        "properties": {}, "created_at": chrono::Utc::now().to_rfc3339(),
        "world_id": "", "actor": "filesystem"});
    assert_eq!(call_retry(&pipe, "create_relation", rel).await["ok"], true);
    // Live watcher on the SAME instance (shared guard is the point).
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw, p2) = (w.clone(), sc.clone(), pipe.clone());
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, &p2, stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    // 20 renames A<->B, same content, scan after each (strengthened).
    let mut at_a = true;
    for _ in 0..20 {
        if at_a {
            std::fs::rename(w.join("a.txt"), w.join("b.txt")).unwrap();
        } else {
            std::fs::rename(w.join("b.txt"), w.join("a.txt")).unwrap();
        }
        at_a = !at_a;
        sc.scan_on(&w, &pipe).await.unwrap();
    }
    // 40 rapid appends with concurrent scans + live watcher (strengthened).
    // Concurrent scans MUST share the instance (same guard); a fresh
    // Scanner here would bypass the exclusion under test.
    let (w3, sc3, p3) = (w.clone(), sc.clone(), pipe.clone());
    let scan_task = tokio::spawn(async move {
        for _ in 0..40 {
            let _ = sc3.scan_on(&w3, &p3).await;
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        }
    });
    let cur = if at_a { "a.txt" } else { "b.txt" }.to_string();
    for i in 0..40 {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(w.join(&cur))
            .unwrap();
        write!(f, "+{i}").unwrap();
    }
    scan_task.await.unwrap();
    // Quiesce + confirm, then assert the final contract.
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    let rows = fs_map(&pipe).await;
    let live: Vec<_> = rows.iter().filter(|(p, _)| p.ends_with(".txt")).collect();
    assert_eq!(live.len(), 1, "exactly one live row, no split");
    let rels = call_retry(&pipe, "query_relations", json!({})).await;
    assert_eq!(
        rels["result"].as_array().unwrap().len(),
        1,
        "relation intact"
    );
    assert_eq!(rels["result"][0]["source_object_id"], json!(id_x));
    checker_clean(&all_ids(&pipe).await, &rows, &rels);
}

// ---------- zombie window: proven by construction, then converged ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zombie_window_proven_then_converged() {
    let pipe = pipe_for("zombie");
    let db = tmp("zombie.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("zombiew");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"v1").unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    // Take a snapshot, then delete the file (no watcher: pure scan world).
    let snap = call_retry(&pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let snap_obj = snap["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["external_id"].as_str().unwrap_or("").ends_with("a.txt"))
        .unwrap()
        .clone();
    let live_id = snap_obj["id"].as_str().unwrap().to_string();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    // A late, in-flight pre-delete observation arrives NOW (update with
    // the stale snapshot) BEFORE any scan sees the deletion...
    // but first confirm the deletion so the row is really gone:
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    assert!(
        !fs_map(&pipe).await.iter().any(|(_, id)| id == &live_id),
        "row confirmed gone"
    );
    // ...then the stale update resurrects it: ZOMBIE (update is REPLACE).
    let r = call_retry(&pipe, "update_object", snap_obj).await;
    assert_eq!(r["ok"], true);
    assert!(
        fs_map(&pipe).await.iter().any(|(_, id)| id == &live_id),
        "zombie proven: stale update resurrects the row"
    );
    // Recovery: subsequent scans recount misses from zero and re-confirm.
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    sc.scan_on(&w, &pipe).await.unwrap();
    assert!(
        !fs_map(&pipe).await.iter().any(|(_, id)| id == &live_id),
        "zombie reconverges to deleted"
    );
    // Exactly two FileDeleted episodes for the path (no storm from this).
    let evs = call_retry(&pipe, "query_events", json!({"event_type": "FileDeleted"})).await;
    assert_eq!(evs["result"].as_array().unwrap().len(), 2);
}
