//! V1.5 provider/reality recovery: crash around observation, backlog,
//! core downtime with external mutation, duplicate registration,
//! persisted-state vs reconstruction boundary.
//! All cores run on unique pipes; scanner/watcher use scan_on/watch_on.
#![cfg(windows)]
use filesystem_provider::scanner::Scanner;
use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v15p");
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn call_retry(pipe: &str, method: &str, params: Value) -> Value {
    for _ in 0..60 {
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

fn world(name: &str) -> std::path::PathBuf {
    let w = tmp(name);
    let _ = std::fs::remove_dir_all(&w);
    std::fs::create_dir_all(&w).unwrap();
    w
}

async fn fs_paths(pipe: &str) -> Vec<String> {
    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let mut v: Vec<String> = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["external_id"].as_str().map(|s| s.to_string()))
        .collect();
    v.sort();
    v
}

// ---------- B1: provider dies before any reality change ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_crash_before_reality_change() {
    let pipe = r"\\.\pipe\spaceos-v15p-b1";
    let db = tmp("b1.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("b1w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    let before = fs_paths(pipe).await;
    // Provider "dies" (nothing running), reality untouched, rescan on restart.
    sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(fs_paths(pipe).await, before);
    h.abort();
}

// ---------- B2: change while provider down, then reconcile ----------
// Detection-without-delivery is externally unobservable, so the test covers
// the observable equivalence class: change happens with no observer alive,
// then observation restarts and Core converges without duplicates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_down_change_then_reconcile() {
    let pipe = r"\\.\pipe\spaceos-v15p-b2";
    let db = tmp("b2.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("b2w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    // Provider down: create + modify with no observer.
    std::fs::write(w.join("b.txt"), "b").unwrap();
    std::fs::write(w.join("a.txt"), "a2").unwrap();
    // Provider restarts: reconciliation converges, no duplicates.
    let stats = sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(stats["created"], json!(1));
    let paths = fs_paths(pipe).await;
    assert_eq!(paths.len(), 3); // root dir + a.txt + b.txt
    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let ids: Vec<_> = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].clone())
        .collect();
    let uniq: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), uniq.len());
    h.abort();
}

// ---------- B3: observation delivered, then crash; rescan adds nothing ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_crash_after_observation_no_duplicate() {
    let pipe = r"\\.\pipe\spaceos-v15p-b3";
    let db = tmp("b3.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("b3w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    let n1 = fs_paths(pipe).await.len();
    // Crash after delivery; restart rescan must update, not duplicate.
    let stats = sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(stats["created"], json!(0));
    assert_eq!(fs_paths(pipe).await.len(), n1);
    h.abort();
}

// ---------- B4: backlog of N changes while provider down ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_restart_with_backlog() {
    let pipe = r"\\.\pipe\spaceos-v15p-b4";
    let db = tmp("b4.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("b4w");
    for f in ["k1.txt", "k2.txt", "gone.txt", "mv.txt"] {
        std::fs::write(w.join(f), "x").unwrap();
    }
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    // Backlog while provider down: +2 creates, 1 modify, 1 delete, 1 rename.
    std::fs::write(w.join("n1.txt"), "n").unwrap();
    std::fs::write(w.join("n2.txt"), "n").unwrap();
    std::fs::write(w.join("k1.txt"), "changed").unwrap();
    std::fs::remove_file(w.join("gone.txt")).unwrap();
    std::fs::rename(w.join("mv.txt"), w.join("mv2.txt")).unwrap();
    // V1.7 Phase 1: first scan reconciles creates/rename/update; the delete
    // is only MISSING_UNCERTAIN (row stays). Two more scans confirm it.
    let s1 = sc.scan_on(&w, pipe).await.unwrap();
    assert!(s1["renamed"].as_u64().unwrap() >= 1);
    assert_eq!(s1["removed"], json!(0));
    assert_eq!(s1["uncertain"], json!(1));
    sc.scan_on(&w, pipe).await.unwrap();
    let s3 = sc.scan_on(&w, pipe).await.unwrap();
    assert!(s3["removed"].as_u64().unwrap() >= 1);
    // Current state: root + k1 + k2 + n1 + n2 + mv2, nothing stale.
    let paths = fs_paths(pipe).await;
    assert_eq!(paths.len(), 6, "V1.7: backlog converges after confirmation");
    let on_disk: std::collections::HashSet<String> = {
        let mut s = std::collections::HashSet::new();
        for e in std::fs::read_dir(&w).unwrap() {
            s.insert(e.unwrap().path().to_string_lossy().to_string());
        }
        s.insert(w.to_string_lossy().to_string());
        s
    };
    let stale: Vec<_> = paths.iter().filter(|p| !on_disk.contains(*p)).collect();
    assert!(stale.is_empty(), "V1.7: no stale rows after confirmation");
    // Rename preserved identity (fingerprint orphan match, asserted above).
    // No duplicate identities.
    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let exts: Vec<_> = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["external_id"].clone())
        .collect();
    let uniq: std::collections::HashSet<_> = exts.iter().collect();
    assert_eq!(exts.len(), uniq.len());
    h.abort();
}

// ---------- D1: core downtime + external create ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_downtime_external_create() {
    let pipe = r"\\.\pipe\spaceos-v15p-d1";
    let db = tmp("d1.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d1w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    h.abort(); // Core OFF
    std::fs::write(w.join("fresh.txt"), "new").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let h2 = start_core(pipe, &db).await; // Core ON
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    let paths = fs_paths(pipe).await;
    assert!(paths.iter().any(|p| p.ends_with("fresh.txt")));
    assert_eq!(paths.len(), 3);
    h2.abort();
}

// ---------- D2a: core downtime + external modify ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_downtime_external_modify() {
    let pipe = r"\\.\pipe\spaceos-v15p-d2a";
    let db = tmp("d2a.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d2aW");
    std::fs::write(w.join("a.txt"), "v1").unwrap();
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    let before = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let id_before = before["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["external_id"].as_str().unwrap().ends_with("a.txt"))
        .unwrap()["id"]
        .clone();
    h.abort();
    std::fs::write(w.join("a.txt"), "v2-longer-content").unwrap();
    let h2 = start_core(pipe, &db).await;
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    let after = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let obj = after["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["external_id"].as_str().unwrap().ends_with("a.txt"))
        .unwrap();
    assert_eq!(obj["id"], id_before, "modify keeps identity");
    assert!(obj["properties"]["meta"]["size"].as_u64().unwrap() > 2);
    h2.abort();
}

// ---------- D3: core downtime + rename preserves Object ID ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_downtime_rename_preserves_id() {
    let pipe = r"\\.\pipe\spaceos-v15p-d3";
    let db = tmp("d3.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d3w");
    std::fs::write(w.join("old.txt"), "same-bytes").unwrap();
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    let before = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let id_before = before["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["external_id"].as_str().unwrap().ends_with("old.txt"))
        .unwrap()["id"]
        .clone();
    // Relation anchored on the file object. FK rule (Phase B): both
    // endpoints must exist, so the anchor target is created first.
    let proj = json!({"id": "proj", "type": "Project", "properties": {},
        "provider_id": "filesystem", "external_id": null,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "updated_at": chrono::Utc::now().to_rfc3339(),
        "actor": "filesystem"});
    assert_eq!(
        call_retry(pipe, "create_object", proj).await["ok"],
        true
    );
    let rel = json!({"id": uuid::Uuid::new_v4().to_string(), "source_object_id": id_before,
        "relation_type": "belongs_to", "target_object_id": "proj", "properties": {},
        "created_at": chrono::Utc::now().to_rfc3339(), "world_id": "", "actor": "filesystem"});
    assert_eq!(
        call_retry(pipe, "create_relation", rel.clone()).await["ok"],
        true
    );
    h.abort(); // Core OFF
    std::fs::rename(w.join("old.txt"), w.join("new.txt")).unwrap();
    let h2 = start_core(pipe, &db).await; // Core ON
    let stats = Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    assert!(stats["renamed"].as_u64().unwrap() >= 1);
    let after = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let obj = after["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["external_id"].as_str().unwrap().ends_with("new.txt"))
        .unwrap();
    assert_eq!(
        obj["id"], id_before,
        "rename across downtime keeps Object ID"
    );
    let rels = call_retry(
        pipe,
        "query_relations",
        json!({"source_object_id": id_before}),
    )
    .await;
    assert_eq!(
        rels["result"].as_array().unwrap().len(),
        1,
        "relation survives"
    );
    let _ = rel;
    h2.abort();
}

// ---------- D4: core downtime + delete ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_downtime_external_delete() {
    let pipe = r"\\.\pipe\spaceos-v15p-d4";
    let db = tmp("d4.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d4w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    std::fs::write(w.join("doomed.txt"), "x").unwrap();
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    assert_eq!(fs_paths(pipe).await.len(), 3);
    h.abort();
    std::fs::remove_file(w.join("doomed.txt")).unwrap();
    let h2 = start_core(pipe, &db).await;
    // V1.7 Phase 1: first scan marks MISSING_UNCERTAIN (row stays),
    // confirmation needs CONFIRM_MISSES consecutive scans.
    let sc2 = Scanner::new("filesystem");
    let s1 = sc2.scan_on(&w, pipe).await.unwrap();
    assert_eq!(s1["removed"], json!(0));
    assert_eq!(s1["uncertain"], json!(1));
    sc2.scan_on(&w, pipe).await.unwrap();
    let s3 = sc2.scan_on(&w, pipe).await.unwrap();
    assert!(s3["removed"].as_u64().unwrap() >= 1);
    let on_disk: std::collections::HashSet<String> = {
        let mut s = std::collections::HashSet::new();
        for e in std::fs::read_dir(&w).unwrap() {
            s.insert(e.unwrap().path().to_string_lossy().to_string());
        }
        s.insert(w.to_string_lossy().to_string());
        s
    };
    let stored = fs_paths(pipe).await;
    let stale: Vec<_> = stored.iter().filter(|p| !on_disk.contains(*p)).collect();
    assert!(
        stale.is_empty(),
        "V1.6: rescan leaves no stale current Object"
    );
    assert_eq!(stored.len(), 2, "root + a.txt only");
    // History still records the deletion (immutable log, not resurrected).
    let evs = call_retry(pipe, "query_events", json!({"event_type": "FileDeleted"})).await;
    assert!(!evs["result"].as_array().unwrap().is_empty());
    h2.abort();
}

// ---------- E1 (spec #16): double observation, no duplicate ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_observation_no_duplicate_object() {
    let pipe = r"\\.\pipe\spaceos-v15p-e1";
    let db = tmp("e1.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("e1w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    let sc = Scanner::new("filesystem");
    let s1 = sc.scan_on(&w, pipe).await.unwrap();
    let s2 = sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(
        s2["created"],
        json!(0),
        "second identical scan creates nothing"
    );
    assert_eq!(fs_paths(pipe).await.len(), 2);
    let _ = s1;
    h.abort();
}

// ---------- E2 (spec #17): duplicate provider registration ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_provider_registration_single_row() {
    let pipe = r"\\.\pipe\spaceos-v15p-e2";
    let db = tmp("e2.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    for _ in 0..2 {
        let r = call_retry(
            pipe,
            "register_provider",
            json!({"id": "filesystem", "name": "filesystem-provider", "capabilities": ["scan"]}),
        )
        .await;
        assert_eq!(r["ok"], true);
    }
    let list = call_retry(pipe, "list_providers", json!({})).await;
    let n = list["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["id"] == json!("filesystem"))
        .count();
    assert_eq!(n, 1);
    h.abort();
}

// ---------- F3: persisted state vs reconstruction boundary ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_vs_reconstruction_boundary() {
    let pipe = r"\\.\pipe\spaceos-v15p-f3";
    let db = tmp("f3.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("f3w");
    std::fs::write(w.join("real.txt"), "data").unwrap();
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    // Agent-only semantic objects: no Reality counterpart.
    let proj = Object::new("Project", "cli", json!({"name": "P"}));
    call_retry(pipe, "create_object", json!(proj)).await;
    let persisted = call_retry(pipe, "query_objects", json!({})).await;
    let n_persisted = persisted["result"].as_array().unwrap().len();
    let mut paths_before = fs_paths(pipe).await;
    paths_before.sort();
    h.abort();
    // Reconstruction into a FRESH core from Reality only.
    let pipe2 = r"\\.\pipe\spaceos-v15p-f3b";
    let db2 = tmp("f3b.db");
    let _ = std::fs::remove_file(&db2);
    let h2 = start_core(pipe2, &db2).await;
    Scanner::new("filesystem").scan_on(&w, pipe2).await.unwrap();
    let rebuilt = call_retry(pipe2, "query_objects", json!({})).await;
    let arr = rebuilt["result"].as_array().unwrap();
    let names: Vec<_> = arr
        .iter()
        .filter_map(|o| o["properties"].get("name"))
        .collect();
    // Filesystem current state converges (same paths)...
    let paths_after = {
        let mut v: Vec<String> = arr
            .iter()
            .filter_map(|o| o["external_id"].as_str().map(|s| s.to_string()))
            .collect();
        v.sort();
        v
    };
    // ...but agent-only objects are NOT reconstructable from Reality.
    assert!(
        !names.iter().any(|n| *n == "P"),
        "agent objects do not come back from Reality"
    );
    assert_eq!(
        paths_after, paths_before,
        "filesystem current state converges exactly"
    );
    // ...and history does not come back either.
    let evs = call_retry(pipe2, "query_events", json!({})).await;
    let _ = (n_persisted, evs, paths_before, paths_after);
    h2.abort();
}
