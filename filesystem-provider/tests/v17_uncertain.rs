//! V1.7 Phase 1 — uncertain evidence lifecycle.
//! Rule: one missing scan is MISSING_UNCERTAIN (identity safe, no events);
//! CONFIRM_MISSES (3) consecutive misses confirm deletion; live watcher
//! Remove stays strong evidence (immediate). Ledger is provider-local,
//! never Core state; restart resets counts (safe direction).
#![cfg(windows)]
use filesystem_provider::scanner::{Scanner, CONFIRM_MISSES};
use serde_json::{json, Value};
use space_core::{ipc, Storage};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v17");
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

async fn fs_ids(pipe: &str) -> Vec<(String, String)> {
    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    objs["result"]
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

// ---------- Case 1: transient miss, file returns => same identity ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_miss_returns_same_identity() {
    let pipe = r"\\.\pipe\spaceos-v17-c1";
    let db = tmp("c1.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("c1w");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"data").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id0 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    // Blink: file gone for one scan, back before confirmation.
    std::fs::remove_file(w.join("a.txt")).unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    assert!(
        fs_ids(pipe).await.iter().any(|(_, id)| id == &id0),
        "no destructive delete on weak evidence"
    );
    assert_eq!(sc.uncertain().len(), 1);
    std::fs::write(w.join("a.txt"), b"data").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id1 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    assert_eq!(id0, id1, "same identity, no duplicate");
    assert!(sc.uncertain().is_empty(), "reappearance clears uncertainty");
    // No deletion event was ever recorded for a transient blink.
    let evs = call_retry(pipe, "query_events", json!({"event_type": "FileDeleted"})).await;
    assert!(evs["result"].as_array().unwrap().is_empty());
    h.abort();
}

// ---------- Case 3: confirmation exactly at K misses ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirm_at_k_consecutive_misses() {
    let pipe = r"\\.\pipe\spaceos-v17-c3";
    let db = tmp("c3.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("c3w");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"x").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    for i in 1..CONFIRM_MISSES {
        let s = sc.scan_on(&w, pipe).await.unwrap();
        assert_eq!(s["removed"], json!(0), "nothing removed at miss {i}");
        assert!(fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")));
    }
    let s = sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(
        s["removed"],
        json!(1),
        "confirmed exactly at miss {CONFIRM_MISSES}"
    );
    assert!(!fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")));
    let evs = call_retry(pipe, "query_events", json!({"event_type": "FileDeleted"})).await;
    assert_eq!(
        evs["result"].as_array().unwrap().len(),
        1,
        "exactly one deletion event"
    );
    h.abort();
}

// ---------- Case 4: rename during uncertain period ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_during_uncertain_period() {
    let pipe = r"\\.\pipe\spaceos-v17-c4";
    let db = tmp("c4.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("c4w");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"payload-123").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id0 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    // Anchor a relation on the file (Phase B: FK — target must exist).
    let mut proj = space_core::Object::new("Project", "filesystem", json!({"name": "proj"}));
    proj.id = "proj".into();
    assert_eq!(call_retry(pipe, "create_object", json!(proj)).await["ok"], true);
    let rel = json!({"id": uuid::Uuid::new_v4().to_string(), "source_object_id": id0,
        "relation_type": "belongs_to", "target_object_id": "proj",
        "properties": {}, "created_at": chrono::Utc::now().to_rfc3339(),
        "world_id": "", "actor": "filesystem"});
    assert_eq!(call_retry(pipe, "create_relation", rel).await["ok"], true);
    // One miss (uncertain), then the file reappears under a new name.
    std::fs::remove_file(w.join("a.txt")).unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    std::fs::write(w.join("b.txt"), b"payload-123").unwrap();
    let s = sc.scan_on(&w, pipe).await.unwrap();
    assert!(s["renamed"].as_u64().unwrap() >= 1);
    let id1 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("b.txt"))
        .unwrap()
        .1
        .clone();
    assert_eq!(
        id0, id1,
        "content continuity during uncertainty keeps identity"
    );
    let rels = call_retry(pipe, "query_relations", json!({"source_object_id": id0})).await;
    assert_eq!(
        rels["result"].as_array().unwrap().len(),
        1,
        "relation unharmed by uncertainty"
    );
    assert!(sc.uncertain().is_empty());
    h.abort();
}

// ---------- Restart resets the ledger (safe direction) ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_resets_ledger_safe() {
    let pipe = r"\\.\pipe\spaceos-v17-rs";
    let db = tmp("rs.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("rsw");
    std::fs::write(w.join("a.txt"), b"x").unwrap();
    let sc1 = Scanner::new("filesystem");
    sc1.scan_on(&w, pipe).await.unwrap();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    sc1.scan_on(&w, pipe).await.unwrap(); // miss 1
    assert!(fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")));
    // "Restart": fresh Scanner, empty ledger. Deletion needs K full new misses.
    let sc2 = Scanner::new("filesystem");
    sc2.scan_on(&w, pipe).await.unwrap();
    assert!(
        fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")),
        "restart must not hasten deletion"
    );
    sc2.scan_on(&w, pipe).await.unwrap();
    sc2.scan_on(&w, pipe).await.unwrap();
    assert!(
        !fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")),
        "confirmed after K new misses"
    );
    h.abort();
}

// ---------- Phase 1B: missing root changes nothing ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_root_changes_nothing() {
    let pipe = r"\\.\pipe\spaceos-v17-root";
    let db = tmp("root.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("rootw");
    std::fs::write(w.join("a.txt"), b"x").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    let n_before = fs_ids(pipe).await.len();
    // Whole root unavailable: scan must refuse, not read as mass deletion.
    std::fs::remove_dir_all(&w).unwrap();
    assert!(
        sc.scan_on(&w, pipe).await.is_err(),
        "missing root aborts reconciliation"
    );
    assert_eq!(
        fs_ids(pipe).await.len(),
        n_before,
        "a dead root deletes nothing"
    );
    h.abort();
}

// ---------- Phase 1D: uncertainty never harms relations ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_keeps_relations() {
    let pipe = r"\\.\pipe\spaceos-v17-rel";
    let db = tmp("rel.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("relw");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"x").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id0 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    // Phase B: FK — create the relation's target endpoint first.
    let mut proj = space_core::Object::new("Project", "filesystem", json!({"name": "proj"}));
    proj.id = "proj".into();
    assert_eq!(call_retry(pipe, "create_object", json!(proj)).await["ok"], true);
    let rel = json!({"id": uuid::Uuid::new_v4().to_string(), "source_object_id": id0,
        "relation_type": "belongs_to", "target_object_id": "proj",
        "properties": {}, "created_at": chrono::Utc::now().to_rfc3339(),
        "world_id": "", "actor": "filesystem"});
    assert_eq!(
        call_retry(pipe, "create_relation", rel).await["ok"],
        true,
        "UNCERTAIN seed: relation must exist"
    );
    std::fs::remove_file(w.join("a.txt")).unwrap();
    sc.scan_on(&w, pipe).await.unwrap(); // miss 1: uncertain
    sc.scan_on(&w, pipe).await.unwrap(); // miss 2: still uncertain
    let rels = call_retry(pipe, "query_relations", json!({"source_object_id": id0})).await;
    assert_eq!(
        rels["result"].as_array().unwrap().len(),
        1,
        "UNCERTAIN must not destroy relations"
    );
    h.abort();
}

// ---------- Phase 1D: reappearance refreshes state, keeps id ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_resume_refreshes_meta() {
    let pipe = r"\\.\pipe\spaceos-v17-rec";
    let db = tmp("rec.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("recw");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"short").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id0 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    std::fs::write(w.join("a.txt"), b"much-longer-content-here").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let obj = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["external_id"].as_str().unwrap_or("").ends_with("a.txt"))
        .unwrap();
    assert_eq!(obj["id"], json!(id0), "ACTIVE again with the same id");
    assert!(
        obj["properties"]["meta"]["size"].as_u64().unwrap() > 5,
        "meta reflects current Reality"
    );
    h.abort();
}

// ---------- Phase 11: rapid writes converge, no duplicates ----------
// Observation at T is a point-in-time fact; convergence comes from
// repeated reconciliation, not from debouncing inside Core.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_writes_converge_stable() {
    let pipe = r"\\.\pipe\spaceos-v17-rapid";
    let db = tmp("rapid.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("rapidw");
    let sc = Scanner::new("filesystem");
    let f = w.join("hot.txt");
    for i in 0..20 {
        std::fs::write(&f, format!("version-{i:02}-payload")).unwrap();
        sc.scan_on(&w, pipe).await.unwrap();
    }
    let ids = fs_ids(pipe).await;
    let rows: Vec<_> = ids.iter().filter(|(p, _)| p.ends_with("hot.txt")).collect();
    assert_eq!(rows.len(), 1, "one identity through 20 rapid observations");
    let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
    let obj = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["external_id"].as_str().unwrap_or("").ends_with("hot.txt"))
        .unwrap();
    assert_eq!(
        obj["properties"]["meta"]["size"],
        json!("version-19-payload".len() as u64),
        "final state reflects the last observation"
    );
    h.abort();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_reconnect_first_scan_uncertain() {
    let pipe = r"\\.\pipe\spaceos-v17-dc";
    let db = tmp("dc.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("dcw");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"x").unwrap();
    std::fs::write(w.join("b.txt"), b"y").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    // Provider disconnected: two files vanish with no observation running.
    std::fs::remove_file(w.join("a.txt")).unwrap();
    std::fs::remove_file(w.join("b.txt")).unwrap();
    // Reconnect: first scan marks uncertain only, deletes nothing.
    let s = sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(s["removed"], json!(0));
    assert_eq!(s["uncertain"], json!(2));
    assert_eq!(fs_ids(pipe).await.len(), 3, "root + 2 uncertain rows");
    h.abort();
}

// ---------- Phase 13: mixed workload + restart => Reality<->Core converge ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_workload_restart_converges() {
    let pipe = r"\\.\pipe\spaceos-v17-conv";
    let db = tmp("conv.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("convw");
    let sc = Scanner::new("filesystem");
    // Round 1: fs creates + direct core mutation interleaved.
    std::fs::write(w.join("a.txt"), b"a").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let note = space_core::Object::new("Document", "cli", json!({"name": "N"}));
    call_retry(pipe, "create_object", json!(note)).await;
    // Round 2: external modify + new file + core crash before reconcile.
    std::fs::write(w.join("a.txt"), b"a2-longer").unwrap();
    std::fs::write(w.join("b.txt"), b"b").unwrap();
    h.abort();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // Restart + reconcile to confirmation.
    let h2 = start_core(pipe, &db).await;
    let sc2 = Scanner::new("filesystem");
    sc2.scan_on(&w, pipe).await.unwrap();
    sc2.scan_on(&w, pipe).await.unwrap();
    sc2.scan_on(&w, pipe).await.unwrap();
    // Reality <-> Core: same file set, agent object intact.
    let mut disk: Vec<String> = vec![w.to_string_lossy().to_string()];
    for e in std::fs::read_dir(&w).unwrap() {
        disk.push(e.unwrap().path().to_string_lossy().to_string());
    }
    disk.sort();
    let mut core_paths: Vec<String> = fs_ids(pipe).await.into_iter().map(|(p, _)| p).collect();
    core_paths.sort();
    assert_eq!(disk, core_paths, "converged after mixed load + restart");
    let cli_objs = call_retry(pipe, "query_objects", json!({"provider_id": "cli"})).await;
    assert_eq!(
        cli_objs["result"].as_array().unwrap().len(),
        1,
        "core-side mutation survived"
    );
    h2.abort();
}
