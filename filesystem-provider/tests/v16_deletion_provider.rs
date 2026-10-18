//! V1.6 deletion lifecycle, provider side: external delete via scan and
//! live watcher, recreate identity rules, combination matrix, crash,
//! reconstruction, storm. Rule R2: rescan reconciles deletions (no stale
//! current Object). Rule R3: deletion terminates identity — recreate
//! always mints a new id, even for same path/content.
#![cfg(windows)]
use filesystem_provider::scanner::Scanner;
use filesystem_provider::watcher;
use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v16p");
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

async fn file_deleted_events(pipe: &str) -> Vec<Value> {
    let evs = call_retry(pipe, "query_events", json!({"event_type": "FileDeleted"})).await;
    evs["result"].as_array().unwrap().clone()
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
    let ack: Value = serde_json::from_str(&ack).unwrap();
    (SubConn { lines }, ack)
}

async fn recv_event(conn: &mut SubConn, ms: u64) -> Option<Value> {
    match tokio::time::timeout(std::time::Duration::from_millis(ms), conn.lines.next_line()).await {
        Ok(Ok(Some(l))) => serde_json::from_str(&l).ok(),
        _ => None,
    }
}

// ---------- Test 2+4: external delete + rescan removes, no stale ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_delete_rescan_removes() {
    let pipe = r"\\.\pipe\spaceos-v16p-d2";
    let db = tmp("d2.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d2w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    std::fs::write(w.join("b.txt"), "b").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(fs_ids(pipe).await.len(), 3); // root + 2 files
    std::fs::remove_file(w.join("b.txt")).unwrap();
    // Phase 1: one missing scan is MISSING_UNCERTAIN, not deletion.
    let s1 = sc.scan_on(&w, pipe).await.unwrap();
    assert_eq!(s1["removed"], json!(0));
    assert_eq!(s1["uncertain"], json!(1));
    assert_eq!(
        fs_ids(pipe).await.len(),
        3,
        "uncertain row stays, identity safe"
    );
    // ...two more consecutive misses confirm.
    sc.scan_on(&w, pipe).await.unwrap();
    let s3 = sc.scan_on(&w, pipe).await.unwrap();
    assert!(s3["removed"].as_u64().unwrap() >= 1);
    let ids = fs_ids(pipe).await;
    assert_eq!(
        ids.len(),
        2,
        "no stale current Object after confirmed rescan"
    );
    assert!(!ids.iter().any(|(p, _)| p.ends_with("b.txt")));
    // History records the inferred deletion honestly marked.
    let evs = file_deleted_events(pipe).await;
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0]["payload"]["observed"], json!("reconciliation"));
    h.abort();
}

// ---------- Test 3: live watcher delete (not rescan-substituted) ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_watcher_delete() {
    let pipe = r"\\.\pipe\spaceos-v16p-d3";
    let db = tmp("d3.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d3w");
    std::fs::write(w.join("live.txt"), "v").unwrap();
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    sc.scan_on(&w, pipe).await.unwrap();
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw) = (w.clone(), sc.clone());
    let p2 = pipe.to_string();
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, &p2, stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    std::fs::remove_file(w.join("live.txt")).unwrap();
    // Wait for the live path to converge (no rescan involved).
    let mut gone = false;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if !fs_ids(pipe)
            .await
            .iter()
            .any(|(p, _)| p.ends_with("live.txt"))
        {
            gone = true;
            break;
        }
    }
    assert!(gone, "live watcher removes the row");
    let evs = file_deleted_events(pipe).await;
    assert_eq!(evs.len(), 1);
    assert!(
        evs[0]["payload"].get("observed").is_none(),
        "live event, not a rescan substitution"
    );
    h.abort();
}

// ---------- Test 5: repeated delete stays quiet ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_delete_quiet() {
    let pipe = r"\\.\pipe\spaceos-v16p-d5";
    let db = tmp("d5.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d5w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    // Exactly one FileDeleted for the path: repeats add nothing, no storm.
    let evs = file_deleted_events(pipe).await;
    assert_eq!(evs.len(), 1, "repeated reconciliation emits once");
    assert_eq!(fs_ids(pipe).await.len(), 1, "only root remains");
    h.abort();
}

// ---------- Tests 9+10: recreate during uncertainty resumes; after
// confirmation mints new identity. Returns (original, uncertain-resume,
// confirmed-new).
async fn recreate_case(
    pipe: &str,
    w: &std::path::Path,
    name: &str,
    content: &[u8],
) -> (String, String, String) {
    let sc = Scanner::new("filesystem");
    let f = w.join(name);
    std::fs::write(&f, content).unwrap();
    sc.scan_on(w, pipe).await.unwrap();
    let before = fs_ids(pipe).await;
    let id_before = before
        .iter()
        .find(|(p, _)| p.ends_with(name))
        .unwrap()
        .1
        .clone();
    // Phase 1: one miss = uncertain; quick recreate resumes same identity.
    std::fs::remove_file(&f).unwrap();
    sc.scan_on(w, pipe).await.unwrap();
    assert!(
        fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with(name)),
        "uncertain row stays"
    );
    std::fs::write(&f, content).unwrap();
    sc.scan_on(w, pipe).await.unwrap();
    let id_resumed = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with(name))
        .unwrap()
        .1
        .clone();
    // Phase 1: three consecutive misses confirm; recreate after that is new.
    std::fs::remove_file(&f).unwrap();
    sc.scan_on(w, pipe).await.unwrap();
    sc.scan_on(w, pipe).await.unwrap();
    sc.scan_on(w, pipe).await.unwrap();
    assert!(
        !fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with(name)),
        "confirmed deletion removes"
    );
    std::fs::write(&f, content).unwrap();
    sc.scan_on(w, pipe).await.unwrap();
    let after = fs_ids(pipe).await;
    let id_after = after
        .iter()
        .find(|(p, _)| p.ends_with(name))
        .unwrap()
        .1
        .clone();
    (id_before, id_resumed, id_after)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_recreate_same_path_new_identity() {
    let pipe = r"\\.\pipe\spaceos-v16p-d9";
    let db = tmp("d9.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d9w");
    let (before, resumed, after) = recreate_case(pipe, &w, "a.txt", b"same-bytes").await;
    assert_eq!(
        before, resumed,
        "reappearance inside the uncertainty window resumes identity"
    );
    assert_ne!(
        before, after,
        "R3: recreate after CONFIRMED deletion mints new id"
    );
    h.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_recreate_same_content_new_identity() {
    let pipe = r"\\.\pipe\spaceos-v16p-d10";
    let db = tmp("d10.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d10w");
    // Same bytes as test 9 on purpose: content similarity must not merge.
    let (before, resumed, after) = recreate_case(pipe, &w, "b.txt", b"same-bytes").await;
    assert_eq!(before, resumed, "uncertainty window favors continuity");
    assert_ne!(
        before, after,
        "content match without continuity is not identity"
    );
    h.abort();
}

// ---------- Test 22: same content at a DIFFERENT path => new identity ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_content_different_path_no_merge() {
    let pipe = r"\\.\pipe\spaceos-v16p-d22";
    let db = tmp("d22.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d22w");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("orig.txt"), b"identical").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id_orig = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("orig.txt"))
        .unwrap()
        .1
        .clone();
    std::fs::remove_file(w.join("orig.txt")).unwrap();
    // Confirm the deletion first (3 misses): no orphan left to steal from.
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    assert!(!fs_ids(pipe)
        .await
        .iter()
        .any(|(p, _)| p.ends_with("orig.txt")));
    std::fs::write(w.join("copy.txt"), b"identical").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id_copy = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("copy.txt"))
        .unwrap()
        .1
        .clone();
    assert_ne!(
        id_orig, id_copy,
        "no cross-path false merge after observed deletion"
    );
    h.abort();
}

// ---------- Test 11: delete A, rename B->C ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_then_rename_other() {
    let pipe = r"\\.\pipe\spaceos-v16p-d11";
    let db = tmp("d11.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d11w");
    std::fs::write(w.join("a.txt"), b"aaa").unwrap();
    std::fs::write(w.join("b.txt"), b"bbb").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    let id_b = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("b.txt"))
        .unwrap()
        .1
        .clone();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    std::fs::rename(w.join("b.txt"), w.join("c.txt")).unwrap();
    let stats = sc.scan_on(&w, pipe).await.unwrap();
    assert!(stats["renamed"].as_u64().unwrap() >= 1);
    // a.txt is only uncertain after one miss: row stays, nothing removed yet.
    assert_eq!(stats["removed"], json!(0));
    assert!(fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")));
    let id_c = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("c.txt"))
        .unwrap()
        .1
        .clone();
    assert_eq!(
        id_b, id_c,
        "rename keeps identity while the delete is still uncertain"
    );
    // Two more misses confirm a.txt's deletion.
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let ids = fs_ids(pipe).await;
    assert!(!ids.iter().any(|(p, _)| p.ends_with("a.txt")));
    h.abort();
}

// ---------- Test 12: rename then delete ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_then_delete() {
    let pipe = r"\\.\pipe\spaceos-v16p-d12";
    let db = tmp("d12.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d12w");
    std::fs::write(w.join("a.txt"), b"aaa").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    std::fs::rename(w.join("a.txt"), w.join("b.txt")).unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    assert!(fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("b.txt")));
    std::fs::remove_file(w.join("b.txt")).unwrap();
    // One miss: still uncertain, row stays.
    sc.scan_on(&w, pipe).await.unwrap();
    assert!(fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("b.txt")));
    // Two more misses confirm.
    sc.scan_on(&w, pipe).await.unwrap();
    let stats = sc.scan_on(&w, pipe).await.unwrap();
    assert!(stats["removed"].as_u64().unwrap() >= 1);
    assert!(!fs_ids(pipe)
        .await
        .iter()
        .any(|(p, _)| p.ends_with("b.txt") || p.ends_with("a.txt")));
    h.abort();
}

// ---------- Test 13: delete + recreate + rename ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_recreate_rename() {
    let pipe = r"\\.\pipe\spaceos-v16p-d13";
    let db = tmp("d13.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d13w");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), b"v1").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id_v1 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    // Confirm first (3 misses): only then is the identity terminated.
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    assert!(!fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")));
    std::fs::write(w.join("a.txt"), b"v2").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id_v2 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    assert_ne!(
        id_v1, id_v2,
        "recreate after CONFIRMED delete is a new entity"
    );
    std::fs::rename(w.join("a.txt"), w.join("c.txt")).unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id_c = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("c.txt"))
        .unwrap()
        .1
        .clone();
    assert_eq!(
        id_v2, id_c,
        "the new entity then renames with identity intact"
    );
    h.abort();
}

// ---------- Extra matrix: create -> delete -> recreate fast ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_delete_recreate_fast() {
    let pipe = r"\\.\pipe\spaceos-v16p-dx";
    let db = tmp("dx.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("dxw");
    let sc = Scanner::new("filesystem");
    let mut ids = vec![];
    for i in 0..3 {
        std::fs::write(w.join("f.txt"), format!("gen-{i}")).unwrap();
        sc.scan_on(&w, pipe).await.unwrap();
        ids.push(
            fs_ids(pipe)
                .await
                .iter()
                .find(|(p, _)| p.ends_with("f.txt"))
                .unwrap()
                .1
                .clone(),
        );
        std::fs::remove_file(w.join("f.txt")).unwrap();
        sc.scan_on(&w, pipe).await.unwrap();
        // Single miss = uncertain, never confirmed in this fast cycle.
        assert!(
            fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("f.txt")),
            "uncertain row stays {i}"
        );
    }
    // Fast delete/recreate never reaches confirmation: one identity throughout.
    assert_eq!(ids[0], ids[1]);
    assert_eq!(ids[1], ids[2]);
    h.abort();
}

// ---------- Test 15: crash after external deletion, before rescan ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_external_delete_before_rescan() {
    let pipe = r"\\.\pipe\spaceos-v16p-d15";
    let db = tmp("d15.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d15w");
    std::fs::write(w.join("a.txt"), "a").unwrap();
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    h.abort(); // crash before any reconciliation
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let h2 = start_core(pipe, &db).await;
    // Provider side also restarted here (fresh ledger): misses restart
    // from zero — the safe direction. Three post-crash scans confirm.
    // (One shared Scanner across the three, as in production runtime.)
    let sc2 = Scanner::new("filesystem");
    sc2.scan_on(&w, pipe).await.unwrap();
    assert!(
        fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")),
        "first post-crash scan is only uncertain"
    );
    sc2.scan_on(&w, pipe).await.unwrap();
    sc2.scan_on(&w, pipe).await.unwrap();
    assert!(!fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")));
    assert_eq!(fs_ids(pipe).await.len(), 1, "only root remains");
    h2.abort();
}

// ---------- Test 16: reconstruction after deletion ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_after_deletion() {
    let pipe = r"\\.\pipe\spaceos-v16p-d16";
    let db = tmp("d16.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d16w");
    std::fs::write(w.join("keep.txt"), "k").unwrap();
    std::fs::write(w.join("drop.txt"), "d").unwrap();
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    std::fs::remove_file(w.join("drop.txt")).unwrap();
    // Confirm through the shared scanner (3 misses) before comparing.
    let sc = Scanner::new("filesystem");
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let live_set: std::collections::HashSet<String> =
        fs_ids(pipe).await.into_iter().map(|(p, _)| p).collect();
    h.abort();
    // Fresh core rebuilt from Reality only.
    let pipe2 = r"\\.\pipe\spaceos-v16p-d16b";
    let db2 = tmp("d16b.db");
    let _ = std::fs::remove_file(&db2);
    let h2 = start_core(pipe2, &db2).await;
    Scanner::new("filesystem").scan_on(&w, pipe2).await.unwrap();
    let rebuilt: std::collections::HashSet<String> =
        fs_ids(pipe2).await.into_iter().map(|(p, _)| p).collect();
    assert_eq!(
        live_set, rebuilt,
        "reconstruction matches post-deletion current state"
    );
    assert!(!rebuilt.iter().any(|p| p.ends_with("drop.txt")));
    // The fresh core has its own new history (FileCreated for keep.txt),
    // but nothing carried over: no FileDeleted, no old object references.
    let evs = call_retry(pipe2, "query_events", json!({})).await;
    let arr2 = evs["result"].as_array().unwrap();
    assert!(
        arr2.iter().all(|e| e["event_type"] != "FileDeleted"),
        "fresh core observed no deletions in its lifetime"
    );
    assert!(
        !arr2.iter().any(|e| e["payload"]
            .get("path")
            .and_then(|p| p.as_str())
            .map(|p| p.ends_with("drop.txt"))
            .unwrap_or(false)),
        "no history about the deleted file comes back from Reality"
    );
    h2.abort();
}

// ---------- Test 21: recreated object is active in World ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recreated_object_active_in_world() {
    let pipe = r"\\.\pipe\spaceos-v16p-d21";
    let db = tmp("d21.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d21w");
    let sc = Scanner::new("filesystem");
    std::fs::write(w.join("a.txt"), "one").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let id1 = fs_ids(pipe)
        .await
        .iter()
        .find(|(p, _)| p.ends_with("a.txt"))
        .unwrap()
        .1
        .clone();
    std::fs::remove_file(w.join("a.txt")).unwrap();
    // Confirm the deletion (3 misses) before recreating.
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    assert!(!fs_ids(pipe).await.iter().any(|(p, _)| p.ends_with("a.txt")));
    std::fs::write(w.join("a.txt"), "two").unwrap();
    sc.scan_on(&w, pipe).await.unwrap();
    let wld = call_retry(pipe, "get_world", json!({})).await;
    let objs = wld["result"]["objects"].as_array().unwrap();
    let cur = objs
        .iter()
        .find(|o| o["external_id"].as_str().unwrap_or("").ends_with("a.txt"))
        .unwrap();
    assert_ne!(cur["id"], json!(id1), "new identity, old id unresolvable");
    assert!(call_retry(pipe, "get_object", json!({"id": id1})).await["result"].is_null());
    h.abort();
}

// ---------- Test 24: 100 live deletes, subscriber counts, no storm ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storm_100_watcher_deletes() {
    let pipe = r"\\.\pipe\spaceos-v16p-d24";
    let db = tmp("d24.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let w = world("d24w");
    for i in 0..100 {
        std::fs::write(w.join(format!("f{i:03}.txt")), "x").unwrap();
    }
    Scanner::new("filesystem").scan_on(&w, pipe).await.unwrap();
    assert_eq!(fs_ids(pipe).await.len(), 101);
    let (mut sub, ack) = subscribe(pipe, json!({"event_types": ["FileDeleted"]})).await;
    assert_eq!(ack["ok"], true);
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw) = (w.clone(), sc.clone());
    let p2 = pipe.to_string();
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, &p2, stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let t0 = std::time::Instant::now();
    for i in 0..100 {
        std::fs::remove_file(w.join(format!("f{i:03}.txt"))).unwrap();
    }
    // Collect until all 100 distinct deletions observed or timeout.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut total = 0u32;
    let deadline = std::time::Duration::from_secs(30);
    let start = std::time::Instant::now();
    while seen.len() < 100 && start.elapsed() < deadline {
        if let Some(ev) = recv_event(&mut sub, 500).await {
            total += 1;
            for oid in ev["event"]["object_ids"].as_array().unwrap() {
                seen.insert(oid.as_str().unwrap().to_string());
            }
        }
    }
    let dt = t0.elapsed();
    eprintln!(
        "100 live deletes: distinct={} total_events={} in {}ms",
        seen.len(),
        total,
        dt.as_millis()
    );
    assert_eq!(
        seen.len(),
        100,
        "every deletion observed exactly via distinct identity"
    );
    assert_eq!(total, 100, "no duplicate FileDeleted storm");
    assert_eq!(fs_ids(pipe).await.len(), 1, "only root remains");
    // Subscriber stayed responsive throughout (ping via fresh call works).
    assert_eq!(call_retry(pipe, "ping", json!({})).await["ok"], true);
    h.abort();
}
