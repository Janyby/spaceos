//! B1 — Watcher storm. Fixed conditions, see docs/BREAKAGE_CAMPAIGN.md.
//! Thresholds (fixed BEFORE the run): pre-rescan observed >=90%;
//! duplicates == 0 (hard); post-rescan convergence == 100% (hard);
//! post-rescan rename same-ID == 100% (hard). Pre-rescan rename
//! behavior is measured only.
#![cfg(windows)]
use filesystem_provider::{scanner::Scanner, watcher};
use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb1");
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

async fn event_count(pipe: &str, t: &str) -> usize {
    call_retry(pipe, "query_events", json!({"event_type": t})).await["result"]
        .as_array()
        .unwrap()
        .len()
}

/// Wait until the FileCreated/FileChanged/FileDeleted count is stable.
async fn quiesce(pipe: &str, types: &[&str], stable_ms: u64, timeout_ms: u64) -> (usize, u128) {
    let t0 = std::time::Instant::now();
    let mut last = usize::MAX;
    let mut stable_since = std::time::Instant::now();
    loop {
        let mut n = 0;
        for t in types {
            n += event_count(pipe, t).await;
        }
        if n == last {
            if stable_since.elapsed().as_millis() >= stable_ms as u128 {
                return (n, t0.elapsed().as_millis());
            }
        } else {
            last = n;
            stable_since = std::time::Instant::now();
        }
        if t0.elapsed().as_millis() > timeout_ms as u128 {
            return (last, t0.elapsed().as_millis());
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
}

async fn fs_rows(pipe: &str) -> Vec<Value> {
    call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await["result"]
        .as_array()
        .unwrap()
        .clone()
}

fn pipe_for(tag: &str) -> String {
    // Unique per process AND per test fn: reruns never collide, and two
    // campaign tests in one binary never share a pipe.
    Box::leak(format!(r"\\.\pipe\spaceos-bb1-{tag}-{}", std::process::id()).into_boxed_str())
        .to_string()
}

// ---------- create storm: 200 rapid creates ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn storm_create_200() {
    let pipe = pipe_for("create");
    let db = tmp("create.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("createw");
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw, p2) = (w.clone(), sc.clone(), pipe.clone());
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, p2.as_str(), stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let t0 = std::time::Instant::now();
    for i in 0..200 {
        std::fs::write(w.join(format!("f{i:03}.txt")), "x").unwrap();
    }
    let write_ms = t0.elapsed().as_millis();
    let (pre_events, quiet_ms) = quiesce(&pipe, &["FileCreated"], 2000, 45000).await;
    let rows = fs_rows(&pipe).await;
    let file_rows = rows
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|p| p.ends_with(".txt"))
                .unwrap_or(false)
        })
        .count();
    eprintln!("create storm: 200 writes in {write_ms}ms, pre-rescan events={pre_events} rows={file_rows} quiesce={quiet_ms}ms");
    assert!(
        file_rows >= 180,
        "pre-rescan observed >=90%: got {file_rows}/200"
    );
    // duplicates: same external_id twice
    let mut seen = std::collections::HashSet::new();
    let mut dups = 0;
    for o in &rows {
        if let Some(e) = o["external_id"].as_str() {
            if !seen.insert(e.to_string()) {
                dups += 1;
            }
        }
    }
    assert_eq!(dups, 0, "duplicates == 0");
    // rescan converges to exactly 100% (shared sc, same guard)
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    let rows2 = fs_rows(&pipe).await;
    let n2 = rows2
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|p| p.ends_with(".txt"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(n2, 200, "post-rescan convergence == 100%");
}

// ---------- create storm STRENGTHENED: 400 rapid creates ----------
// Strengthened retest after the transport retry fix: double the burst.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn storm_create_400_strengthened() {
    let pipe = pipe_for("create400");
    let db = tmp("create400.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("create400w");
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw, p2) = (w.clone(), sc.clone(), pipe.clone());
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, p2.as_str(), stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    for i in 0..400 {
        std::fs::write(w.join(format!("g{i:03}.txt")), "x").unwrap();
    }
    let (pre_events, quiet_ms) = quiesce(&pipe, &["FileCreated"], 2000, 90000).await;
    let rows = fs_rows(&pipe).await;
    let file_rows = rows
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|p| p.ends_with(".txt"))
                .unwrap_or(false)
        })
        .count();
    eprintln!("create storm x2: 400 writes, pre-rescan events={pre_events} rows={file_rows} quiesce={quiet_ms}ms");
    assert!(
        file_rows >= 360,
        "pre-rescan observed >=90%: got {file_rows}/400"
    );
    let mut seen = std::collections::HashSet::new();
    let mut dups = 0;
    for o in &rows {
        if let Some(e) = o["external_id"].as_str() {
            if !seen.insert(e.to_string()) {
                dups += 1;
            }
        }
    }
    assert_eq!(dups, 0, "duplicates == 0");
    // rescan with the SHARED instance (same guard + ledger).
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    let rows2 = fs_rows(&pipe).await;
    let n2 = rows2
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|p| p.ends_with(".txt"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(n2, 400, "post-rescan convergence == 100%");
}

// ---------- rename storm: 50 rapid renames ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn storm_rename_50() {
    let pipe = pipe_for("rename");
    let db = tmp("rename.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("renamew");
    for i in 0..50 {
        std::fs::write(w.join(format!("r{i:02}.txt")), format!("body-{i}")).unwrap();
    }
    // One shared instance for baseline scan, watcher, and rescans.
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    let mut before = std::collections::HashMap::new();
    for o in fs_rows(&pipe).await {
        if let (Some(e), Some(id)) = (o["external_id"].as_str(), o["id"].as_str()) {
            before.insert(e.to_string(), id.to_string());
        }
    }
    assert_eq!(before.len(), 51); // root + 50
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw, p2) = (w.clone(), sc.clone(), pipe.clone());
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, p2.as_str(), stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    for i in 0..50 {
        std::fs::rename(
            w.join(format!("r{i:02}.txt")),
            w.join(format!("s{i:02}.txt")),
        )
        .unwrap();
    }
    let (pre_events, quiet_ms) = quiesce(
        &pipe,
        &["FileChanged", "FileCreated", "FileMoved"],
        2000,
        45000,
    )
    .await;
    // Pre-rescan: MEASURE ONLY (live rename path is known to not preserve).
    let rows = fs_rows(&pipe).await;
    let pre_new_rows = rows
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|e| e.rsplit(['/', '\\']).next().unwrap_or("").starts_with('s'))
                .unwrap_or(false)
        })
        .count();
    let pre_old_rows = rows
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|e| e.rsplit(['/', '\\']).next().unwrap_or("").starts_with('r'))
                .unwrap_or(false)
        })
        .count();
    eprintln!("rename storm: pre-rescan events={pre_events} quiesce={quiet_ms}ms new-path-rows={pre_new_rows} stale-old-rows={pre_old_rows}");
    // Rescan must reconcile every rename to the same Object ID.
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    let rows2 = fs_rows(&pipe).await;
    let mut same = 0;
    let mut total = 0;
    for o in &rows2 {
        if let (Some(e), Some(id)) = (o["external_id"].as_str(), o["id"].as_str()) {
            if e.ends_with(".txt")
                && !std::path::Path::new(e)
                    .parent()
                    .map(|p| p.as_os_str().is_empty())
                    .unwrap_or(true)
            {
                total += 1;
                let fname = e.rsplit(['/', '\\']).next().unwrap_or("");
                if fname.starts_with('s') {
                    let old_fname = fname.replacen('s', "r", 1);
                    let dir = &e[..e.len() - fname.len()];
                    let old_full = format!("{dir}{old_fname}");
                    if before.get(&old_full).map(|s| s.as_str()) == Some(id) {
                        same += 1;
                    }
                }
            }
        }
    }
    eprintln!("rename storm: post-rescan same-ID {same}/{total}");
    assert_eq!(total, 50);
    assert_eq!(same, total, "post-rescan rename same-ID == 100%");
}

// ---------- modify storm: 200 rapid appends to one file ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn storm_modify_200() {
    let pipe = pipe_for("modify");
    let db = tmp("modify.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("modifyw");
    std::fs::write(w.join("hot.txt"), "0").unwrap();
    // One shared instance for baseline scan, watcher, and rescans.
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw, p2) = (w.clone(), sc.clone(), pipe.clone());
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, p2.as_str(), stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    for i in 1..=200 {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(w.join("hot.txt"))
            .unwrap();
        write!(f, "+{i}").unwrap();
    }
    let (changed_events, quiet_ms) = quiesce(&pipe, &["FileChanged"], 2000, 45000).await;
    let rows = fs_rows(&pipe).await;
    let hot: Vec<_> = rows
        .iter()
        .filter(|o| {
            o["external_id"]
                .as_str()
                .map(|p| p.ends_with("hot.txt"))
                .unwrap_or(false)
        })
        .collect();
    eprintln!("modify storm: FileChanged events={changed_events} quiesce={quiet_ms}ms");
    assert_eq!(hot.len(), 1, "exactly one object row for the hot file");
    let expected = "0".len() + (1..=200).map(|i| format!("+{i}").len()).sum::<usize>();
    assert_eq!(
        hot[0]["properties"]["meta"]["size"].as_u64().unwrap() as usize,
        expected,
        "final state converges"
    );
}

// ---------- delete storm: 100 rapid deletes ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn storm_delete_100() {
    let pipe = pipe_for("delete");
    let db = tmp("delete.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("deletew");
    for i in 0..100 {
        std::fs::write(w.join(format!("d{i:03}.txt")), "x").unwrap();
    }
    // One shared instance for baseline scan, watcher, and rescans.
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    assert_eq!(fs_rows(&pipe).await.len(), 101);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw, p2) = (w.clone(), sc.clone(), pipe.clone());
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, p2.as_str(), stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    for i in 0..100 {
        std::fs::remove_file(w.join(format!("d{i:03}.txt"))).unwrap();
    }
    let (del_events, quiet_ms) = quiesce(&pipe, &["FileDeleted"], 2000, 45000).await;
    let rows = fs_rows(&pipe).await;
    let remaining = rows.len().saturating_sub(1); // minus root
    eprintln!(
        "delete storm: FileDeleted events={del_events} remaining={remaining} quiesce={quiet_ms}ms"
    );
    assert!(
        remaining <= 10,
        "pre-rescan removed >=90%: {remaining} left"
    );
    assert_eq!(
        del_events as usize,
        100 - remaining,
        "one event per removal, no dup storm"
    );
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    assert_eq!(
        fs_rows(&pipe).await.len(),
        1,
        "post-rescan only root remains"
    );
}

// ---------- mixed storm: create+modify+rename+delete ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn storm_mixed_100() {
    let pipe = pipe_for("mixed");
    let db = tmp("mixed.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let w = world("mixedw");
    for i in 0..25 {
        std::fs::write(w.join(format!("m{i:02}.txt")), format!("v{i}")).unwrap();
    }
    // One shared instance for baseline scan, watcher, and rescans.
    let sc = std::sync::Arc::new(Scanner::new("filesystem"));
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (w2, scw, p2) = (w.clone(), sc.clone(), pipe.clone());
    tokio::spawn(async move {
        let _ = watcher::watch_on(&w2, scw, p2.as_str(), stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    for i in 0..25 {
        std::fs::write(w.join(format!("n{i:02}.txt")), "new").unwrap(); // create
    }
    for i in 0..25 {
        std::fs::write(w.join(format!("m{i:02}.txt")), "changed").unwrap(); // modify
    }
    for i in 0..10 {
        std::fs::rename(
            w.join(format!("m{i:02}.txt")),
            w.join(format!("r{i:02}.txt")),
        )
        .unwrap(); // rename
    }
    for i in 10..25 {
        std::fs::remove_file(w.join(format!("m{i:02}.txt"))).unwrap(); // delete
    }
    let (_, quiet_ms) = quiesce(
        &pipe,
        &["FileCreated", "FileChanged", "FileDeleted"],
        2500,
        60000,
    )
    .await;
    eprintln!("mixed storm quiesce={quiet_ms}ms");
    // Reconcile to confirmation and verify exact expected world.
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    sc.scan_on(&w, pipe.as_str()).await.unwrap();
    let rows = fs_rows(&pipe).await;
    let mut names: Vec<_> = rows
        .iter()
        .filter_map(|o| o["external_id"].as_str().map(|s| s.to_string()))
        .collect();
    names.sort();
    let mut expected: Vec<String> = vec![w.to_string_lossy().to_string()];
    for i in 0..25 {
        expected.push(w.join(format!("n{i:02}.txt")).to_string_lossy().to_string());
    }
    for i in 0..10 {
        expected.push(w.join(format!("r{i:02}.txt")).to_string_lossy().to_string());
    }
    expected.sort();
    assert_eq!(names, expected, "mixed storm converges exactly");
}
