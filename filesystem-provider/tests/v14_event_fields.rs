//! V1.4 T2 contract + deletion attribution regressions over the REAL
//! watcher path. Two independent checks:
//! 1) payload.observed_at is present on FileCreated, FileChanged and
//!    FileDeleted (provider observation time, not a history-read time).
//! 2) FileDeleted carries the deleted object's id in event.object_ids for
//!    BOTH provider paths: live watcher and reconciliation scanner.
//! Windows-only. Lives here (like v14_reactive_latency.rs) to reuse the
//! watch_on test endpoint without a dev-dependency cycle.
#![cfg(windows)]
use filesystem_provider::{scanner::Scanner, watcher};
use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v14ef");
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

/// Wait for the first live event of `kind` whose payload path matches.
async fn wait_event(conn: &mut SubConn, kind: &str, path: &std::path::Path, ms: u64) -> Value {
    let want = path.to_string_lossy().to_string();
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_millis(ms) {
        if let Some(ev) = recv_event(conn, 500).await {
            let e = &ev["event"];
            if e["event_type"].as_str() == Some(kind)
                && e["payload"]["path"].as_str() == Some(want.as_str())
            {
                return ev;
            }
        }
    }
    panic!("no {kind} for {want} within {ms} ms");
}

fn parse_observed_at(raw: &str) -> std::time::SystemTime {
    let dt = chrono::DateTime::parse_from_rfc3339(raw)
        .unwrap_or_else(|e| panic!("observed_at is not RFC3339 ({raw:?}): {e}"));
    std::time::SystemTime::from(dt.with_timezone(&chrono::Utc))
}

/// Poll current filesystem objects until `pred` holds (or 15 s pass).
async fn objects_until(pipe: &str, pred: impl Fn(&[Value]) -> bool) -> Vec<Value> {
    let start = std::time::Instant::now();
    loop {
        let objs = call_retry(pipe, "query_objects", json!({"provider_id": "filesystem"})).await;
        let arr = objs["result"].as_array().cloned().unwrap_or_default();
        if pred(&arr) || start.elapsed() > std::time::Duration::from_secs(15) {
            return arr;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Poll history for a FileDeleted event whose payload path ends with `suffix`.
async fn deleted_event_for(pipe: &str, suffix: &str) -> Value {
    let start = std::time::Instant::now();
    loop {
        let evs = call_retry(pipe, "query_events", json!({"event_type": "FileDeleted"})).await;
        if let Some(ev) = evs["result"].as_array().and_then(|a| {
            a.iter().find(|e| {
                e["payload"]["path"]
                    .as_str()
                    .map(|p| p.ends_with(suffix))
                    .unwrap_or(false)
            })
        }) {
            return ev.clone();
        }
        if start.elapsed() > std::time::Duration::from_secs(15) {
            panic!("no FileDeleted for *{suffix} within 15 s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Unique pipe per process run: stale kernel pipes from killed runs must
/// never collide with a fresh run (same rule as v14_reactive_latency).
fn unique_pipe(tag: &str) -> &'static str {
    let owned = format!(r"\\.\pipe\spaceos-{tag}");
    Box::leak(owned.into_boxed_str()) as &str
}

// ---------- 1: T2 provider timestamp on all three lifecycle events ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watcher_lifecycle_events_carry_observed_at() {
    let tag = format!("v14ef-{}", std::process::id());
    let pipe = unique_pipe(&tag);
    let db = tmp_path(&format!("{tag}.db"));
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;

    let dir = tmp_path(&format!("w-{tag}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("probe.txt");

    // Real provider watcher on the test pipe, started BEFORE the writes.
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (wdir, p2) = (dir.clone(), pipe.to_string());
    let scw = std::sync::Arc::new(Scanner::new("filesystem"));
    tokio::spawn(async move {
        let _ = watcher::watch_on(&wdir, scw, &p2, stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let (mut sub, ack) = subscribe(
        pipe,
        json!({"event_types": ["FileCreated", "FileChanged", "FileDeleted"]}),
    )
    .await;
    assert_eq!(ack["ok"], true, "subscribe: {ack}");

    // Observation timestamps must be >= this instant: fresh provider
    // observations of the writes below, not placeholders or read times.
    let t_before = std::time::SystemTime::now();

    std::fs::write(&target, "one").unwrap();
    let created = wait_event(&mut sub, "FileCreated", &target, 10_000).await;
    std::fs::write(&target, "two").unwrap();
    let changed = wait_event(&mut sub, "FileChanged", &target, 10_000).await;
    std::fs::remove_file(&target).unwrap();
    let deleted = wait_event(&mut sub, "FileDeleted", &target, 10_000).await;

    for (kind, ev) in [
        ("FileCreated", &created),
        ("FileChanged", &changed),
        ("FileDeleted", &deleted),
    ] {
        let raw = ev["event"]["payload"]["observed_at"]
            .as_str()
            .unwrap_or_else(|| panic!("{kind}: payload.observed_at missing: {ev}"));
        let t = parse_observed_at(raw);
        assert!(
            t >= t_before,
            "{kind}: observed_at {raw} predates the write -- not an observation of it"
        );
    }
    h.abort();
}

// ---------- 2: deletion attribution -- deleted id rides on the event ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_events_carry_deleted_object_id() {
    let tag = format!("v14efd-{}", std::process::id());
    let pipe = unique_pipe(&tag);
    let db = tmp_path(&format!("{tag}.db"));
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;

    // ---- watcher path: live Remove -> FileDeleted with the object id ----
    let wdir = tmp_path(&format!("wd-{tag}"));
    let _ = std::fs::remove_dir_all(&wdir);
    std::fs::create_dir_all(&wdir).unwrap();
    let live = wdir.join("live.txt");

    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (dir2, p2) = (wdir.clone(), pipe.to_string());
    let scw = std::sync::Arc::new(Scanner::new("filesystem"));
    tokio::spawn(async move {
        let _ = watcher::watch_on(&dir2, scw, &p2, stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    std::fs::write(&live, "live").unwrap();
    let live_s = live.to_string_lossy().to_string();
    let objs = objects_until(pipe, |a| {
        a.iter()
            .any(|o| o["external_id"].as_str() == Some(live_s.as_str()))
    })
    .await;
    let live_id = objs
        .iter()
        .find(|o| o["external_id"].as_str() == Some(live_s.as_str()))
        .unwrap_or_else(|| panic!("watcher object for {live_s} never appeared"))["id"]
        .as_str()
        .unwrap()
        .to_string();

    std::fs::remove_file(&live).unwrap();
    let del = deleted_event_for(pipe, "live.txt").await;
    assert_eq!(
        del["object_ids"],
        json!([live_id]),
        "watcher FileDeleted must carry exactly the deleted object's id"
    );
    assert_eq!(del["payload"]["object_id"], json!(live_id));
    // The deleted object is gone from current state (no re-minted twin).
    let after = objects_until(pipe, |a| {
        !a.iter()
            .any(|o| o["external_id"].as_str() == Some(live_s.as_str()))
    })
    .await;
    assert!(!after.iter().any(|o| o["id"] == json!(live_id)));

    // ---- scanner path: reconciliation-confirmed delete, same rule ----
    let sdir = tmp_path(&format!("sd-{tag}"));
    let _ = std::fs::remove_dir_all(&sdir);
    std::fs::create_dir_all(&sdir).unwrap();
    let gone = sdir.join("gone.txt");
    std::fs::write(&gone, "gone").unwrap();
    let sc = Scanner::new("filesystem");
    sc.scan_on(&sdir, pipe).await.unwrap();
    let gone_s = gone.to_string_lossy().to_string();
    let objs = objects_until(pipe, |a| {
        a.iter()
            .any(|o| o["external_id"].as_str() == Some(gone_s.as_str()))
    })
    .await;
    let gone_id = objs
        .iter()
        .find(|o| o["external_id"].as_str() == Some(gone_s.as_str()))
        .unwrap_or_else(|| panic!("scanner object for {gone_s} never appeared"))["id"]
        .as_str()
        .unwrap()
        .to_string();

    std::fs::remove_file(&gone).unwrap();
    // CONFIRM_MISSES = 3 consecutive misses confirm the deletion.
    let mut removed = 0u64;
    for _ in 0..3 {
        let stats = sc.scan_on(&sdir, pipe).await.unwrap();
        removed = stats["removed"].as_u64().unwrap_or(0);
    }
    assert!(
        removed >= 1,
        "scanner must confirm the deletion: removed={removed}"
    );
    let del = deleted_event_for(pipe, "gone.txt").await;
    assert_eq!(
        del["object_ids"],
        json!([gone_id]),
        "scanner FileDeleted must carry exactly the deleted object's id"
    );
    let after = objects_until(pipe, |a| {
        !a.iter()
            .any(|o| o["external_id"].as_str() == Some(gone_s.as_str()))
    })
    .await;
    assert!(!after.iter().any(|o| o["id"] == json!(gone_id)));
    h.abort();
}
