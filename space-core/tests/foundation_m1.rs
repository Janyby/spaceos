//! M1 — Concurrent mutation conflict (same row).
//!
//! Hypothesis: two+ authenticated participants concurrently updating ONE
//! semantic object may cause silent lost update beyond LWW or invariant
//! violation (corrupt row, errors, crash).
//! What may break: final row corrupt (value never written), IPC errors.
//! Failure boundary: Core model / Storage (update = single-statement
//! INSERT OR REPLACE; no versioning/OCC exists by design).
//! Why it may break: no OCC or version column; concurrent writers race.
//! Exact workload: 1 object; 10 writers x 50 update_object with distinct
//! markers via real IPC against one Core; concurrent tasks.
//! Dataset: temp DB + temp pipe per run.
//! Fixed threshold: 0 errors; final marker in written set; row identity
//! intact; p95 update latency < 5000ms.
//! Expected behavior: HELD — last-writer-wins, no corruption. LWW itself is
//! documented current semantics, NOT a defect (no automatic conflict model
//! without proven requirement).
//! Measurement: errors, final-marker membership, p50/p95/p99, throughput.
//! Pass criterion: 0 errors AND final marker in set AND row intact.
//! Failure criterion: any error OR non-member final value OR missing row.
//! Reproduction protocol: 3 identical runs (run via --test-threads=1 suite
//! plus two manual reruns recorded in docs).

use serde_json::json;
use space_core::{ipc, Object, Storage};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-fm1");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
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

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn m1_same_row_concurrent_update() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm1-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("m1");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    // One shared object.
    let mut base = Object::new(
        "File",
        "filesystem",
        json!({"path": "shared.txt", "marker": "init"}),
    );
    base.id = "fm1-shared".to_string();
    base.external_id = Some("shared.txt".to_string());
    let r = ipc::send_request(pipe, "create_object", json!(base))
        .await
        .unwrap();
    assert_eq!(r["ok"], true);

    // 10 writers x 50 updates, distinct markers, fully concurrent.
    let mut handles = vec![];
    let lat = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let errs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    for w in 0..10 {
        let (p, l, e) = (pipe.to_string(), lat.clone(), errs.clone());
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                let marker = format!("w{w}-{i}");
                let mut o = Object::new(
                    "File",
                    "filesystem",
                    json!({"path": "shared.txt", "marker": marker}),
                );
                o.id = "fm1-shared".to_string();
                o.external_id = Some("shared.txt".to_string());
                let t0 = std::time::Instant::now();
                match ipc::send_request(&p, "update_object", json!(o)).await {
                    Ok(v) if v["ok"].as_bool() == Some(true) => {
                        l.lock().unwrap().push(t0.elapsed().as_micros() as i64)
                    }
                    Ok(v) => e
                        .lock()
                        .unwrap()
                        .push(v["error"].as_str().unwrap_or("denied").to_string()),
                    Err(err) => e.lock().unwrap().push(err.to_string()),
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let errs = errs.lock().unwrap().clone();
    assert!(
        errs.is_empty(),
        "M1 zero errors, got {:?}",
        &errs[..errs.len().min(3)]
    );

    let mut lat = lat.lock().unwrap().clone();
    assert_eq!(lat.len(), 500, "all 500 updates acknowledged");
    let (p50, p95, p99) = (
        pct(&mut lat.clone(), 50.0),
        pct(&mut lat.clone(), 95.0),
        pct(&mut lat.clone(), 99.0),
    );
    eprintln!("M1: ops=500 p50={p50}us p95={p95}us p99={p99}us");
    assert!(p95 < 5_000_000, "p95 < 5000ms");

    // Final state must be exactly one of the written markers (LWW).
    let got = ipc::send_request(pipe, "get_object", json!({"id": "fm1-shared"}))
        .await
        .unwrap();
    let obj: Object = serde_json::from_value(got["result"].clone()).unwrap();
    assert_eq!(obj.id, "fm1-shared");
    assert_eq!(obj.r#type, "File");
    assert_eq!(obj.external_id.as_deref(), Some("shared.txt"));
    let marker = obj
        .properties
        .get("marker")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let valid: Vec<String> = (0..10)
        .flat_map(|w| (0..50).map(move |i| format!("w{w}-{i}")))
        .collect();
    assert!(
        valid.iter().any(|m| m == marker),
        "M1 final marker must be a written value, got {marker:?}"
    );
    eprintln!("M1: final marker={marker} (LWW, member of written set)");
}
