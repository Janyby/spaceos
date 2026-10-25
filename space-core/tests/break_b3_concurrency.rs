//! B3 — SQLite concurrent writers. Fixed conditions, see docs/BREAKAGE_CAMPAIGN.md.
//! Thresholds (fixed BEFORE the run): zero errors + exact final counts
//! at every level; write p99 < 2000ms. Levels 5/10/20/50 writers x 40
//! mixed mutations each, plus one level with concurrent full-scan readers.
//! NOTE: suite runs --test-threads=1, but THESE tests spawn their own
//! concurrency internally — that is the point of the experiment.
use serde_json::{json, Value};
use space_core::{ipc, Storage};
use std::sync::{Arc, Mutex};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb3");
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

async fn op(pipe: &str, method: &str, params: Value) -> Result<Value, String> {
    match ipc::send_request(pipe, method, params).await {
        Ok(v) => {
            if v["ok"].as_bool() == Some(true) {
                Ok(v)
            } else {
                Err(v["error"].as_str().unwrap_or("denied").to_string())
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

async fn level(nwriters: usize, with_readers: bool, tag: &str) {
    let pipe_owned = format!(r"\\.\pipe\spaceos-bb3-{tag}-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db(&format!("{tag}-{nwriters}"));
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    let lat = Arc::new(Mutex::new(Vec::new()));
    let errs = Arc::new(Mutex::new(Vec::new()));
    let err_at = Arc::new(Mutex::new(Vec::new()));
    let t_start_all = std::time::Instant::now();
    let mut handles = vec![];
    for w in 0..nwriters {
        let (p, l, errs_w) = (pipe.to_string(), lat.clone(), errs.clone());
        let ea = err_at.clone();
        let t0all = t_start_all;
        handles.push(tokio::spawn(async move {
            for i in 0..40 {
                // FK rule (Phase B): relation endpoints must exist as
                // objects. Each writer chains its relations to the object
                // it created one step earlier (same writer, sequential
                // loop, so the endpoint is guaranteed present). Object
                // ids are explicit (`obj-{w}-{i}`) instead of random so
                // the later step can address them; counts are unchanged
                // (14 objects/writer).
                let params = match i % 3 {
                    0 => {
                        let mut o = space_core::Object::new("File", "filesystem", json!({"w": w, "i": i}));
                        o.id = format!("obj-{w}-{i}");
                        ("create_object", json!(o))
                    }
                    1 => {
                        let ep = format!("obj-{w}-{}", i - 1);
                        let r = json!({"id": uuid::Uuid::new_v4().to_string(), "source_object_id": ep,
                            "relation_type": "x", "target_object_id": format!("obj-{w}-{}", i - 1),
                            "properties": {}, "created_at": chrono::Utc::now().to_rfc3339(),
                            "world_id": "", "actor": "filesystem"});
                        ("create_relation", r)
                    }
                    _ => {
                        let ev = space_core::Event::new("Tick", "filesystem", json!({"w": w, "i": i}));
                        ("record_event", json!(ev))
                    }
                };
                let t0 = std::time::Instant::now();
                match op(&p, params.0, params.1).await {
                    Ok(_) => l.lock().unwrap().push(t0.elapsed().as_micros() as i64),
                    Err(msg) => {
                        errs_w.lock().unwrap().push(msg);
                        ea.lock().unwrap().push(t0all.elapsed().as_millis() as u64);
                    }
                }
            }
        }));
    }
    if with_readers {
        for _ in 0..2 {
            let p = pipe.to_string();
            handles.push(tokio::spawn(async move {
                for _ in 0..30 {
                    let _ = op(&p, "query_objects", json!({})).await;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }));
        }
    }
    for h in handles {
        h.await.unwrap();
    }
    let mut l = lat.lock().unwrap().clone();
    let e = errs.lock().unwrap().clone();
    l.sort_unstable();
    let (p50, p95, p99) = (
        pct(&mut l.clone(), 50.0),
        pct(&mut l.clone(), 95.0),
        pct(&mut l.clone(), 99.0),
    );
    let thr = l.iter().sum::<i64>() as f64 / l.len().max(1) as f64 / 1000.0;
    eprintln!("writers={nwriters} readers={with_readers}: ops={} errs={} write p50={}us p95={}us p99={}us throughput={:.0}/s",
        l.len(), e.len(), p50, p95, p99, thr);
    if !e.is_empty() {
        let times = err_at.lock().unwrap().clone();
        eprintln!("error times ms: {:?}", &times[..times.len().min(25)]);
        let mut kinds: std::collections::HashMap<String, usize> = Default::default();
        for msg in &e {
            let k = if msg.contains("231") || msg.contains("busy") || msg.contains("pipe") {
                "transport"
            } else if msg.contains("denied") {
                "policy-denial"
            } else {
                "other"
            };
            *kinds
                .entry(format!("{k}: {}", &msg[..msg.len().min(120)]))
                .or_default() += 1;
        }
        eprintln!("error kinds: {kinds:?}");
    }
    assert!(e.is_empty(), "zero errors, got {:?}", &e[..e.len().min(3)]);
    // exact counts: objects+relations+events landed
    let objs = op(pipe, "query_objects", json!({})).await.unwrap();
    let n_objs = objs["result"].as_array().unwrap().len();
    // per writer: i%3==0 over 40 ops -> 14 objects (i=0,3,...,39)
    assert_eq!(n_objs, nwriters * 14, "exact object count");
    assert!(p99 < 2_000_000, "write p99 < 2000ms");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn concurrent_5_writers() {
    level(5, false, "w5").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn concurrent_10_writers() {
    level(10, false, "w10").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn concurrent_20_writers() {
    level(20, false, "w20").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn concurrent_50_writers() {
    level(50, false, "w50").await;
}

// Strengthened retest after the accept-loop fix (B3 protocol step 9).
#[tokio::test(flavor = "multi_thread", worker_threads = 128)]
async fn concurrent_100_writers_strengthened() {
    level(100, false, "w100").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn concurrent_writers_plus_readers() {
    level(10, true, "wr").await;
}
