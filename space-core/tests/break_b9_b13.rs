//! B9 (backpressure at scale) + B13 (locality re-probe).
//! Fixed conditions, see docs/BREAKAGE_CAMPAIGN.md.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::ipc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb913");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
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

fn pipe_for(tag: &str) -> String {
    Box::leak(format!(r"\\.\pipe\spaceos-bb913-{tag}-{}", std::process::id()).into_boxed_str())
        .to_string()
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
    (SubConn { lines }, serde_json::from_str(&ack).unwrap())
}

// ---------- B9: 2000-event flood, core stays responsive, lag accounted ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn flood_2000_core_responsive() {
    let pipe = pipe_for("flood");
    let db = tmp_db("flood.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let (mut sub, ack) = subscribe(&pipe, json!({})).await;
    assert_eq!(ack["ok"], true);
    // Slow subscriber: connected but never read during the flood.
    let t0 = std::time::Instant::now();
    for i in 0..2000 {
        let mut e = space_core::Event::new("Tick", "filesystem", json!({"i": i}));
        e.world_id = "W".into();
        call(&pipe, "record_event", json!(e)).await;
        if i % 500 == 0 {
            // Ping mid-flood: core must answer while flooded.
            let p = call(&pipe, "ping", json!({})).await;
            assert_eq!(p["ok"], true, "core responsive at flood mark {i}");
        }
    }
    let fill_ms = t0.elapsed().as_millis();
    eprintln!("2000 events absorbed in {fill_ms}ms, ping ok throughout");
    // All durable... (Phase B: a numeric `limit` is clamped to
    // MAX_EVENT_LIMIT keeping the newest rows; omit it to count all.)
    let stored = call(&pipe, "query_events", json!({})).await;
    assert_eq!(stored["result"].as_array().unwrap().len(), 2000);
    // ...subscriber lagged behind (broadcast 512 + unread pipe): drain now,
    // count what arrives; lag is documented, loss would be rows missing above.
    let mut got = 0;
    let t1 = std::time::Instant::now();
    while t1.elapsed().as_millis() < 8000 {
        match tokio::time::timeout(std::time::Duration::from_millis(300), sub.lines.next_line())
            .await
        {
            Ok(Ok(Some(_))) => got += 1,
            _ => break,
        }
        if got >= 2000 {
            break;
        }
    }
    eprintln!("slow subscriber drained {got}/2000 post-flood");
    assert!(
        got < 2000,
        "subscriber demonstrably lagged (else this test proves nothing)"
    );
}

// ---------- B13: locality re-probe 10/100/1000 on current code ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locality_curve_current() {
    for n in [10usize, 100, 1000] {
        let db = tmp_db(&format!("loc{n}"));
        let _ = std::fs::remove_file(&db);
        {
            let s = space_core::Storage::open(&db).unwrap();
            for i in 0..n {
                let w = format!("W{i:04}");
                s.create_object(&space_core::Object::new(
                    "Project",
                    "cli",
                    json!({"world_id": w}),
                ))
                .unwrap();
            }
        }
        let pipe = pipe_for(&format!("loc{n}"));
        start_core(&pipe, &db).await;
        // Warmup (pipe setup, caches).
        let _ = call(&pipe, "query_objects", json!({"world_id": "W0000"})).await;
        let t0 = std::time::Instant::now();
        for _ in 0..20 {
            let q = call(&pipe, "query_objects", json!({"world_id": "W0000"})).await;
            assert_eq!(q["result"].as_array().unwrap().len(), 1);
        }
        let us = t0.elapsed().as_micros() / 20;
        eprintln!("worlds={n}: scoped query p50-ish={us}us");
        if n == 1000 {
            // Documented expectation from V1.4/V1.7: linear, milliseconds.
            assert!(us < 100_000, "scoped query stays interactive-scale");
        }
    }
}
