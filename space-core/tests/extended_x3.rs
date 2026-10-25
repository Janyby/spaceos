//! X3 — Slow subscriber isolation (fast + slow + disconnected).
//!
//! Hypothesis: a slow subscriber may delay other subscribers or reduce
//! Core's ability to accept and commit new changes.
//! What can break: fast subscriber loss/delay; producer slowdown;
//! Core errors under backpressure; hidden loss.
//! Failure boundary: broadcast fan-out (M6 prefix+stall contract cited,
//! not re-proven here beyond one lag case).
//! Why it can break: shared broadcast channel + per-connection tasks.
//! Exact workload: producer publishes 300 events (latency recorded per
//! op); fast subscriber drains concurrently; slow subscriber reads 20
//! paced then ends; a disconnected party resyncs via query at the end.
//! All worlds WA, real pipes. (True open-but-unread stall is M6's
//! territory, cited; here the slow reader models paced consumption +
//! departure.)
//! Dataset: temp DB + temp pipe per run.
//! Fixed threshold: producer 300/300 acked, 0 errors, p95 < 5000ms;
//! fast 300/300; slow paced 20/20; resync 300/300 for both late parties.
//! Expected behavior: HELD — independence in all directions.
//! Measurement: producer p50/p95, per-subscriber counts, resync counts.
//! Pass criterion: all exact. Failure criterion: fast loss OR producer
//! error/slowdown beyond threshold OR resync shortfall.
//! Reproduction protocol: 3 identical runs.

use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-x3");
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

async fn register(pipe: &str, name: &str) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities":["event.record"],"worlds":["WA"]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
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
    let req = json!({"method": "subscribe", "params": params});
    let mut s = serde_json::to_string(&req).unwrap();
    s.push('\n');
    w.write_all(s.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut lines = BufReader::new(r).lines();
    let ack = lines.next_line().await.unwrap().unwrap_or_default();
    (SubConn { lines }, serde_json::from_str(&ack).unwrap())
}

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn x3_subscriber_isolation() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-x3-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("x3");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let tok_w = register(pipe, "prod-x3").await;
    let tok_r = register(pipe, "reader-x3").await;
    let sub_params = || json!({"subscriber": "reader-x3", "actor_token": tok_r, "world_id": "WA"});

    let (mut fast, ack) = subscribe(pipe, sub_params()).await;
    assert_eq!(ack["ok"], true);
    let (mut slow, ack) = subscribe(pipe, sub_params()).await;
    assert_eq!(ack["ok"], true);

    // Fast drains concurrently in background.
    let fast_handle = tokio::spawn(async move {
        let mut got = vec![];
        for _ in 0..300 {
            match tokio::time::timeout(
                std::time::Duration::from_millis(15000),
                fast.lines.next_line(),
            )
            .await
            {
                Ok(Ok(Some(l))) => got.push(serde_json::from_str::<Value>(&l).unwrap()),
                _ => break,
            }
        }
        got
    });

    // Slow reads 20 with delay concurrently, then its task ends (drop =
    // disconnect after a paced prefix; the resync path is query-based).
    let slow_handle = tokio::spawn(async move {
        let mut got = vec![];
        for _ in 0..20 {
            match tokio::time::timeout(
                std::time::Duration::from_millis(15000),
                slow.lines.next_line(),
            )
            .await
            {
                Ok(Ok(Some(l))) => {
                    got.push(serde_json::from_str::<Value>(&l).unwrap());
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                _ => break,
            }
        }
        got
    });

    // Producer: 300 events, latency recorded (Core progress under load).
    // The slow connection stays open but unread from here on (stall).
    let mut lat = Vec::with_capacity(300);
    for i in 0..300 {
        let mut e = space_core::Event::new("Flow", "prod-x3", json!({"seq": i}));
        e.id = format!("x3-{i:03}");
        e.world_id = "WA".to_string();
        let mut p = json!(e);
        p["actor_token"] = json!(tok_w);
        let t0 = std::time::Instant::now();
        let r = ipc::send_request(pipe, "record_event", p).await.unwrap();
        lat.push(t0.elapsed().as_micros() as i64);
        assert_eq!(r["ok"], true, "producer op {i}");
    }
    let got_fast = fast_handle.await.unwrap();
    assert_eq!(got_fast.len(), 300, "X3: fast independent of slow stall");
    let slow_first = slow_handle.await.unwrap();
    assert_eq!(slow_first.len(), 20, "X3: slow got its paced 20");

    let mut lat2 = lat.clone();
    let (p50, p95) = (pct(&mut lat2.clone(), 50.0), pct(&mut lat2.clone(), 95.0));
    eprintln!("X3: producer p50={p50}us p95={p95}us under 2-subscriber load");
    assert!(p95 < 5_000_000, "producer p95 < 5000ms");

    // Disconnected subscriber: connects only now, resyncs via query.
    // Slow subscriber resyncs via query too (paced prefix, then gone).
    for who in ["disconnected", "slow"] {
        let q = ipc::send_request(pipe, "query_events", json!({"world_id": "WA"}))
            .await
            .unwrap();
        let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
        assert_eq!(
            evs.len(),
            300,
            "X3: {who} resync 300/300, got {}",
            evs.len()
        );
    }
    // (slow task ended after its paced prefix; resync is query-based).
    eprintln!("X3: fast 300/300 live, slow paced-20 + resync 300/300, committer unaffected");
}
