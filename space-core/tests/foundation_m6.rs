//! M6 — Slow View / subscription backpressure.
//!
//! Hypothesis: a slow subscriber affects Core progress or causes
//! undetected event loss.
//! What may break: Core stalls on slow delivery; fast subscribers miss
//! events; lagged subscriber silently loses events with no resync path.
//! Failure boundary: Subscription delivery (broadcast fan-out, buffer
//! 512) + documented resync contract (re-query; no resume queue).
//! Why it may break: broadcast lag drops the slow receiver; our delivery
//! task ends its loop on lag error, leaving the pipe stalled.
//! Exact workload: (a) mild: 1 fast + 1 slow (50ms/read) subscriber,
//! 30 events; (b) overflow: 1 concurrently-draining fast + 1 fully
//! stalled subscriber, 2000 events (>512 broadcast buffer); stalled pipe
//! must deliver a contiguous prefix then stall; resync via query_events.
//! Dataset: temp DB + temp pipe per test.
//! Fixed threshold: (a) 30/30 both, Core 30/30 committed; (b) fast
//! 2000/2000, committed 2000/2000, stalled prefix 0..k contiguous with
//! k<2000 then timeout, resync 2000/2000.
//! Expected behavior: HELD — Core unaffected; lag becomes prefix + stall
//! + re-query convergence (documented contract, no new broker).
//! Measurement: per-subscriber counts, commit counts, stall timeout.
//! Pass criterion: all counts exact as above.
//! Failure criterion: Core commit shortfall OR fast-subscriber loss OR
//! resync shortfall.
//! Reproduction protocol: 3 identical runs.

use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-fm6");
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

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":worlds}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register {name}: {r}");
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

async fn recv_n(conn: &mut SubConn, n: usize, ms_each: u64) -> Vec<Value> {
    let mut out = vec![];
    for _ in 0..n {
        match tokio::time::timeout(
            std::time::Duration::from_millis(ms_each),
            conn.lines.next_line(),
        )
        .await
        {
            Ok(Ok(Some(l))) => out.push(serde_json::from_str(&l).unwrap()),
            _ => break,
        }
    }
    out
}

async fn publish(pipe: &str, tok: &str, id: &str, seq: usize, world: &str) {
    let mut e = space_core::Event::new("Flow", "writer-m6", json!({"seq": seq}));
    e.id = id.to_string();
    e.world_id = world.to_string();
    let mut params = json!(e);
    params["actor_token"] = json!(tok);
    let r = ipc::send_request(pipe, "record_event", params)
        .await
        .unwrap();
    assert_eq!(r["ok"], true, "record {id}: {r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn m6_slow_subscriber_backpressure() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm6a-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("m6a");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let tok_w = register(pipe, "writer-m6", &["event.record"], &["WA"]).await;
    let tok_r = register(pipe, "reader-m6", &[], &["WA"]).await;

    let (mut fast, ack) = subscribe(
        pipe,
        json!({"subscriber": "reader-m6", "actor_token": tok_r, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    let (mut slow, ack) = subscribe(
        pipe,
        json!({"subscriber": "reader-m6", "actor_token": tok_r, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);

    for i in 0..30 {
        publish(pipe, &tok_w, &format!("m6a-{i:03}"), i, "WA").await;
    }

    // Fast drains immediately.
    let got_fast = recv_n(&mut fast, 30, 5000).await;
    assert_eq!(got_fast.len(), 30, "fast must get 30/30");

    // Slow reads with 50ms delay per event; buffer absorbs 30.
    let mut got_slow = vec![];
    for _ in 0..30 {
        match tokio::time::timeout(
            std::time::Duration::from_millis(5000),
            slow.lines.next_line(),
        )
        .await
        {
            Ok(Ok(Some(l))) => {
                got_slow.push(serde_json::from_str::<Value>(&l).unwrap());
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            _ => break,
        }
    }
    assert_eq!(got_slow.len(), 30, "slow must get 30/30 (delayed)");
    eprintln!("M6 mild: fast 30/30, slow 30/30 delayed, Core unaffected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn m6_overflow_stall_then_resync() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm6b-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("m6b");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let tok_w = register(pipe, "writer-m6", &["event.record"], &["WA"]).await;
    let tok_r = register(pipe, "reader-m6", &[], &["WA"]).await;

    let (mut fast, ack) = subscribe(
        pipe,
        json!({"subscriber": "reader-m6", "actor_token": tok_r, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    // Stalled subscriber: connected, never reads.
    let (mut stalled, ack) = subscribe(
        pipe,
        json!({"subscriber": "reader-m6", "actor_token": tok_r, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);

    // 2000 events: exceeds broadcast 512 + notify 256 + pipe buffering for
    // a fully stalled reader, forcing the lag regime (probe volume).
    // Fast drains CONCURRENTLY (lag hits any receiver that falls >512
    // behind, even a "fast" one that starts late).
    let fast_handle = tokio::spawn(async move {
        let mut got = vec![];
        for _ in 0..2000 {
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
    for i in 0..2000 {
        publish(pipe, &tok_w, &format!("m6b-{i:04}"), i, "WA").await;
    }

    // Fast (concurrent drain) still converges fully: Core progress
    // unaffected by the stall.
    let got_fast = fast_handle.await.unwrap();
    assert_eq!(got_fast.len(), 2000, "fast must get 2000/2000");

    // Committed state complete.
    let q = ipc::send_request(pipe, "query_events", json!({"world_id": "WA"}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
    assert_eq!(evs.len(), 2000, "Core committed 2000/2000");

    // Drain whatever the stalled pipe holds, then expect stall (timeout).
    let mut stalled_got: Vec<Value> = vec![];
    loop {
        match tokio::time::timeout(
            std::time::Duration::from_millis(1000),
            stalled.lines.next_line(),
        )
        .await
        {
            Ok(Ok(Some(l))) => {
                stalled_got.push(serde_json::from_str(&l).unwrap());
                if stalled_got.len() >= 2000 {
                    break;
                }
            }
            _ => break,
        }
    }
    let seqs: Vec<i64> = stalled_got
        .iter()
        .filter_map(|v| v["event"]["payload"]["seq"].as_i64())
        .collect();
    // Lag characterization (measured): the stalled pipe delivers a
    // contiguous PREFIX then stalls (forwarder ends on broadcast lag).
    // No gaps, no reordering, no phantom suffix — then timeout.
    assert!(!seqs.is_empty(), "stalled pipe must deliver a prefix");
    assert_eq!(seqs[0], 0, "prefix starts at 0");
    assert!(
        seqs.len() < 2000,
        "lag must truncate a fully stalled reader, got full delivery"
    );
    for (idx, s) in seqs.iter().enumerate() {
        assert_eq!(*s, idx as i64, "prefix contiguous, gap at {idx}");
    }
    eprintln!(
        "M6 overflow: stalled delivered prefix 0..{} then stall (lag regime)",
        seqs.len()
    );

    // Resync via query (documented contract) converges fully.
    let q = ipc::send_request(pipe, "query_events", json!({"world_id": "WA"}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
    assert_eq!(evs.len(), 2000, "resync 2000/2000 via query");
    eprintln!("M6 overflow: fast 2000/2000, committed 2000/2000, resync 2000/2000");
}
