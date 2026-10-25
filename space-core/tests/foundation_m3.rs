//! M3 — Multi-View convergence.
//!
//! Hypothesis: several Views (subscribers) cannot guaranteed-converge to
//! one current World State after a burst of changes or a reconnect.
//! What may break: missed/duplicated delivery while connected; resync
//! after reconnect incomplete.
//! Failure boundary: Subscription delivery (broadcast fan-out) / documented
//! resync contract (re-query; NO resume queue by design).
//! Why it may break: broadcast lagged receivers, disconnect gaps.
//! Exact workload: 3 subscribers on world WA via real pipes; burst of 50
//! events; drop sub3; 20 more events; sub3 reconnects fresh and resyncs
//! via query_events (the documented mechanism).
//! Dataset: temp DB + temp pipe per run.
//! Fixed threshold: each live subscriber receives 50/50 burst, no dupes;
//! resync sees 70/70; per-subscriber order == publish order.
//! Expected behavior: HELD (convergence via re-query, not redelivery).
//! Measurement: per-subscriber received counts, dupe counts, order check.
//! Pass criterion: 3x50/50 + 0 dupes + ordered + resync 70/70.
//! Failure criterion: any miss/dupe while connected OR incomplete resync.
//! Reproduction protocol: 3 identical runs.

use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-fm3");
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
    let ack: Value = serde_json::from_str(&ack).unwrap();
    (SubConn { lines }, ack)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn m3_multi_view_convergence() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm3-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("m3");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let tok_w = register(pipe, "writer-m3", &["event.record"], &["WA"]).await;
    let tok_r = register(pipe, "reader-m3", &[], &["WA"]).await;

    // 3 live views on the same world.
    let mut subs = vec![];
    for _ in 0..3 {
        let (c, ack) = subscribe(
            pipe,
            json!({"subscriber": "reader-m3", "actor_token": tok_r, "world_id": "WA"}),
        )
        .await;
        assert_eq!(ack["ok"], true, "subscribe: {ack}");
        subs.push(c);
    }

    // Burst of 50 events, single-threaded publish => deterministic order.
    for i in 0..50 {
        let mut e = space_core::Event::new("Burst", "writer-m3", json!({"seq": i}));
        e.id = format!("m3-b-{i:03}");
        e.world_id = "WA".to_string();
        e.correlation_id = Some("m3-burst".to_string());
        let mut p = json!(e);
        p["actor_token"] = json!(tok_w);
        let r = ipc::send_request(pipe, "record_event", p).await.unwrap();
        assert_eq!(r["ok"], true, "record {i}: {r}");
    }

    // Each view must converge to 50/50, no dupes, in publish order.
    for (vi, sub) in subs.iter_mut().enumerate() {
        let got = recv_n(sub, 50, 5000).await;
        assert_eq!(got.len(), 50, "view {vi} must receive 50/50");
        let mut seqs: Vec<i64> = got
            .iter()
            .map(|v| v["event"]["payload"]["seq"].as_i64().unwrap())
            .collect();
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 50, "view {vi}: 0 dupes, 0 misses");
        let mut ordered = seqs.clone();
        ordered.sort_unstable();
        assert_eq!(seqs, ordered, "view {vi}: publish order preserved");
        let _ = seqs;
    }
    eprintln!("M3: 3 views x 50/50, ordered, 0 dupes");

    // Disconnect view 3 (drop connection), publish 20 more.
    subs.pop();
    for i in 50..70 {
        let mut e = space_core::Event::new("Burst", "writer-m3", json!({"seq": i}));
        e.id = format!("m3-b-{i:03}");
        e.world_id = "WA".to_string();
        e.correlation_id = Some("m3-burst".to_string());
        let mut p = json!(e);
        p["actor_token"] = json!(tok_w);
        let r = ipc::send_request(pipe, "record_event", p).await.unwrap();
        assert_eq!(r["ok"], true);
    }
    // Remaining live views still converge (20/20 new).
    for (vi, sub) in subs.iter_mut().enumerate() {
        let got = recv_n(sub, 20, 5000).await;
        assert_eq!(got.len(), 20, "live view {vi} must receive 20/20");
    }

    // Reconnected view resyncs via query (documented contract, no redelivery).
    let q = ipc::send_request(pipe, "query_events", json!({"world_id": "WA"}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
    let burst: Vec<_> = evs.iter().filter(|e| e.event_type == "Burst").collect();
    assert_eq!(
        burst.len(),
        70,
        "resync must see 70/70, got {}",
        burst.len()
    );
    eprintln!("M3: reconnect resync 70/70 via query (no resume queue by design)");
}
