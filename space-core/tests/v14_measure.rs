//! V1.4 reliability + reactive measurement (tests 19-20 + T1/T2/T3/T4 suite).
//! Windows-only: subscriptions ride named-pipe connections.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::{ipc, percentile, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v14m");
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

async fn start_core_on_db(pipe: &str, db: &std::path::Path) {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":worlds}),
    )
    .await;
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

fn with_token(mut p: Value, tok: &str) -> Value {
    p["actor_token"] = json!(tok);
    p
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
    // core principals authenticate with the install token (same as send_request)
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

fn sys_to_ms(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

fn parse_rfc3339(s: &str) -> Option<std::time::SystemTime> {
    let dt = chrono::DateTime::parse_from_rfc3339(s).ok()?;
    Some(std::time::SystemTime::from(dt.with_timezone(&chrono::Utc)))
}

fn report(name: &str, v: &mut Vec<i64>) -> (i64, i64, i64, i64, i64) {
    let mut s = v.clone();
    s.sort_unstable();
    let min = s[0];
    let max = s[s.len() - 1];
    let p50 = percentile(&mut s.clone(), 50.0);
    let p95 = percentile(&mut s.clone(), 95.0);
    let p99 = percentile(&mut s.clone(), 99.0);
    eprintln!(
        "{name}: n={} min={min} p50={p50} p95={p95} p99={p99} max={max} (ms)",
        s.len()
    );
    (min, p50, p95, p99, max)
}

// ---- 19. slow subscriber / backpressure: core never blocks ----
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_subscriber_backpressure() {
    let pipe = r"\\.\pipe\spaceos-v14-bp";
    let db = tmp_path("bp.db");
    let _ = std::fs::remove_file(&db);
    start_core_on_db(pipe, &db).await;
    // slow subscriber: connected but never read during the flood
    let (_slow, ack) = subscribe(pipe, json!({})).await;
    assert_eq!(ack["ok"], true);
    // flood past the 512 broadcast capacity as a trusted core principal
    for i in 0..700 {
        let mut e = space_core::Event::new("Tick", "filesystem", json!({"i": i}));
        e.world_id = "W".into();
        call(pipe, "record_event", json!(e)).await;
    }
    // core is still responsive: storage is durable, delivery may lag
    let pong = call(pipe, "ping", json!({})).await;
    assert_eq!(pong["ok"], true);
    // Phase B: a numeric `limit` is clamped to MAX_EVENT_LIMIT and keeps
    // the NEWEST rows; omit it to count the full durable log.
    let stored = call(pipe, "query_events", json!({})).await;
    assert_eq!(stored["result"].as_array().unwrap().len(), 700);
    eprintln!(
        "backpressure: 700 events stored durably; core ping ok after flood past broadcast capacity"
    );
}

// ---- 20. ordering within one world ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordering_single_world() {
    let pipe = r"\\.\pipe\spaceos-v14-ord";
    let db = tmp_path("ord.db");
    let _ = std::fs::remove_file(&db);
    start_core_on_db(pipe, &db).await;
    let (mut c, ack) = subscribe(pipe, json!({})).await;
    assert_eq!(ack["ok"], true);
    for t in ["E1", "E2", "E3"] {
        let mut e = space_core::Event::new(t, "filesystem", json!({}));
        e.world_id = "W".into();
        call(pipe, "record_event", json!(e)).await;
    }
    let mut got = vec![];
    for _ in 0..3 {
        let ev = recv_event(&mut c, 2000).await.expect("ordered event");
        got.push(ev["event"]["event_type"].as_str().unwrap().to_string());
    }
    assert_eq!(got, vec!["E1", "E2", "E3"]);
}

// ---- perf: 10 / 100 / 1000 worlds locality probe ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn perf_worlds_locality() {
    for n in [10usize, 100, 1000] {
        let db = tmp_path(&format!("perf-{n}.db"));
        let _ = std::fs::remove_file(&db);
        {
            // bulk-seed via Storage (query-path probe, not IPC throughput)
            let s = Storage::open(&db).unwrap();
            for i in 0..n {
                let w = format!("W{i:04}");
                let mut ids = vec![];
                for k in 0..3 {
                    let o = space_core::Object::new(
                        "Project",
                        "filesystem",
                        json!({"world_id": w, "name": format!("P{k}")}),
                    );
                    ids.push(o.id.clone());
                    s.create_object(&o).unwrap();
                }
                // Phase B: FK — relation between two existing objects.
                let mut r = space_core::Relation::new(&ids[0], "contains", &ids[1]);
                r.world_id = w.clone();
                s.create_relation(&r).unwrap();
            }
        }
        let pipe = format!(r"\\.\pipe\spaceos-v14-perf-{n}");
        start_core_on_db(Box::leak(pipe.clone().into_boxed_str()), &db).await;
        let t0 = std::time::Instant::now();
        let q = call(&pipe, "query_objects", json!({"world_id":"W0000"})).await;
        let dq = t0.elapsed();
        assert_eq!(q["result"].as_array().unwrap().len(), 3);
        let t0 = std::time::Instant::now();
        let w = call(&pipe, "get_world", json!({"world_id":"W0000"})).await;
        let dw = t0.elapsed();
        assert_eq!(w["result"]["objects"].as_array().unwrap().len(), 3);
        // unrelated delivery: scoped subscriber must get nothing from W0001
        let tok = register(&pipe, &format!("perf-a-{n}"), &["event.record"], &["W0000"]).await;
        let (mut c, _) = subscribe(
            &pipe,
            with_token(
                json!({"subscriber": format!("perf-a-{n}"), "world_id":"W0000"}),
                &tok,
            ),
        )
        .await;
        let mut e = space_core::Event::new("ObjectCreated", &format!("perf-a-{n}"), json!({}));
        e.world_id = "W0001".into();
        call(&pipe, "record_event", with_token(json!(e), &tok)).await;
        let t0 = std::time::Instant::now();
        let leaked = recv_event(&mut c, 500).await;
        let de = t0.elapsed();
        assert!(leaked.is_none(), "no unrelated delivery");
        let size = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "worlds={n}: scoped_query={}us get_world={}us event_probe={}us unrelated=0 db={size}B",
            dq.as_micros(),
            dw.as_micros(),
            de.as_micros()
        );
    }
}
