//! V1.4 T1/T2/T3/T4 reactive measurement over the REAL watcher path.
//! T1 = filesystem mtime (reality), T2 = payload.observed_at (provider),
//! T3 = event.timestamp (core commit), T4 = subscriber pipe receipt.
//! Windows-only.
//! Lives here (not in space-core tests) to avoid a dev-dependency cycle.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::{ipc, percentile, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v14t");
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
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

fn sys_to_ms(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

fn parse_rfc3339(s: &str) -> Option<std::time::SystemTime> {
    let dt = chrono::DateTime::parse_from_rfc3339(s).ok()?;
    Some(std::time::SystemTime::from(dt.with_timezone(&chrono::Utc)))
}

fn report(name: &str, v: &[i64]) {
    let mut s = v.to_vec();
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactive_latency_100_samples() {
    // Unique pipe per process run: stale kernel pipes from killed runs
    // must never collide with a fresh measurement.
    let pipe_tag = format!("t-{}", std::process::id());
    let pipe_owned = format!(r"\\.\pipe\spaceos-v14-{pipe_tag}");
    let pipe = Box::leak(pipe_owned.into_boxed_str()) as &str;
    eprintln!("MEASURE pipe={pipe}");
    let db = tmp_path(&format!("{pipe_tag}.db"));
    let _ = std::fs::remove_file(&db);
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        if let Err(e) = ipc::run_server(s2, &p).await {
            eprintln!("test core server error on {p}: {e:?}");
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let dir = tmp_path(&format!("tworld-{pipe_tag}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("probe.txt");
    eprintln!("dir recreated, target pre-exists: {}", target.exists());

    // real provider watcher on the test pipe (watch_on, not the default pipe).
    // NOTE: the seed file must be created AFTER the watcher starts,
    // otherwise no FileCreated fires, no Object exists, and the watcher
    // correctly emits nothing for later FileChanged (nothing to attribute).
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let wdir = dir.clone();
    let scw = std::sync::Arc::new(filesystem_provider::scanner::Scanner::new("filesystem"));
    let p2 = pipe.to_string();
    tokio::spawn(async move {
        let _ = filesystem_provider::watcher::watch_on(&wdir, scw, &p2, stop_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // event-driven subscriber (NOT polling): T4 is pipe receipt.
    // NOTE: subscribe BEFORE the seed write (same order as the proven
    // debug flow); the seed FileCreated must be observed live.
    let (mut sub, ack) = subscribe(pipe, json!({"event_types":["FileChanged"]})).await;
    assert_eq!(ack["ok"], true);
    std::fs::write(&target, "seed").unwrap();
    // let the initial FileCreated settle so samples measure FileChanged only
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    while recv_event(&mut sub, 200).await.is_some() {}
    {
        let objs = call(pipe, "query_objects", json!({})).await;
        let evs = call(pipe, "query_events", json!({})).await;
        eprintln!(
            "after drain: objects={} events={}",
            objs["result"].as_array().map(|a| a.len()).unwrap_or(999),
            evs["result"].as_array().map(|a| a.len()).unwrap_or(999)
        );
    }
    // drain the seed FileCreated from queryable history is unnecessary;
    // we match samples by path below.

    let mut d12 = vec![];
    let mut d23 = vec![];
    let mut d34 = vec![];
    let mut d14 = vec![];
    let mut stale = 0u64;
    let mut attempts = 0;
    while d14.len() < 100 && attempts < 200 {
        attempts += 1;
        std::fs::write(&target, format!("sample-{attempts}")).unwrap();
        // T1 = filesystem mtime (reality changed)
        let t1 = std::fs::metadata(&target).unwrap().modified().unwrap();
        let t1ms = sys_to_ms(t1);
        let deadline = std::time::Duration::from_millis(4000);
        // Precise attribution: accept the FIRST event whose provider
        // observation (t2) is not older than this write's mtime.
        // Older events are pipeline backlog from previous writes.
        let mut got = None;
        let t_start = std::time::Instant::now();
        while t_start.elapsed() < deadline {
            let Some(ev) = recv_event(&mut sub, 200).await else {
                continue;
            };
            if ev["event"]["payload"]["path"].as_str() != Some(target.to_string_lossy().as_ref()) {
                continue;
            }
            let t2 = ev["event"]["payload"]["observed_at"]
                .as_str()
                .and_then(parse_rfc3339)
                .map(sys_to_ms);
            match t2 {
                Some(t2ms) if t2ms >= t1ms => {
                    got = Some((ev, t2ms));
                    break;
                }
                _ => stale += 1,
            }
        }
        let t4 = std::time::SystemTime::now();
        let Some((ev, t2ms)) = got else { continue };
        let Some(t3) = ev["event"]["timestamp"].as_str().and_then(parse_rfc3339) else {
            continue;
        };
        let (t3ms, t4ms) = (sys_to_ms(t3), sys_to_ms(t4));
        d12.push(t2ms - t1ms);
        d23.push(t3ms - t2ms);
        d34.push(t4ms - t3ms);
        d14.push(t4ms - t1ms);
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert_eq!(d14.len(), 100, "must collect 100 samples");
    eprintln!("stale backlog events skipped: {stale}");
    eprintln!("T1/T2/T3/T4 over real watcher+subscriber (ms):");
    report("T1->T2 provider-detect", &d12);
    report("T2->T3 core-commit", &d23);
    report("T3->T4 subscriber-delivery", &d34);
    report("T1->T4 end-to-end", &d14);
    eprintln!("target T1->T4 p50 <= 100ms is a probe target, not a guarantee");
    let _ = call(pipe, "ping", json!({})).await;
}
