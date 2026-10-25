//! B6 — Many-World isolation under load. Fixed conditions, see
//! docs/BREAKAGE_CAMPAIGN.md. 100 worlds, 20 active agents mutating
//! only their own world + one subscription each + cross queries.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::ipc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb6");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
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
    let storage = space_core::Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call_retry(
        pipe,
        "register_agent",
        json!({"principal": "system", "name": name, "capabilities": caps, "worlds": worlds}),
    )
    .await;
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

fn with_token(mut p: Value, tok: &str) -> Value {
    p["actor_token"] = json!(tok);
    p
}

fn pipe_for() -> String {
    Box::leak(format!(r"\\.\pipe\spaceos-bb6-{}", std::process::id()).into_boxed_str()).to_string()
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

async fn recv_event(conn: &mut SubConn, ms: u64) -> Option<Value> {
    match tokio::time::timeout(std::time::Duration::from_millis(ms), conn.lines.next_line()).await {
        Ok(Ok(Some(l))) => serde_json::from_str(&l).ok(),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn hundred_worlds_twenty_active() {
    let pipe = pipe_for();
    let db = tmp_db("w100.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    // Seed 100 worlds (spaces as world registry entries).
    for i in 0..100 {
        let mut sp = json!(space_core::Space::new(format!("W{i:03}"), json!({})));
        sp["actor"] = json!("cli");
        assert_eq!(call_retry(&pipe, "create_space", sp).await["ok"], true);
    }
    // 20 active agents, each bound to its own world, each subscribed.
    let mut agents = vec![];
    let mut subs = vec![];
    for i in 0..20 {
        let w = format!("W{i:03}");
        let name = format!("ag-{i:02}");
        let tok = register(
            &pipe,
            &name,
            &["object.create", "relation.create", "event.record"],
            &[w.as_str()],
        )
        .await;
        let (sub, ack) = subscribe(
            &pipe,
            with_token(json!({"subscriber": name, "world_id": w}), &tok),
        )
        .await;
        assert_eq!(ack["ok"], true);
        agents.push((name, w, tok));
        subs.push(sub);
    }
    // Concurrent storm: each agent writes object+relation+event to OWN world.
    let mut handles = vec![];
    for (idx, (name, world, tok)) in agents.iter().enumerate() {
        let (p, n, w, t) = (pipe.clone(), name.clone(), world.clone(), tok.clone());
        handles.push(tokio::spawn(async move {
            let mut o =
                space_core::Object::new("File", n.as_str(), json!({"world_id": w, "n": idx}));
            o.external_id = Some(format!("{w}:f"));
            let r = call_retry(&p, "create_object", with_token(json!(o), &t)).await;
            assert_eq!(r["ok"], true, "{n} object landed");
            // relation needs real endpoints: point at own object
            let objs = call_retry(&p, "query_objects", json!({"world_id": w})).await;
            let oid = objs["result"][0]["id"].as_str().unwrap().to_string();
            let mut rel = json!({"id": uuid::Uuid::new_v4().to_string(),
                "source_object_id": oid, "relation_type": "contains",
                "target_object_id": oid, "properties": {},
                "created_at": chrono::Utc::now().to_rfc3339(), "world_id": w});
            rel["actor"] = json!(n);
            let r = call_retry(&p, "create_relation", with_token(rel, &t)).await;
            assert_eq!(r["ok"], true, "{n} relation landed");
            let mut e = space_core::Event::new("Tick", n.as_str(), json!({"w": w}));
            e.world_id = w.clone();
            let r = call_retry(&p, "record_event", with_token(json!(e), &t)).await;
            assert_eq!(r["ok"], true, "{n} event landed");
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    // Each subscriber sees exactly its own Tick, nothing foreign.
    for ((name, world, _), sub) in agents.iter().zip(subs.iter_mut()) {
        let ev = recv_event(sub, 5000)
            .await
            .unwrap_or_else(|| panic!("{name} must receive its world event"));
        assert_eq!(ev["event"]["world_id"], json!(*world));
        assert!(
            recv_event(sub, 400).await.is_none(),
            "{name} gets nothing foreign"
        );
    }
    // Per-world counts exact across all 100 (80 dormant untouched-empty).
    for i in 0..100 {
        let w = format!("W{i:03}");
        let t0 = std::time::Instant::now();
        let objs = call_retry(&pipe, "query_objects", json!({"world_id": w})).await;
        let dt = t0.elapsed();
        let n = objs["result"].as_array().unwrap().len();
        if i < 20 {
            assert_eq!(n, 1, "active world {w} has its object");
        } else {
            assert_eq!(n, 0, "dormant world {w} untouched");
        }
        if i == 0 {
            eprintln!("per-world scoped query under load: {}us", dt.as_micros());
        }
    }
    // Cross-cutting query still consistent.
    let all = call_retry(&pipe, "query_objects", json!({})).await;
    assert_eq!(all["result"].as_array().unwrap().len(), 20);
}
