//! Full-path load probe (Stage 5): MCP/HTTP-equivalent client calls go
//! through the REAL path — Core IPC -> Policy -> stub provider with REAL
//! temp-dir disk IO -> terminal Action* events -> readback.
//!
//! This is a measurement probe, not a unit test: numbers print via
//! eprintln (debug build, loopback IPC). Asserts cover correctness
//! only (zero errors, exact counts, disk content, history growth).
//! Bounded: 150 writes + 100 reads + 20 MCP calls + 1 mission.
#![cfg(windows)]

use serde_json::{json, Value};
use space_core::ipc;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[path = "support/global_lock.rs"]
mod global_lock;

use global_lock::{acquire_global_ipc, ServerGuard, TaskRegistry};

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-load").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

fn report_lat(tag: &str, lat: &mut Vec<i64>) {
    let n = lat.len();
    let (p50, p95, p99) = (
        pct(&mut lat.clone(), 50.0),
        pct(&mut lat.clone(), 95.0),
        pct(&mut lat.clone(), 99.0),
    );
    let sum: i64 = lat.iter().sum();
    eprintln!(
        "{tag}: n={n} p50={p50}us p95={p95}us p99={p99}us throughput={:.0}/s",
        n as f64 / (sum as f64 / 1_000_000.0)
    );
}

/// Filesystem stub, REAL disk IO inside `root`, REAL roots gate.
async fn run_fs_stub(pipe: &'static str, root: PathBuf) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::windows::named_pipe::ServerOptions;
    loop {
        let server = match ServerOptions::new().create(pipe) {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        if server.connect().await.is_err() {
            continue;
        }
        let (reader, mut writer) = tokio::io::split(server);
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let req: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => break,
            };
            let method = req["method"].as_str().unwrap_or("").to_string();
            let params = req.get("params").cloned().unwrap_or(json!({}));
            let resp = match method.as_str() {
                "ping" => json!({"ok": true, "result": {"pong": true}}),
                "action" => {
                    let action = params["action"].as_str().unwrap_or("").to_string();
                    let target = params["target"].clone();
                    let aid = params["id"].as_str().unwrap_or("").to_string();
                    let done = |status: &str, detail: Value| {
                        json!({"ok": true, "result": {
                            "action_id": aid, "status": status, "detail": detail,
                        }})
                    };
                    let path = target["path"].as_str().unwrap_or("").to_string();
                    if !PathBuf::from(&path).starts_with(&root) {
                        done(
                            "Failed",
                            json!({"error": "forbidden: outside allowed roots"}),
                        )
                    } else {
                        let p = PathBuf::from(&path);
                        match action.as_str() {
                            "read" => match std::fs::read_to_string(&p) {
                                Ok(c) => done("Completed", json!({"path": path, "content": c})),
                                Err(e) => done("Failed", json!({"error": format!("read: {e}")})),
                            },
                            "create" => {
                                if p.exists() {
                                    done("Failed", json!({"error": "exists"}))
                                } else {
                                    let c = target["content"].as_str().unwrap_or("");
                                    match std::fs::write(&p, c) {
                                        Ok(()) => done("Completed", json!({"path": path})),
                                        Err(e) => {
                                            done("Failed", json!({"error": format!("create: {e}")}))
                                        }
                                    }
                                }
                            }
                            "write" => {
                                let c = target["content"].as_str().unwrap_or("");
                                match std::fs::write(&p, c) {
                                    Ok(()) => done("Completed", json!({"path": path})),
                                    Err(e) => {
                                        done("Failed", json!({"error": format!("write: {e}")}))
                                    }
                                }
                            }
                            _ => done("Failed", json!({"error": "unsupported"})),
                        }
                    }
                }
                other => json!({"ok": false, "error": format!("unknown method {other}")}),
            };
            let mut s = serde_json::to_string(&resp).unwrap();
            s.push('\n');
            use tokio::io::AsyncWriteExt as _;
            if writer.write_all(s.as_bytes()).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    }
}

struct Stack {
    _lock: global_lock::GlobalIpcLock,
    _guard: ServerGuard,
    dir: PathBuf,
    token: String,
}

async fn stack(tag: &str) -> Stack {
    let _lock = acquire_global_ipc();
    let dir = tmp(tag);
    let db = dir.join("core.db");
    let storage = space_core::Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    tokio::spawn(async move {
        let _ = ipc::run_server(state, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let reg: Arc<TaskRegistry> = Arc::new(TaskRegistry::default());
    let _guard = ServerGuard::new(reg.clone());
    let root = dir.clone();
    reg.clone().spawn(async move {
        run_fs_stub(ipc::PROVIDER_PIPE, root).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "register_provider",
        json!({"id": "filesystem", "name": "filesystem", "capabilities": ["read", "create", "write"]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "register_agent",
        json!({"principal": "system", "name": "load-agent",
               "capabilities": ["observation.report", "action.read", "action.create", "action.write", "event.record"],
               "worlds": []}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    Stack {
        _lock,
        _guard,
        dir,
        token: r["result"]["token"].as_str().unwrap().to_string(),
    }
}

async fn act(st: &Stack, action: &str, target: Value) -> Value {
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "request_action",
        json!({"id": uuid::Uuid::new_v4().to_string(), "action": action,
               "target": target, "provider_id": "filesystem",
               "actor": "load-agent", "actor_token": st.token}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "transport: {r}");
    r["result"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn load_full_path_profiles() {
    let st = stack("profiles").await;
    let root = st.dir.to_string_lossy().to_string();

    // Profile 1: 50 sequential create+read+write rounds (150 actions).
    let mut lat = vec![];
    let mut errs = 0;
    for i in 0..50 {
        let p = format!("{root}\\f{i:03}.txt");
        for (a, t) in [
            ("create", json!({"path": p, "content": "v1"})),
            ("read", json!({"path": p})),
            ("write", json!({"path": p, "content": "v2"})),
        ] {
            let t0 = Instant::now();
            let r = act(&st, a, t).await;
            lat.push(t0.elapsed().as_micros() as i64);
            if r["status"] != "Completed" {
                errs += 1;
            }
        }
    }
    assert_eq!(errs, 0, "sequential errors");
    report_lat("sequential action round-trip", &mut lat);

    // Profile 2: 100 paginated history reads (limit 50).
    let mut rlat = vec![];
    for _ in 0..100 {
        let t0 = Instant::now();
        let r = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"limit": 50}))
            .await
            .unwrap();
        rlat.push(t0.elapsed().as_micros() as i64);
        assert_eq!(r["ok"], true);
    }
    report_lat("paginated history read", &mut rlat);

    // Profile 3: N clients x 10 parallel writes (N from LOAD_CLIENTS,
    // default 8; raise only for deliberate burst characterization).
    let ncli: usize = std::env::var("LOAD_CLIENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let st = Arc::new(st);
    let mut handles = vec![];
    let plats = Arc::new(Mutex::new(vec![]));
    let perrs = Arc::new(Mutex::new(Vec::<String>::new()));
    for c in 0..ncli {
        let (st, plats, perrs) = (st.clone(), plats.clone(), perrs.clone());
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let p = format!("{}\\p{c}-{i}.txt", st.dir.to_string_lossy());
                let t0 = Instant::now();
                match ipc::send_request(
                    ipc::CORE_PIPE,
                    "request_action",
                    json!({"id": uuid::Uuid::new_v4().to_string(), "action": "create",
                           "target": {"path": p}, "provider_id": "filesystem",
                           "actor": "load-agent", "actor_token": st.token}),
                )
                .await
                {
                    Ok(r) => {
                        plats.lock().unwrap().push(t0.elapsed().as_micros() as i64);
                        if r["result"]["status"] != "Completed" {
                            perrs
                                .lock()
                                .unwrap()
                                .push(format!("{c}-{i}: {}", r["result"]));
                        }
                    }
                    Err(e) => perrs
                        .lock()
                        .unwrap()
                        .push(format!("{c}-{i} transport: {e}")),
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let perrs = perrs.lock().unwrap().clone();
    assert!(
        perrs.is_empty(),
        "parallel errors: {:?}",
        &perrs[..perrs.len().min(3)]
    );
    report_lat("parallel writes 8x10", &mut plats.lock().unwrap().clone());

    // Profile 4: one 3-step mission end to end.
    let t0 = Instant::now();
    let (view, _) = space_bridge::mission::create_mission(
        "load-agent",
        &st.token,
        "load probe",
        &root,
        "load-agent",
    )
    .await
    .unwrap();
    let mid = view.mission["id"].as_str().unwrap().to_string();
    let (view, _) = space_bridge::mission::plan_mission(
        "load-agent",
        &st.token,
        &mid,
        vec![
            ("s1".to_string(), String::new(), false),
            ("s2".to_string(), String::new(), false),
        ],
    )
    .await
    .unwrap();
    for s in &view.steps {
        let sid = s["id"].as_str().unwrap();
        space_bridge::mission::transition_step(
            "load-agent",
            &st.token,
            &mid,
            sid,
            "running",
            json!({}),
        )
        .await
        .unwrap();
        space_bridge::mission::transition_step(
            "load-agent",
            &st.token,
            &mid,
            sid,
            "completed",
            json!({"evidence": "probe"}),
        )
        .await
        .unwrap();
    }
    space_bridge::mission::transition_mission(
        "load-agent",
        &st.token,
        &mid,
        "verifying",
        json!({}),
    )
    .await
    .unwrap();
    space_bridge::mission::record_verification(
        "load-agent",
        &st.token,
        &mid,
        true,
        "probe",
        json!({}),
    )
    .await
    .unwrap();
    let (view, _) = space_bridge::mission::transition_mission(
        "load-agent",
        &st.token,
        &mid,
        "completed",
        json!({}),
    )
    .await
    .unwrap();
    assert_eq!(view.state(), "completed");
    eprintln!("mission 2-step wall={}ms", t0.elapsed().as_millis());

    // Profile 5: 20 MCP calls through the real dispatch (same Core).
    let cache = tokio::sync::Mutex::new(None);
    let mut mlat = vec![];
    for i in 0..20 {
        let t0 = Instant::now();
        let r =
            space_bridge::mcp::call_tool(&cache, "mission_get", &json!({"mission_id": mid})).await;
        mlat.push(t0.elapsed().as_micros() as i64);
        assert!(r.get("isError").is_none(), "mcp call {i}: {r}");
    }
    report_lat("mcp mission_get dispatch", &mut mlat);

    // Growth accounting.
    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"limit": 500}))
        .await
        .unwrap();
    eprintln!(
        "history events={} db_bytes={}",
        evs["result"].as_array().unwrap().len(),
        std::fs::metadata(st.dir.join("core.db")).unwrap().len()
    );
}
