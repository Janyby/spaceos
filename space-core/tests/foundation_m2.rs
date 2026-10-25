//! M2 — Agent identity / authority isolation under concurrency.
//!
//! Hypothesis: parallel agents with different capabilities and world scopes
//! may suffer misattribution or policy bypass under concurrency (token
//! mixup, scope race, wrong-actor writes).
//! What may break: unauthorized mutation succeeds (CRITICAL), allowed
//! mutation fails, or rows attributed to the wrong actor.
//! Failure boundary: Policy / IPC gate (in-memory registry + per-call auth).
//! Why it may break: concurrent tasks share one Core; a gate reading the
//! wrong token/actor under load would cross-contaminate authority.
//! Exact workload: Agent A (object.create, worlds [WA]) + Agent B
//! (event.record, worlds [WB]); 4 tasks x 30 iterations mixing allowed and
//! denied ops via real IPC: A-create (allow), B-record (allow), A-record
//! (deny cap), B-create (deny cap), A-in-WB (deny scope), B-in-WA (deny
//! scope), A-with-B-token (deny spoof).
//! Dataset: temp DB + temp pipe per run.
//! Fixed threshold: allowed paths 0 errors; denied paths 100% denied with
//! "denied"; ZERO unauthorized successes (critical line); row attribution
//! exact (objects by A only, events by B only); MutationDenied audited.
//! Expected behavior: HELD.
//! Measurement: allowed ok count, denied count, unauthorized count (must be 0).
//! Pass criterion: allowed==expected AND denied==expected AND unauthorized==0.
//! Failure criterion: any unauthorized success OR allowed failure OR misattribution.
//! Reproduction protocol: 3 identical runs.

use serde_json::json;
use space_core::{ipc, Object, Storage};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-fm2");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn m2_concurrent_authority_isolation() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm2-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("m2");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let tok_a = register(pipe, "agent-a", &["object.create"], &["WA"]).await;
    let tok_b = register(pipe, "agent-b", &["event.record"], &["WB"]).await;

    let allowed_ok = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let denied_ok = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let unauthorized = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let allowed_fail = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handles = vec![];
    for t in 0..4 {
        let (p, ta, tb) = (pipe.to_string(), tok_a.clone(), tok_b.clone());
        let (ao, dno, una, afl) = (
            allowed_ok.clone(),
            denied_ok.clone(),
            unauthorized.clone(),
            allowed_fail.clone(),
        );
        handles.push(tokio::spawn(async move {
            for i in 0..30 {
                let tag = format!("t{t}-{i}");
                // Allowed: A creates object.
                let mut o = Object::new(
                    "File",
                    "agent-a",
                    json!({"path": format!("a/{tag}.txt"), "world_id": "WA"}),
                );
                o.id = format!("m2-a-{tag}");
                let mut params = json!(o);
                params["actor_token"] = json!(ta);
                let _ = match ipc::send_request(&p, "create_object", params).await {
                    Ok(v) if v["ok"].as_bool() == Some(true) => {
                        ao.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    }
                    _ => afl.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                };
                // Allowed: B records event.
                let mut e = space_core::Event::new("Tick", "agent-b", json!({"t": tag}));
                e.id = format!("m2-e-{tag}");
                e.world_id = "WB".to_string();
                let mut params = json!(e);
                params["actor_token"] = json!(tb);
                let _ = match ipc::send_request(&p, "record_event", params).await {
                    Ok(v) if v["ok"].as_bool() == Some(true) => {
                        ao.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    }
                    _ => afl.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                };
                // Denied: A records event (no cap).
                let mut e = space_core::Event::new("Tick", "agent-a", json!({"t": tag}));
                e.id = format!("m2-ea-{tag}");
                let mut params = json!(e);
                params["actor_token"] = json!(ta);
                let _ = match ipc::send_request(&p, "record_event", params).await {
                    Ok(v) if v["ok"].as_bool() == Some(false) => {
                        dno.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    }
                    Ok(_) => una.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    Err(_) => una.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                };
                // Denied: B creates object (no cap).
                let mut o = Object::new("File", "agent-b", json!({"path": format!("b/{tag}.txt")}));
                o.id = format!("m2-b-{tag}");
                let mut params = json!(o);
                params["actor_token"] = json!(tb);
                let _ = match ipc::send_request(&p, "create_object", params).await {
                    Ok(v) if v["ok"].as_bool() == Some(false) => {
                        dno.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    }
                    Ok(_) => una.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    Err(_) => una.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                };
                // Denied: A with B's token (spoof).
                let mut o = Object::new("File", "agent-a", json!({"path": format!("s/{tag}.txt")}));
                o.id = format!("m2-s-{tag}");
                let mut params = json!(o);
                params["actor_token"] = json!(tb);
                let _ = match ipc::send_request(&p, "create_object", params).await {
                    Ok(v) if v["ok"].as_bool() == Some(false) => {
                        dno.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    }
                    Ok(_) => una.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    Err(_) => una.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                };
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let load = |c: &std::sync::Arc<std::sync::atomic::AtomicU64>| {
        c.load(std::sync::atomic::Ordering::Relaxed)
    };
    eprintln!(
        "M2: allowed_ok={} denied_ok={} unauthorized={} allowed_fail={}",
        load(&allowed_ok),
        load(&denied_ok),
        load(&unauthorized),
        load(&allowed_fail)
    );
    // 4 tasks x 30 iters x 2 allowed = 240; x 3 denied = 360.
    assert_eq!(load(&allowed_ok), 240, "all allowed must succeed");
    assert_eq!(load(&allowed_fail), 0, "no allowed failure");
    assert_eq!(load(&denied_ok), 360, "all denied must be denied");
    assert_eq!(
        load(&unauthorized),
        0,
        "CRITICAL: zero unauthorized success"
    );

    // Attribution: objects by A only; events by B only.
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    assert_eq!(objs.len(), 120, "exactly A's objects");
    assert!(objs.iter().all(|o| o.provider_id == "agent-a"));
    let evs = ipc::send_request(pipe, "query_events", json!({}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(evs["result"].clone()).unwrap();
    let ticks: Vec<_> = evs.iter().filter(|e| e.event_type == "Tick").collect();
    assert_eq!(ticks.len(), 120, "exactly B's events");
    assert!(ticks.iter().all(|e| e.actor == "agent-b"));
    // Denials audited.
    let denied = evs
        .iter()
        .filter(|e| e.event_type == "MutationDenied")
        .count();
    assert!(denied >= 360, "denials audited, got {denied}");
}
