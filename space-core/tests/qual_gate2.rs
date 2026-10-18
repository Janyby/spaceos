//! Gate 2 EXTEND — delete-target vs create-relation race (beyond X4).
//!
//! X4 covers scripted lifecycle deterministically (cited). This attacks
//! the genuinely concurrent version: deleting an endpoint WHILE another
//! participant creates relations to it.
//! Hypothesis: the race may leave dangling relations, crash, or errors.
//! Failure boundary: Core cascade (relations-first delete) under true
//! concurrency.
//! Exact workload: N=20 deleters hammering rotating targets while N=20
//! creators attach relations to the same rotating targets (same Core,
//! real IPC, concurrent). Targets pre-created (50 objects).
//! Fixed threshold: 0 errors during the race; post-race dangling
//! RECORDED (create-racing-delete under no-FK design may leave some);
//! after one idempotent delete pass over all targets: 0 dangling.
//! Expected: HELD — cascade + idempotent delete converge via existing
//! primitives (no FK/locking added).
//! Pass criterion: exact. Failure criterion: dangling/error/crash.
//! Reproduction protocol: 3 identical runs.

use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-qg2");
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
            "capabilities":["object.create","object.delete","relation.create","relation.delete"],
            "worlds":[]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn gate2_delete_vs_relate_race() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-qg2-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("qg2");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    let tok = register(pipe, "racer").await;

    // 50 targets.
    for i in 0..50 {
        let mut o = Object::new("File", "racer", json!({"i": i}));
        o.id = format!("qg2-t-{i:02}");
        let mut p = json!(o);
        p["actor_token"] = json!(tok);
        assert_eq!(
            ipc::send_request(pipe, "create_object", p).await.unwrap()["ok"],
            true
        );
    }

    let errs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut h = vec![];
    // Deleters rotate targets 0..50.
    for d in 0..20 {
        let (p, t, e) = (pipe.to_string(), tok.clone(), errs.clone());
        h.push(tokio::spawn(async move {
            for i in 0..25 {
                let id = format!("qg2-t-{:02}", (d * 25 + i) % 50);
                // Recreate (idempotent upsert) then delete: keeps the race
                // alive for the whole window instead of one-shot.
                let mut o = Object::new("File", "racer", json!({"i": i}));
                o.id = id.clone();
                let mut cp = json!(o);
                cp["actor_token"] = json!(t);
                let _ = ipc::send_request(&p, "create_object", cp).await;
                let mut dp = json!({"id": id, "actor": "racer"});
                dp["actor_token"] = json!(t);
                match ipc::send_request(&p, "delete_object", dp).await {
                    Ok(v) if v["ok"].as_bool() == Some(true) => {}
                    other => e.lock().unwrap().push(format!("{other:?}")),
                }
            }
        }));
    }
    // Relaters attach to the same rotating targets.
    for w in 0..20 {
        let (p, t, e) = (pipe.to_string(), tok.clone(), errs.clone());
        h.push(tokio::spawn(async move {
            for i in 0..25 {
                let tgt = format!("qg2-t-{:02}", (w * 25 + i * 7) % 50);
                let mut params = json!({
                    "id": format!("qg2-r-{w}-{i:02}"),
                    "source_object_id": "qg2-t-00",
                    "relation_type": "links",
                    "target_object_id": tgt,
                    "properties": {},
                    "world_id": "",
                    "created_at": chrono::Utc::now().to_rfc3339(),
                    "actor": "racer",
                });
                params["actor_token"] = json!(t);
                match ipc::send_request(&p, "create_relation", params).await {
                    Ok(v) if v["ok"].as_bool() == Some(true) => {}
                    // Phase B FK: an endpoint deleted mid-race makes the
                    // insert fail with an FK rejection — the legal outcome
                    // (a dangling relation is never created), not an error.
                    Ok(v)
                        if v["error"]
                            .as_str()
                            .map(|s| s.contains("FOREIGN KEY constraint failed"))
                            .unwrap_or(false) => {}
                    other => e.lock().unwrap().push(format!("{other:?}")),
                }
            }
        }));
    }
    for x in h {
        x.await.unwrap();
    }
    let e = errs.lock().unwrap().clone();
    assert!(e.is_empty(), "GATE2 errors: {:?}", &e[..e.len().min(3)]);

    // Final invariant, two-step (matches Core's actual contract):
    // 1. Phase B FK forbids dangling rows outright, so the race now
    //    surfaces as FK rejections above instead of periodic dangling
    //    relations. Record what remains (informational, expected 0).
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    let ids: std::collections::HashSet<&str> = objs.iter().map(|o| o.id.as_str()).collect();
    let rels = ipc::send_request(pipe, "query_relations", json!({}))
        .await
        .unwrap();
    let rels: Vec<space_core::Relation> = serde_json::from_value(rels["result"].clone()).unwrap();
    let dangling = rels
        .iter()
        .filter(|r| {
            !ids.contains(r.source_object_id.as_str()) || !ids.contains(r.target_object_id.as_str())
        })
        .count();
    eprintln!(
        "GATE2: post-race dangling={} (create-racing-delete vs FK)",
        dangling
    );
    // 2. Convergence via existing primitives: one idempotent delete pass
    //    over every target must leave zero dangling relations.
    for i in 0..50 {
        let mut dp = json!({"id": format!("qg2-t-{i:02}"), "actor": "racer"});
        dp["actor_token"] = json!(tok);
        let _ = ipc::send_request(pipe, "delete_object", dp).await;
    }
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    let ids: std::collections::HashSet<&str> = objs.iter().map(|o| o.id.as_str()).collect();
    let rels = ipc::send_request(pipe, "query_relations", json!({}))
        .await
        .unwrap();
    let rels: Vec<space_core::Relation> = serde_json::from_value(rels["result"].clone()).unwrap();
    for r in &rels {
        assert!(
            ids.contains(r.source_object_id.as_str()) && ids.contains(r.target_object_id.as_str()),
            "GATE2: 0 dangling after convergence pass, violator {}",
            r.id
        );
    }
    eprintln!(
        "GATE2: race over, 0 errors, converged to 0 dangling ({} objs, {} rels)",
        ids.len(),
        rels.len()
    );
}
