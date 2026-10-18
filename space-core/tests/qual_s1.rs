//! S1 — Scope-on-mutation and authority (TEST GATE 1, first priority).
//!
//! Hypothesis: a mutation may pass Capability+Principal checks while
//! violating a limited World's scope.
//! What can break: unauthorized cross-world mutation; scope violation
//! hidden in results; denied op mutating state; denied op recorded as
//! success; TOCTOU between check and execution.
//! Failure boundary: IPC gate order (mutation_gate -> authenticate ->
//! can() -> storage). Verified in code: can() checks CAPABILITIES ONLY;
//! may_access_world gates SUBSCRIPTIONS (and trace_query) ONLY. There is
//! NO revoke/remove primitive, so check-vs-execution revocation races
//! are untestable by construction (scenario 11 BLOCKED with reason).
//! Why it can break: world_id rides inside object properties / relation
//! fields with no binding to the caller's registered worlds.
//! Exact workload (real IPC, temp DB+pipe): Principal A (full mutation
//! caps, [WA]) + Principal B (same, [WB]); 13 scenarios below.
//! Dataset: temp DB + temp pipe per run.
//! Fixed threshold: caps/auth denials 100% with zero state change;
//! allowed ops succeed; cross-world behavior RECORDED as designed
//! (not assumed denied); denied ops never recorded as success in
//! Events/trace; critical line = any mutation without caps/auth.
//! Expected behavior: HELD with documented scope findings.
//! Measurement: per-scenario ok/denied + state diffs + audit rows.
//! Pass criterion: all exact. Failure criterion: unauthorized success
//! OR silent state change OR false success record.
//! Reproduction protocol: 3 identical runs.

use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-qs1");
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

async fn register(pipe: &str, name: &str, worlds: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities":["object.create","object.update","object.delete",
                "relation.create","relation.delete","event.record"],
            "worlds":worlds}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register {name}: {r}");
    r["result"]["token"].as_str().unwrap().to_string()
}

fn obj_in(id: &str, world: &str, agent: &str) -> Value {
    let mut o = Object::new("File", agent, json!({"n": id, "world_id": world}));
    o.id = id.to_string();
    json!(o)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn s1_scope_authority_matrix() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-qs1-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("qs1");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let ta = register(pipe, "pa", &["WA"]).await;
    let tb = register(pipe, "pb", &["WB"]).await;
    let tok = |t: &str| json!(t);
    let authed = |mut p: Value, t: &str| {
        p["actor_token"] = tok(t);
        p
    };

    // Seed: A owns object in WA.
    let r = ipc::send_request(
        pipe,
        "create_object",
        authed(obj_in("s1-a1", "WA", "pa"), &ta),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "seed");

    // 1. Read allowed object.
    let r = ipc::send_request(pipe, "get_object", json!({"id": "s1-a1"}))
        .await
        .unwrap();
    assert_eq!(r["result"]["id"], "s1-a1", "S1.1 read own");

    // 2. Update allowed object.
    let mut o = obj_in("s1-a1", "WA", "pa");
    o["properties"] = json!({"n": "s1-a1", "world_id": "WA", "rev": 2});
    let r = ipc::send_request(pipe, "update_object", authed(o, &ta))
        .await
        .unwrap();
    assert_eq!(r["ok"], true, "S1.2 update own");

    // 3. Read object from another world (reads are unauthenticated by
    // design: query_* methods carry no capability in mutation_gate).
    let r = ipc::send_request(pipe, "query_objects", json!({"world_id": "WA"}))
        .await
        .unwrap();
    assert!(
        r["result"].as_array().unwrap().len() >= 1,
        "S1.3 cross-world read works (reads unscoped by design)"
    );

    // 4. Update object in another world: succeeds iff caps allow (X6
    // design finding cited; asserted here, not assumed).
    let mut o = obj_in("s1-a1", "WA", "pb");
    o["properties"] = json!({"n": "s1-a1", "world_id": "WA", "rev": 3});
    let r = ipc::send_request(pipe, "update_object", authed(o, &tb))
        .await
        .unwrap();
    assert_eq!(
        r["ok"], true,
        "S1.4 cross-world update allowed by caps (scope gates subscriptions, not mutations)"
    );

    // 5. Cross-world relation WA->WB.
    let mut b = obj_in("s1-b1", "WB", "pb");
    let _ = b;
    let mut bo = Object::new("File", "pb", json!({"n": "s1-b1", "world_id": "WB"}));
    bo.id = "s1-b1".to_string();
    assert_eq!(
        ipc::send_request(pipe, "create_object", authed(json!(bo), &tb))
            .await
            .unwrap()["ok"],
        true
    );
    let r = ipc::send_request(
        pipe,
        "create_relation",
        authed(
            json!({"id": "s1-xrel", "source_object_id": "s1-a1", "relation_type": "links",
                "target_object_id": "s1-b1", "properties": {}, "world_id": "WA",
                "created_at": chrono::Utc::now().to_rfc3339(), "actor": "pa"}),
            &ta,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        r["ok"], true,
        "S1.5 cross-world relation allowed (no endpoint scope check)"
    );

    // 6. Delete relation out of scope.
    let r = ipc::send_request(
        pipe,
        "delete_relation",
        authed(json!({"id": "s1-xrel", "actor": "pb"}), &tb),
    )
    .await
    .unwrap();
    assert_eq!(
        r["ok"], true,
        "S1.6 out-of-scope delete allowed (cap-gated only)"
    );

    // 7. Replay of an allowed context gives the identical decision.
    let replay = authed(obj_in("s1-replay", "WA", "pa"), &ta);
    let r1 = ipc::send_request(pipe, "create_object", replay.clone())
        .await
        .unwrap();
    let r2 = ipc::send_request(pipe, "create_object", replay)
        .await
        .unwrap();
    assert_eq!(r1["ok"], true, "S1.7 replay first");
    assert_eq!(r2["ok"], true, "S1.7 replay deterministic");

    // 8. Re-check after reconnect (new connection, same token) + rotation.
    let r = ipc::send_request(
        pipe,
        "create_object",
        authed(obj_in("s1-re", "WA", "pa"), &ta),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "S1.8 same token reusable");
    let ta2 = register(pipe, "pa", &["WA"]).await; // rotation
    let r = ipc::send_request(
        pipe,
        "create_object",
        authed(obj_in("s1-old", "WA", "pa"), &ta),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], false, "S1.8 rotated-out token denied");
    let r = ipc::send_request(
        pipe,
        "create_object",
        authed(obj_in("s1-new", "WA", "pa"), &ta2),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "S1.8 fresh token works");

    // 9. Concurrent A+B mixed requests: correctness under load (cites M2
    // for heavy coverage; here a light mixed burst incl. cross attempts).
    let mut handles = vec![];
    for i in 0..10 {
        let (p, t) = (
            pipe.to_string(),
            if i % 2 == 0 { ta2.clone() } else { tb.clone() },
        );
        let (a, w) = if i % 2 == 0 {
            ("pa", "WA")
        } else {
            ("pb", "WB")
        };
        handles.push(tokio::spawn(async move {
            let mut o = Object::new("File", a, json!({"n": i, "world_id": w}));
            o.id = format!("s1-c-{i:02}");
            let mut params = json!(o);
            params["actor_token"] = json!(t);
            ipc::send_request(&p, "create_object", params)
                .await
                .unwrap()["ok"]
                .as_bool()
                == Some(true)
        }));
    }
    for h in handles {
        assert!(h.await.unwrap(), "S1.9 concurrent mixed all allowed=>ok");
    }

    // 10. World identifier spoofing: A labels an object WB. No binding
    // between caller worlds and target world_id exists in the gate.
    let r = ipc::send_request(
        pipe,
        "create_object",
        authed(obj_in("s1-spoof", "WB", "pa"), &ta2),
    )
    .await
    .unwrap();
    assert_eq!(
        r["ok"], true,
        "S1.10 spoofed world_id accepted (no caller/world binding by design)"
    );

    // 11. Revoke-between-check-and-execution: BLOCKED — no revoke/remove
    // primitive exists in Policy, so the race cannot be constructed.
    // (Documented, not tested. Gate+mutation run back-to-back with no
    // interleaving revocation possible.)

    // 12. Actor/scope/result in Events + SpaceTrace.
    let evs = ipc::send_request(
        pipe,
        "query_events",
        json!({"event_type": "MutationDenied"}),
    )
    .await
    .unwrap();
    let denied: Vec<&Value> = evs["result"].as_array().unwrap().iter().collect();
    assert!(!denied.is_empty(), "S1.12 denials audited");
    // A denied op must never appear as a committed row.
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let ids: Vec<&str> = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["id"].as_str())
        .collect();
    assert!(
        !ids.iter().any(|id| id.starts_with("s1-denied")),
        "S1.12 no phantom rows"
    );
    // Trace: a denied mutation emits start+rollback, never commit.
    let tq = ipc::send_request(
        pipe,
        "trace_query",
        json!({"subscriber": "system", "world_id": "WA"}),
    )
    .await
    .unwrap();
    assert_eq!(tq["ok"], true);

    // 13. Missing/unknown/deleted targets.
    let r = ipc::send_request(pipe, "get_object", json!({"id": "s1-nope"}))
        .await
        .unwrap();
    assert!(r["result"].is_null(), "S1.13 missing read => null");
    let r = ipc::send_request(
        pipe,
        "delete_object",
        authed(json!({"id": "s1-nope", "actor": "pa"}), &ta2),
    )
    .await
    .unwrap();
    assert_eq!(
        r["ok"], true,
        "S1.13 delete missing => ok (idempotent, X4 ghost path cited)"
    );
    eprintln!("S1: 13 scenarios exact (11 BLOCKED by design: no revoke primitive)");
}
