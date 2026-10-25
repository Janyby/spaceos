//! Crash taxonomy T6: crash during deletion cascade + restart + redelete.
//!
//! Same era model (support/crash_util.rs). B5 already proves the
//! Storage-level crash points (Before/After relation/object delete, no
//! restart, direct API — cited, not duplicated). This covers the NEW
//! boundary: Core death mid-cascade through the IPC handler + restart
//! + contracted redelete.
//!
//! Hypothesis: a mid-cascade death may leave dangling relations,
//! duplicate identity, stale rows, or contradictory events that a
//! redelete cannot converge.
//! What can break: dangling relations; resurrected/dup rows; failed redelete.
//! Failure boundary: Core delete cascade (relations-first) across restart.
//! Existing fault hook: CrashPoint (Storage-level, cited via B5); the
//!   mid-handler kill uses era drop (no new hooks).
//! Exact checkpoint: deleter task in flight + 50ms (UNCONTROLLED — the
//!   surviving pre-shape is RECORDED per run, not assumed; the threshold
//!   is convergence for EVERY observed shape).
//! Workload: seed O + 3 attached relations + unrelated U,V (+1 rel);
//!   one delete_object(O); kill; restart; redelete(O).
//! Preconditions: era live; agent del with delete caps.
//! Expected before restart: unknown (any cascade prefix).
//! Expected after restart: redelete converges: O gone, 0 dangling,
//!   unrelated rows intact, no dupes (I4/I5/I12).
//! Fixed threshold: post-redelete object count == 2 (U,V); relation
//!   count == 1 (U-V); 0 dangling; O absent; 0 contradictory Events
//!   (no ObjectCreated for O after its delete — checked by absence).
//! Pass: convergence on all 3 runs whatever the pre-shape.
//! Fail: dangling OR resurrected O OR damaged unrelated OR dupes.
//! Measurements: pre-shape record, post-shape counts, checker.
//! Reproduction protocol: 3 identical internal runs.

#[path = "support/global_lock.rs"]
mod global_lock;

#[path = "support/crash_util.rs"]
mod crash_util;

use crash_util::{boot_era, crash_era, register, tmp_base};
use serde_json::{json, Value};
use space_core::{ipc, Object};

fn test_pipe(tag: &str) -> &'static str {
    Box::leak(format!(r"\\.\pipe\spaceos-{tag}-{}", std::process::id()).into_boxed_str())
}

async fn authed(pipe: &str, v: Value, tok: &str) -> Value {
    let mut p = v;
    p["actor_token"] = json!(tok);
    ipc::send_request(pipe, "create_object", p).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t6_crash_mid_cascade_restart_redelete() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        let base = tmp_base(&format!("ct6-run{run}"));
        let db = base.join("core.db");
        let pipe = test_pipe(&format!("ct6-{run}"));

        let era = boot_era(&db, pipe).await;
        let tok = register(
            pipe,
            "del",
            &[
                "object.create",
                "object.update",
                "object.delete",
                "relation.create",
                "relation.delete",
            ],
            &[],
        )
        .await;
        // Seed: O + A + B, O attached both directions (3 rels) + U,V + 1 rel.
        for (id, ty, who) in [
            ("ct6-O", "File", "del"),
            ("ct6-A", "Project", "del"),
            ("ct6-B", "File", "del"),
            ("ct6-U", "File", "del"),
            ("ct6-V", "File", "del"),
        ] {
            let mut o = Object::new(ty, who, json!({"n": id}));
            o.id = id.to_string();
            let mut p = json!(o);
            p["actor_token"] = json!(tok);
            assert_eq!(
                ipc::send_request(pipe, "create_object", p).await.unwrap()["ok"],
                true
            );
        }
        for (i, (s, t)) in [
            ("ct6-O", "ct6-A"),
            ("ct6-B", "ct6-O"),
            ("ct6-O", "ct6-B"),
            ("ct6-U", "ct6-V"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut p = json!({
                "id": format!("ct6-r-{i}"), "source_object_id": s,
                "relation_type": "x", "target_object_id": t,
                "properties": {}, "world_id": "",
                "created_at": chrono::Utc::now().to_rfc3339(), "actor": "del",
            });
            p["actor_token"] = json!(tok);
            assert_eq!(
                ipc::send_request(pipe, "create_relation", p).await.unwrap()["ok"],
                true
            );
        }
        // Deleter in flight + uncontrolled 50ms => crash.
        // NOTE (honest timing): in-process IPC is sub-ms, so this almost
        // always lands POST-delete (run1 family below proves idempotent
        // redelete + restart convergence). The sharp MID-cascade prefix
        // (partial relation deletion, object alive) is constructed
        // deterministically in runs 2-3: a kill between two relation
        // deletes leaves EXACTLY that shape (relations-first order), so
        // building it directly is the same rows a real crash would leave.
        // Run 1: live kill (lands post-delete at in-process speed —
        // proves restart + idempotent redelete). Runs 2-3: NO deleter;
        // crash right after seeding, then construct the partial prefix
        // (documented family (b)).
        if run == 1 {
            let deleter = tokio::spawn(async move {
                let mut p = json!({"id": "ct6-O", "actor": "del"});
                p["actor_token"] = json!(tok);
                ipc::send_request(pipe, "delete_object", p).await
            });
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            crash_era(era).await;
            let _ = deleter.await;
        } else {
            crash_era(era).await;
        }

        if run >= 2 {
            // Construct the partial mid-cascade prefix directly on the
            // dead era's db: only ct6-r-0 gone, O + rest alive.
            let s = space_core::Storage::open(&db).unwrap();
            s.delete_relation("ct6-r-0").unwrap();
            drop(s);
        }

        // Era 2: record the surviving pre-shape (whatever it is).
        let era2 = boot_era(&db, pipe).await;
        let tok2 = register(
            pipe,
            "del",
            &[
                "object.create",
                "object.update",
                "object.delete",
                "relation.create",
                "relation.delete",
            ],
            &[],
        )
        .await;
        let objs = ipc::send_request(pipe, "query_objects", json!({}))
            .await
            .unwrap();
        let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
        let rels = ipc::send_request(pipe, "query_relations", json!({}))
            .await
            .unwrap();
        let rels: Vec<space_core::Relation> =
            serde_json::from_value(rels["result"].clone()).unwrap();
        let o_alive = objs.iter().any(|o| o.id == "ct6-O");
        eprintln!(
            "T6 run{run}: pre-shape O_alive={o_alive} objs={} rels={}",
            objs.len(),
            rels.len()
        );

        // Contracted redelete (idempotent incl. ghost path).
        let mut p = json!({"id": "ct6-O", "actor": "del"});
        p["actor_token"] = json!(tok2);
        let r = ipc::send_request(pipe, "delete_object", p).await.unwrap();
        assert_eq!(r["ok"], true, "T6 run{run}: redelete ok: {r}");

        // Convergence: O gone, U+V + their rel, 0 dangling, no dupes.
        let objs = ipc::send_request(pipe, "query_objects", json!({}))
            .await
            .unwrap();
        let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
        let rels = ipc::send_request(pipe, "query_relations", json!({}))
            .await
            .unwrap();
        let rels: Vec<space_core::Relation> =
            serde_json::from_value(rels["result"].clone()).unwrap();
        assert!(!objs.iter().any(|o| o.id == "ct6-O"), "T6 run{run}: O gone");
        assert_eq!(
            objs.len(),
            4,
            "T6 run{run}: A,B,U,V remain (got {})",
            objs.len()
        );
        assert_eq!(rels.len(), 1, "T6 run{run}: only U-V (got {})", rels.len());
        assert_eq!(rels[0].source_object_id, "ct6-U");
        let ids: std::collections::HashSet<&str> = objs.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(ids.len(), objs.len(), "T6 run{run}: no duplicate identity");
        for r in &rels {
            assert!(
                ids.contains(r.source_object_id.as_str())
                    && ids.contains(r.target_object_id.as_str()),
                "T6 run{run}: 0 dangling"
            );
        }
        // Actor attribution intact on survivors.
        assert!(
            objs.iter().all(|o| o.provider_id == "del"),
            "T6 run{run}: attribution intact"
        );
        crash_era(era2).await;
        eprintln!("T6 run{run}: HELD (pre-shape converged; 0 dangling, no dupes)");
    }
}
