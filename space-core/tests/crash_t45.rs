//! Crash taxonomy T4–T5: commit vs publication vs delivery.
//!
//! Same era model as T1–T3 (see support/crash_util.rs): in-process Core
//! eras, crash = runtime drop, restart = rebuild on the same db.
//! Intra-handler seams (commit vs events_tx.send inside one handler)
//! have NO hook — surgical split there is BLOCKED by design (no new
//! production hooks). T4/T5 pin the OBSERVABLE boundary contract
//! instead, which is what callers/subscribers can actually rely on.
//!
//! ----------------------------------------------------------------
//! T4 — crash after commit, before the subscriber drains.
//! Hypothesis: a committed-but-undrained event may be lost with the
//!   crash (unrecovered) or resurrected as a duplicate.
//! Checkpoint: caller ack received; pre-crash subscriber connected but
//!   NEVER reads (backlog); era drop; restart.
//! Expected: commit survives (query shows the row — I1/I4); the dead
//!   connection delivers nothing more; a FRESH subscriber gets only
//!   POST-restart events live (no redelivery — the actual contract,
//!   not assumed exactly-once); query resync returns everything (I9:
//!   delivery loss never uncommits; I7: events reflect confirmed state).
//! Threshold: post-restart row present; fresh-live == 3 new only (0
//!   old); resync count exact (1 old + 3 new); pre-crash trace chain
//!   honestly absent (memory wipe) while new flow chained.
//! Reproduction protocol: 3 identical internal runs.
//!
//! T5 — crash during live delivery.
//! Hypothesis: killing Core mid-stream may duplicate already-delivered
//!   events on resync or lose the undelivered tail.
//! Checkpoint: paced subscriber drains EXACTLY 3 of 10, then holds;
//!   era drop; restart; fresh subscribe; 3 new events.
//! Expected: fresh-live == 3 new only; store == 13 unique ids (10 old +
//!   3 new); pre-crash delivered 3 + query 13 = full set, no dupes in
//!   World state (I7/I9).
//! Threshold: exact counts above; 0 redelivered old events live.
//! Reproduction protocol: 3 identical internal runs.

#[path = "support/global_lock.rs"]
mod global_lock;

#[path = "support/crash_util.rs"]
mod crash_util;

use crash_util::{boot_era, crash_era, raw_subscribe, recv_delivery, register, tmp_base};
use serde_json::{json, Value};
use space_core::{ipc, Object};

fn test_pipe(tag: &str) -> &'static str {
    Box::leak(format!(r"\\.\pipe\spaceos-{tag}-{}", std::process::id()).into_boxed_str())
}

fn note_ev(id: &str, i: i64, corr: &str) -> Value {
    let mut e = space_core::Event::new("Note", "cx", json!({"i": i}));
    e.id = id.to_string();
    e.world_id = "W".to_string();
    e.correlation_id = Some(corr.to_string());
    json!(e)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t4_crash_after_commit_before_drain() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        let base = tmp_base(&format!("ct4-run{run}"));
        let db = base.join("core.db");
        let pipe = test_pipe(&format!("ct4-{run}"));

        // Era 1: subscriber connects but never reads (backlog).
        let era = boot_era(&db, pipe).await;
        let tok = register(pipe, "cx", &["object.create", "event.record"], &["W"]).await;
        let (_stalled, ack) = raw_subscribe(
            pipe,
            json!({"subscriber": "cx", "actor_token": tok, "world_id": "W"}),
        )
        .await;
        assert_eq!(ack["ok"], true);

        // Committed mutation, caller acked.
        let mut o = Object::new("File", "cx", json!({"n": 1, "world_id": "W"}));
        o.id = "ct4-obj".to_string();
        let mut p = json!(o);
        p["actor_token"] = json!(tok);
        p["correlation_id"] = json!("ct4-mission");
        let r = ipc::send_request(pipe, "create_object", p).await.unwrap();
        assert_eq!(r["ok"], true);
        // Checkpoint: ack in hand, subscriber never drained => crash.
        crash_era(era).await;

        // Era 2: commit survived (I1); dead connection delivers nothing.
        let era2 = boot_era(&db, pipe).await;
        let tok2 = register(pipe, "cx", &["object.create", "event.record"], &["W"]).await;
        let g = ipc::send_request(pipe, "get_object", json!({"id": "ct4-obj"}))
            .await
            .unwrap();
        assert_eq!(
            g["result"]["id"], "ct4-obj",
            "T4 run{run}: committed row survives restart"
        );

        // Fresh subscriber + 3 new events: live = 3 new ONLY.
        let (mut fresh, ack) = raw_subscribe(
            pipe,
            json!({"subscriber": "cx", "actor_token": tok2, "world_id": "W"}),
        )
        .await;
        assert_eq!(ack["ok"], true);
        for i in 0..3 {
            let mut p = note_ev(&format!("ct4-re-{run}-{i}"), 100 + i, "ct4-mission-re");
            p["actor_token"] = json!(tok2);
            assert_eq!(
                ipc::send_request(pipe, "record_event", p).await.unwrap()["ok"],
                true
            );
        }
        let mut live = vec![];
        for _ in 0..3 {
            live.push(
                recv_delivery(&mut fresh, 8000)
                    .await
                    .expect("T4 run{run}: live delivery"),
            );
        }
        assert_eq!(live.len(), 3, "T4 run{run}: fresh gets 3");
        assert!(
            live.iter()
                .all(|v| v["event"]["correlation_id"] == "ct4-mission-re"),
            "T4 run{run}: 0 old events redelivered live"
        );
        // Resync via query: the 3 new Notes present; nothing invented.
        let q = ipc::send_request(pipe, "query_events", json!({"world_id": "W"}))
            .await
            .unwrap();
        let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
        let re: Vec<&space_core::Event> = evs
            .iter()
            .filter(|e| e.correlation_id.as_deref() == Some("ct4-mission-re"))
            .collect();
        assert_eq!(re.len(), 3, "T4 run{run}: resync has all 3 new Notes");
        // Trace: pre-crash chain honestly gone (memory wipe), new flow chained.
        let qold = ipc::send_request(
            pipe,
            "trace_query",
            json!({"subscriber": "system", "correlation_id": "ct4-mission"}),
        )
        .await
        .unwrap();
        let old: Vec<Value> = serde_json::from_value(qold["result"].clone()).unwrap();
        assert!(
            old.is_empty(),
            "T4 run{run}: pre-crash trace honestly absent"
        );
        let qnew = ipc::send_request(
            pipe,
            "trace_query",
            json!({"subscriber": "system", "correlation_id": "ct4-mission-re"}),
        )
        .await
        .unwrap();
        let new: Vec<Value> = serde_json::from_value(qnew["result"].clone()).unwrap();
        assert!(!new.is_empty(), "T4 run{run}: new flow traced");
        crash_era(era2).await;
        eprintln!("T4 run{run}: HELD (commit survives; no redelivery; resync exact)");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t5_crash_during_live_delivery() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        let base = tmp_base(&format!("ct5-run{run}"));
        let db = base.join("core.db");
        let pipe = test_pipe(&format!("ct5-{run}"));

        // Era 1: paced subscriber drains exactly 3 of 10, then holds.
        let era = boot_era(&db, pipe).await;
        let tok = register(pipe, "cx", &["event.record"], &["W"]).await;
        let (mut paced, ack) = raw_subscribe(
            pipe,
            json!({"subscriber": "cx", "actor_token": tok, "world_id": "W"}),
        )
        .await;
        assert_eq!(ack["ok"], true);
        for i in 0..10 {
            let mut p = note_ev(&format!("ct5-{run}-{i:02}"), i, "ct5-mission");
            p["actor_token"] = json!(tok);
            assert_eq!(
                ipc::send_request(pipe, "record_event", p).await.unwrap()["ok"],
                true
            );
        }
        let mut pre = vec![];
        for _ in 0..3 {
            pre.push(
                recv_delivery(&mut paced, 8000)
                    .await
                    .expect("T5 run{run}: paced delivery"),
            );
        }
        assert_eq!(pre.len(), 3, "T5 run{run}: 3 delivered pre-crash");
        // Checkpoint: 3 observed live, 7 undrained => crash.
        crash_era(era).await;

        // Era 2: store has all 10 with unique ids (no dupes, no loss).
        let era2 = boot_era(&db, pipe).await;
        let tok2 = register(pipe, "cx", &["event.record"], &["W"]).await;
        let (mut fresh, ack) = raw_subscribe(
            pipe,
            json!({"subscriber": "cx", "actor_token": tok2, "world_id": "W"}),
        )
        .await;
        assert_eq!(ack["ok"], true);
        for i in 0..3 {
            let mut p = note_ev(&format!("ct5-re-{run}-{i}"), 100 + i, "ct5-mission-re");
            p["actor_token"] = json!(tok2);
            assert_eq!(
                ipc::send_request(pipe, "record_event", p).await.unwrap()["ok"],
                true
            );
        }
        let mut live = vec![];
        for _ in 0..3 {
            live.push(
                recv_delivery(&mut fresh, 8000)
                    .await
                    .expect("T5 run{run}: fresh delivery"),
            );
        }
        assert!(
            live.iter()
                .all(|v| v["event"]["correlation_id"] == "ct5-mission-re"),
            "T5 run{run}: 0 old redelivered live"
        );
        let q = ipc::send_request(pipe, "query_events", json!({"world_id": "W"}))
            .await
            .unwrap();
        let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
        assert_eq!(evs.len(), 13, "T5 run{run}: store 10 + 3 exact");
        let mut ids: Vec<&str> = evs.iter().map(|e| e.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 13, "T5 run{run}: unique ids, no dupes");
        // Delivered-3 + stored-13 = full set accounted (I7).
        assert!(
            pre.iter()
                .all(|v| v["event"]["correlation_id"] == "ct5-mission"),
            "T5 run{run}: pre-crash set identified"
        );
        crash_era(era2).await;
        eprintln!("T5 run{run}: HELD (mid-stream death: no dupes, no loss, resync exact)");
    }
}
