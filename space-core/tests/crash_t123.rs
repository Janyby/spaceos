//! Crash taxonomy T1–T3: Core stop around provider acceptance.
//!
//! Crash model: in-process eras (see support/crash_util.rs): boot =
//! own runtime + CoreState on a test pipe + SQLite file; crash = drop
//! the runtime (all tasks die, no cleanup — observable kill -9;
//! no Storage lock is held across the provider-forward await, read in
//! ipc.rs handle_action); restart = rebuild on the SAME db file
//! (SQLite persists; Policy/subs/trace wipe — the X5/X13 contract).
//! Real-process mid-flight stop is BLOCKED (no existing hook can stop
//! Core mid-forward; new production hooks forbidden): this minimal TEST
//! mechanism is the justified alternative, production untouched.
//! Global PROVIDER_PIPE stub => global IPC lock held by every test.
//!
//! ----------------------------------------------------------------
//! T1 — crash BEFORE provider acceptance.
//! Hypothesis: Core may record the Action as running/successful though
//!   the provider never accepted it.
//! What can break: phantom success; false external-effect event.
//! Failure boundary: Core action lifecycle (Requested vs completion).
//! Existing fault hook: none can stop mid-forward (BLOCKED for real
//!   processes); test mechanism = gated stub DOWN + timed era drop.
//! Exact checkpoint: 200ms into the ~1.2s dial-retry window (margin 6x;
//!   a >1s stall mistimes the injection and fails LOUDLY on Failed×0).
//! Workload: 1 request_action (create, content v1), agent cx.
//! Preconditions: stub down; era live; cx registered.
//! Expected before restart: history(aid) == [Requested]; no completion;
//!   caller transport error (unknown, never success).
//! Expected after restart: same + old token dead; retry same id =>
//!   Completed with exactly 1 execution.
//! Fixed threshold: Requested×1 pre-retry; Completed/Failed×0 pre-retry;
//!   executed==[aid] once; file bytes == v1; post-retry Requested×2 +
//!   Completed×1 + Failed×0.
//! Pass: all exact. Fail: any phantom success OR missing convergence.
//! Measurements: history vectors pre/post, executed log, file bytes,
//!   caller outcome, token probes.
//! Reproduction protocol: 3 identical internal runs.
//!
//! T2 — crash AFTER provider acceptance, result unknown (orphan).
//! Hypothesis: an accepted attempt whose ack never reaches Core may be
//!   lost (never retried => effect missing) or double-executed without
//!   an honest record.
//! Checkpoint: stub ACCEPTED (HoldPre, pre-execution) => era drop =>
//!   orphan RELEASE (stub executes with Core dead, ack dies on broken
//!   pipe) => restart => contracted retry.
//! Expected: pre-retry history Requested-only DESPITE a real orphan
//!   effect on disk (I3: Core cannot know); retry converges with the
//!   orphan attempt VISIBLE in history (2 attempts, 1 completion).
//! Threshold: executed==[aid,aid] (orphan + retry); 1 file, bytes==v1;
//!   pre-retry Completed/Failed×0; post-retry Requested×2+Completed×1.
//!
//! T3 — crash AFTER Reality changed, BEFORE Storage commit.
//! Hypothesis: a pre-commit death may fake-rollback Reality (deny the
//!   effect) or fake-confirm it.
//! Checkpoint: stub EXECUTED (ExecHold, ack held) => era drop =>
//!   restart => contracted retry.
//! Expected: post-restart observable IDENTICAL to T2 (file + Requested-
//!   only) although causality differs (effect pre-kill vs post-kill) —
//!   the I3/I13 demonstration: only retry+reconcile converge, never
//!   assumed certainty. Same thresholds as T2, separately registered.
//!
//! Invariants under test: I2 (no false success), I3 (unknown stays
//! unknown), I6 (retry semantics), I13 (ambiguity explicit).

#[path = "support/global_lock.rs"]
mod global_lock;

#[path = "support/crash_util.rs"]
mod crash_util;

use crash_util::{boot_era, crash_era, history_via_db, poll_deadline, register, run_gated_stub};
use crash_util::{tmp_base, StubCtl, StubPhase};
use serde_json::json;
use space_core::ipc;
use std::sync::{Arc, Mutex};

fn test_pipe(tag: &str) -> &'static str {
    Box::leak(format!(r"\\.\pipe\spaceos-{tag}-{}", std::process::id()).into_boxed_str())
}

fn action_req(aid: &str, tok: &str) -> serde_json::Value {
    json!({"id": aid, "action": "create",
        "target": {"path": "t.txt", "content": "v1"},
        "provider_id": "filesystem", "actor": "cx", "actor_token": tok,
        "requested_at": chrono::Utc::now().to_rfc3339()})
}

fn hist(db: &std::path::Path, aid: &str, et: &str) -> usize {
    history_via_db(db, et)
        .iter()
        .filter(|(_, c)| c == aid)
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t1_crash_before_provider_acceptance() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        let base = tmp_base(&format!("ct1-run{run}"));
        let db = base.join("core.db");
        let reality = base.join("reality");
        std::fs::create_dir_all(&reality).unwrap();
        let pipe = test_pipe(&format!("ct1-{run}"));
        let aid = format!("ct1-aid-{run}");

        // Era 1: no stub (provider down).
        let era = boot_era(&db, pipe).await;
        let tok = register(pipe, "cx", &["action.create"], &[]).await;
        let aid_c = aid.clone();
        let caller = tokio::spawn(async move {
            ipc::send_request(pipe, "request_action", action_req(&aid_c, &tok)).await
        });
        // Checkpoint: 200ms into the ~1.2s dial window.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        crash_era(era).await;
        let outcome = caller.await.unwrap();
        assert!(
            outcome.is_err(),
            "T1 run{run}: caller must see transport error (unknown), got {outcome:?}"
        );

        // Pre-restart state: Requested-only, nothing else.
        assert_eq!(
            hist(&db, &aid, "ActionRequested"),
            1,
            "T1 run{run}: exactly 1 Requested"
        );
        assert_eq!(
            hist(&db, &aid, "ActionCompleted"),
            0,
            "T1 run{run}: no phantom completion"
        );
        assert_eq!(
            hist(&db, &aid, "ActionFailed"),
            0,
            "T1 run{run}: no Failed (injection landed in-window)"
        );
        assert!(
            !reality.join(format!("{aid}.txt")).exists(),
            "T1 run{run}: no Reality effect"
        );

        // Era 2 (restart, same db): rebootstrap, old token dead.
        let era2 = boot_era(&db, pipe).await;
        let tok2 = register(pipe, "cx", &["action.create"], &[]).await;
        let ctl = Arc::new(StubCtl {
            phase: Mutex::new(StubPhase::Normal),
            accepted: Mutex::new(vec![]),
            executed: Mutex::new(vec![]),
            reality: reality.clone(),
        });
        let Some(_stub) = run_gated_stub(ctl.clone()).await else {
            panic!("T1 run{run}: provider pipe busy");
        };
        // Contracted retry with the SAME action id.
        let r = ipc::send_request(pipe, "request_action", action_req(&aid, &tok2))
            .await
            .unwrap();
        assert_eq!(
            r["result"]["status"], "Completed",
            "T1 run{run}: retry completes: {r}"
        );
        assert_eq!(
            *ctl.executed.lock().unwrap(),
            vec![aid.clone()],
            "T1 run{run}: exactly 1 execution"
        );
        assert_eq!(
            std::fs::read_to_string(reality.join(format!("{aid}.txt"))).unwrap(),
            "v1",
            "T1 run{run}: Reality bytes"
        );
        assert_eq!(
            hist(&db, &aid, "ActionRequested"),
            2,
            "T1 run{run}: 2 attempts visible"
        );
        assert_eq!(
            hist(&db, &aid, "ActionCompleted"),
            1,
            "T1 run{run}: 1 completion"
        );
        assert_eq!(
            hist(&db, &aid, "ActionFailed"),
            0,
            "T1 run{run}: 0 failures"
        );
        crash_era(era2).await;
        eprintln!("T1 run{run}: HELD (no phantom success; retry converged, 1 execution)");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t2_crash_after_acceptance_orphan() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        let base = tmp_base(&format!("ct2-run{run}"));
        let db = base.join("core.db");
        let reality = base.join("reality");
        std::fs::create_dir_all(&reality).unwrap();
        let pipe = test_pipe(&format!("ct2-{run}"));
        let aid = format!("ct2-aid-{run}");

        let era = boot_era(&db, pipe).await;
        let tok = register(pipe, "cx", &["action.create"], &[]).await;
        let ctl = Arc::new(StubCtl {
            phase: Mutex::new(StubPhase::HoldPre),
            accepted: Mutex::new(vec![]),
            executed: Mutex::new(vec![]),
            reality: reality.clone(),
        });
        let Some(_stub) = run_gated_stub(ctl.clone()).await else {
            panic!("T2 run{run}: provider pipe busy");
        };
        let aid_c = aid.clone();
        let caller = tokio::spawn(async move {
            ipc::send_request(pipe, "request_action", action_req(&aid_c, &tok)).await
        });
        // Checkpoint: provider ACCEPTED (pre-execution) => crash.
        poll_deadline("stub ACCEPTED", 15000, || {
            !ctl.accepted.lock().unwrap().is_empty()
        })
        .await;
        assert_eq!(*ctl.accepted.lock().unwrap(), vec![aid.clone()]);
        crash_era(era).await;
        let outcome = caller.await.unwrap();
        assert!(
            outcome.is_err(),
            "T2 run{run}: caller unknown, got {outcome:?}"
        );
        assert!(
            ctl.executed.lock().unwrap().is_empty(),
            "T2 run{run}: nothing executed pre-release"
        );
        assert_eq!(hist(&db, &aid, "ActionRequested"), 1);
        assert_eq!(hist(&db, &aid, "ActionCompleted"), 0);
        assert_eq!(hist(&db, &aid, "ActionFailed"), 0);

        // Orphan release: the accepted attempt acts with Core dead; its
        // ack dies on the broken pipe (never recorded anywhere in Core).
        *ctl.phase.lock().unwrap() = StubPhase::Normal;
        poll_deadline("orphan EXECUTED", 15000, || {
            !ctl.executed.lock().unwrap().is_empty()
        })
        .await;
        assert!(
            reality.join(format!("{aid}.txt")).exists(),
            "T2 run{run}: orphan effect on disk"
        );

        // Era 2: Core still knows NOTHING of the orphan (I3) — history
        // unchanged despite Reality having changed.
        let era2 = boot_era(&db, pipe).await;
        let tok2 = register(pipe, "cx", &["action.create"], &[]).await;
        assert_eq!(
            hist(&db, &aid, "ActionRequested"),
            1,
            "T2 run{run}: still 1 Requested"
        );
        assert_eq!(
            hist(&db, &aid, "ActionCompleted"),
            0,
            "T2 run{run}: still 0 completions"
        );
        // Contracted retry: executes AGAIN (overwrite, convergent).
        let r = ipc::send_request(pipe, "request_action", action_req(&aid, &tok2))
            .await
            .unwrap();
        assert_eq!(
            r["result"]["status"], "Completed",
            "T2 run{run}: retry completes: {r}"
        );
        assert_eq!(
            *ctl.executed.lock().unwrap(),
            vec![aid.clone(), aid.clone()],
            "T2 run{run}: orphan + retry = 2 executions, honestly logged stub-side"
        );
        assert_eq!(
            std::fs::read_to_string(reality.join(format!("{aid}.txt"))).unwrap(),
            "v1"
        );
        assert_eq!(
            hist(&db, &aid, "ActionRequested"),
            2,
            "T2 run{run}: both attempts visible"
        );
        assert_eq!(hist(&db, &aid, "ActionCompleted"), 1);
        assert_eq!(hist(&db, &aid, "ActionFailed"), 0);
        crash_era(era2).await;
        eprintln!(
            "T2 run{run}: HELD (orphan effect converged; history honest, no certainty invented)"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t3_crash_after_reality_before_commit() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        let base = tmp_base(&format!("ct3-run{run}"));
        let db = base.join("core.db");
        let reality = base.join("reality");
        std::fs::create_dir_all(&reality).unwrap();
        let pipe = test_pipe(&format!("ct3-{run}"));
        let aid = format!("ct3-aid-{run}");

        let era = boot_era(&db, pipe).await;
        let tok = register(pipe, "cx", &["action.create"], &[]).await;
        let ctl = Arc::new(StubCtl {
            phase: Mutex::new(StubPhase::ExecHold),
            accepted: Mutex::new(vec![]),
            executed: Mutex::new(vec![]),
            reality: reality.clone(),
        });
        let Some(_stub) = run_gated_stub(ctl.clone()).await else {
            panic!("T3 run{run}: provider pipe busy");
        };
        let aid_c = aid.clone();
        let caller = tokio::spawn(async move {
            ipc::send_request(pipe, "request_action", action_req(&aid_c, &tok)).await
        });
        // Checkpoint: Reality CHANGED (executed), ack held => crash.
        poll_deadline("stub EXECUTED", 15000, || {
            !ctl.executed.lock().unwrap().is_empty()
        })
        .await;
        crash_era(era).await;
        let outcome = caller.await.unwrap();
        assert!(
            outcome.is_err(),
            "T3 run{run}: caller unknown, got {outcome:?}"
        );

        // Post-crash observable: file + Requested-only — IDENTICAL shape
        // to T2 although causality differs (pre-kill vs post-kill effect).
        assert!(
            reality.join(format!("{aid}.txt")).exists(),
            "T3 run{run}: effect on disk"
        );
        assert_eq!(
            hist(&db, &aid, "ActionRequested"),
            1,
            "T3 run{run}: 1 Requested"
        );
        assert_eq!(
            hist(&db, &aid, "ActionCompleted"),
            0,
            "T3 run{run}: 0 completions"
        );
        assert_eq!(
            hist(&db, &aid, "ActionFailed"),
            0,
            "T3 run{run}: 0 failures"
        );

        // Era 2 + contracted retry: second execution overwrites convergently.
        let era2 = boot_era(&db, pipe).await;
        let tok2 = register(pipe, "cx", &["action.create"], &[]).await;
        *ctl.phase.lock().unwrap() = StubPhase::Normal;
        let r = ipc::send_request(pipe, "request_action", action_req(&aid, &tok2))
            .await
            .unwrap();
        assert_eq!(
            r["result"]["status"], "Completed",
            "T3 run{run}: retry completes: {r}"
        );
        assert_eq!(
            *ctl.executed.lock().unwrap(),
            vec![aid.clone(), aid.clone()],
            "T3 run{run}: 2 executions"
        );
        assert_eq!(
            std::fs::read_to_string(reality.join(format!("{aid}.txt"))).unwrap(),
            "v1"
        );
        assert_eq!(hist(&db, &aid, "ActionRequested"), 2);
        assert_eq!(hist(&db, &aid, "ActionCompleted"), 1);
        assert_eq!(hist(&db, &aid, "ActionFailed"), 0);
        crash_era(era2).await;
        eprintln!(
            "T3 run{run}: HELD (pre-commit death converges; no fake rollback, no fake confirm)"
        );
    }
}
