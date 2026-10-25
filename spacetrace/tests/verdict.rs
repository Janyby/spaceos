//! Verdict classification tests (Stage 1).
//!
//! All inputs are synthetic: they pin the evidentiary boundary of
//! `classify`, and must never be presented as a real ETW/eBPF check.
//! A separate integration test proves a published verdict travels the
//! real SpaceTrace path (buffer -> index -> provider query, the exact
//! function Core calls for `trace.query`).
//!
//! Global hooks are touched by exactly one test (chain-linkage proof),
//! hence the same intra-binary exclusion as vertical_slice.

use serde_json::json;
use spacetrace::verdict::reason;
use spacetrace::{
    GapFlags, OsEvidence, ProcessInstance, SemanticEvidence, VerdictStatus, classify, verdict_event,
};

#[path = "support/global_lock.rs"]
mod global_lock;

fn sem_full() -> SemanticEvidence {
    SemanticEvidence {
        action_id: "act-001".into(),
        correlation_id: "corr-001".into(),
        agent_instance_id: Some("inst-001".into()),
        target: "/tmp/a.txt".into(),
        provider_id: "filesystem".into(),
        action_completed: true,
        file_key: Some("filekey-1".into()),
        performer: Some(ProcessInstance {
            pid: 1234,
            start_time: Some("boot-100".into()),
            source: None, // pre-Stage-2 shape: provenance not recorded
        }),
        window_us: Some((1_000, 2_000)),
        trace_ids: vec!["tid-sem-1".into()],
    }
}

fn os_full() -> OsEvidence {
    OsEvidence {
        source: "etw".into(),
        pid: 1234,
        process_start_time: Some("boot-100".into()),
        file_key: Some("filekey-1".into()),
        path: Some("/tmp/a.txt".into()),
        op: "FileCreate".into(),
        time_us: Some(1_500),
        backend_ref: "etw-rec-1".into(),
        instance_key: None,
    }
}

fn gaps_ok() -> GapFlags {
    GapFlags {
        trace_dropped: false,
        observation_rights: true,
        trace_complete: true,
    }
}

// ---------- 1: sufficient independent evidence -> Confirmed ----------
#[test]
fn full_evidence_confirms() {
    let v = classify(&sem_full(), &[os_full()], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::Confirmed);
    assert_eq!(
        v.reasons,
        vec![
            reason::SAME_ACTION_CHAIN,
            reason::FILE_KEY_MATCH,
            reason::PERFORMER_INSTANCE_BOUND,
            reason::WINDOW_CONTAINED,
        ]
    );
    assert_eq!(v.subject_action_id, "act-001");
    assert_eq!(v.semantic_refs, vec!["tid-sem-1".to_string()]);
    assert_eq!(v.os_refs, vec!["etw-rec-1".to_string()]);
}

// ---------- 2/6: pid without start-time (reuse) -> Probable, never Confirmed ----------
#[test]
fn pid_reuse_stays_probable() {
    let mut os = os_full();
    os.process_start_time = Some("boot-999".into()); // same pid, other boot
    let v = classify(&sem_full(), &[os], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::Probable);
    assert!(
        v.reasons
            .contains(&reason::PID_REUSE_UNRESOLVED.to_string())
    );
    assert!(v.reasons.contains(&reason::FILE_KEY_MATCH.to_string()));

    // Missing start time entirely is the same hole.
    let mut os2 = os_full();
    os2.process_start_time = None;
    let v2 = classify(&sem_full(), &[os2], &gaps_ok());
    assert_eq!(v2.status, VerdictStatus::Probable);
    assert!(
        v2.reasons
            .contains(&reason::PID_REUSE_UNRESOLVED.to_string())
    );
}

// ---------- 3: performer unbound -> Probable ----------
#[test]
fn unbound_performer_stays_probable() {
    let mut sem = sem_full();
    sem.performer = None; // today's reality: provider captures nothing
    let v = classify(&sem, &[os_full()], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::Probable);
    assert!(
        v.reasons
            .contains(&reason::PERFORMER_INSTANCE_UNBOUND.to_string())
    );
}

// ---------- 4: no OS evidence -> Unknown, never disproof ----------
#[test]
fn missing_os_evidence_is_unknown_gap() {
    let v = classify(&sem_full(), &[], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::UnknownGap);
    assert_eq!(v.reasons, vec![reason::MISSING_OS_EVIDENCE.to_string()]);
    assert!(v.os_refs.is_empty());
}

// ---------- 5 (rule 1): same timestamps alone never confirm ----------
#[test]
fn timestamps_alone_never_confirm() {
    // Window holds and even the path coincides, but there is no file
    // identity and no performer binding: at most Probable.
    let mut sem = sem_full();
    sem.file_key = None;
    sem.performer = None;
    let mut os = os_full();
    os.file_key = None;
    let v = classify(&sem, &[os], &gaps_ok());
    assert_ne!(v.status, VerdictStatus::Confirmed);
    assert_eq!(v.status, VerdictStatus::Probable);
    assert!(
        v.reasons
            .contains(&reason::PATH_INFORMATIONAL_ONLY.to_string())
    );
}

// ---------- 7: known loss caps at Unknown ----------
#[test]
fn known_loss_is_unknown_gap() {
    let gaps = GapFlags {
        trace_dropped: true,
        ..gaps_ok()
    };
    let v = classify(&sem_full(), &[os_full()], &gaps);
    assert_eq!(v.status, VerdictStatus::UnknownGap);
    assert_eq!(v.reasons, vec![reason::TRACE_GAP_LOSS.to_string()]);
}

// ---------- 8: incomplete chain / rights / failed action ----------
#[test]
fn incomplete_inputs_are_unknown_gap() {
    let gaps = GapFlags {
        trace_complete: false,
        ..gaps_ok()
    };
    let v = classify(&sem_full(), &[os_full()], &gaps);
    assert_eq!(v.status, VerdictStatus::UnknownGap);
    assert_eq!(v.reasons, vec![reason::INCOMPLETE_CHAIN.to_string()]);

    let gaps = GapFlags {
        observation_rights: false,
        ..gaps_ok()
    };
    let v = classify(&sem_full(), &[os_full()], &gaps);
    assert_eq!(v.status, VerdictStatus::UnknownGap);
    assert_eq!(v.reasons, vec![reason::RIGHTS_UNCONFIRMED.to_string()]);

    // Rule 6: a recorded failure is not an effect to confirm.
    let mut sem = sem_full();
    sem.action_completed = false;
    let v = classify(&sem, &[os_full()], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::UnknownGap);
    assert_eq!(v.reasons, vec![reason::SEMANTIC_EFFECT_ABSENT.to_string()]);
}

// ---------- 9: ambiguous match across candidates ----------
#[test]
fn ambiguous_candidates_stay_probable() {
    let mut os2 = os_full();
    os2.backend_ref = "etw-rec-2".into();
    os2.time_us = Some(1_600);
    let v = classify(&sem_full(), &[os_full(), os2], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::Probable);
    assert_eq!(v.reasons, vec![reason::AMBIGUOUS_CANDIDATES.to_string()]);
    assert_eq!(v.os_refs.len(), 2);
}

// ---------- 10: OS evidence present but nothing matches ----------
#[test]
fn non_matching_os_evidence_is_unknown_gap() {
    let mut os = os_full();
    os.file_key = Some("filekey-other".into());
    os.path = Some("/tmp/other.txt".into());
    let v = classify(&sem_full(), &[os], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::UnknownGap);
    assert_eq!(v.reasons, vec![reason::OS_EVIDENCE_NO_MATCH.to_string()]);
}

// ---------- 11: determinism ----------
#[test]
fn same_input_same_verdict_and_reasons() {
    let a = classify(&sem_full(), &[os_full()], &gaps_ok());
    let b = classify(&sem_full(), &[os_full()], &gaps_ok());
    assert_eq!(a, b);
    let mut os2 = os_full();
    os2.backend_ref = "etw-rec-2".into();
    let c = classify(&sem_full(), &[os_full(), os2.clone()], &gaps_ok());
    let d = classify(&sem_full(), &[os_full(), os2], &gaps_ok());
    assert_eq!(c, d);
}

// ---------- 12: verdict travels the real trace.query path ----------
#[test]
fn verdict_reachable_through_existing_trace_query() {
    let st = spacetrace::SpaceTrace::new().unwrap();
    let v = classify(&sem_full(), &[os_full()], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::Confirmed);
    // Publish exactly like any other trace record: buffer + index.
    let ev = verdict_event(&v, "corr-001", Some("world-001"));
    assert_eq!(format!("{:?}", ev.kind), "CorrelationLink");
    st.buffer().push(ev.clone());
    st.correlation().index(&ev);

    // The exact function Core calls for trace.query.
    let resp = spacetrace::provider_ipc::handle(
        st.provider(),
        "query",
        json!({"correlation_id": "corr-001"}),
    );
    assert_eq!(resp["ok"], true);
    let found: Vec<&serde_json::Value> = resp["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "CorrelationLink")
        .collect();
    assert_eq!(found.len(), 1, "verdict record retrievable: {resp}");
    assert_eq!(found[0]["payload"]["verdict"], "confirmed");
    assert_eq!(found[0]["payload"]["subject_action_id"], "act-001");
    assert!(
        found[0]["payload"]["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r == "file-key-match")
    );

    // World scoping survives on the record (Core-side retain unchanged).
    let by_world = st.provider().query_by_world("world-001");
    assert!(
        by_world
            .iter()
            .any(|e| format!("{:?}", e.kind) == "CorrelationLink")
    );
    let by_action = st.provider().query_by_action("act-001");
    assert!(
        by_action
            .iter()
            .any(|e| format!("{:?}", e.kind) == "CorrelationLink")
    );
}

// ---------- 13: verdict links into a hook-emitted chain ----------
#[test]
fn verdict_links_into_hook_emitted_chain() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = spacetrace::SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    // Semantic side via the real hook path (as production emits it).
    hooks.core_policy_check(
        "corr-002",
        "agent:X",
        "object.create",
        true,
        Some("world-002"),
    );
    hooks.core_mutation_commit("corr-002", "create_object", "agent:X", Some("world-002"), 1);

    // Verdict published into the same chain.
    let mut sem = sem_full();
    sem.action_id = "act-002".into();
    sem.correlation_id = "corr-002".into();
    sem.trace_ids = vec!["tid-sem-2".into()];
    let v = classify(&sem, &[], &gaps_ok()); // no OS side here: Unknown
    assert_eq!(v.status, VerdictStatus::UnknownGap);
    let ev = verdict_event(&v, "corr-002", Some("world-002"));
    st.buffer().push(ev.clone());
    st.correlation().index(&ev);

    let chain = st.provider().query_by_correlation("corr-002");
    assert!(
        chain.len() >= 3,
        "hooks + verdict share one chain: {}",
        chain.len()
    );
    assert!(
        chain
            .iter()
            .any(|e| format!("{:?}", e.kind) == "CorrelationLink")
    );
}
