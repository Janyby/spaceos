//! Performer process identity (Stage 2).
//!
//! The performer is the provider process itself (`actions::perform` runs
//! only in-process `std::fs`, never child processes). These are real
//! Windows integration tests for the system capture (FILETIME units documented
//! in `performer.rs`) plus unit checks that the captured identity plugs the
//! Stage-1 evidentiary boundary without ever confirming by pid/time alone.
//! Mocks are never presented as system-API verification.
#![cfg(windows)]
use filesystem_provider::actions::perform;
use filesystem_provider::performer::{
    current_performer, SOURCE_UNAVAILABLE, SOURCE_WIN32_PROCESS_TIMES,
};
use serde_json::json;
use spacetrace::verdict::reason;
use spacetrace::{
    classify, GapFlags, OsEvidence, ProcessInstance, SemanticEvidence, VerdictStatus,
};

fn tmp(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-performer").join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn p(dir: &std::path::Path, n: &str) -> String {
    dir.join(n).to_string_lossy().to_string()
}

/// FILETIME (100ns since 1601-01-01 UTC) -> unix microseconds.
fn filetime_to_unix_us(ft: u64) -> i64 {
    ((ft - 116444736000000000) / 10) as i64
}

fn now_unix_us() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

// ---------- 1: pid is this process ----------
#[test]
fn pid_matches_current_process() {
    let me = current_performer();
    assert_eq!(me.pid, std::process::id());
}

// ---------- 2: start time from the system source, documented format ----------
#[test]
fn start_time_from_system_source() {
    let me = current_performer();
    assert_eq!(me.source, SOURCE_WIN32_PROCESS_TIMES);
    assert_ne!(me.source, SOURCE_UNAVAILABLE);
    let raw = me.start_time.expect("start time must be captured");
    let ft: u64 = raw.parse().expect("decimal FILETIME");
    assert!(ft > 0);
    let started = filetime_to_unix_us(ft);
    assert!(
        started <= now_unix_us(),
        "creation must not be in the future"
    );
    // Sanity: this process started recently (test binary lifetime scale).
    assert!(now_unix_us() - started < 24 * 3600 * 1_000_000);
}

// ---------- 3: stable within one process lifetime ----------
#[test]
fn identity_stable_within_process() {
    assert_eq!(current_performer(), current_performer());
}

// ---------- 4+8: linked to the action; pid/time alone never confirm ----------
fn sem_with_captured_performer(action_id: &str) -> SemanticEvidence {
    let me = current_performer();
    SemanticEvidence {
        action_id: action_id.into(),
        correlation_id: "corr-perf-1".into(),
        agent_instance_id: None,
        target: "/tmp/perf.txt".into(),
        provider_id: "filesystem".into(),
        action_completed: true,
        file_key: None, // no backend yet: file identity unavailable
        performer: Some(ProcessInstance {
            pid: me.pid,
            start_time: me.start_time.clone(),
            source: Some(me.source.into()),
        }),
        window_us: {
            let now = now_unix_us();
            Some((now - 60_000_000, now + 60_000_000))
        },
        trace_ids: vec!["tid-perf-1".into()],
    }
}

fn os_matching_capture() -> OsEvidence {
    let me = current_performer();
    OsEvidence {
        source: "etw".into(),
        pid: me.pid,
        process_start_time: me.start_time.clone(),
        file_key: None, // no backend yet
        path: Some("/tmp/perf.txt".into()),
        op: "FileCreate".into(),
        time_us: Some(now_unix_us()),
        backend_ref: "etw-perf-1".into(),
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

#[tokio::test]
async fn performer_attached_to_action_detail() {
    let d = tmp("link");
    let t = json!({"path": p(&d, "a.txt"), "content": "hi"});
    let detail = perform("create", &t).await.expect("create works");
    // Original shape intact (no semantic change).
    assert_eq!(detail["created"], json!(p(&d, "a.txt")));
    // Identity attached and bound to this process.
    let me = current_performer();
    assert_eq!(detail["performer"]["pid"], json!(me.pid));
    assert_eq!(
        detail["performer"]["start_time"],
        json!(me.start_time.expect("captured"))
    );
    assert_eq!(
        detail["performer"]["source"],
        json!(SOURCE_WIN32_PROCESS_TIMES)
    );
    // Effect itself unchanged.
    assert_eq!(std::fs::read_to_string(d.join("a.txt")).unwrap(), "hi");
}

#[test]
fn captured_identity_does_not_confirm_by_itself() {
    // Real captured performer + matching pid/start/window/path, but no file
    // identity anywhere: at most Probable (rule: pid/time alone never confirm).
    let v = classify(
        &sem_with_captured_performer("act-p1"),
        &[os_matching_capture()],
        &gaps_ok(),
    );
    assert_ne!(v.status, VerdictStatus::Confirmed);
    assert_eq!(v.status, VerdictStatus::Probable);
    assert!(v
        .reasons
        .contains(&reason::PATH_INFORMATIONAL_ONLY.to_string()));
}

// ---------- 5: unavailable is explicit, never invented ----------
#[test]
fn unavailable_identity_is_explicit_gap() {
    let mut sem = sem_with_captured_performer("act-p2");
    sem.file_key = Some("filekey-p2".into());
    sem.performer = Some(ProcessInstance {
        pid: 99999,
        start_time: None,
        source: Some(SOURCE_UNAVAILABLE.into()),
    });
    let mut os = os_matching_capture();
    os.file_key = Some("filekey-p2".into());
    let v = classify(&sem, &[os], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::Probable);
    assert!(v
        .reasons
        .contains(&reason::PERFORMER_INSTANCE_UNBOUND.to_string()));
}

// ---------- 6: same pid, other start time = other instance ----------
#[test]
fn pid_reuse_is_distinct_instance() {
    let me = current_performer();
    let a = ProcessInstance {
        pid: me.pid,
        start_time: me.start_time.clone(),
        source: Some(me.source.into()),
    };
    let b = ProcessInstance {
        pid: me.pid,
        start_time: Some("other-boot".into()),
        source: Some(SOURCE_WIN32_PROCESS_TIMES.into()),
    };
    assert_ne!(a, b, "pid reuse must not compare equal");

    let mut sem = sem_with_captured_performer("act-p3");
    sem.file_key = Some("filekey-p3".into());
    sem.performer = Some(a);
    let mut os = os_matching_capture();
    os.file_key = Some("filekey-p3".into());
    os.process_start_time = Some("other-boot".into()); // same pid, other life
    let v = classify(&sem, &[os], &gaps_ok());
    assert_eq!(v.status, VerdictStatus::Probable);
    assert!(v
        .reasons
        .contains(&reason::PID_REUSE_UNRESOLVED.to_string()));
}

// ---------- 7: capture changes neither result nor side effects ----------
#[tokio::test]
async fn capture_preserves_action_semantics() {
    let d = tmp("semantics");
    // write + delete roundtrip with identity attached throughout.
    let w = json!({"path": p(&d, "b.txt"), "content": "v"});
    let w1 = perform("write", &w).await.expect("write ok");
    assert_eq!(w1["wrote"], json!(p(&d, "b.txt")));
    assert!(w1.get("performer").is_some());
    let del = perform("delete", &json!({"path": p(&d, "b.txt")}))
        .await
        .expect("delete ok");
    assert_eq!(del["deleted"], json!(p(&d, "b.txt")));
    assert!(!d.join("b.txt").exists());
    // Unknown action still fails loudly with no detail at all.
    assert!(perform("frobnicate", &json!({})).await.is_err());
}
