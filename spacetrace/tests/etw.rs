//! ETW collector tests (Stage 3).
//!
//! Three levels, strictly separated:
//! - unit: pure decode/normalize/time/state-mapping logic (no ETW session);
//! - integration (Windows): real collector lifecycle with an honest outcome —
//!   either real kernel events (elevated) or an explicit unavailable reason
//!   (this environment). Synthetic inputs are never presented as ETW proof.
#![cfg(windows)]
use spacetrace::etw::{
    Collector, CollectorState, EtwProcessRecord, FILETIME_UNIX_EPOCH_DIFF,
    KERNEL_PROCESS_PROVIDER_GUID, KEYWORD_PROCESS, OPCODE_END, OPCODE_START, ProcessLifecycle,
    kernel_ts_to_unix_us, map_setup_error, normalize_process_event,
};

// ---------- unit: constants ----------
#[test]
fn kernel_provider_identity_is_pinned() {
    assert_eq!(KERNEL_PROCESS_PROVIDER_GUID.data1, 0x22FB2CD6);
    assert_eq!(KERNEL_PROCESS_PROVIDER_GUID.data2, 0x0E7B);
    assert_eq!(KERNEL_PROCESS_PROVIDER_GUID.data3, 0x422B);
    assert_eq!(
        KERNEL_PROCESS_PROVIDER_GUID.data4,
        [0xA0, 0xC7, 0x2F, 0xAD, 0x1F, 0xD0, 0xE7, 0x16]
    );
    assert_eq!(KEYWORD_PROCESS, 0x10);
    assert_eq!((OPCODE_START, OPCODE_END), (1, 1 + 1));
}

// ---------- unit: timestamp conversion ----------
#[test]
fn kernel_timestamp_conversion() {
    // Epoch maps to zero.
    assert_eq!(kernel_ts_to_unix_us(FILETIME_UNIX_EPOCH_DIFF), Some(0));
    // One second later.
    assert_eq!(
        kernel_ts_to_unix_us(FILETIME_UNIX_EPOCH_DIFF + 10_000_000),
        Some(1_000_000)
    );
    // Pre-1601-epoch input is a legitimate negative unix time (year 1601),
    // not an error; only true overflow yields None.
    assert_eq!(kernel_ts_to_unix_us(0), Some(-11_644_473_600_000_000));
    assert_eq!(kernel_ts_to_unix_us(i64::MIN), None);
}

// ---------- unit: setup-error mapping ----------
#[test]
fn setup_errors_map_honestly() {
    match map_setup_error("start-trace", 5) {
        CollectorState::Unavailable { reason } => {
            assert!(
                reason.contains("elevated"),
                "rights must be named: {reason}"
            )
        }
        other => panic!("access-denied must be Unavailable, got {other:?}"),
    }
    match map_setup_error("start-trace", 183) {
        CollectorState::Unavailable { reason } => {
            assert!(reason.contains("not stealing"), "{reason}")
        }
        other => panic!("name collision must be Unavailable, got {other:?}"),
    }
    match map_setup_error("enable-provider", 4201) {
        CollectorState::Unavailable { .. } => {}
        other => panic!("unknown guid must be Unavailable, got {other:?}"),
    }
    match map_setup_error("open-trace", 12345) {
        CollectorState::Failed { stage, code } => {
            assert_eq!((stage, code), ("open-trace", 12345))
        }
        other => panic!("unexpected code must be Failed, got {other:?}"),
    }
}

// ---------- unit: normalize + OsEvidence mapping ----------
#[test]
fn normalize_and_evidence_mapping() {
    // CreateTime chosen so the wall value is deterministic:
    // unix 1700000000000000us <-> FILETIME 133444736000000000.
    let creation_ft: u64 = 133444736000000000;
    let rec = normalize_process_event(
        100,
        Some(4),
        0xabcdu64,
        None,
        ProcessLifecycle::Start,
        1_700_000_000_000_000,
        Some(creation_ft),
        1,
        1,
    );
    assert_eq!(rec.pid, 100);
    assert_eq!(rec.ppid, Some(4));
    assert_eq!(rec.exit_status, None, "schema carries no exit code");
    let ev = rec.to_os_evidence();
    assert_eq!(ev.source, "etw");
    assert_eq!(ev.pid, 100);
    assert_eq!(ev.op, "ProcessStart");
    // Instance key travels; creation wall time is real (not invented).
    assert_eq!(ev.instance_key, Some(format!("{:016x}", 0xabcdu64)));
    assert_eq!(ev.process_start_time, Some("1700000000000000".into()));
    assert_eq!(ev.file_key, None);
    assert_eq!(ev.path, None);
    assert!(ev.time_us.is_some());
    assert!(ev.backend_ref.contains("ProcessStart"));

    // Missing CreateTime stays missing (never defaulted).
    let rec2 = normalize_process_event(
        100,
        Some(4),
        0xabcdu64,
        None,
        ProcessLifecycle::End,
        1_700_000_001_000_000,
        None,
        2,
        2,
    );
    let ev2 = rec2.to_os_evidence();
    assert_eq!(ev2.op, "ProcessEnd");
    assert_eq!(ev2.process_start_time, None);
}

// ---------- unit: loss counter distinguishes unknown from zero ----------
#[test]
fn loss_counter_unknown_vs_zero() {
    let q = spacetrace::etw::QualitySnapshot::default();
    assert_eq!(
        q.etw_lost_events, None,
        "no buffers yet: unavailable, not zero"
    );
    assert_eq!(q.received, 0);
}

// ---------- integration: honest lifecycle outcome ----------
#[test]
fn collector_lifecycle_honest_outcome() {
    let mut col = Collector::start();
    eprintln!("[etw-test] state after start: {:?}", col.state());
    match col.state() {
        CollectorState::Running | CollectorState::Starting => {
            // Real session: prove a genuine kernel event for our own child.
            let mut child = std::process::Command::new("ping")
                .args(["-n", "3", "127.0.0.1"])
                .stdout(std::process::Stdio::null())
                .spawn()
                .expect("spawn ping");
            let wanted = child.id();
            let t0 = std::time::Instant::now();
            let mut seen = None;
            while t0.elapsed() < std::time::Duration::from_secs(20) {
                while let Some(rec) = col.try_recv() {
                    if rec.pid == wanted && rec.kind == ProcessLifecycle::Start {
                        seen = Some(rec);
                        break;
                    }
                }
                if seen.is_some() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            let _ = child.wait(); // natural exit first; kill only on failure below
            let rec: EtwProcessRecord = seen.expect("real Start event for our child while Running");
            assert_eq!(rec.opcode, 1);
            assert!(rec.unique_process_key != 0);
            assert!(rec.kernel_time_100ns > FILETIME_UNIX_EPOCH_DIFF);
            // OsEvidence mapping holds on real data.
            let ev = rec.to_os_evidence();
            assert_eq!(ev.pid, wanted);
            assert!(ev.instance_key.is_some());

            // End record for the same instance after natural exit.
            let t1 = std::time::Instant::now();
            let mut ended = None;
            while t1.elapsed() < std::time::Duration::from_secs(20) {
                while let Some(r) = col.try_recv() {
                    if r.pid == wanted
                        && r.kind == ProcessLifecycle::End
                        && r.unique_process_key == rec.unique_process_key
                    {
                        ended = Some(r);
                        break;
                    }
                }
                if ended.is_some() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            let end = ended.expect("real End event for the same instance");
            assert_eq!(end.opcode, 2);
            assert!(end.kernel_time_100ns >= rec.kernel_time_100ns);

            col.stop();
            assert!(matches!(col.state(), CollectorState::Stopped { .. }));
            let q = col.quality();
            assert!(q.received >= 1, "quality: {q:?}");
            assert!(q.buffers_seen >= 1, "quality: {q:?}");
            // Loss counter was actually observed (Some), whatever its value.
            assert!(q.etw_lost_events.is_some(), "quality: {q:?}");
        }
        CollectorState::Unavailable { reason } => {
            // Honest environmental outcome (e.g. unelevated shell):
            // no events may be claimed, stop must be a safe no-op.
            eprintln!("ETW unavailable here (expected without elevation): {reason}");
            assert!(
                reason.contains("elevated")
                    || reason.contains("not stealing")
                    || reason.contains("unknown on this build")
                    || reason.contains("another collector"),
                "reason must name the cause: {reason}"
            );
            assert!(col.try_recv().is_none());
            let q = col.quality();
            assert_eq!(q.received, 0);
            assert_eq!(
                q.etw_lost_events, None,
                "never observed: unavailable, not zero"
            );
            col.stop(); // safe no-op
        }
        other => panic!("unexpected collector state: {other:?}"),
    }
}

// ---------- integration: restartability + foreign sessions untouched ----------
#[test]
fn collector_restart_and_session_hygiene() {
    let sessions_before = live_etw_sessions();
    {
        let mut first = Collector::start();
        let s1 = first.state();
        first.stop();
        assert!(
            matches!(first.state(), CollectorState::Stopped { .. })
                || matches!(
                    s1,
                    CollectorState::Unavailable { .. } | CollectorState::Failed { .. }
                )
        );
        // Restart in the same process must behave identically (no leaked session).
        let mut second = Collector::start();
        let s2 = second.state();
        second.stop();
        // Both runs report the same class of outcome in one environment.
        assert_eq!(
            std::mem::discriminant(&s1),
            std::mem::discriminant(&s2),
            "restart must not change the outcome class: {s1:?} vs {s2:?}"
        );
    }
    // Our pid-suffixed session must be gone; anything else is not ours.
    if let Some(after) = live_etw_sessions() {
        let leaked: Vec<_> = after.iter().filter(|s| s.contains("spaceos-etw")).collect();
        assert!(leaked.is_empty(), "our session must be gone: {leaked:?}");
        if let Some(before) = sessions_before {
            for s in &before {
                assert!(after.contains(s), "foreign session disturbed: {s}");
            }
        }
    } else {
        eprintln!("logman query unavailable here; session hygiene asserted via stop() only");
    }
}

/// Best-effort list of live ETW sessions via logman (read-only).
/// `None` when the query tool itself is unavailable/blocked.
fn live_etw_sessions() -> Option<Vec<String>> {
    let out = std::process::Command::new("logman")
        .args(["query", "-ets"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Some(
        text.lines()
            .filter_map(|l| {
                let t = l.trim();
                // Session list lines look like: ` spaceos-etw-1234      Running`
                let name = t.split_whitespace().next()?;
                if name.eq_ignore_ascii_case("data") || name.eq_ignore_ascii_case("collector") {
                    return None;
                }
                Some(name.to_string())
            })
            .collect(),
    )
}
