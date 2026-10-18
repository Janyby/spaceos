//! SpaceTrace breakage campaign (spec §29-30).
//!
//! Pre-registered hypotheses, thresholds fixed before runs:
//! - ST-B1 trace storm: burst causes trace loss; threshold: loss exactly accounted (kept+dropped == sent).
//! - ST-B3 buffer bottleneck: overflow drops counted, never silent; threshold: 0 unaccounted.
//! - ST-B6 Core restart: orphan/incomplete traces honest; threshold: TRACE PARTIAL, never fabricated.
//! - ST-B13 transaction visibility: no mutation/commit trace before durable commit; threshold: 0 pre-commit commits.
//! - ST-B14 world locality: world-scoped query returns only that world; threshold: 0 cross-world leak.
//! - ST-B15 failure propagation: trace failure never fails Core; threshold: 0 Core errors induced.

use serde_json::json;
use spacetrace::{SpaceTrace, TraceEvent, TraceKind, TraceLayer};

/// Intra-binary exclusion (see support/global_lock.rs): each test
/// installs its own SpaceTrace into the process-global hooks
/// (last-wins overwrite, decision #22) while asserting on its OWN
/// provider — parallel execution misdelivers traces across tests.
#[path = "support/global_lock.rs"]
mod global_lock;

// ST-B1 + ST-B3: storm loss exactly accounted.
#[test]
fn st_b1_storm_loss_exactly_accounted() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());

    let sent = 150_000usize; // exceeds default 100k capacity
    for i in 0..sent {
        let e = TraceEvent::new(
            TraceLayer::Provider,
            TraceKind::WatcherEvent,
            format!("storm-{i}"),
            json!({"i": i}),
        );
        st.buffer().push(e.clone());
        st.correlation().index(&e);
    }
    let stats = st.provider().buffer_stats();
    assert_eq!(
        stats.current_len + stats.dropped_count as usize,
        sent,
        "ST-B1 HELD iff kept+dropped == sent"
    );
    assert!(
        stats.dropped_count > 0,
        "storm must overflow default buffer (else test proves nothing)"
    );
    eprintln!(
        "ST-B1: sent={sent} kept={} dropped={} (HELD: exact accounting)",
        stats.current_len, stats.dropped_count
    );
}

// ST-B6: restart => honest partial, never fabricated.
#[test]
fn st_b6_restart_honest_partial() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    // Partial chain before "restart": start + mutation, no commit.
    hooks.core_mutation_start("restart-corr-1", "create_object", "a", Some("w"), json!({}));

    // Simulate SpaceTrace restart: fresh instance (buffer lost).
    let st2 = SpaceTrace::new().unwrap();
    let partial = st2.provider().query_by_correlation("restart-corr-1");
    assert!(
        partial.is_empty(),
        "ST-B6: after restart, old correlation must be EMPTY (honest), not fabricated"
    );

    // Old instance still has the partial chain — marked incomplete by absence of commit.
    let old_chain = st.provider().query_by_correlation("restart-corr-1");
    let has_commit = old_chain
        .iter()
        .any(|e| format!("{:?}", e.kind) == "MutationCommit");
    assert!(
        !has_commit,
        "partial chain must lack commit => TRACE PARTIAL"
    );
    eprintln!("ST-B6: HELD (honest partial, no fabrication)");
}

// ST-B13: commit trace only after durable commit.
#[test]
fn st_b13_no_precommit_commit_trace() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());

    // Simulate Core wrapper logic: rollback path must emit rollback, never commit.
    let corr = "precommit-001";
    let hooks = spacetrace::hooks::global_hooks();
    hooks.core_mutation_start(corr, "create_object", "a", Some("w"), json!({}));
    hooks.core_mutation_rollback(
        corr,
        "create_object",
        "a",
        Some("w"),
        "simulated storage error",
    );

    let chain = st.provider().query_by_correlation(corr);
    let kinds: Vec<String> = chain.iter().map(|e| format!("{:?}", e.kind)).collect();
    assert!(kinds.contains(&"MutationStart".to_string()));
    assert!(kinds.contains(&"MutationRollback".to_string()));
    assert!(
        !kinds.contains(&"MutationCommit".to_string()),
        "ST-B13: commit trace before/after failure = architectural violation"
    );
    eprintln!("ST-B13: HELD (no pre-commit commit trace)");
}

// ST-B14: world-scoped query leaks nothing.
#[test]
fn st_b14_world_query_no_cross_leak() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    for i in 0..500 {
        hooks.core_mutation_start(
            &format!("wA-{i}"),
            "create_object",
            "a",
            Some("world-A"),
            json!({}),
        );
        hooks.core_mutation_start(
            &format!("wB-{i}"),
            "create_object",
            "a",
            Some("world-B"),
            json!({}),
        );
    }
    let t = std::time::Instant::now();
    let a_events = st.provider().query_by_world("world-A");
    let elapsed = t.elapsed();
    assert_eq!(a_events.len(), 500, "exactly world-A events");
    assert!(
        a_events
            .iter()
            .all(|e| e.world_id.as_deref() == Some("world-A")),
        "ST-B14: 0 cross-world leak"
    );
    eprintln!(
        "ST-B14: HELD (500/500 world-A, 0 leak, query={}ms; note: linear scan O(Space), same as Core)",
        elapsed.as_millis()
    );
}

// ST-B15: trace failure never propagates to Core.
#[test]
fn st_b15_trace_failure_isolated() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    // Fill buffer to force drops, then verify hook calls still succeed
    // without panic (isolation = no panic/error propagation to caller).
    // Full Core-path isolation is covered by space-core regression suites.
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    for i in 0..120_000 {
        hooks.core_mutation_start(
            &format!("flood-{i}"),
            "create_object",
            "a",
            Some("w"),
            json!({}),
        );
    }
    let stats = st.provider().buffer_stats();
    assert!(stats.dropped_count > 0, "flood must overflow");

    // Post-overflow hook calls must not panic and must keep exact accounting.
    let before_kept = st.provider().buffer_stats().current_len;
    let before_dropped = st.provider().buffer_stats().dropped_count;
    for i in 0..1000 {
        hooks.core_mutation_start(
            &format!("post-{i}"),
            "create_object",
            "a",
            Some("w"),
            json!({}),
        );
        hooks.core_mutation_commit(&format!("post-{i}"), "create_object", "a", Some("w"), 1);
    }
    let after = st.provider().buffer_stats();
    assert_eq!(
        (after.current_len as u64 + after.dropped_count) - (before_kept as u64 + before_dropped),
        2000,
        "ST-B15: every post-overflow emission exactly accounted"
    );
    eprintln!("ST-B15: HELD (drops counted, callers never fail)");
}
