//! X7 — Trace saturation with Core success (the one uncovered X7 scenario).
//!
//! All other X7 scenarios map to existing evidence (see FOUNDATION_GATE.md
//! X7 mapping table): normal operation, query denial, incomplete output,
//! high volume, failed-op partial trace, restart query, pub/sub semantics.
//! This file proves: Core mutations succeed with exact semantics while
//! the trace buffer is saturated and dropping, drops stay counted, and
//! trace queries keep serving the retained prefix.
//! Fixed threshold: 100/100 mutations ok; drops > 0 and exactly
//! accounted; query returns only retained spans, all well-formed.
//! Expected behavior: HELD.
//! Pass criterion: all exact. Failure criterion: Core error OR
//! unaccounted drop OR malformed retained span.
//! Reproduction protocol: 3 identical runs.

use serde_json::json;
use spacetrace::{SpaceTrace, TraceEvent, TraceKind, TraceLayer};

#[test]
fn x7_saturated_trace_core_unaffected() {
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());

    // Saturate the 100k ring with high-volume spans.
    for i in 0..130_000 {
        let e = TraceEvent::new(
            TraceLayer::Provider,
            TraceKind::WatcherEvent,
            format!("sat-{i}"),
            json!({"i": i}),
        );
        st.buffer().push(e.clone());
        st.correlation().index(&e);
    }
    let before = st.provider().buffer_stats();
    assert!(before.dropped_count > 0, "X7: saturation must drop");
    eprintln!(
        "X7: saturated kept={} dropped={}",
        before.current_len, before.dropped_count
    );

    // Core mutations proceed with exact semantics under saturation.
    let dir = std::env::temp_dir().join("spaceos-x7");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("x7.db");
    let _ = std::fs::remove_file(&db);
    let s = space_core::Storage::open(&db).unwrap();
    for i in 0..100 {
        let mut o = space_core::Object::new("File", "x7", json!({"i": i}));
        o.id = format!("x7-obj-{i:03}");
        s.create_object(&o).unwrap();
        let mut e = space_core::Event::new("Tick", "x7", json!({"i": i}));
        e.id = format!("x7-ev-{i:03}");
        s.record_event(&e).unwrap();
    }
    let objs = s
        .query_objects(&space_core::query::ObjectQuery::default())
        .unwrap();
    let evs = s
        .query_events(&space_core::query::EventQuery::default())
        .unwrap();
    assert_eq!(objs.len(), 100, "X7: 100/100 objects under saturation");
    assert_eq!(evs.len(), 100, "X7: 100/100 events under saturation");
    assert_eq!(s.integrity_check().unwrap(), "ok");

    // Drops still exactly accounted; retained spans well-formed.
    let after = st.provider().buffer_stats();
    assert_eq!(
        after.current_len as u64 + after.dropped_count,
        before.current_len as u64 + before.dropped_count,
        "X7: no unaccounted spans (nothing new indexed in this path)"
    );
    let sample = st.provider().query_by_correlation("sat-0");
    assert!(!sample.is_empty(), "X7: retained prefix still queryable");
    for e in sample.iter().take(10) {
        assert!(!e.trace_id.is_empty() && !e.correlation_id.is_empty());
    }
    eprintln!("X7: Core 200/200 exact under saturation, drops counted, queries serve");
}
